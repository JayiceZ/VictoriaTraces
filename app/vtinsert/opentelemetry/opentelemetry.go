package opentelemetry

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/VictoriaMetrics/metrics"
	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

var maxRequestSize = flagutil.NewBytes("opentelemetry.traces.maxRequestSize", 64*1024*1024, "The maximum size in bytes of a single OpenTelemetry trace export request.")

var (
	requestsProtobufTotal = metrics.NewCounter(`vt_http_requests_total{path="/insert/opentelemetry/v1/traces",format="protobuf"}`)
	errorsProtobufTotal   = metrics.NewCounter(`vt_http_errors_total{path="/insert/opentelemetry/v1/traces",format="protobuf"}`)
	requestsJSONTotal     = metrics.NewCounter(`vt_http_requests_total{path="/insert/opentelemetry/v1/traces",format="JSON"}`)
	errorsJSONTotal       = metrics.NewCounter(`vt_http_errors_total{path="/insert/opentelemetry/v1/traces",format="JSON"}`)

	requestProtobufDuration = metrics.NewHistogram(`vt_http_request_duration_seconds{path="/insert/opentelemetry/v1/traces",format="protobuf"}`)
	requestJSONDuration     = metrics.NewHistogram(`vt_http_request_duration_seconds{path="/insert/opentelemetry/v1/traces",format="json"}`)
)

var (
	mandatoryStreamFields = []string{otelpb.ResourceAttrServiceName, otelpb.NameField}
	msgFieldValue         = "-"
)

var (
	// traceIDCache for deduplicating trace_id
	traceIDCache = fastcache.New(32 * 1024 * 1024)
)

const (
	contentTypeProtobuf = "application/x-protobuf"
	contentTypeJSON     = "application/json"
)

// RequestHandler processes Opentelemetry insert requests
func RequestHandler(path string, w http.ResponseWriter, r *http.Request) bool {
	switch path {
	// use the same path as opentelemetry collector
	// https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
	case "/insert/opentelemetry/v1/traces":
		handleInsertTracesRequest(r, w)
		return true
	default:
		return false
	}
}

func handleInsertTracesRequest(r *http.Request, w http.ResponseWriter) {
	startTime := time.Now()
	var err error

	contentType := r.Header.Get("Content-Type")
	// update request duration only for successfully parsed requests
	// There is no need in updating request duration for request errors,
	// since their timings are usually much smaller than the timing for successful request parsing.
	switch contentType {
	case contentTypeProtobuf:
		requestsProtobufTotal.Inc()
		defer func() {
			if err == nil {
				requestProtobufDuration.UpdateDuration(startTime)
			}
		}()
	case contentTypeJSON:
		requestsJSONTotal.Inc()
		defer func() {
			if err == nil {
				requestJSONDuration.UpdateDuration(startTime)
			}
		}()
	default:
		httpserver.Errorf(w, r, "Content-Type %s isn't supported for opentelemetry format. Use protobuf or JSON encoding", contentType)
		return
	}

	cp, err := insertutil.GetCommonParams(r)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse common params from request: %s", err)
		return
	}
	// stream fields must contain the service name and span name.
	// by using arguments and headers, users can also add other fields as stream fields
	// for potentially better efficiency.
	cp.StreamFields = append(mandatoryStreamFields, cp.StreamFields...)

	if err := insertutil.CanWriteData(); err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	encoding := r.Header.Get("Content-Encoding")
	err = protoparserutil.ReadUncompressedData(r.Body, encoding, maxRequestSize, func(data []byte) error {
		lmp := cp.NewLogMessageProcessor("opentelemetry_traces", false)
		err := pushExportTraceServiceRequest(data, lmp, contentType)
		lmp.MustClose()
		return err
	})
	if err != nil {
		httpserver.Errorf(w, r, "cannot read OpenTelemetry protocol data: %s", err)
		return
	}

}

func pushExportTraceServiceRequest(data []byte, lmp insertutil.LogMessageProcessor, contentType string) error {
	var (
		req otelpb.ExportTraceServiceRequest
		err error
	)
	switch contentType {
	case contentTypeJSON:
		if err = req.UnmarshalJSONCustom(data); err != nil {
			errorsJSONTotal.Inc()
			return fmt.Errorf("cannot unmarshal request from %d JSON bytes: %w", len(data), err)
		}
	case contentTypeProtobuf:
		if err = req.UnmarshalProtobuf(data); err != nil {
			errorsProtobufTotal.Inc()
			return fmt.Errorf("cannot unmarshal request from %d protobuf bytes: %w", len(data), err)
		}
	}

	var commonFields []logstorage.Field
	for _, rs := range req.ResourceSpans {
		commonFields = commonFields[:0]
		attributes := rs.Resource.Attributes
		commonFields = appendKeyValuesWithPrefix(commonFields, attributes, "", otelpb.ResourceAttrPrefix)
		commonFieldsLen := len(commonFields)
		for _, ss := range rs.ScopeSpans {
			commonFields = pushFieldsFromScopeSpans(ss, commonFields[:commonFieldsLen], lmp)
		}
	}
	return nil
}

func pushFieldsFromScopeSpans(ss *otelpb.ScopeSpans, commonFields []logstorage.Field, lmp insertutil.LogMessageProcessor) []logstorage.Field {
	commonFields = append(commonFields, logstorage.Field{
		Name:  otelpb.InstrumentationScopeName,
		Value: ss.Scope.Name,
	}, logstorage.Field{
		Name:  otelpb.InstrumentationScopeVersion,
		Value: ss.Scope.Version,
	})
	commonFields = appendKeyValuesWithPrefix(commonFields, ss.Scope.Attributes, "", otelpb.InstrumentationScopeAttrPrefix)
	commonFieldsLen := len(commonFields)
	for _, span := range ss.Spans {
		commonFields = pushFieldsFromSpan(span, commonFields[:commonFieldsLen], lmp)
	}
	return commonFields
}

func pushFieldsFromSpan(span *otelpb.Span, scopeCommonFields []logstorage.Field, lmp insertutil.LogMessageProcessor) []logstorage.Field {
	fields := scopeCommonFields
	fields = append(fields,
		logstorage.Field{Name: otelpb.TraceIDField, Value: span.TraceID},
		logstorage.Field{Name: otelpb.SpanIDField, Value: span.SpanID},
		logstorage.Field{Name: otelpb.TraceStateField, Value: span.TraceState},
		logstorage.Field{Name: otelpb.ParentSpanIDField, Value: span.ParentSpanID},
		logstorage.Field{Name: otelpb.FlagsField, Value: strconv.FormatUint(uint64(span.Flags), 10)},
		logstorage.Field{Name: otelpb.NameField, Value: span.Name},
		logstorage.Field{Name: otelpb.KindField, Value: strconv.FormatInt(int64(span.Kind), 10)},
		logstorage.Field{Name: otelpb.StartTimeUnixNanoField, Value: strconv.FormatUint(span.StartTimeUnixNano, 10)},
		logstorage.Field{Name: otelpb.EndTimeUnixNanoField, Value: strconv.FormatUint(span.EndTimeUnixNano, 10)},
		logstorage.Field{Name: otelpb.DurationField, Value: strconv.FormatUint(span.EndTimeUnixNano-span.StartTimeUnixNano, 10)},

		logstorage.Field{Name: otelpb.DroppedAttributesCountField, Value: strconv.FormatUint(uint64(span.DroppedAttributesCount), 10)},
		logstorage.Field{Name: otelpb.DroppedEventsCountField, Value: strconv.FormatUint(uint64(span.DroppedEventsCount), 10)},
		logstorage.Field{Name: otelpb.DroppedLinksCountField, Value: strconv.FormatUint(uint64(span.DroppedLinksCount), 10)},

		logstorage.Field{Name: otelpb.StatusMessageField, Value: span.Status.Message},
		logstorage.Field{Name: otelpb.StatusCodeField, Value: strconv.FormatInt(int64(span.Status.Code), 10)},
	)

	// append span attributes
	fields = appendKeyValuesWithPrefix(fields, span.Attributes, "", otelpb.SpanAttrPrefixField)

	for idx, event := range span.Events {
		eventFieldPrefix := otelpb.EventPrefix
		eventFieldSuffix := ":" + strconv.Itoa(idx)
		fields = append(fields,
			logstorage.Field{Name: eventFieldPrefix + otelpb.EventTimeUnixNanoField + eventFieldSuffix, Value: strconv.FormatUint(event.TimeUnixNano, 10)},
			logstorage.Field{Name: eventFieldPrefix + otelpb.EventNameField + eventFieldSuffix, Value: event.Name},
			logstorage.Field{Name: eventFieldPrefix + otelpb.EventDroppedAttributesCountField + eventFieldSuffix, Value: strconv.FormatUint(uint64(event.DroppedAttributesCount), 10)},
		)
		// append event attributes
		fields = appendKeyValuesWithPrefixSuffix(fields, event.Attributes, "", eventFieldPrefix+otelpb.EventAttrPrefix, eventFieldSuffix)
	}

	for idx, link := range span.Links {
		linkFieldPrefix := otelpb.LinkPrefix
		linkFieldSuffix := ":" + strconv.Itoa(idx)
		fields = append(fields,
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkTraceIDField + linkFieldSuffix, Value: link.TraceID},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkSpanIDField + linkFieldSuffix, Value: link.SpanID},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkTraceStateField + linkFieldSuffix, Value: link.TraceState},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkDroppedAttributesCountField + linkFieldSuffix, Value: strconv.FormatUint(uint64(link.DroppedAttributesCount), 10)},
			logstorage.Field{Name: linkFieldPrefix + otelpb.LinkFlagsField + linkFieldSuffix, Value: strconv.FormatUint(uint64(link.Flags), 10)},
		)

		// append link attributes
		fields = appendKeyValuesWithPrefixSuffix(fields, link.Attributes, "", linkFieldPrefix+otelpb.LinkAttrPrefix, linkFieldSuffix)
	}
	fields = append(fields, logstorage.Field{
		Name:  "_msg",
		Value: msgFieldValue,
	})
	lmp.AddRow(int64(span.EndTimeUnixNano), fields, nil)

	// create an entity in trace-id-idx stream, if this trace_id hasn't been seen before.
	if !traceIDCache.Has([]byte(span.TraceID)) {
		lmp.AddRow(int64(span.StartTimeUnixNano), []logstorage.Field{
			{Name: otelpb.TraceIDIndexFieldName, Value: span.TraceID},
			{Name: "_msg", Value: msgFieldValue},
		}, []logstorage.Field{{Name: otelpb.TraceIDIndexStreamName, Value: strconv.FormatUint(xxhash.Sum64String(span.TraceID)%otelpb.TraceIDIndexPartitionCount, 10)}})
		traceIDCache.Set([]byte(span.TraceID), nil)
	}
	return fields
}

func appendKeyValuesWithPrefix(fields []logstorage.Field, kvs []*otelpb.KeyValue, parentField, prefix string) []logstorage.Field {
	return appendKeyValuesWithPrefixSuffix(fields, kvs, parentField, prefix, "")
}

func appendKeyValuesWithPrefixSuffix(fields []logstorage.Field, kvs []*otelpb.KeyValue, parentField, prefix, suffix string) []logstorage.Field {
	for _, attr := range kvs {
		fieldName := attr.Key
		if parentField != "" {
			fieldName = parentField + "." + fieldName
		}

		if attr.Value.KeyValueList != nil {
			fields = appendKeyValuesWithPrefixSuffix(fields, attr.Value.KeyValueList.Values, fieldName, prefix, suffix)
			continue
		}

		v := attr.Value.FormatString(true)
		if len(v) == 0 {
			// VictoriaLogs does not support empty string as field value. set it to "-" to preserve the field.
			v = "-"
		}
		fields = append(fields, logstorage.Field{
			Name:  prefix + fieldName + suffix,
			Value: v,
		})
	}
	return fields
}
