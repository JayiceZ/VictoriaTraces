package opentelemetry

import (
	"encoding/binary"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
	"io"
	"net/http"
)

func getProtobufData(r *http.Request) ([]byte, error) {
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("cannot read request body: %s", err)}
	}
	// +--------+-------------------------------------------------+
	// | 1 byte |                    4 bytes                      |
	// +--------+-------------------------------------------------+
	// | Compressed |               Message Length                |
	// |   Flag     |                 (uint32)                    |
	// +------------+---------------------------------------------+
	// |                                                          |
	// |                   Message Data                           |
	// |                 (variable length)                        |
	// |                                                          |
	// +----------------------------------------------------------+
	if len(reqBody) < 5 {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("invalid grpc header length: %d", len(reqBody))}
	}
	grpcHeader := reqBody[:5]
	if isCompress := grpcHeader[0]; isCompress != 0 && isCompress != 1 {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("grpc compression not supporte")}
	}
	messageLength := binary.BigEndian.Uint32(grpcHeader[1:5])
	if len(reqBody) != 5+int(messageLength) {
		return nil, &httpserver.ErrorWithStatusCode{StatusCode: http.StatusBadRequest, Err: fmt.Errorf("invalid message length: %d", messageLength)}
	}
	return reqBody[5:], nil
}

// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#message-encoding
func writeExportTraceResponses(w http.ResponseWriter, rejectedSpans int64, errorMessage string) {
	resp := pb.ExportTraceServiceResponse{
		ExportTracePartialSuccess: pb.ExportTracePartialSuccess{
			RejectedSpans: rejectedSpans,
			ErrorMessage:  errorMessage,
		},
	}
	respData := resp.MarshalProtobuf(nil)
	grpcRespData := make([]byte, 5+len(respData))
	grpcRespData[0] = 0
	binary.BigEndian.PutUint32(grpcRespData[1:5], uint32(len(respData)))
	copy(grpcRespData[5:], respData)
	w.Header().Set("Content-Type", "application/grpc+proto")
	w.Header().Set("Trailer", "grpc-status, grpc-message")

	writtenLen, err := w.Write(grpcRespData)
	if writtenLen != len(grpcRespData) {
		logger.Errorf("unexpected write of %d bytes in replying OLTP export grpc request, expected:%d", writtenLen, len(grpcRespData))
		return
	}
	if err != nil {
		logger.Errorf("failed to reply OLTP export grpc request , error:%s", err)
		return
	}

	w.Header().Set("Grpc-Status", "0")
	w.Header().Set("Grpc-Message", "")

}
