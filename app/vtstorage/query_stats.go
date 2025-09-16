package vtstorage

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/metrics"
)

var (
	bytesReadPerQueryColumnsHeaders       = metrics.NewHistogram(`vt_storage_per_query_columns_headers_read_bytes`)
	bytesReadPerQueryColumnsHeaderIndexes = metrics.NewHistogram(`vt_storage_per_query_columns_header_indexes_read_bytes`)
	bytesReadPerQueryBloomFilters         = metrics.NewHistogram(`vt_storage_per_query_bloom_filters_read_bytes`)
	bytesReadPerQueryValues               = metrics.NewHistogram(`vt_storage_per_query_values_read_bytes`)
	bytesReadPerQueryTimestamps           = metrics.NewHistogram(`vt_storage_per_query_timestamps_read_bytes`)
	bytesReadPerQueryBlockHeaders         = metrics.NewHistogram(`vt_storage_per_query_block_headers_read_bytes`)

	bytesReadPerQueryTotal = metrics.NewHistogram(`vt_storage_per_query_total_read_bytes`)

	blocksProcessedPerQuery                  = metrics.NewHistogram(`vt_storage_per_query_processed_blocks`)
	rowsProcessedPerQuery                    = metrics.NewHistogram(`vt_storage_per_query_processed_rows`)
	rowsFoundPerQuery                        = metrics.NewHistogram(`vt_storage_per_query_found_rows`)
	valuesReadPerQuery                       = metrics.NewHistogram(`vt_storage_per_query_read_values`)
	timestampsReadPerQuery                   = metrics.NewHistogram(`vt_storage_per_query_read_timestamps`)
	bytesProcessedPerQueryUncompressedValues = metrics.NewHistogram(`vt_storage_per_query_uncompressed_values_processed_bytes`)
)

// UpdatePerQueryStatsMetrics updates query stats metrics with the given qs.
func UpdatePerQueryStatsMetrics(qs *logstorage.QueryStats) {
	bytesReadPerQueryColumnsHeaders.Update(float64(qs.BytesReadColumnsHeaders))
	bytesReadPerQueryColumnsHeaderIndexes.Update(float64(qs.BytesReadColumnsHeaderIndexes))
	bytesReadPerQueryBloomFilters.Update(float64(qs.BytesReadBloomFilters))
	bytesReadPerQueryValues.Update(float64(qs.BytesReadValues))
	bytesReadPerQueryTimestamps.Update(float64(qs.BytesReadTimestamps))
	bytesReadPerQueryBlockHeaders.Update(float64(qs.BytesReadBlockHeaders))

	bytesReadPerQueryTotal.Update(float64(qs.GetBytesReadTotal()))

	blocksProcessedPerQuery.Update(float64(qs.BlocksProcessed))
	rowsProcessedPerQuery.Update(float64(qs.RowsProcessed))
	rowsFoundPerQuery.Update(float64(qs.RowsFound))
	valuesReadPerQuery.Update(float64(qs.ValuesRead))
	timestampsReadPerQuery.Update(float64(qs.TimestampsRead))
	bytesProcessedPerQueryUncompressedValues.Update(float64(qs.BytesProcessedUncompressedValues))
}
