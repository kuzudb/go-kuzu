package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"unsafe"
)

type QueryResult struct {
	CQueryResult C.kuzu_query_result
	isClosed     bool
}

func (queryResult QueryResult) ToString() string {
	cString := C.kuzu_query_result_to_string(&queryResult.CQueryResult)
	str := C.GoString(cString)
	C.free(unsafe.Pointer(cString))
	return str
}

func (queryResult QueryResult) Close() {
	if queryResult.isClosed {
		return
	}
	C.kuzu_query_result_destroy(&queryResult.CQueryResult)
	queryResult.isClosed = true
}

func (queryResult QueryResult) ResetIterator() {
	C.kuzu_query_result_reset_iterator(&queryResult.CQueryResult)
}

func (queryResult QueryResult) GetColumnNames() []string {
	numColumns := int64(C.kuzu_query_result_get_num_columns(&queryResult.CQueryResult))
	columns := make([]string, 0, numColumns)
	for i := int64(0); i < numColumns; i++ {
		var outColumn *C.char
		C.kuzu_query_result_get_column_name(&queryResult.CQueryResult, C.uint64_t(i), &outColumn)
		defer C.kuzu_destroy_string(outColumn)
		columns = append(columns, C.GoString(outColumn))
	}
	return columns
}

func (queryResult QueryResult) GetNumberOfRows() uint64 {
	return uint64(C.kuzu_query_result_get_num_tuples(&queryResult.CQueryResult))
}

func (queryResult QueryResult) HasNext() bool {
	return bool(C.kuzu_query_result_has_next(&queryResult.CQueryResult))
}

func (queryResult QueryResult) GetCompilingTime() float64 {
	var cQuerySummary C.kuzu_query_summary
	C.kuzu_query_result_get_query_summary(&queryResult.CQueryResult, &cQuerySummary)
	defer C.kuzu_query_summary_destroy(&cQuerySummary)
	return float64(C.kuzu_query_summary_get_compiling_time(&cQuerySummary))
}

func (queryResult QueryResult) GetExecutionTime() float64 {
	var cQuerySummary C.kuzu_query_summary
	C.kuzu_query_result_get_query_summary(&queryResult.CQueryResult, &cQuerySummary)
	defer C.kuzu_query_summary_destroy(&cQuerySummary)
	return float64(C.kuzu_query_summary_get_execution_time(&cQuerySummary))
}
