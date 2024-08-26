package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

type QueryResult struct {
	cQueryResult C.kuzu_query_result
	isClosed     bool
	columnNames  []string
}

func (queryResult *QueryResult) ToString() string {
	cString := C.kuzu_query_result_to_string(&queryResult.cQueryResult)
	str := C.GoString(cString)
	C.free(unsafe.Pointer(cString))
	return str
}

func (queryResult *QueryResult) Close() {
	if queryResult.isClosed {
		return
	}
	C.kuzu_query_result_destroy(&queryResult.cQueryResult)
	queryResult.isClosed = true
}

func (queryResult *QueryResult) ResetIterator() {
	C.kuzu_query_result_reset_iterator(&queryResult.cQueryResult)
}

func (queryResult *QueryResult) GetColumnNames() []string {
	if queryResult.columnNames != nil {
		return queryResult.columnNames
	}
	numColumns := int64(C.kuzu_query_result_get_num_columns(&queryResult.cQueryResult))
	columns := make([]string, 0, numColumns)
	for i := int64(0); i < numColumns; i++ {
		var outColumn *C.char
		C.kuzu_query_result_get_column_name(&queryResult.cQueryResult, C.uint64_t(i), &outColumn)
		defer C.kuzu_destroy_string(outColumn)
		columns = append(columns, C.GoString(outColumn))
	}
	queryResult.columnNames = columns
	return columns
}

func (queryResult *QueryResult) GetNumberOfColumns() uint64 {
	return uint64(C.kuzu_query_result_get_num_columns(&queryResult.cQueryResult))
}

func (queryResult *QueryResult) GetNumberOfRows() uint64 {
	if queryResult.columnNames != nil {
		return uint64(len(queryResult.columnNames))
	}
	return uint64(C.kuzu_query_result_get_num_tuples(&queryResult.cQueryResult))
}

func (queryResult *QueryResult) HasNext() bool {
	return bool(C.kuzu_query_result_has_next(&queryResult.cQueryResult))
}

func (queryResult *QueryResult) Next() (FlatTuple, error) {
	tuple := FlatTuple{}
	runtime.SetFinalizer(&tuple, func(tuple *FlatTuple) {
		tuple.Close()
	})
	tuple.queryResult = queryResult
	status := C.kuzu_query_result_get_next(&queryResult.cQueryResult, &tuple.cFlatTuple)
	if status != C.KuzuSuccess {
		return tuple, fmt.Errorf("failed to get next tuple with status %d", status)
	}
	return tuple, nil
}

func (queryResult *QueryResult) HasNextQueryResult() bool {
	return bool(C.kuzu_query_result_has_next_query_result(&queryResult.cQueryResult))
}

func (queryResult *QueryResult) NextQueryResult() (QueryResult, error) {
	nextQueryResult := QueryResult{}
	runtime.SetFinalizer(&nextQueryResult, func(nextQueryResult *QueryResult) {
		nextQueryResult.Close()
	})
	status := C.kuzu_query_result_get_next_query_result(&queryResult.cQueryResult, &nextQueryResult.cQueryResult)
	if status != C.KuzuSuccess {
		return nextQueryResult, fmt.Errorf("failed to get next query result with status %d", status)
	}
	return nextQueryResult, nil
}

func (queryResult *QueryResult) GetCompilingTime() float64 {
	var cQuerySummary C.kuzu_query_summary
	C.kuzu_query_result_get_query_summary(&queryResult.cQueryResult, &cQuerySummary)
	defer C.kuzu_query_summary_destroy(&cQuerySummary)
	return float64(C.kuzu_query_summary_get_compiling_time(&cQuerySummary))
}

func (queryResult *QueryResult) GetExecutionTime() float64 {
	var cQuerySummary C.kuzu_query_summary
	C.kuzu_query_result_get_query_summary(&queryResult.cQueryResult, &cQuerySummary)
	defer C.kuzu_query_summary_destroy(&cQuerySummary)
	return float64(C.kuzu_query_summary_get_execution_time(&cQuerySummary))
}
