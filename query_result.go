package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// QueryResult represents the result of a query, which can be used to iterate
// over the result set.
// QueryResult is returned by the `Query` and `Execute` methods of Connection.
type QueryResult struct {
	cQueryResult C.kuzu_query_result
	connection   *Connection
	isClosed     bool
	columnNames  []string
}

// ToString returns the string representation of the QueryResult.
// The string representation contains the column names and the tuples in the
// result set.
func (queryResult *QueryResult) ToString() string {
	cString := C.kuzu_query_result_to_string(&queryResult.cQueryResult)
	str := C.GoString(cString)
	C.free(unsafe.Pointer(cString))
	return str
}

// Close closes the QueryResult. Calling this method is optional.
// The QueryResult will be closed automatically when it is garbage collected.
func (queryResult *QueryResult) Close() {
	if queryResult.isClosed {
		return
	}
	C.kuzu_query_result_destroy(&queryResult.cQueryResult)
	queryResult.isClosed = true
}

// ResetIterator resets the iterator of the QueryResult. After calling this method, the `Next`
// method can be called to iterate over the result set from the beginning.
func (queryResult *QueryResult) ResetIterator() {
	C.kuzu_query_result_reset_iterator(&queryResult.cQueryResult)
}

// GetColumnNames returns the column names of the QueryResult as a slice of strings.
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

// GetNumberOfColumns returns the number of columns in the QueryResult.
func (queryResult *QueryResult) GetNumberOfColumns() uint64 {
	return uint64(C.kuzu_query_result_get_num_columns(&queryResult.cQueryResult))
}

// GetNumberOfRows returns the number of rows in the QueryResult.
func (queryResult *QueryResult) GetNumberOfRows() uint64 {
	if queryResult.columnNames != nil {
		return uint64(len(queryResult.columnNames))
	}
	return uint64(C.kuzu_query_result_get_num_tuples(&queryResult.cQueryResult))
}

// HasNext returns true if there is at least one more tuple in the result set.
func (queryResult *QueryResult) HasNext() bool {
	return bool(C.kuzu_query_result_has_next(&queryResult.cQueryResult))
}

// Next returns the next tuple in the result set.
func (queryResult *QueryResult) Next() (*FlatTuple, error) {
	tuple := &FlatTuple{}
	runtime.SetFinalizer(tuple, func(tuple *FlatTuple) {
		tuple.Close()
	})
	tuple.queryResult = queryResult
	status := C.kuzu_query_result_get_next(&queryResult.cQueryResult, &tuple.cFlatTuple)
	if status != C.KuzuSuccess {
		return tuple, fmt.Errorf("failed to get next tuple with status %d", status)
	}
	return tuple, nil
}

// HasNextQueryResult returns true not all the query results is consumed when
// multiple query statements are executed.
func (queryResult *QueryResult) HasNextQueryResult() bool {
	return bool(C.kuzu_query_result_has_next_query_result(&queryResult.cQueryResult))
}

// NextQueryResult returns the next query result when multiple query statements are executed.
func (queryResult *QueryResult) NextQueryResult() (*QueryResult, error) {
	nextQueryResult := &QueryResult{}
	runtime.SetFinalizer(nextQueryResult, func(nextQueryResult *QueryResult) {
		nextQueryResult.Close()
	})
	status := C.kuzu_query_result_get_next_query_result(&queryResult.cQueryResult, &nextQueryResult.cQueryResult)
	if status != C.KuzuSuccess {
		return nextQueryResult, fmt.Errorf("failed to get next query result with status %d", status)
	}
	return nextQueryResult, nil
}

// GetCompilingTime returns the compiling time of the query in milliseconds.
func (queryResult *QueryResult) GetCompilingTime() float64 {
	var cQuerySummary C.kuzu_query_summary
	C.kuzu_query_result_get_query_summary(&queryResult.cQueryResult, &cQuerySummary)
	defer C.kuzu_query_summary_destroy(&cQuerySummary)
	return float64(C.kuzu_query_summary_get_compiling_time(&cQuerySummary))
}

// GetExecutionTime returns the execution time of the query in milliseconds.
func (queryResult *QueryResult) GetExecutionTime() float64 {
	var cQuerySummary C.kuzu_query_summary
	C.kuzu_query_result_get_query_summary(&queryResult.cQueryResult, &cQuerySummary)
	defer C.kuzu_query_summary_destroy(&cQuerySummary)
	return float64(C.kuzu_query_summary_get_execution_time(&cQuerySummary))
}
