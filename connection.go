package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"unsafe"
)

type Connection struct {
	CConnection C.kuzu_connection
}

func OpenConnection(database Database) (Connection, error) {
	conn := Connection{}
	status := C.kuzu_connection_init(&database.CDatabase, &conn.CConnection)
	if status != C.KuzuSuccess {
		return conn, fmt.Errorf("failed to open connection with status %d", status)
	}
	return conn, nil
}

func (conn Connection) Close() {
	C.kuzu_connection_destroy(&conn.CConnection)
}

func (conn Connection) GetMaxNumThreads() uint64 {
	numThreads := C.uint64_t(0)
	C.kuzu_connection_get_max_num_thread_for_exec(&conn.CConnection, &numThreads)
	return uint64(numThreads)
}

func (conn Connection) SetMaxNumThreads(numThreads uint64) {
	C.kuzu_connection_set_max_num_thread_for_exec(&conn.CConnection, C.uint64_t(numThreads))
}

func (conn Connection) Interrupt() {
	C.kuzu_connection_interrupt(&conn.CConnection)
}

func (conn Connection) SetTimeout(timeout uint64) {
	C.kuzu_connection_set_query_timeout(&conn.CConnection, C.uint64_t(timeout))
}

func (conn Connection) Query(query string) (QueryResult, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))
	queryResult := QueryResult{}
	status := C.kuzu_connection_query(&conn.CConnection, cquery, &queryResult.CQueryResult)
	if status != C.KuzuSuccess || !C.kuzu_query_result_is_success(&queryResult.CQueryResult) {
		cErrMsg := C.kuzu_query_result_get_error_message(&queryResult.CQueryResult)
		defer C.free(unsafe.Pointer(cErrMsg))
		return queryResult, fmt.Errorf(C.GoString(cErrMsg))
	}
	return queryResult, nil
}

func (conn Connection) Prepare(query string) (PreparedStatement, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))
	preparedStatement := PreparedStatement{}
	status := C.kuzu_connection_prepare(&conn.CConnection, cquery, &preparedStatement.CPreparedStatement)
	if status != C.KuzuSuccess || !C.kuzu_prepared_statement_is_success(&preparedStatement.CPreparedStatement) {
		cErrMsg := C.kuzu_prepared_statement_get_error_message(&preparedStatement.CPreparedStatement)
		defer C.free(unsafe.Pointer(cErrMsg))
		return preparedStatement, fmt.Errorf(C.GoString(cErrMsg))
	}
	return preparedStatement, nil
}
