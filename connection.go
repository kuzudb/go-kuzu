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
