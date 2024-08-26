package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"runtime"
	"time"
	"unsafe"
)

type Connection struct {
	cConnection C.kuzu_connection
	isClosed    bool
}

func OpenConnection(database Database) (Connection, error) {
	conn := Connection{}
	runtime.SetFinalizer(&conn, func(conn *Connection) {
		conn.Close()
	})
	status := C.kuzu_connection_init(&database.cDatabase, &conn.cConnection)
	if status != C.KuzuSuccess {
		return conn, fmt.Errorf("failed to open connection with status %d", status)
	}
	return conn, nil
}

func (conn *Connection) Close() {
	if conn.isClosed {
		return
	}
	C.kuzu_connection_destroy(&conn.cConnection)
	conn.isClosed = true
}

func (conn *Connection) GetMaxNumThreads() uint64 {
	numThreads := C.uint64_t(0)
	C.kuzu_connection_get_max_num_thread_for_exec(&conn.cConnection, &numThreads)
	return uint64(numThreads)
}

func (conn *Connection) SetMaxNumThreads(numThreads uint64) {
	C.kuzu_connection_set_max_num_thread_for_exec(&conn.cConnection, C.uint64_t(numThreads))
}

func (conn *Connection) Interrupt() {
	C.kuzu_connection_interrupt(&conn.cConnection)
}

func (conn *Connection) SetTimeout(timeout uint64) {
	C.kuzu_connection_set_query_timeout(&conn.cConnection, C.uint64_t(timeout))
}

func (conn *Connection) Query(query string) (QueryResult, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	queryResult := QueryResult{}
	runtime.SetFinalizer(&queryResult, func(queryResult *QueryResult) {
		queryResult.Close()
	})
	status := C.kuzu_connection_query(&conn.cConnection, cQuery, &queryResult.cQueryResult)
	if status != C.KuzuSuccess || !C.kuzu_query_result_is_success(&queryResult.cQueryResult) {
		cErrMsg := C.kuzu_query_result_get_error_message(&queryResult.cQueryResult)
		defer C.kuzu_destroy_string(cErrMsg)
		return queryResult, fmt.Errorf(C.GoString(cErrMsg))
	}
	return queryResult, nil
}

func (conn *Connection) Execute(preparedStatement PreparedStatement, args map[string]any) (QueryResult, error) {
	for key, value := range args {
		err := conn.bindParameter(preparedStatement, key, value)
		if err != nil {
			return QueryResult{}, err
		}
	}
	queryResult := QueryResult{}
	runtime.SetFinalizer(&queryResult, func(queryResult *QueryResult) {
		queryResult.Close()
	})
	status := C.kuzu_connection_execute(&conn.cConnection, &preparedStatement.cPreparedStatement, &queryResult.cQueryResult)
	if status != C.KuzuSuccess || !C.kuzu_query_result_is_success(&queryResult.cQueryResult) {
		cErrMsg := C.kuzu_query_result_get_error_message(&queryResult.cQueryResult)
		defer C.kuzu_destroy_string(cErrMsg)
		return queryResult, fmt.Errorf(C.GoString(cErrMsg))
	}
	return queryResult, nil
}

func (conn *Connection) bindParameter(preparedStatement PreparedStatement, key string, value any) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var status C.kuzu_state
	if value == nil {
		cValue := C.kuzu_value_create_null()
		defer C.kuzu_value_destroy(cValue)
		status = C.kuzu_prepared_statement_bind_value(&preparedStatement.cPreparedStatement, cKey, cValue)
		if status != C.KuzuSuccess {
			return fmt.Errorf("failed to bind null value with status %d", status)
		}
		return nil
	}
	switch v := value.(type) {
	case string:
		cValue := C.CString(v)
		defer C.free(unsafe.Pointer(cValue))
		status = C.kuzu_prepared_statement_bind_string(&preparedStatement.cPreparedStatement, cKey, cValue)
	case bool:
		status = C.kuzu_prepared_statement_bind_bool(&preparedStatement.cPreparedStatement, cKey, C.bool(v))
	case int64, int:
		status = C.kuzu_prepared_statement_bind_int64(&preparedStatement.cPreparedStatement, cKey, C.int64_t(v.(int64)))
	case int32:
		status = C.kuzu_prepared_statement_bind_int32(&preparedStatement.cPreparedStatement, cKey, C.int32_t(v))
	case int16:
		status = C.kuzu_prepared_statement_bind_int16(&preparedStatement.cPreparedStatement, cKey, C.int16_t(v))
	case int8:
		status = C.kuzu_prepared_statement_bind_int8(&preparedStatement.cPreparedStatement, cKey, C.int8_t(v))
	case uint:
		status = C.kuzu_prepared_statement_bind_uint64(&preparedStatement.cPreparedStatement, cKey, C.uint64_t(v))
	case uint64:
		status = C.kuzu_prepared_statement_bind_uint64(&preparedStatement.cPreparedStatement, cKey, C.uint64_t(v))
	case uint32:
		status = C.kuzu_prepared_statement_bind_uint32(&preparedStatement.cPreparedStatement, cKey, C.uint32_t(v))
	case uint16:
		status = C.kuzu_prepared_statement_bind_uint16(&preparedStatement.cPreparedStatement, cKey, C.uint16_t(v))
	case uint8:
		status = C.kuzu_prepared_statement_bind_uint8(&preparedStatement.cPreparedStatement, cKey, C.uint8_t(v))
	case float64:
		status = C.kuzu_prepared_statement_bind_double(&preparedStatement.cPreparedStatement, cKey, C.double(v))
	case float32:
		status = C.kuzu_prepared_statement_bind_float(&preparedStatement.cPreparedStatement, cKey, C.float(v))
	case time.Time:
		if timeHasNanoseconds(v) {
			status = C.kuzu_prepared_statement_bind_timestamp_ns(&preparedStatement.cPreparedStatement, cKey, timeToKuzuTimestampNs(v))
		} else {
			status = C.kuzu_prepared_statement_bind_timestamp(&preparedStatement.cPreparedStatement, cKey, timeToKuzuTimestamp(v))
		}
	case time.Duration:
		status = C.kuzu_prepared_statement_bind_interval(&preparedStatement.cPreparedStatement, cKey, durationToKuzuInterval(v))
	default:
		return fmt.Errorf("unsupported type")
	}
	if status != C.KuzuSuccess {
		return fmt.Errorf("failed to bind value with status %d", status)
	}
	return nil
}

func (conn *Connection) Prepare(query string) (PreparedStatement, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	preparedStatement := PreparedStatement{}
	runtime.SetFinalizer(&preparedStatement, func(preparedStatement *PreparedStatement) {
		preparedStatement.Close()
	})
	status := C.kuzu_connection_prepare(&conn.cConnection, cQuery, &preparedStatement.cPreparedStatement)
	if status != C.KuzuSuccess || !C.kuzu_prepared_statement_is_success(&preparedStatement.cPreparedStatement) {
		cErrMsg := C.kuzu_prepared_statement_get_error_message(&preparedStatement.cPreparedStatement)
		defer C.kuzu_destroy_string(cErrMsg)
		return preparedStatement, fmt.Errorf(C.GoString(cErrMsg))
	}
	return preparedStatement, nil
}
