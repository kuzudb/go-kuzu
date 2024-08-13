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
	CConnection C.kuzu_connection
	isClosed    bool
}

func OpenConnection(database Database) (Connection, error) {
	conn := Connection{}
	runtime.SetFinalizer(&conn, func(conn *Connection) {
		conn.Close()
	})
	status := C.kuzu_connection_init(&database.CDatabase, &conn.CConnection)
	if status != C.KuzuSuccess {
		return conn, fmt.Errorf("failed to open connection with status %d", status)
	}
	return conn, nil
}

func (conn Connection) Close() {
	if conn.isClosed {
		return
	}
	C.kuzu_connection_destroy(&conn.CConnection)
	conn.isClosed = true
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
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	queryResult := QueryResult{}
	runtime.SetFinalizer(&queryResult, func(queryResult *QueryResult) {
		queryResult.Close()
	})
	status := C.kuzu_connection_query(&conn.CConnection, cQuery, &queryResult.CQueryResult)
	if status != C.KuzuSuccess || !C.kuzu_query_result_is_success(&queryResult.CQueryResult) {
		cErrMsg := C.kuzu_query_result_get_error_message(&queryResult.CQueryResult)
		defer C.kuzu_destroy_string(cErrMsg)
		return queryResult, fmt.Errorf(C.GoString(cErrMsg))
	}
	return queryResult, nil
}

func (conn Connection) Execute(preparedStatement PreparedStatement, args map[string]any) (QueryResult, error) {
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
	status := C.kuzu_connection_execute(&conn.CConnection, &preparedStatement.CPreparedStatement, &queryResult.CQueryResult)
	if status != C.KuzuSuccess || !C.kuzu_query_result_is_success(&queryResult.CQueryResult) {
		cErrMsg := C.kuzu_query_result_get_error_message(&queryResult.CQueryResult)
		defer C.kuzu_destroy_string(cErrMsg)
		return queryResult, fmt.Errorf(C.GoString(cErrMsg))
	}
	return queryResult, nil
}

func (conn Connection) bindParameter(preparedStatement PreparedStatement, key string, value any) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	switch v := value.(type) {
	case string:
		cValue := C.CString(v)
		defer C.free(unsafe.Pointer(cValue))
		C.kuzu_prepared_statement_bind_string(&preparedStatement.CPreparedStatement, cKey, cValue)
	case bool:
		C.kuzu_prepared_statement_bind_bool(&preparedStatement.CPreparedStatement, cKey, C.bool(v))
	case int:
		C.kuzu_prepared_statement_bind_int64(&preparedStatement.CPreparedStatement, cKey, C.int64_t(v))
	case int64:
		C.kuzu_prepared_statement_bind_int64(&preparedStatement.CPreparedStatement, cKey, C.int64_t(v))
	case int32:
		C.kuzu_prepared_statement_bind_int32(&preparedStatement.CPreparedStatement, cKey, C.int32_t(v))
	case int16:
		C.kuzu_prepared_statement_bind_int16(&preparedStatement.CPreparedStatement, cKey, C.int16_t(v))
	case int8:
		C.kuzu_prepared_statement_bind_int8(&preparedStatement.CPreparedStatement, cKey, C.int8_t(v))
	case uint:
		C.kuzu_prepared_statement_bind_uint64(&preparedStatement.CPreparedStatement, cKey, C.uint64_t(v))
	case uint64:
		C.kuzu_prepared_statement_bind_uint64(&preparedStatement.CPreparedStatement, cKey, C.uint64_t(v))
	case uint32:
		C.kuzu_prepared_statement_bind_uint32(&preparedStatement.CPreparedStatement, cKey, C.uint32_t(v))
	case uint16:
		C.kuzu_prepared_statement_bind_uint16(&preparedStatement.CPreparedStatement, cKey, C.uint16_t(v))
	case uint8:
		C.kuzu_prepared_statement_bind_uint8(&preparedStatement.CPreparedStatement, cKey, C.uint8_t(v))
	case float64:
		C.kuzu_prepared_statement_bind_double(&preparedStatement.CPreparedStatement, cKey, C.double(v))
	case float32:
		C.kuzu_prepared_statement_bind_float(&preparedStatement.CPreparedStatement, cKey, C.float(v))
	case time.Time:
		if timeHasNanoseconds(v) {
			C.kuzu_prepared_statement_bind_timestamp_ns(&preparedStatement.CPreparedStatement, cKey, timeToKuzuTimestampNs(v))
		} else {
			C.kuzu_prepared_statement_bind_timestamp(&preparedStatement.CPreparedStatement, cKey, timeToKuzuTimestamp(v))
		}
	case time.Duration:
		C.kuzu_prepared_statement_bind_interval(&preparedStatement.CPreparedStatement, cKey, durationToKuzuInterval(v))
	default:
		return fmt.Errorf("unsupported type")
	}
	return nil
}

func (conn Connection) Prepare(query string) (PreparedStatement, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	preparedStatement := PreparedStatement{}
	runtime.SetFinalizer(&preparedStatement, func(preparedStatement *PreparedStatement) {
		preparedStatement.Close()
	})
	status := C.kuzu_connection_prepare(&conn.CConnection, cQuery, &preparedStatement.CPreparedStatement)
	if status != C.KuzuSuccess || !C.kuzu_prepared_statement_is_success(&preparedStatement.CPreparedStatement) {
		cErrMsg := C.kuzu_prepared_statement_get_error_message(&preparedStatement.CPreparedStatement)
		defer C.kuzu_destroy_string(cErrMsg)
		return preparedStatement, fmt.Errorf(C.GoString(cErrMsg))
	}
	return preparedStatement, nil
}
