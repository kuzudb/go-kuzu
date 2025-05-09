package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// Connection represents a connection to a Kuzu database.
type Connection struct {
	cConnection C.kuzu_connection
	database    *Database
	isClosed    bool
}

// OpenConnection opens a connection to the specified database.
func OpenConnection(database *Database) (*Connection, error) {
	conn := &Connection{}
	conn.database = database
	runtime.SetFinalizer(conn, func(conn *Connection) {
		conn.Close()
	})
	status := C.kuzu_connection_init(&database.cDatabase, &conn.cConnection)
	if status != C.KuzuSuccess {
		return conn, fmt.Errorf("failed to open connection with status %d", status)
	}
	return conn, nil
}

// Close closes the Connection. Calling this method is optional.
// The Connection will be closed automatically when it is garbage collected.
func (conn *Connection) Close() {
	if conn.isClosed {
		return
	}
	C.kuzu_connection_destroy(&conn.cConnection)
	conn.isClosed = true
}

// GetMaxNumThreads returns the maximum number of threads that can be used for
// executing a query in parallel.
func (conn *Connection) GetMaxNumThreads() uint64 {
	numThreads := C.uint64_t(0)
	C.kuzu_connection_get_max_num_thread_for_exec(&conn.cConnection, &numThreads)
	return uint64(numThreads)
}

// SetMaxNumThreads sets the maximum number of threads that can be used for
// executing a query in parallel.
func (conn *Connection) SetMaxNumThreads(numThreads uint64) {
	C.kuzu_connection_set_max_num_thread_for_exec(&conn.cConnection, C.uint64_t(numThreads))
}

// Interrupt interrupts the execution of the current query on the connection.
func (conn *Connection) Interrupt() {
	C.kuzu_connection_interrupt(&conn.cConnection)
}

// SetTimeout sets the timeout for the queries executed on the connection.
// The timeout is specified in milliseconds. A value of 0 means no timeout.
// If a query takes longer than the specified timeout, it will be interrupted.
func (conn *Connection) SetTimeout(timeout uint64) {
	C.kuzu_connection_set_query_timeout(&conn.cConnection, C.uint64_t(timeout))
}

// Query executes the specified query string and returns the result.
func (conn *Connection) Query(query string) (*QueryResult, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	queryResult := &QueryResult{}
	queryResult.connection = conn
	runtime.SetFinalizer(queryResult, func(queryResult *QueryResult) {
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

// Execute executes the specified prepared statement with the specified arguments and returns the result.
// The arguments are a map of parameter names to values.
func (conn *Connection) Execute(preparedStatement *PreparedStatement, args map[string]any) (*QueryResult, error) {
	queryResult := &QueryResult{}
	queryResult.connection = conn
	for key, value := range args {
		err := conn.bindParameter(preparedStatement, key, value)
		if err != nil {
			return queryResult, err
		}
	}
	runtime.SetFinalizer(queryResult, func(queryResult *QueryResult) {
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

// BindParameter binds a parameter to the prepared statement.
func (conn *Connection) bindParameter(preparedStatement *PreparedStatement, key string, value any) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var status C.kuzu_state
	var cValue *C.kuzu_value
	var valueConversionError error
	cValue, valueConversionError = goValueToKuzuValue(value)
	if valueConversionError != nil {
		return fmt.Errorf("failed to convert Go value to Kuzu value: %v", valueConversionError)
	}
	defer C.kuzu_value_destroy(cValue)
	status = C.kuzu_prepared_statement_bind_value(&preparedStatement.cPreparedStatement, cKey, cValue)
	if status != C.KuzuSuccess {
		return fmt.Errorf("failed to bind value with status %d", status)
	}
	return nil
}

// Prepare returns a prepared statement for the specified query string.
// The prepared statement can be used to execute the query with parameters.
func (conn *Connection) Prepare(query string) (*PreparedStatement, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	preparedStatement := &PreparedStatement{}
	preparedStatement.connection = conn
	runtime.SetFinalizer(preparedStatement, func(preparedStatement *PreparedStatement) {
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
