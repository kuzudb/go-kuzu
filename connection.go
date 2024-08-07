package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"unsafe"
)

type Connection struct {
	CConnection C.kuzu_connection
}

func OpenConnection(database Database) Connection {
	conn := Connection{}
	status := C.kuzu_connection_init(&database.CDatabase, &conn.CConnection)
	if status != 0 {
		panic("Failed to open connection")
	}
	return conn
}

func (conn Connection) Query(query string) QueryResult {
	cquery := C.CString(query)
	queryResult := QueryResult{}
	status := C.kuzu_connection_query(&conn.CConnection, cquery, &queryResult.CQueryResult)
	if status != 0 {
		panic("Failed to execute query")
	}
	defer C.free(unsafe.Pointer(cquery))
	return queryResult
}
