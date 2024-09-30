package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

// PreparedStatement represents a prepared statement in Kùzu, which can be
// used to execute a query with parameters.
// PreparedStatement is returned by the `Prepare` method of Connection.
type PreparedStatement struct {
	cPreparedStatement C.kuzu_prepared_statement
	connection         *Connection
	isClosed           bool
}

// Close closes the PreparedStatement. Calling this method is optional.
// The PreparedStatement will be closed automatically when it is garbage collected.
func (stmt *PreparedStatement) Close() {
	if stmt.isClosed {
		return
	}
	C.kuzu_prepared_statement_destroy(&stmt.cPreparedStatement)
	stmt.isClosed = true
}
