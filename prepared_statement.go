package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

type PreparedStatement struct {
	cPreparedStatement C.kuzu_prepared_statement
	isClosed           bool
}

func (stmt *PreparedStatement) Close() {
	if stmt.isClosed {
		return
	}
	C.kuzu_prepared_statement_destroy(&stmt.cPreparedStatement)
	stmt.isClosed = true
}
