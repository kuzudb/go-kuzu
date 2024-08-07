package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

type PreparedStatement struct {
	CPreparedStatement C.kuzu_prepared_statement
}

func (stmt PreparedStatement) Close() {
	C.kuzu_prepared_statement_destroy(&stmt.CPreparedStatement)
}
