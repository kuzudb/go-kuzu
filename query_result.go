package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"unsafe"
)

type QueryResult struct {
	CQueryResult C.kuzu_query_result
}

func (queryResult QueryResult) ToString() string {
	cstring := C.kuzu_query_result_to_string(&queryResult.CQueryResult)
	str := C.GoString(cstring)
	C.free(unsafe.Pointer(cstring))
	return str
}
