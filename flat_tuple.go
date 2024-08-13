package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

type FlatTuple struct {
	CFlatTuple  C.kuzu_flat_tuple
	queryResult *QueryResult
	isClosed    bool
}

func (tuple FlatTuple) Close() {
	if tuple.isClosed {
		return
	}
	C.kuzu_flat_tuple_destroy(&tuple.CFlatTuple)
}

func (tuple FlatTuple) GetAsString() string {
	cString := C.kuzu_flat_tuple_to_string(&tuple.CFlatTuple)
	defer C.kuzu_destroy_string(cString)
	return C.GoString(cString)
}
