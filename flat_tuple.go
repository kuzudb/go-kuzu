package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"
import "fmt"

type FlatTuple struct {
	cFlatTuple  C.kuzu_flat_tuple
	queryResult *QueryResult
	isClosed    bool
}

func (tuple *FlatTuple) Close() {
	if tuple.isClosed {
		return
	}
	C.kuzu_flat_tuple_destroy(&tuple.cFlatTuple)
}

func (tuple *FlatTuple) GetAsString() string {
	cString := C.kuzu_flat_tuple_to_string(&tuple.cFlatTuple)
	defer C.kuzu_destroy_string(cString)
	return C.GoString(cString)
}

func (tuple *FlatTuple) GetAsSlice() ([]any, error) {
	length := uint64(tuple.queryResult.GetNumberOfColumns())
	values := make([]any, 0, length)
	var errors []error
	for i := uint64(0); i < length; i++ {
		value, err := tuple.GetValue(i)
		if err != nil {
			errors = append(errors, err)
		}
		values = append(values, value)
	}
	if len(errors) > 0 {
		return values, fmt.Errorf("failed to get values: %v", errors)
	}
	return values, nil
}

func (tuple *FlatTuple) GetAsMap() (map[string]any, error) {
	columnNames := tuple.queryResult.GetColumnNames()
	values, err := tuple.GetAsSlice()
	if err != nil {
		if len(columnNames) != len(values) {
			return nil, err
		}
	}
	m := make(map[string]any)
	for i, columnName := range columnNames {
		m[columnName] = values[i]
	}
	return m, err
}

func (tuple *FlatTuple) GetValue(index uint64) (any, error) {
	var cValue C.kuzu_value
	status := C.kuzu_flat_tuple_get_value(&tuple.cFlatTuple, C.uint64_t(index), &cValue)
	if status != C.KuzuSuccess {
		return nil, fmt.Errorf("failed to get value with status: %d", status)
	}
	return kuzuValueToGoValue(cValue)
}
