package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"
import "fmt"

// FlatTuple represents a row in the result set of a query.
type FlatTuple struct {
	cFlatTuple  C.kuzu_flat_tuple
	queryResult *QueryResult
	isClosed    bool
}

// Close closes the FlatTuple. Calling this method is optional.
// The FlatTuple will be closed automatically when it is garbage collected.
func (tuple *FlatTuple) Close() {
	if tuple.isClosed {
		return
	}
	C.kuzu_flat_tuple_destroy(&tuple.cFlatTuple)
	tuple.isClosed = true
}

// GetAsString returns the string representation of the FlatTuple.
// The string representation contains the values of the tuple separated by vertical bars.
func (tuple *FlatTuple) GetAsString() string {
	cString := C.kuzu_flat_tuple_to_string(&tuple.cFlatTuple)
	defer C.kuzu_destroy_string(cString)
	return C.GoString(cString)
}

// GetAsSlice returns the values of the FlatTuple as a slice.
// The order of the values in the slice is the same as the order of the columns
// in the query result.
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

// GetAsMap returns the values of the FlatTuple as a map.
// The keys of the map are the column names in the query result.
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

// GetValue returns the value at the given index in the FlatTuple.
func (tuple *FlatTuple) GetValue(index uint64) (any, error) {
	var cValue C.kuzu_value
	status := C.kuzu_flat_tuple_get_value(&tuple.cFlatTuple, C.uint64_t(index), &cValue)
	if status != C.KuzuSuccess {
		return nil, fmt.Errorf("failed to get value with status: %d", status)
	}
	return kuzuValueToGoValue(cValue)
}
