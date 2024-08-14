package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"
)

func kuzuValueToGoValue(kuzuValue C.kuzu_value) (any, error) {
	if C.kuzu_value_is_null(&kuzuValue) {
		return nil, nil
	}
	var logicalType C.kuzu_logical_type
	defer C.kuzu_data_type_destroy(&logicalType)
	C.kuzu_value_get_data_type(&kuzuValue, &logicalType)
	logicalTypeId := C.kuzu_data_type_get_id(&logicalType)
	switch logicalTypeId {
	case C.KUZU_INT64:
		var value C.int64_t
		status := C.kuzu_value_get_int64(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get int64 value with status: %d", status)
		}
		return int64(value), nil

	default:
		valueString := C.kuzu_value_to_string(&kuzuValue)
		defer C.kuzu_destroy_string(valueString)
		return valueString, fmt.Errorf("unsupported data type with type id: %d. the value is force-casted to string", logicalTypeId)
	}

}
