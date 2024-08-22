package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"fmt"

	"math/big"

	"github.com/google/uuid"
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
	case C.KUZU_BOOL:
		var value C.bool
		status := C.kuzu_value_get_bool(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get bool value with status: %d", status)
		}
		return bool(value), nil
	case C.KUZU_INT64, C.KUZU_SERIAL:
		var value C.int64_t
		status := C.kuzu_value_get_int64(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get int64 value with status: %d", status)
		}
		return int64(value), nil
	case C.KUZU_INT32:
		var value C.int32_t
		status := C.kuzu_value_get_int32(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get int32 value with status: %d", status)
		}
		return int32(value), nil
	case C.KUZU_INT16:
		var value C.int16_t
		status := C.kuzu_value_get_int16(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get int16 value with status: %d", status)
		}
		return int16(value), nil
	case C.KUZU_INT128:
		var value C.kuzu_int128_t
		status := C.kuzu_value_get_int128(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get int128 value with status: %d", status)
		}
		return int128ToBigInt(value)
	case C.KUZU_INT8:
		var value C.int8_t
		status := C.kuzu_value_get_int8(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get int8 value with status: %d", status)
		}
		return int8(value), nil
	case C.KUZU_UUID:
		var value *C.char
		status := C.kuzu_value_get_uuid(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get uuid value with status: %d", status)
		}
		defer C.kuzu_destroy_string(value)
		uuidString := C.GoString(value)
		return uuid.Parse(uuidString)
	case C.KUZU_UINT64:
		var value C.uint64_t
		status := C.kuzu_value_get_uint64(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get uint64 value with status: %d", status)
		}
		return uint64(value), nil
	case C.KUZU_UINT32:
		var value C.uint32_t
		status := C.kuzu_value_get_uint32(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get uint32 value with status: %d", status)
		}
		return uint32(value), nil
	case C.KUZU_UINT16:
		var value C.uint16_t
		status := C.kuzu_value_get_uint16(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get uint16 value with status: %d", status)
		}
		return uint16(value), nil
	case C.KUZU_UINT8:
		var value C.uint8_t
		status := C.kuzu_value_get_uint8(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get uint8 value with status: %d", status)
		}
		return uint8(value), nil
	case C.KUZU_DOUBLE:
		var value C.double
		status := C.kuzu_value_get_double(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get double value with status: %d", status)
		}
		return float64(value), nil
	case C.KUZU_FLOAT:
		var value C.float
		status := C.kuzu_value_get_float(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get float value with status: %d", status)
		}
		return float32(value), nil
	case C.KUZU_STRING:
		var outString *C.char
		status := C.kuzu_value_get_string(&kuzuValue, &outString)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get string value with status: %d", status)
		}
		defer C.kuzu_destroy_string(outString)
		return C.GoString(outString), nil
	default:
		valueString := C.kuzu_value_to_string(&kuzuValue)
		defer C.kuzu_destroy_string(valueString)
		return valueString, fmt.Errorf("unsupported data type with type id: %d. the value is force-casted to string", logicalTypeId)
	}
}

func int128ToBigInt(value C.kuzu_int128_t) (*big.Int, error) {
	var outString *C.char
	status := C.kuzu_int128_t_to_string(value, &outString)
	if status != C.KuzuSuccess {
		return nil, fmt.Errorf("failed to convert int128 to string with status: %d", status)
	}
	defer C.kuzu_destroy_string(outString)
	valueString := C.GoString(outString)
	bigInt := new(big.Int)
	_, success := bigInt.SetString(valueString, 10)
	if !success {
		return nil, fmt.Errorf("failed to convert string to big.Int")
	}
	return bigInt, nil
}
