package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"
	"time"
	"unsafe"

	"math/big"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// InternalID represents the internal ID of a node or relationship in Kùzu.
type InternalID struct {
	TableID uint64
	Offset  uint64
}

// Node represents a node retrieved from Kùzu.
// A node has an ID, a label, and properties.
type Node struct {
	ID         InternalID
	Label      string
	Properties map[string]any
}

// Relationship represents a relationship retrieved from Kùzu.
// A relationship has a source ID, a destination ID, a label, and properties.
type Relationship struct {
	SourceID      InternalID
	DestinationID InternalID
	Label         string
	Properties    map[string]any
}

// RecursiveRelationship represents a recursive relationship retrieved from a
// path query in Kùzu. A recursive relationship has a list of nodes and a list
// of relationships.
type RecursiveRelationship struct {
	Nodes         []Node
	Relationships []Relationship
}

// kuzuNodeValueToGoValue converts a kuzu_value representing a node to a Node
// struct in Go.
func kuzuNodeValueToGoValue(kuzuValue C.kuzu_value) (Node, error) {
	node := Node{}
	node.Properties = make(map[string]any)
	idValue := C.kuzu_value{}
	C.kuzu_node_val_get_id_val(&kuzuValue, &idValue)
	nodeId, _ := kuzuValueToGoValue(idValue)
	node.ID = nodeId.(InternalID)
	C.kuzu_value_destroy(&idValue)
	labelValue := C.kuzu_value{}
	C.kuzu_node_val_get_label_val(&kuzuValue, &labelValue)
	nodeLabel, _ := kuzuValueToGoValue(labelValue)
	node.Label = nodeLabel.(string)
	C.kuzu_value_destroy(&labelValue)
	var propertySize C.uint64_t
	C.kuzu_node_val_get_property_size(&kuzuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.kuzu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.kuzu_node_val_get_property_name_at(&kuzuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.kuzu_destroy_string(currentKey)
		C.kuzu_node_val_get_property_value_at(&kuzuValue, i, &currentVal)
		value, err := kuzuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		node.Properties[keyString] = value
		C.kuzu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return node, fmt.Errorf("failed to get values: %v", errors)
	}
	return node, nil
}

// kuzuRelValueToGoValue converts a kuzu_value representing a relationship to a
// Relationship struct in Go.
func kuzuRelValueToGoValue(kuzuValue C.kuzu_value) (Relationship, error) {
	relation := Relationship{}
	relation.Properties = make(map[string]any)
	idValue := C.kuzu_value{}
	C.kuzu_rel_val_get_src_id_val(&kuzuValue, &idValue)
	src, _ := kuzuValueToGoValue(idValue)
	relation.SourceID = src.(InternalID)
	C.kuzu_value_destroy(&idValue)
	C.kuzu_rel_val_get_dst_id_val(&kuzuValue, &idValue)
	dst, _ := kuzuValueToGoValue(idValue)
	relation.DestinationID = dst.(InternalID)
	C.kuzu_value_destroy(&idValue)
	labelValue := C.kuzu_value{}
	C.kuzu_rel_val_get_label_val(&kuzuValue, &labelValue)
	label, _ := kuzuValueToGoValue(labelValue)
	relation.Label = label.(string)
	C.kuzu_value_destroy(&labelValue)
	var propertySize C.uint64_t
	C.kuzu_rel_val_get_property_size(&kuzuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.kuzu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.kuzu_rel_val_get_property_name_at(&kuzuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.kuzu_destroy_string(currentKey)
		C.kuzu_rel_val_get_property_value_at(&kuzuValue, i, &currentVal)
		value, err := kuzuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		relation.Properties[keyString] = value
		C.kuzu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return relation, fmt.Errorf("failed to get values: %v", errors)
	}
	return relation, nil
}

// kuzuRecursiveRelValueToGoValue converts a kuzu_value representing a recursive
// relationship to a RecursiveRelationship struct in Go.
func kuzuRecursiveRelValueToGoValue(kuzuValue C.kuzu_value) (RecursiveRelationship, error) {
	var nodesVal C.kuzu_value
	var relsVal C.kuzu_value
	C.kuzu_value_get_recursive_rel_node_list(&kuzuValue, &nodesVal)
	C.kuzu_value_get_recursive_rel_rel_list(&kuzuValue, &relsVal)
	defer C.kuzu_value_destroy(&nodesVal)
	defer C.kuzu_value_destroy(&relsVal)
	nodes, _ := kuzuListValueToGoValue(nodesVal)
	rels, _ := kuzuListValueToGoValue(relsVal)
	recursiveRel := RecursiveRelationship{}
	recursiveRel.Nodes = make([]Node, len(nodes))
	for i, n := range nodes {
		recursiveRel.Nodes[i] = n.(Node)
	}
	relationships := make([]Relationship, len(rels))
	for i, r := range rels {
		relationships[i] = r.(Relationship)
	}
	recursiveRel.Relationships = relationships
	return recursiveRel, nil
}

// kuzuListValueToGoValue converts a kuzu_value representing a LIST or ARRAY to
// a slice of any in Go.
func kuzuListValueToGoValue(kuzuValue C.kuzu_value) ([]any, error) {
	var listSize C.uint64_t
	cLogicalType := C.kuzu_logical_type{}
	defer C.kuzu_data_type_destroy(&cLogicalType)
	C.kuzu_value_get_data_type(&kuzuValue, &cLogicalType)
	logicalTypeId := C.kuzu_data_type_get_id(&cLogicalType)
	if logicalTypeId == C.KUZU_ARRAY {
		C.kuzu_data_type_get_num_elements_in_array(&cLogicalType, &listSize)
	} else {
		C.kuzu_value_get_list_size(&kuzuValue, &listSize)
	}
	list := make([]any, 0, int(listSize))
	var currentVal C.kuzu_value
	var errors []error
	for i := C.uint64_t(0); i < listSize; i++ {
		C.kuzu_value_get_list_element(&kuzuValue, i, &currentVal)
		value, err := kuzuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		list = append(list, value)
		C.kuzu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return list, fmt.Errorf("failed to get values: %v", errors)
	}
	return list, nil
}

// kuzuStructValueToGoValue converts a kuzu_value representing a STRUCT to a
// map of string to any in Go.
func kuzuStructValueToGoValue(kuzuValue C.kuzu_value) (map[string]any, error) {
	structure := make(map[string]any)
	var propertySize C.uint64_t
	C.kuzu_value_get_struct_num_fields(&kuzuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.kuzu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.kuzu_value_get_struct_field_name(&kuzuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.kuzu_destroy_string(currentKey)
		C.kuzu_value_get_struct_field_value(&kuzuValue, i, &currentVal)
		value, err := kuzuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		structure[keyString] = value
		C.kuzu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return structure, fmt.Errorf("failed to get values: %v", errors)
	}
	return structure, nil
}

// kuzuMapValueToGoValue converts a kuzu_value representing a MAP to a
// map of string to any in Go.
func kuzuMapValueToGoValue(kuzuValue C.kuzu_value) (map[string]any, error) {
	structure := make(map[string]any)
	var propertySize C.uint64_t
	C.kuzu_value_get_map_num_fields(&kuzuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.kuzu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.kuzu_value_get_map_field_name(&kuzuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.kuzu_destroy_string(currentKey)
		C.kuzu_value_get_map_field_value(&kuzuValue, i, &currentVal)
		value, err := kuzuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		structure[keyString] = value
		C.kuzu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return structure, fmt.Errorf("failed to get values: %v", errors)
	}
	return structure, nil
}

// kuzuValueToGoValue converts a kuzu_value to a corresponding Go value.
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
	case C.KUZU_TIMESTAMP:
		var value C.kuzu_timestamp_t
		status := C.kuzu_value_get_timestamp(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get timestamp value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)*1000), nil
	case C.KUZU_TIMESTAMP_NS:
		var value C.kuzu_timestamp_ns_t
		status := C.kuzu_value_get_timestamp_ns(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_ns value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)), nil
	case C.KUZU_TIMESTAMP_MS:
		var value C.kuzu_timestamp_ms_t
		status := C.kuzu_value_get_timestamp_ms(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_ms value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)*1000000), nil
	case C.KUZU_TIMESTAMP_SEC:
		var value C.kuzu_timestamp_sec_t
		status := C.kuzu_value_get_timestamp_sec(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_sec value with status: %d", status)
		}
		return time.Unix(int64(value.value), 0), nil
	case C.KUZU_TIMESTAMP_TZ:
		var value C.kuzu_timestamp_tz_t
		status := C.kuzu_value_get_timestamp_tz(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_tz value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)*1000), nil
	case C.KUZU_DATE:
		var value C.kuzu_date_t
		status := C.kuzu_value_get_date(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get date value with status: %d", status)
		}
		return kuzuDateToTime(value), nil
	case C.KUZU_INTERVAL:
		var value C.kuzu_interval_t
		status := C.kuzu_value_get_interval(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get interval value with status: %d", status)
		}
		return kuzuIntervalToDuration(value), nil
	case C.KUZU_INTERNAL_ID:
		var value C.kuzu_internal_id_t
		status := C.kuzu_value_get_internal_id(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get internal_id value with status: %d", status)
		}
		return InternalID{TableID: uint64(value.table_id), Offset: uint64(value.offset)}, nil
	case C.KUZU_BLOB:
		var value *C.uint8_t
		status := C.kuzu_value_get_blob(&kuzuValue, &value)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get blob value with status: %d", status)
		}
		defer C.kuzu_destroy_blob(value)
		blobSize := C.strlen((*C.char)(unsafe.Pointer(value)))
		blob := C.GoBytes(unsafe.Pointer(value), C.int(blobSize))
		return blob, nil
	case C.KUZU_NODE:
		return kuzuNodeValueToGoValue(kuzuValue)
	case C.KUZU_REL:
		return kuzuRelValueToGoValue(kuzuValue)
	case C.KUZU_RECURSIVE_REL:
		return kuzuRecursiveRelValueToGoValue(kuzuValue)
	case C.KUZU_LIST, C.KUZU_ARRAY:
		return kuzuListValueToGoValue(kuzuValue)
	case C.KUZU_STRUCT, C.KUZU_UNION:
		return kuzuStructValueToGoValue(kuzuValue)
	case C.KUZU_MAP:
		return kuzuMapValueToGoValue(kuzuValue)
	case C.KUZU_DECIMAL:
		var outString *C.char
		status := C.kuzu_value_get_decimal_as_string(&kuzuValue, &outString)
		if status != C.KuzuSuccess {
			return nil, fmt.Errorf("failed to get string value of decimal type with status: %d", status)
		}
		goString := C.GoString(outString)
		C.kuzu_destroy_string(outString)
		goDecimal, casting_error := decimal.NewFromString(goString)
		if casting_error != nil {
			return nil, fmt.Errorf("failed to convert decimal value with error: %w", casting_error)
		}
		return goDecimal, casting_error
	default:
		valueString := C.kuzu_value_to_string(&kuzuValue)
		defer C.kuzu_destroy_string(valueString)
		return C.GoString(valueString), fmt.Errorf("unsupported data type with type id: %d. the value is force-casted to string", logicalTypeId)
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
