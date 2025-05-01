package kuzu

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BasicParamTestHelper(t *testing.T, param any) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": param,
	}
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, param, value)
}

func FloatParamTestHelper(t *testing.T, param any) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": param,
	}
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, param, value, floatEpsilon)
}

func TimeParamTestHelper(t *testing.T, param any) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": param,
	}
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	valueTime := value.(time.Time).UTC()
	paramTime := param.(time.Time).UTC()
	assert.Equal(t, paramTime, valueTime)
}

func TestStringParam(t *testing.T) {
	BasicParamTestHelper(t, "Hello World")
}

func TestBoolParam(t *testing.T) {
	BasicParamTestHelper(t, true)
	BasicParamTestHelper(t, false)
}

func TestInt64Param(t *testing.T) {
	BasicParamTestHelper(t, int64(1000000000000))
}

func TestInt32Param(t *testing.T) {
	BasicParamTestHelper(t, int32(200))
}

func TestInt16Param(t *testing.T) {
	BasicParamTestHelper(t, int16(300))
}

func TestInt8Param(t *testing.T) {
	BasicParamTestHelper(t, int8(4))
}

func TestUint64Param(t *testing.T) {
	uintMax := ^uint(0)
	uintMax64 := uint64(uintMax)
	BasicParamTestHelper(t, uintMax64)
}

func TestUint32Param(t *testing.T) {
	BasicParamTestHelper(t, uint32(600))
}

func TestUint16Param(t *testing.T) {
	BasicParamTestHelper(t, uint16(700))
}

func TestUint8Param(t *testing.T) {
	BasicParamTestHelper(t, uint8(8))
}

func TestFloat64Param(t *testing.T) {
	FloatParamTestHelper(t, 3.14159)
}

func TestFloat32Param(t *testing.T) {
	FloatParamTestHelper(t, float32(2.71828))
}

func TestTimeParam(t *testing.T) {
	TimeParamTestHelper(t, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
}

func TestTimeWithNanosecondsParam(t *testing.T) {
	TimeParamTestHelper(t, time.Date(2020, 1, 1, 0, 0, 0, 1, time.UTC))
}

func TestDurationParam(t *testing.T) {
	duration := time.Duration(1000000000)
	BasicParamTestHelper(t, duration)
}

func TestNilParam(t *testing.T) {
	BasicParamTestHelper(t, nil)
}

func TestStructParam(t *testing.T) {
	goMap := map[string]any{
		"name":      "Alice",
		"age":       (int64)(30),
		"isStudent": false,
	}
	BasicParamTestHelper(t, goMap)
}

func TestStructWithNestedStructParam(t *testing.T) {
	goMap := map[string]any{
		"name": "Alice",
		"address": map[string]any{
			"city":    "New York",
			"country": "USA",
		},
	}
	BasicParamTestHelper(t, goMap)
}

func TestStructWithUnsupportedTypeParam(t *testing.T) {
	goMap := map[string]any{
		"name": "Alice",
		"age":  regexp.MustCompile(".*"),
	}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	_, err = conn.Execute(preparedStatement, map[string]any{"1": goMap})
	assert.NotNil(t, err)
	expected := "failed to convert value in the map with error: unsupported type"
	assert.Contains(t, err.Error(), expected)
}

func TestEmptyMapParam(t *testing.T) {
	goMap := map[string]any{}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	_, err = conn.Execute(preparedStatement, map[string]any{"1": goMap})
	assert.NotNil(t, err)
	expected := "failed to create STRUCT value because the map is empty"
	assert.Contains(t, err.Error(), expected)
}

func TestMapParam(t *testing.T) {
	goMap := []MapItem{
		{(int64)(1), "One"},
		{(int64)(2), "Two"},
		{(int64)(3), "Three"},
	}
	BasicParamTestHelper(t, goMap)
}

func TestMapParamNested(t *testing.T) {
	goMap := []MapItem{
		{(int64)(1),
			[]MapItem{
				{"a", "A"},
			}},
		{(int64)(2),
			[]MapItem{
				{"b", "B"},
			}},
		{(int64)(3),
			[]MapItem{
				{"c", "C"},
			}},
	}
	BasicParamTestHelper(t, goMap)
}

func TestMapParamWithUnsupportedType(t *testing.T) {
	goMap := []MapItem{
		{(int64)(1), regexp.MustCompile(".*")},
	}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	_, err = conn.Execute(preparedStatement, map[string]any{"1": goMap})
	assert.NotNil(t, err)
	expected := "failed to convert value in the slice with error: unsupported type:"
	assert.Contains(t, err.Error(), expected)
}

func TestMapWithMixedTypesParam(t *testing.T) {
	goMap := []MapItem{
		{(int64)(1), "One"},
		{(int64)(2), "Two"},
		{(int64)(3), "Three"},
		{(int64)(4), 4},
	}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	_, err = conn.Execute(preparedStatement, map[string]any{"1": goMap})
	assert.NotNil(t, err)
	expected := "failed to create MAP value with status: 1"
	assert.Contains(t, err.Error(), expected)
}

func TestSliceParam(t *testing.T) {
	goSlice := []any{"One", "Two", "Three"}
	BasicParamTestHelper(t, goSlice)
}

func TestSliceParamNested(t *testing.T) {
	goSlice := []any{
		[]any{"a", "A"},
		[]any{"b", "B"},
		[]any{"c", "C"},
	}
	BasicParamTestHelper(t, goSlice)
}

func TestSliceParamNestedStruct(t *testing.T) {
	goSlice := []any{
		map[string]any{"name": "Alice", "age": (int64)(30)},
		map[string]any{"name": "Bob", "age": (int64)(40)},
		map[string]any{"name": "Charlie", "age": (int64)(50)},
	}
	BasicParamTestHelper(t, goSlice)
}

func TestSliceParamWithUnsupportedType(t *testing.T) {
	goSlice := []any{"One", regexp.MustCompile(".*")}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	_, err = conn.Execute(preparedStatement, map[string]any{"1": goSlice})
	assert.NotNil(t, err)
	expected := "failed to convert value in the slice with error: unsupported type:"
	assert.Contains(t, err.Error(), expected)
}

func TestSliceWithMixedTypesParam(t *testing.T) {
	goSlice := []any{"One", "Two", "Three", 4}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	_, err = conn.Execute(preparedStatement, map[string]any{"1": goSlice})
	assert.NotNil(t, err)
	expected := "failed to create LIST value with status: 1"
	assert.Contains(t, err.Error(), expected)
}

func TestInt64SliceParam(t *testing.T) {
	goSlice := []int64{1, 2, 3}
	expected := []any{int64(1), int64(2), int64(3)}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, map[string]any{"1": goSlice})
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, expected, value)
	assert.False(t, res.HasNext())
	res.Close()
}

func TestStringSliceParam(t *testing.T) {
	goSlice := []string{"One", "Two", "Three"}
	expected := []any{"One", "Two", "Three"}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, map[string]any{"1": goSlice})
	defer res.Close()
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, expected, value)
	assert.False(t, res.HasNext())
}

func TestNestedInt64SliceParam(t *testing.T) {
	goSlice := [][]uint8{
		{0, 1, 2, 3},
		{4, 5, 6, 7},
	}
	expected := []any{
		[]any{uint8(0), uint8(1), uint8(2), uint8(3)},
		[]any{uint8(4), uint8(5), uint8(6), uint8(7)},
	}
	_, conn := SetupTestDatabase(t)
	preparedStatement, err := conn.Prepare("RETURN $1")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, map[string]any{"1": goSlice})
	defer res.Close()
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, expected, value)
	assert.False(t, res.HasNext())
}
