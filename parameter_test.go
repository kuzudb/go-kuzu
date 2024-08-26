package kuzu

import (
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
