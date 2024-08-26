package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var FLOAT_EPSILON = 0.0000001

func TestBool(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.True(t, value.(bool))
	res.Close()
}

func TestInt64(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int64(35), value)
	res.Close()
}

func TestInt8(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.level;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int8(5), value)
	res.Close()
}

func TestUint8(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulevel;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint8(250), value)
	res.Close()
}

func TestUint16(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulength;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint16(33768), value)
	res.Close()
}

func TestUint32(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.temperature;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint32(32800), value)
	res.Close()
}

func TestUint64(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.code;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint64(9223372036854775808), value)
	res.Close()
}

func TestSerial(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int64(2), value)
	res.Close()
}

func TestDouble(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, float64(5.0), value, FLOAT_EPSILON)
	res.Close()
}

func TestString(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, "Alice", value)
	res.Close()
}

func TestBlob(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A')")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, byte(0xAA), value.([]byte)[0])
	assert.Equal(t, byte(0xBB), value.([]byte)[1])
	assert.Equal(t, byte(0xCD), value.([]byte)[2])
	assert.Equal(t, byte(0x1A), value.([]byte)[3])
	res.Close()
}
