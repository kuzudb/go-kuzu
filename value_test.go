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
	assert.Equal(t, value, int64(35))
	res.Close()
}

func TestInt8(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.level;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int8(5))
	res.Close()
}

func TestUint8(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulevel;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint8(250))
	res.Close()
}

func TestUint16(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulength;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint16(33768))
	res.Close()
}

func TestUint32(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.temperature;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint32(32800))
	res.Close()
}

func TestUint64(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.code;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint64(9223372036854775808))
	res.Close()
}

func TestSerial(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(2))
	res.Close()
}

func TestDouble(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, value, float64(5.0), FLOAT_EPSILON)
	res.Close()
}

func TestString(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, "Alice")
	res.Close()
}

func TestBlob(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A')")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value.([]byte)[0], byte(0xAA))
	assert.Equal(t, value.([]byte)[1], byte(0xBB))
	assert.Equal(t, value.([]byte)[2], byte(0xCD))
	assert.Equal(t, value.([]byte)[3], byte(0x1A))
	res.Close()
}
