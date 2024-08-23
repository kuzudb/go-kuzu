package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBool(t *testing.T) {
	db , conn := makeDB(t)
	result, _ := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, true)
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestInt(t *testing.T) {
	db , conn := makeDB(t)
	result, _ := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(35))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestInt8(t *testing.T) {
	db , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.level;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int8(5))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestUint8(t *testing.T) {
	db , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulevel;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint8(250))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestUint16(t *testing.T){
	db , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulength;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint16(33768))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestUint32(t *testing.T) {
	db , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.temperature;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint32(32800))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestUint64(t *testing.T) {
	db , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.code;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, uint64(9223372036854775808))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

// func TestUint128(t *testing.T) {
// 	_ , conn := makeDB(t)
//     result, _ := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.code;")
// 	assert.True(t, result.HasNext())
// 	next, _ := result.Next()
// 	value, _ := next.GetValue(0)
// 	assert.Equal(t, value, (1844674407370955161811111111))
// }

// func TestSerial(t *testing.T){
// 	_ , conn := makeDB(t)
//     result, _ := conn.Query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;")
// 	assert.True(t, result.HasNext())
// 	next, _ := result.Next()
// 	value, _ := next.GetValue(0)
// 	assert.Equal(t, value, int(2))
// }

func TestDouble(t *testing.T){
	_ , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, float64(5.0))
}

func TestString(t *testing.T){
	_ , conn := makeDB(t)
    result, _ := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
	assert.True(t, result.HasNext())
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, "Alice")
}

// func TestBlob(t *testing.T){
// 	_ , conn := makeDB(t)
//     result, _ := conn.Query("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A')")
// 	assert.True(t, result.HasNext())
// 	next, _ := result.Next()
// 	value, _ := next.GetValue(0)
// 	assert.Equal(t, value, )
// }

