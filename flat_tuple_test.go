package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTupleClose(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) RETURN a.fName;")
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	tuple.Close()
	assert.True(t, tuple.isClosed)
	// Double close should not panic
	tuple.Close()
	assert.True(t, tuple.isClosed)
}

func TestTupleGetAsString(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	query := "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName LIMIT 1;"
	res, err := conn.Query(query)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	assert.Contains(t, tuple.GetAsString(), "Alice|35")
	tuple.Close()
}

func TestTupleGetAsSlice(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	query := "MATCH (a:person) RETURN a.fName, a.gender, a.age ORDER BY a.fName LIMIT 1;"
	res, err := conn.Query(query)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	values, err := tuple.GetAsSlice()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(values))
	assert.Contains(t, values[0], "Alice")
	assert.Equal(t, int64(1), values[1])
	assert.Equal(t, int64(35), values[2])
	tuple.Close()
}

func TestTupleGetAsMap(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	query := "MATCH (a:person) RETURN a.fName, a.gender, a.age ORDER BY a.fName LIMIT 1;"
	res, err := conn.Query(query)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	m, err := tuple.GetAsMap()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(m))
	assert.Contains(t, m, "a.fName")
	assert.Contains(t, m, "a.gender")
	assert.Contains(t, m, "a.age")
	assert.Equal(t, "Alice", m["a.fName"])
	assert.Equal(t, int64(1), m["a.gender"])
	assert.Equal(t, int64(35), m["a.age"])
	tuple.Close()
}

func TestGetValue(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	query := "MATCH (a:person) RETURN a.fName, a.gender, a.age ORDER BY a.fName LIMIT 1;"
	res, err := conn.Query(query)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	value, err := tuple.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, "Alice", value)
	value, err = tuple.GetValue(1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), value)
	value, err = tuple.GetValue(2)
	assert.Nil(t, err)
	assert.Equal(t, int64(35), value)
	tuple.Close()
}
