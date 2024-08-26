package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoolParam(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": false,
		"k": false,
	}
	preparedStatement, err := conn.Prepare("MATCH (a:person) WHERE a.isStudent = $1 AND a.isWorker = $k RETURN COUNT(*)")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(1))
}

func TestInt64Param(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": int64(0),
	}
	preparedStatement, err := conn.Prepare("MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(1))
}

func TestInt32Param(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": int32(200),
	}
	preparedStatement, err := conn.Prepare("MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(2))
}

func TestInt16Param(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	var params = map[string]any{
		"1": int16(10),
	}
	preparedStatement, err := conn.Prepare("MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)")
	assert.Nil(t, err)
	res, err := conn.Execute(preparedStatement, params)
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(2))
}
