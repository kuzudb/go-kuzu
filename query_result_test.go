package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryResultToString(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	queryResultString := res.ToString()
	assert.Equal(t, "a.fName|a.age|a.isStudent|a.isWorker\nAlice|35|True|False\n", queryResultString)
	res.Close()
}

func TestQueryResultClose(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	res.Close()
	assert.True(t, res.isClosed)
	// Double close should not panic
	res.Close()
	assert.True(t, res.isClosed)
}

func TestQueryResultResetIterator(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.ID;")
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	value, err := tuple.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), value)
	res.ResetIterator()
	assert.True(t, res.HasNext())
	tuple, err = res.Next()
	assert.Nil(t, err)
	value, err = tuple.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), value)
	res.Close()
}

func TestQueryResultGetColumnNames(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	columnNames := res.GetColumnNames()
	assert.Equal(t, 4, len(columnNames))
	assert.Equal(t, "a.fName", columnNames[0])
	assert.Equal(t, "a.age", columnNames[1])
	assert.Equal(t, "a.isStudent", columnNames[2])
	assert.Equal(t, "a.isWorker", columnNames[3])
	res.Close()
}

func TestQueryResultGetNumberOfColumns(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	numColumns := res.GetNumberOfColumns()
	assert.Equal(t, uint64(4), numColumns)
	res.Close()
}

func TestQueryResultGetNumberOfRows(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) RETURN a;")
	assert.Nil(t, err)
	numRows := res.GetNumberOfRows()
	assert.Equal(t, uint64(8), numRows)
	res.Close()
}

func TestQueryResultHasNext(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) RETURN a LIMIT 1;")
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	_, err = res.Next()
	assert.Nil(t, err)
	assert.False(t, res.HasNext())
	res.Close()
}

func TestQueryResultNext(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	values, err := tuple.GetAsSlice()
	assert.Nil(t, err)
	assert.Equal(t, 4, len(values))
	assert.Equal(t, "Alice", values[0].(string))
	assert.Equal(t, int64(35), values[1].(int64))
	assert.Equal(t, true, values[2].(bool))
	assert.Equal(t, false, values[3].(bool))
	res.Close()
}

func TestMultipleQueryResults(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("RETURN 1; RETURN 2; RETURN 3;")
	assert.Nil(t, err)
	defer res.Close()
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	value, err := tuple.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), value)
	assert.False(t, res.HasNext())
	assert.True(t, res.HasNextQueryResult())
	res, err = res.NextQueryResult()
	assert.Nil(t, err)
	defer res.Close()
	assert.True(t, res.HasNext())
	tuple, err = res.Next()
	assert.Nil(t, err)
	value, err = tuple.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), value)
	assert.False(t, res.HasNext())
	assert.True(t, res.HasNextQueryResult())
	res, err = res.NextQueryResult()
	assert.Nil(t, err)
	defer res.Close()
	assert.True(t, res.HasNext())
	tuple, err = res.Next()
	assert.Nil(t, err)
	value, err = tuple.GetValue(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), value)
	assert.False(t, res.HasNext())
	assert.False(t, res.HasNextQueryResult())
}

func TestQueryResultGetCompilingTime(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	assert.Greater(t, res.GetCompilingTime(), float64(0))
	res.Close()
}

func TestQueryResultGetExecutionTime(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, err := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName, a.age, a.isStudent, a.isWorker;")
	assert.Nil(t, err)
	assert.Greater(t, res.GetExecutionTime(), float64(0))
	res.Close()
}
