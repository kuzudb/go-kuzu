package kuzu

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpenConnection(t *testing.T) {
	db, _ := SetupTestDatabase(t)
	conn, err := OpenConnection(db)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
	assert.NotNil(t, conn.cConnection)
	conn.Close()
}

func TestCloseConnection(t *testing.T) {
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	conn.Close()
	assert.True(t, conn.isClosed)
	// Double close should not panic
	conn.Close()
	assert.True(t, conn.isClosed)
}

func TestGetMaxNumThreads(t *testing.T) {
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	assert.Equal(t, conn.GetMaxNumThreads(), defaultNumThreads)
	conn.Close()
}

func TestSetMaxNumThreads(t *testing.T) {
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	conn.SetMaxNumThreads(3)
	assert.Equal(t, conn.GetMaxNumThreads(), uint64(3))
	conn.Close()
}

func TestInterrupt(t *testing.T) {
	query := "MATCH (a:person)-[k:knows*1..28]->(b:person) RETURN COUNT(*);"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err = conn.Query(query)
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	conn.Interrupt()
	wg.Wait()
	assert.NotNil(t, err)
	assert.Equal(t, "Interrupted.", err.Error())
	conn.Close()
}

func TestSetTimeout(t *testing.T) {
	query := "MATCH (a:person)-[k:knows*1..28]->(b:person) RETURN COUNT(*);"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	conn.SetTimeout(100)
	_, err := conn.Query(query)
	assert.NotNil(t, err)
	assert.Equal(t, "Interrupted.", err.Error())
	conn.Close()
}

func TestQuery(t *testing.T) {
	query := "RETURN CAST(1, \"INT64\");"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	result, err := conn.Query(query)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.cQueryResult)
	assert.True(t, result.HasNext())
	flatTuple, err := result.Next()
	assert.Nil(t, err)
	assert.NotNil(t, flatTuple)
	slice, err := flatTuple.GetAsSlice()
	assert.Nil(t, err)
	assert.Equal(t, slice[0], int64(1))
	result.Close()
	conn.Close()
}

func TestQueryError(t *testing.T) {
	query := "RETURN a;"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	_, err := conn.Query(query)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Variable a is not in scope.")
	conn.Close()
}

func TestPrepare(t *testing.T) {
	query := "RETURN $a;"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	stmt, err := conn.Prepare(query)
	assert.Nil(t, err)
	assert.NotNil(t, stmt)
	assert.NotNil(t, stmt.cPreparedStatement)
	stmt.Close()
	assert.True(t, stmt.isClosed)
	// Double close should not panic
	stmt.Close()
	assert.True(t, stmt.isClosed)
	conn.Close()
}

func TestPrepareError(t *testing.T) {
	query := "MATCH RETURN $a;"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	stmt, err := conn.Prepare(query)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Parser exception")
	stmt.Close()
	conn.Close()
}

func TestExecute(t *testing.T) {
	query := "RETURN $a;"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	stmt, _ := conn.Prepare(query)
	args := map[string]any{"a": int64(1)}
	result, err := conn.Execute(stmt, args)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.cQueryResult)
	assert.True(t, result.HasNext())
	flatTuple, err := result.Next()
	assert.Nil(t, err)
	assert.NotNil(t, flatTuple)
	slice, err := flatTuple.GetAsSlice()
	assert.Nil(t, err)
	assert.Equal(t, slice[0], int64(1))
	result.Close()
	stmt.Close()
	conn.Close()
}

func TestExecuteError(t *testing.T) {
	query := "RETURN $a;"
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	stmt, _ := conn.Prepare(query)
	args := map[string]any{"b": int64(1)}
	result, err := conn.Execute(stmt, args)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Parameter b not found")
	result.Close()
	stmt.Close()
	conn.Close()
}
