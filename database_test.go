package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDatabaseWithDefaultConfig(t *testing.T) {
	db, err := OpenDatabase(t.TempDir(), DefaultSystemConfig())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, db.cDatabase)
	assert.False(t, db.isClosed)
	db.Close()
}

func TestOpenDatabaseWithCustomConfig(t *testing.T) {
	systemConfig := DefaultSystemConfig()
	systemConfig.BufferPoolSize = 256 * 1024 * 1024 // 256 MB
	systemConfig.MaxNumThreads = 4
	db, err := OpenDatabase(t.TempDir(), systemConfig)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, db.cDatabase)
	assert.False(t, db.isClosed)
	db.Close()
}

func TestOpenDatabaseInMemory(t *testing.T) {
	db, err := OpenInMemoryDatabase(DefaultSystemConfig())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, db.cDatabase)
	conn, err := OpenConnection(db)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
	assert.NotNil(t, conn.cConnection)
	_, err = conn.Query("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
	assert.Nil(t, err)
	_, err = conn.Query("CREATE (:person {name: 'Alice', age: 30});")
	assert.Nil(t, err)
	_, err = conn.Query("CREATE (:person {name: 'Bob', age: 40});")
	assert.Nil(t, err)
	res, err := conn.Query("MATCH (a:person) RETURN a.name, a.age;")
	assert.Nil(t, err)
	assert.True(t, res.HasNext())
	tuple, err := res.Next()
	assert.Nil(t, err)
	values, err := tuple.GetAsSlice()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, "Alice", values[0].(string))
	assert.Equal(t, int64(30), values[1].(int64))
	assert.True(t, res.HasNext())
	tuple, err = res.Next()
	assert.Nil(t, err)
	values, err = tuple.GetAsSlice()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, "Bob", values[0].(string))
	assert.Equal(t, int64(40), values[1].(int64))
	assert.False(t, res.HasNext())
	res.Close()
	conn.Close()
	db.Close()
}

func TestCloseDatabase(t *testing.T) {
	systemConfig := DefaultSystemConfig()
	systemConfig.BufferPoolSize = 256 * 1024
	db, err := OpenDatabase(t.TempDir(), systemConfig)
	assert.Nil(t, err)
	db.Close()
	assert.True(t, db.isClosed)
	// Closing a database twice should not panic
	db.Close()
	assert.True(t, db.isClosed)
}
