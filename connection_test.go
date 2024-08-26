package kuzu

import (
	"testing"

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
	assert.Equal(t, defaultNumThreads, conn.GetMaxNumThreads())
	conn.Close()
}

func TestSetMaxNumThreads(t *testing.T) {
	db, _ := SetupTestDatabase(t)
	conn, _ := OpenConnection(db)
	conn.SetMaxNumThreads(3)
	assert.Equal(t, uint64(3), conn.GetMaxNumThreads())
	conn.Close()
}
