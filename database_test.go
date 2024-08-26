package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDatabase(t *testing.T) {
	db, _ := makeDB(t)
	assert.False(t, db.isClosed, "Expected database to be open")
} 

func TestCloseDatabase(t *testing.T) {
	db, _ := makeDB(t)
	db.Close()
	assert.False(t, db.isClosed, "Expected databse to be closed")
}