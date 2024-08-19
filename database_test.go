package kuzu

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDatabase(t *testing.T) {
	tempDir, dirErr := os.MkdirTemp("", "testDb")
	defer os.RemoveAll(tempDir) 

	db, err := OpenDatabase(tempDir, DefaultSystemConfig())

	assert.NoError(t, dirErr, "Expected no error when making directory")
	assert.NoError(t, err, "Expected no error when opening the databse")
	assert.False(t, db.isClosed, "Expected database to be open")
	assert.DirExists(t, tempDir, "Expected temporary directory to be open")
} 

func TestCloseDatabase(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "testDb")
	defer os.RemoveAll((tempDir))

	db, _ := OpenDatabase(tempDir, DefaultSystemConfig())
	db.Close()

	assert.False(t, db.isClosed, "Expected databse to be closed")
}
