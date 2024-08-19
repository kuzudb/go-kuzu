package kuzu

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenConnection(t *testing.T) {
	tempDir, dirErr := os.MkdirTemp("", "testConnectio ")
	defer os.RemoveAll(tempDir) 

	db, _ := OpenDatabase(tempDir, DefaultSystemConfig())
	conn, err := OpenConnection(db)


	assert.NoError(t, dirErr, "Expected no error when making directory")
	assert.NoError(t, err, "Expected no error when opening the connection")
	assert.False(t, conn.isClosed, "Expected connection to be open")
	assert.DirExists(t, tempDir, "Expected temporary directory to be open")
}