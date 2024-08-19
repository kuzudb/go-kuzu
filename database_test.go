package kuzu

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDatabase(t *testing.T) {
	dbDir := os.TempDir()
	tempDir, _ := os.MkdirTemp("", "testDb")
	dbPath  := filepath.Join(dbDir, tempDir)
	defer os.RemoveAll(tempDir) 

	db, err := OpenDatabase(dbPath, DefaultSystemConfig())

	assert.NoError(t, err, "Expected no error when opening the databse")
	assert.False(t, db.isClosed, "Expected database to be open")
} 
