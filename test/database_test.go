package kuzu

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kuzudb/go-kuzu"
	"github.com/stretchr/testify/assert"
)

func TestOpenDatabase(t *testing.T) {
	dbDir := os.TempDir()
	dbPath  := filepath.Join(dbDir, "newDatabase")
	
	db, err := kuzu.OpenDatabase(dbPath, kuzu.DefaultSystemConfig())

	assert.NoError(t, err, "Expected no error when opening the databse")
	assert.False(t, db.IsClosed, "Expected database to be open")
} 