package kuzu

import (
	"testing"
	"github.com/stretchr/testify/assert"

)

func makeDB(tempDir string, dbConfig SystemConfig)(db Database, err error){
	db, dirErr := OpenDatabase(tempDir, dbConfig)
	return db, dirErr
}

func testDir(t *testing.T, tempDir string, dirErr error) {
	assert.NoError(t, dirErr, "Expected no error when making directory")
	assert.DirExists(t, tempDir, "Expected temporary directory to be open")
}

func testState(t *testing.T, closed bool, err error, itemType string, labelS string){
	assert.NoError(t, err, "Expected no error when opening the %s", itemType)
	assert.False(t, closed, "Expected %s to be %s", itemType, labelS)
}