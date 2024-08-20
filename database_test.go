package kuzu

import (
	"os"
	"testing"
)

func TestOpenDatabase(t *testing.T) {
	tempDir, dirErr := os.MkdirTemp("", "testDb")
	defer os.RemoveAll(tempDir) 

	db, err := makeDB(tempDir, DefaultSystemConfig())


	testDir(&testing.T{}, tempDir, dirErr)
	testState(&testing.T{}, db.isClosed, err, "databse", "open")
} 

func TestCloseDatabase(t *testing.T) {
	tempDir, dirErr := os.MkdirTemp("", "testDb")
	defer os.RemoveAll((tempDir))

	db, err := OpenDatabase(tempDir, DefaultSystemConfig())
	db.Close()

	testDir(&testing.T{}, tempDir, dirErr)
	testState(&testing.T{}, db.isClosed, err, "database", "closed")
}
