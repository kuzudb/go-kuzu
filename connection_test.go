package kuzu

import (
	"os"
	"testing"

)

func TestOpenConnection(t *testing.T) {	
	tempDir, dirErr := os.MkdirTemp("", "testConnection")
	defer os.RemoveAll(tempDir)
	db, _ := makeDB(tempDir, DefaultSystemConfig())
	conn, err := OpenConnection(db)

	testDir(&testing.T{}, tempDir, dirErr)
	testState(&testing.T{}, conn.isClosed, err, "connection", "open")
}

func TestCloseConnection(t *testing.T) {
	tempDir, dirErr := os.MkdirTemp("", "testConnection")
	defer os.RemoveAll(tempDir)

	db, _ := makeDB(tempDir, DefaultSystemConfig())
	conn, err := OpenConnection(db)
	conn.Close()

	testDir(&testing.T{}, tempDir, dirErr)
	testState(&testing.T{}, conn.isClosed, err, "connection", "closed")
}