package kuzu

import(
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenConnection(t *testing.T) {
	_, conn := makeDB(t)
	
	assert.False(t, conn.isClosed, "Expected connection to be open")
}

func TestCloseConnection(t *testing.T) {
	_, conn := makeDB(t)

	conn.Close()

	assert.False(t, conn.isClosed, "Expected connection to be closed")
}