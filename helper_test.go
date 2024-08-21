package kuzu

import (
	"testing"
    "os"
    "bufio"
    "path/filepath"
    "strings"

	"github.com/stretchr/testify/assert"

)

func init_tinysnb(conn Connection) {
	tinySnbPath, err := filepath.Abs(filepath.Join("kuzu-src", "dataset", "tinysnb"))
	if err != nil {
		panic(err)
	}

	schemaPath := filepath.Join(tinySnbPath, "schema.cypher")
	executeCypherFromFile(schemaPath, conn, "")

	copyPath := filepath.Join(tinySnbPath, "copy.cypher")
	executeCypherFromFile(copyPath, conn, filepath.Join("kuzu-src", "dataset", "tinysnb"))
}

func executeCypherFromFile(filePath string, conn Connection, replacePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if replacePath != "" {
			line = strings.ReplaceAll(line, "dataset/tinysnb", replacePath)
		}
		if line != "" {
			if _, err := conn.Query(line); err != nil {
                panic(err)
            }
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}

func makeDB(t testing.TB) (returnDB Database, returnConn Connection){
    //TempDir function
    tempDir := t.TempDir()
	assert.DirExists(t, tempDir, "Expected temporary directory to be open")
    db, dirErr := OpenDatabase(tempDir, DefaultSystemConfig())
    if dirErr != nil {
        panic(dirErr)
    }
    conn, err := OpenConnection(db)
    if err != nil {
        panic(err)
    }
    init_tinysnb(conn)
    return db, conn
}
