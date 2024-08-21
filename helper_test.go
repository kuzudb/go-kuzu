package kuzu

import (
	"testing"
    "os"
    "bufio"
    "path/filepath"
    "strings"

	"github.com/stretchr/testify/assert"

)

func init_tinysnb(conn Connection) error{
	tinySnbPath, err := filepath.Abs(filepath.Join("kuzu-src", "dataset", "tinysnb"))
	if err != nil {
		return err
	}

	// Execute schema.cypher
	schemaPath := filepath.Join(tinySnbPath, "schema.cypher")
	if err := executeSQLFromFile(schemaPath, conn, ""); err != nil {
		return err
	}

	// Execute copy.cypher with replacements
	copyPath := filepath.Join(tinySnbPath, "copy.cypher")
    
	if err := executeSQLFromFile(copyPath, conn, filepath.Join("kuzu-src", "dataset", "tinysnb")); err != nil {
		return err
	}

	return nil
}

func executeSQLFromFile(filePath string, conn Connection, replacePath string) error {
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
            statement, _ := conn.Prepare(line)
			if _, err := conn.Execute(statement, map[string]interface{}{"line": line}); err != nil {
                panic(err)
            }
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func makeDB(t testing.TB) (returnDB Database, returnConn Connection){
    //TempDir function
    tempDir, dirErr := os.MkdirTemp("", "test")
    assert.NoError(t, dirErr, "Expected no error when making directory")
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
