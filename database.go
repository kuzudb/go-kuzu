package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"
import (
	"fmt"
	"runtime"
	"unsafe"
)

type SystemConfig struct {
	BufferPoolSize    uint64
	MaxNumThreads     uint64
	EnableCompression bool
	ReadOnly          bool
	MaxDbSize         uint64
}

func DefaultSystemConfig() SystemConfig {
	cSystemConfig := C.kuzu_default_system_config()
	return SystemConfig{
		BufferPoolSize:    uint64(cSystemConfig.buffer_pool_size),
		MaxNumThreads:     uint64(cSystemConfig.max_num_threads),
		EnableCompression: bool(cSystemConfig.enable_compression),
		ReadOnly:          bool(cSystemConfig.read_only),
		MaxDbSize:         uint64(cSystemConfig.max_db_size),
	}
}

func (config SystemConfig) toC() C.kuzu_system_config {
	cSystemConfig := C.kuzu_default_system_config()
	cSystemConfig.buffer_pool_size = C.uint64_t(config.BufferPoolSize)
	cSystemConfig.max_num_threads = C.uint64_t(config.MaxNumThreads)
	cSystemConfig.enable_compression = C.bool(config.EnableCompression)
	cSystemConfig.read_only = C.bool(config.ReadOnly)
	cSystemConfig.max_db_size = C.uint64_t(config.MaxDbSize)
	return cSystemConfig
}

type Database struct {
	CDatabase C.kuzu_database
	isClosed  bool
}

func OpenDatabase(path string, systemConfig SystemConfig) (Database, error) {
	db := Database{}
	runtime.SetFinalizer(&db, func(db *Database) {
		db.Close()
	})
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cSystemConfig := systemConfig.toC()
	status := C.kuzu_database_init(cPath, cSystemConfig, &db.CDatabase)
	if status != C.KuzuSuccess {
		return db, fmt.Errorf("failed to open database with status %d", status)
	}
	return db, nil
}

func (db Database) Close() {
	if db.isClosed {
		return
	}
	C.kuzu_database_destroy(&db.CDatabase)
	db.isClosed = true
}
