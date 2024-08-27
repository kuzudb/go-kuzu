// Package kuzu provides a Go interface to Kùzu graph database management system.
// The package is a wrapper around the C API of Kùzu.
package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"
import (
	"fmt"
	"runtime"
	"unsafe"
)

// SystemConfig represents the configuration of Kùzu database system.
// BufferPoolSize is the size of the buffer pool in bytes.
// MaxNumThreads is the maximum number of threads that can be used by the database system.
// EnableCompression is a boolean flag to enable or disable compression.
// ReadOnly is a boolean flag to open the database in read-only mode.
// MaxDbSize is the maximum size of the database in bytes.
type SystemConfig struct {
	BufferPoolSize    uint64
	MaxNumThreads     uint64
	EnableCompression bool
	ReadOnly          bool
	MaxDbSize         uint64
}

// DefaultSystemConfig returns the default system configuration.
// The default system configuration is as follows:
// BufferPoolSize: 80% of the total system memory.
// MaxNumThreads: Number of CPU cores.
// EnableCompression: true.
// ReadOnly: false.
// MaxDbSize: 0 (unlimited).
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

// toC converts the SystemConfig Go struct to the C struct.
func (config SystemConfig) toC() C.kuzu_system_config {
	cSystemConfig := C.kuzu_default_system_config()
	cSystemConfig.buffer_pool_size = C.uint64_t(config.BufferPoolSize)
	cSystemConfig.max_num_threads = C.uint64_t(config.MaxNumThreads)
	cSystemConfig.enable_compression = C.bool(config.EnableCompression)
	cSystemConfig.read_only = C.bool(config.ReadOnly)
	cSystemConfig.max_db_size = C.uint64_t(config.MaxDbSize)
	return cSystemConfig
}

// Database represents a Kùzu database instance.
type Database struct {
	cDatabase C.kuzu_database
	isClosed  bool
}

// OpenDatabase opens a Kùzu database at the given path with the given system configuration.
func OpenDatabase(path string, systemConfig SystemConfig) (Database, error) {
	db := Database{}
	runtime.SetFinalizer(&db, func(db *Database) {
		db.Close()
	})
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cSystemConfig := systemConfig.toC()
	status := C.kuzu_database_init(cPath, cSystemConfig, &db.cDatabase)
	if status != C.KuzuSuccess {
		return db, fmt.Errorf("failed to open database with status %d", status)
	}
	return db, nil
}

// OpenInMemoryDatabase opens a Kùzu database in in-memory mode with the given system configuration.
func OpenInMemoryDatabase(systemConfig SystemConfig) (Database, error) {
	return OpenDatabase(":memory:", systemConfig)
}

// Close closes the database. Calling this method is optional.
// The database will be closed automatically when it is garbage collected.
func (db *Database) Close() {
	if db.isClosed {
		return
	}
	C.kuzu_database_destroy(&db.cDatabase)
	db.isClosed = true
}
