package kuzu

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"
)

func init() {
	var _ driver.Result = new(resultSet)
	var _ driver.Rows = new(rowSet)
	var _ SQLConnection = new(connection)
	var _ SQLStatement = new(statement)
	var _ SQLConnector = new(connector)
	var _ driver.DriverContext = new(sqlDriver)
	sql.Register(Name, &sqlDriver{cc: map[string]driver.Connector{}})
}

const Name = "kuzu"

type Finalizer interface {
	Close()
}

type SQLStatement interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

type SQLConnection interface {
	driver.Conn
	driver.Pinger
	driver.ConnPrepareContext
	driver.QueryerContext
	driver.ExecerContext
}

type SQLConnector interface {
	driver.Connector
	io.Closer
}

type sqlDriver struct {
	sync.RWMutex
	cc map[string]driver.Connector
}

// OpenConnector kuzu://path?poolSize=1024&threads=1024&dbSize=1024&compression=1&readOnly=1
func (that *sqlDriver) OpenConnector(dsn string) (driver.Connector, error) {
	u, err := url.Parse(dsn)
	if nil != err {
		return nil, err
	}
	q := u.Query()
	systemConfig := DefaultSystemConfig()
	if err = parse(q.Get("poolSize"), func(v uint64) {
		systemConfig.BufferPoolSize = v
	}); nil != err {
		return nil, err
	}
	if err = parse(q.Get("threads"), func(v uint64) {
		systemConfig.MaxNumThreads = v
	}); nil != err {
		return nil, err
	}
	if err = parse(q.Get("dbSize"), func(v uint64) {
		systemConfig.MaxDbSize = v
	}); nil != err {
		return nil, err
	}
	if err = parse(q.Get("compression"), func(v uint64) {
		systemConfig.EnableCompression = v == uint64(1)
	}); nil != err {
		return nil, err
	}
	if err = parse(q.Get("readOnly"), func(v uint64) {
		systemConfig.ReadOnly = v == uint64(1)
	}); nil != err {
		return nil, err
	}
	db, err := OpenDatabase(u.Path, systemConfig)
	if nil != err {
		release(db)
		return nil, err
	}
	return &connector{
		d:   that,
		dsn: dsn,
		db:  db,
	}, nil
}

func (that *sqlDriver) Open(dsn string) (driver.Conn, error) {
	if cc := func() driver.Connector {
		that.RLock()
		defer that.RUnlock()

		return that.cc[dsn]
	}(); nil != cc {
		return cc.Connect(nextContext())
	}
	that.Lock()
	defer that.Unlock()

	cc, err := that.OpenConnector(dsn)
	if nil != err {
		return nil, err
	}
	that.cc[dsn] = cc
	return cc.Connect(nextContext())
}

type connector struct {
	dsn string
	d   driver.Driver
	db  *Database
}

func (that *connector) Close() error {
	that.db.Close()
	return nil
}

func (that *connector) Driver() driver.Driver {
	return that.d
}

func (that *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := OpenConnection(that.db)
	if nil != err {
		release(conn)
		return nil, err
	}
	return &connection{
		conn: conn,
	}, nil
}

type connection struct {
	conn *Connection
}

func (that *connection) Ping(ctx context.Context) error {
	return nil
}

func (that *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := that.prepareContext(ctx, query)
	if nil != err {
		return nil, err
	}
	defer closeQuiet(stmt)
	return stmt.QueryContext(ctx, args)
}

func (that *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := that.prepareContext(ctx, query)
	if nil != err {
		return nil, err
	}
	defer closeQuiet(stmt)
	return stmt.ExecContext(ctx, args)
}

func (that *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return that.prepareContext(ctx, query)
}

func (that *connection) Prepare(query string) (driver.Stmt, error) {
	return that.prepareContext(nextContext(), query)
}

func (that *connection) prepareContext(ctx context.Context, query string) (SQLStatement, error) {
	stmt, err := that.conn.Prepare(query)
	if nil != err {
		release(stmt)
		return nil, err
	}
	return &statement{
		stmt:  stmt,
		conn:  that.conn,
		query: query,
		num:   -1,
	}, nil
}

func (that *connection) Close() error {
	that.conn.Close()
	return nil
}

func (that *connection) Begin() (driver.Tx, error) {
	return &transaction{
		conn: that,
	}, nil
}

type statement struct {
	stmt  *PreparedStatement
	conn  *Connection
	query string
	num   int // -1
}

func (that *statement) Close() error {
	that.stmt.Close()
	return nil
}

func (that *statement) NumInput() int {
	return that.num
}

func (that *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	raw := make(map[string]any, len(args))
	for _, arg := range args {
		raw[arg.Name] = arg.Value
	}
	rs, err := that.conn.Execute(that.stmt, raw)
	if nil != err {
		release(rs)
		return nil, err
	}
	defer rs.Close()

	return &resultSet{
		lastInsertId: 0,
		rowsAffected: int64(rs.GetNumberOfRows()),
	}, nil
}

func (that *statement) Exec(args []driver.Value) (driver.Result, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		na, ok := v.(sql.NamedArg)
		if !ok {
			return nil, fmt.Errorf("only support named arguments")
		}
		list[i] = driver.NamedValue{
			Name:    na.Name,
			Ordinal: i + 1,
			Value:   na.Value,
		}
	}
	return that.ExecContext(nextContext(), list)
}

func (that *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	raw := make(map[string]any, len(args))
	for _, arg := range args {
		raw[arg.Name] = arg.Value
	}
	rs, err := that.conn.Execute(that.stmt, raw)
	if nil != err {
		release(rs)
		return nil, err
	}
	return &rowSet{rs: rs}, nil
}

func (that *statement) Query(args []driver.Value) (driver.Rows, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		na, ok := v.(sql.NamedArg)
		if !ok {
			return nil, fmt.Errorf("only support named arguments")
		}
		list[i] = driver.NamedValue{
			Name:    na.Name,
			Ordinal: i + 1,
			Value:   na.Value,
		}
	}
	return that.QueryContext(nextContext(), list)
}

// transaction is not support by now.
type transaction struct {
	conn SQLConnection
}

func (that *transaction) Commit() error {
	return nil
}

func (that *transaction) Rollback() error {
	return nil
}

type rowSet struct {
	rs *QueryResult
}

func (that *rowSet) Columns() []string {
	return that.rs.GetColumnNames()
}

func (that *rowSet) Close() error {
	that.rs.Close()
	return nil
}

func (that *rowSet) Next(dest []driver.Value) error {
	if !that.rs.HasNext() {
		return io.EOF
	}
	row, err := that.rs.Next()
	if nil != err {
		release(row)
		return err
	}
	defer row.Close()

	values, err := row.GetAsSlice()
	if nil != err {
		return err
	}
	for idx := range dest {
		if len(values) <= idx {
			break
		}
		dest[idx] = values[idx]
	}
	return nil
}

type resultSet struct {
	lastInsertId int64
	rowsAffected int64
}

func (that *resultSet) LastInsertId() (int64, error) {
	return that.lastInsertId, nil
}

func (that *resultSet) RowsAffected() (int64, error) {
	return that.rowsAffected, nil
}

// Release C resource
func release(f Finalizer) {
	if nil != f {
		f.Close()
	}
}

func nextContext() context.Context {
	return context.Background()
}

func closeQuiet(closer io.Closer) {
	_ = closer.Close()
}

func parse(v string, fn func(v uint64)) error {
	if "" == v {
		return nil
	}
	iv, err := strconv.ParseUint(v, 10, 64)
	if nil != err {
		return err
	}
	fn(iv)
	return nil
}
