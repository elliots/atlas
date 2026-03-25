// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package wasidriver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func init() {
	sql.Register("wasi", &Driver{})
}

// ExecFunc is the function signature for executing SQL on the host.
// It takes a JSON-encoded Request and returns a JSON-encoded Response.
type ExecFunc func(request []byte) (response []byte, err error)

// hostExec is the package-level function that performs SQL execution.
// It must be set before opening a connection (set by platform-specific init).
var hostExec ExecFunc

// SetExecFunc sets the function used to execute SQL on the host.
// This must be called before sql.Open("wasi", "").
func SetExecFunc(fn ExecFunc) {
	hostExec = fn
}

// Driver implements database/sql/driver.Driver.
type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	if hostExec == nil {
		return nil, errors.New("wasidriver: host exec function not set; call SetExecFunc first")
	}
	if dsn == "" {
		return nil, errors.New("wasidriver: connection name (DSN) is required — use \"dev\", \"from\", or \"to\"")
	}
	return &conn{connection: dsn}, nil
}

// conn implements driver.Conn.
type conn struct {
	connection string // "dev" (default), "from", or "to"
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{query: query, connection: c.connection}, nil
}

func (c *conn) Close() error { return nil }

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{c: c}, nil
}

// Implement driver.QueryerContext and driver.ExecerContext for efficiency
// (avoids the Prepare/Execute/Close cycle for each query).

func (c *conn) QueryContext(_ interface{}, query string, args []driver.NamedValue) (driver.Rows, error) {
	return doQuery(c.connection, query, namedToValues(args))
}

func (c *conn) ExecContext(_ interface{}, query string, args []driver.NamedValue) (driver.Result, error) {
	return doExec(c.connection, query, namedToValues(args))
}

// stmt implements driver.Stmt.
type stmt struct {
	query      string
	connection string
}

func (s *stmt) Close() error { return nil }

func (s *stmt) NumInput() int { return -1 } // unknown

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return doExec(s.connection, s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return doQuery(s.connection, s.query, args)
}

// tx implements driver.Tx.
type tx struct {
	c *conn
}

func (t *tx) Commit() error {
	_, err := doExec(t.c.connection, "COMMIT", nil)
	return err
}

func (t *tx) Rollback() error {
	_, err := doExec(t.c.connection, "ROLLBACK", nil)
	return err
}

// doQuery executes a query via the host and returns rows.
func doQuery(connection, query string, args []driver.Value) (driver.Rows, error) {
	resp, err := callHost(connection, "query", query, args)
	if err != nil {
		return nil, err
	}
	return &rows{
		columns: resp.Columns,
		data:    resp.Rows,
		pos:     0,
	}, nil
}

// doExec executes a statement via the host and returns the result.
func doExec(connection, query string, args []driver.Value) (driver.Result, error) {
	resp, err := callHost(connection, "exec", query, args)
	if err != nil {
		return nil, err
	}
	return &result{
		rowsAffected: resp.RowsAffected,
		lastInsertID: resp.LastInsertID,
	}, nil
}

// callHost marshals a request, calls the host, and unmarshals the response.
func callHost(connection, typ, query string, args []driver.Value) (*Response, error) {
	// Convert driver.Value args to []any for JSON.
	jsonArgs := make([]any, len(args))
	for i, a := range args {
		jsonArgs[i] = a
	}
	req := Request{
		Type:       typ,
		SQL:        query,
		Args:       jsonArgs,
		Connection: connection,
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("wasidriver: marshal request: %w", err)
	}
	respBytes, err := hostExec(reqBytes)
	if err != nil {
		return nil, fmt.Errorf("wasidriver: host exec: %w", err)
	}
	var resp Response
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("wasidriver: unmarshal response: %w", err)
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return &resp, nil
}

// rows implements driver.Rows.
type rows struct {
	columns []string
	data    [][]any
	pos     int
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	r.pos = len(r.data)
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.pos]
	r.pos++
	for i := range dest {
		if i < len(row) {
			v := row[i]
			// JSON numbers are always float64 in Go. Convert whole-number floats
			// to int64 so database/sql Scan can convert them to bool, int, etc.
			if f, ok := v.(float64); ok {
				if f == float64(int64(f)) {
					v = int64(f)
				}
			}
			dest[i] = v
		}
	}
	return nil
}

// result implements driver.Result.
type result struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r *result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// namedToValues converts NamedValue args to plain Values.
func namedToValues(named []driver.NamedValue) []driver.Value {
	vals := make([]driver.Value, len(named))
	for i, n := range named {
		vals[i] = n.Value
	}
	return vals
}
