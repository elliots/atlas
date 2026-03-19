// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

// Package wasidriver provides a database/sql driver that delegates SQL
// execution to the WASI host environment. This allows Atlas to run as
// a WASI module while the host provides actual database connectivity.
package wasidriver

// Request is sent from the WASI module to the host to execute SQL.
type Request struct {
	Type string `json:"type"` // "query" or "exec"
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

// Response is returned by the host after executing SQL.
type Response struct {
	// For queries (SELECT-style).
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`

	// For exec (INSERT, UPDATE, DELETE, DDL).
	RowsAffected int64 `json:"rows_affected,omitempty"`
	LastInsertID int64 `json:"last_insert_id,omitempty"`

	// Error from the host, if any.
	Error string `json:"error,omitempty"`
}
