// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/mysql"
	"ariga.io/atlas/sql/postgres"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlite"
	_ "ariga.io/atlas/sql/wasidriver" // register "wasi" driver
)

// Command types that the host can send.
const (
	CmdInspect = "inspect" // Execute SQL, inspect schema, return with tags.
	CmdDiff    = "diff"    // Diff two schemas and return migration SQL.
	CmdApply   = "apply"   // Apply SQL statements to the DB.
)

// Cmd is a command from the host.
type Cmd struct {
	Type    string `json:"type"`
	Dialect string `json:"dialect"` // "postgres", "mysql", "sqlite"

	// For "inspect" and "apply": SQL file contents to execute.
	// These are full file texts (not individual statements) so that
	// comments and @-tags can be extracted before execution.
	Files []string `json:"files,omitempty"`

	// For "inspect": optional schema name filter.
	Schema string `json:"schema,omitempty"`

	// For "diff": two sets of SQL files to compare.
	From []string `json:"from,omitempty"`
	To   []string `json:"to,omitempty"`
}

// Result is returned to the host.
type Result struct {
	// For "inspect": the schema as JSON (with tags attached).
	Schema *schema.Realm `json:"schema,omitempty"`

	// For "diff": the migration SQL statements.
	Statements []string `json:"statements,omitempty"`

	// Error if the command failed.
	Error string `json:"error,omitempty"`
}

func main() {
	var cmd Cmd
	if err := json.NewDecoder(os.Stdin).Decode(&cmd); err != nil {
		fatal("decode command: %v", err)
	}
	result, err := handleCmd(cmd)
	if err != nil {
		result = &Result{Error: err.Error()}
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatal("encode result: %v", err)
	}
}

func handleCmd(cmd Cmd) (*Result, error) {
	drv, err := openDriver(cmd.Dialect)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	switch cmd.Type {
	case CmdInspect:
		return cmdInspect(ctx, drv, cmd)
	case CmdDiff:
		return cmdDiff(ctx, drv, cmd)
	case CmdApply:
		return cmdApply(ctx, drv, cmd)
	default:
		return nil, fmt.Errorf("unknown command type: %q", cmd.Type)
	}
}

// execAndInspect executes SQL files, inspects the schema, and applies tags.
func execAndInspect(ctx context.Context, drv migrate.Driver, files []string, schemaName string) (*schema.Realm, error) {
	// Extract tags from SQL comments before executing (comments are lost on execution).
	dir, err := filesToDir(files)
	if err != nil {
		return nil, err
	}
	tagIdx, err := migrate.ExtractTags(drv, dir)
	if err != nil {
		return nil, fmt.Errorf("extract tags: %w", err)
	}

	// Execute the SQL on the database.
	stmts, err := allStmts(drv, dir)
	if err != nil {
		return nil, err
	}
	for _, s := range stmts {
		if _, err := drv.ExecContext(ctx, s); err != nil {
			return nil, fmt.Errorf("exec: %w", err)
		}
	}

	// Inspect the resulting schema.
	var realm *schema.Realm
	if schemaName != "" {
		s, err := drv.InspectSchema(ctx, schemaName, nil)
		if err != nil {
			return nil, err
		}
		realm = schema.NewRealm(s)
	} else {
		realm, err = drv.InspectRealm(ctx, nil)
		if err != nil {
			return nil, err
		}
	}

	// Attach tags to schema objects.
	tagIdx.ApplyTags(realm)
	return realm, nil
}

// cmdInspect executes SQL files and returns the schema with tags.
func cmdInspect(ctx context.Context, drv migrate.Driver, cmd Cmd) (*Result, error) {
	realm, err := execAndInspect(ctx, drv, cmd.Files, cmd.Schema)
	if err != nil {
		return nil, err
	}
	return &Result{Schema: realm}, nil
}

// cmdApply executes SQL files on the database.
func cmdApply(ctx context.Context, drv migrate.Driver, cmd Cmd) (*Result, error) {
	dir, err := filesToDir(cmd.Files)
	if err != nil {
		return nil, err
	}
	stmts, err := allStmts(drv, dir)
	if err != nil {
		return nil, err
	}
	for _, s := range stmts {
		if _, err := drv.ExecContext(ctx, s); err != nil {
			return nil, fmt.Errorf("exec: %w", err)
		}
	}
	return &Result{}, nil
}

// cmdDiff compares two sets of SQL files and returns migration statements.
func cmdDiff(ctx context.Context, drv migrate.Driver, cmd Cmd) (*Result, error) {
	fromRealm, err := execAndInspect(ctx, drv, cmd.From, cmd.Schema)
	if err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}

	toRealm, err := execAndInspect(ctx, drv, cmd.To, cmd.Schema)
	if err != nil {
		return nil, fmt.Errorf("to: %w", err)
	}

	changes, err := drv.RealmDiff(fromRealm, toRealm)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}
	if len(changes) == 0 {
		return &Result{}, nil
	}

	plan, err := drv.PlanChanges(ctx, "migration", changes)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}

	stmts := make([]string, len(plan.Changes))
	for i, c := range plan.Changes {
		stmts[i] = c.Cmd
	}
	return &Result{Statements: stmts}, nil
}

// filesToDir converts raw SQL file contents into a migrate.Dir.
func filesToDir(files []string) (*migrate.MemDir, error) {
	dir := migrate.OpenMemDir("work")
	dir.Reset()
	for i, f := range files {
		if err := dir.WriteFile(fmt.Sprintf("%d.sql", i), []byte(f)); err != nil {
			return nil, err
		}
	}
	return dir, nil
}

// allStmts extracts all SQL statement texts from a directory.
func allStmts(drv migrate.Driver, dir migrate.Dir) ([]string, error) {
	files, err := dir.Files()
	if err != nil {
		return nil, err
	}
	var stmts []string
	for _, f := range files {
		ss, err := migrate.FileStmts(drv, f)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, ss...)
	}
	return stmts, nil
}

// openDriver creates an Atlas driver using the WASI database/sql connection.
func openDriver(dialect string) (migrate.Driver, error) {
	db, err := sql.Open("wasi", "")
	if err != nil {
		return nil, fmt.Errorf("open wasi db: %w", err)
	}
	switch dialect {
	case "postgres":
		return postgres.Open(db)
	case "mysql":
		return mysql.Open(db)
	case "sqlite":
		return sqlite.Open(db)
	default:
		return nil, fmt.Errorf("unsupported dialect: %q", dialect)
	}
}

func fatal(format string, args ...any) {
	result := Result{Error: fmt.Sprintf(format, args...)}
	json.NewEncoder(os.Stdout).Encode(result)
	os.Exit(1)
}
