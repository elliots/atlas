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

	// Optional filenames corresponding to Files entries (for error messages).
	FileNames []string `json:"fileNames,omitempty"`

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
	// Flatten to break circular references (Schema→Table→Schema→Realm→…)
	flat := flattenResult(result)
	if err := json.NewEncoder(os.Stdout).Encode(flat); err != nil {
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
func execAndInspect(ctx context.Context, drv migrate.Driver, files []string, fileNames []string, schemaName string) (*schema.Realm, error) {
	// Extract tags from SQL comments before executing (comments are lost on execution).
	dir, err := filesToDir(files, fileNames)
	if err != nil {
		return nil, err
	}
	// Extract tags per-file so errors include the filename.
	tagIdx := &migrate.TagIndex{
		TableTags:  make(map[string][]*schema.Tag),
		ColumnTags: make(map[string][]*schema.Tag),
	}
	perFileDir := migrate.OpenMemDir("single")
	for i, f := range files {
		perFileDir.Reset()
		name := fmt.Sprintf("%d.sql", i)
		if i < len(fileNames) && fileNames[i] != "" {
			name = fileNames[i]
		}
		if err := perFileDir.WriteFile(name, []byte(f)); err != nil {
			return nil, err
		}
		idx, err := migrate.ExtractTags(drv, perFileDir)
		if err != nil {
			return nil, fmt.Errorf("%s:%s", name, err.Error())
		}
		for k, v := range idx.TableTags {
			tagIdx.TableTags[k] = append(tagIdx.TableTags[k], v...)
		}
		for k, v := range idx.ColumnTags {
			tagIdx.ColumnTags[k] = append(tagIdx.ColumnTags[k], v...)
		}
	}

	// Execute the SQL on the database, tracking which file each statement came from.
	dirFiles, err := dir.Files()
	if err != nil {
		return nil, err
	}
	for _, f := range dirFiles {
		fileStmts, err := migrate.FileStmts(drv, f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name(), err)
		}
		for _, s := range fileStmts {
			if _, err := drv.ExecContext(ctx, s); err != nil {
				return nil, fmt.Errorf("%s: %w", f.Name(), err)
			}
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
	// Ensure clean database before inspecting
	if err := ensureClean(ctx, drv); err != nil {
		return nil, fmt.Errorf("clean: %w", err)
	}
	realm, err := execAndInspect(ctx, drv, cmd.Files, cmd.FileNames, cmd.Schema)
	if err != nil {
		return nil, err
	}
	return &Result{Schema: realm}, nil
}

// cmdApply executes SQL files on the database.
func cmdApply(ctx context.Context, drv migrate.Driver, cmd Cmd) (*Result, error) {
	dir, err := filesToDir(cmd.Files, cmd.FileNames)
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
	// Clean, execute "from" SQL, inspect, clean again, execute "to" SQL, inspect.
	// Each side gets a fresh database — handles multi-schema SQL correctly.
	if err := ensureClean(ctx, drv); err != nil {
		return nil, fmt.Errorf("clean: %w", err)
	}
	fromRealm, err := execAndInspect(ctx, drv, cmd.From, nil, cmd.Schema)
	if err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}

	if err := ensureClean(ctx, drv); err != nil {
		return nil, fmt.Errorf("reset: %w", err)
	}

	toRealm, err := execAndInspect(ctx, drv, cmd.To, nil, cmd.Schema)
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

// ensureClean checks the dev database is empty using the driver's CheckClean,
// then drops and recreates public schema to guarantee a clean slate.
// This mirrors Atlas CLI's behavior — the dev database must be disposable.
func ensureClean(ctx context.Context, drv migrate.Driver) error {
	if _, err := drv.ExecContext(ctx, "DROP SCHEMA IF EXISTS public CASCADE"); err != nil {
		return err
	}
	if _, err := drv.ExecContext(ctx, "CREATE SCHEMA public"); err != nil {
		return err
	}
	return nil
}

// filesToDir converts raw SQL file contents into a migrate.Dir.
// If names are provided, they're used as filenames (for better error messages).
func filesToDir(files []string, names []string) (*migrate.MemDir, error) {
	dir := migrate.OpenMemDir("work")
	dir.Reset()
	for i, f := range files {
		name := fmt.Sprintf("%d.sql", i)
		if i < len(names) && names[i] != "" {
			name = names[i]
		}
		if err := dir.WriteFile(name, []byte(f)); err != nil {
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
