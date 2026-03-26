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
	"strings"

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

// Rename represents a known rename (from @docs.previously tags).
type Rename struct {
	Type    string `json:"type"`    // "column" or "table"
	Table   string `json:"table"`   // table name (for column renames)
	OldName string `json:"oldName"` // old column/table name
	NewName string `json:"newName"` // new column/table name
}

// RenameCandidate is a potential rename detected during diff (drop+add with same type).
type RenameCandidate struct {
	Type    string `json:"type"`              // "column" or "table"
	Table   string `json:"table"`             // table name
	OldName string `json:"oldName"`           // dropped name
	NewName string `json:"newName"`           // added name
	ColType string `json:"colType,omitempty"` // column type (for display)
}

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

	// For "diff": use a live database connection instead of SQL files.
	// Values: "from" or "to" — the host routes SQL to the right adapter.
	FromConnection string `json:"fromConnection,omitempty"`
	ToConnection   string `json:"toConnection,omitempty"`

	// For "diff": known renames from @docs.previously tags.
	Renames []Rename `json:"renames,omitempty"`
}

// Change describes a single structured schema change for CLI rendering.
type Change struct {
	Type   string `json:"type"`             // add_table, drop_table, rename_table, add_column, drop_column, rename_column, modify_column, add_index, drop_index, add_view, drop_view, add_function, drop_function
	Table  string `json:"table"`            // parent table/view name
	Name   string `json:"name,omitempty"`   // column/index/constraint name (empty for table-level changes)
	Detail string `json:"detail,omitempty"` // type info for adds, "old -> new" for renames, modification description
}

// Result is returned to the host.
type Result struct {
	// For "inspect": the schema as JSON (with tags attached).
	Schema *schema.Realm `json:"schema,omitempty"`

	// For "diff": the migration SQL statements.
	Statements []string `json:"statements,omitempty"`

	// For "diff": structured change descriptions for pretty rendering.
	Changes []Change `json:"changes,omitempty"`

	// For "diff": potential renames detected (drop+add pairs with same type).
	RenameCandidates []RenameCandidate `json:"renameCandidates,omitempty"`

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

	// Execute SQL statements one by one using FileStmts for correct splitting
	// (handles dollar-quoted function bodies, multi-line strings, etc.)
	dir, err := filesToDir(files, fileNames)
	if err != nil {
		return nil, err
	}
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
		var err error
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
	// Snapshot verifies the database is empty (returns NotCleanError if not)
	// and provides a restore function to clean up after.
	sn, ok := drv.(migrate.Snapshoter)
	if !ok {
		return nil, fmt.Errorf("driver does not implement Snapshoter")
	}
	restore, err := sn.Snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	defer restore(ctx) //nolint
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
	var fromRealm, toRealm *schema.Realm
	var err error

	// Inspect "from" side: live connection or SQL files
	if cmd.FromConnection != "" {
		fromRealm, err = inspectConnection(ctx, cmd.Dialect, cmd.FromConnection, cmd.Schema)
		if err != nil {
			return nil, fmt.Errorf("from connection: %w", err)
		}
	}

	// Inspect "to" side: live connection or SQL files
	if cmd.ToConnection != "" {
		toRealm, err = inspectConnection(ctx, cmd.Dialect, cmd.ToConnection, cmd.Schema)
		if err != nil {
			return nil, fmt.Errorf("to connection: %w", err)
		}
	}

	// For sides that use SQL files, load them into the dev DB via snapshot
	if fromRealm == nil || toRealm == nil {
		sn, ok := drv.(migrate.Snapshoter)
		if !ok {
			return nil, fmt.Errorf("driver does not implement Snapshoter")
		}
		restore, err := sn.Snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}
		defer restore(ctx) //nolint

		if fromRealm == nil {
			fromRealm, err = execAndInspect(ctx, drv, cmd.From, nil, cmd.Schema)
			if err != nil {
				return nil, fmt.Errorf("from: %w", err)
			}
		}

		if toRealm == nil {
			if err := restore(ctx); err != nil {
				return nil, fmt.Errorf("restore: %w", err)
			}
			toRealm, err = execAndInspect(ctx, drv, cmd.To, nil, cmd.Schema)
			if err != nil {
				return nil, fmt.Errorf("to: %w", err)
			}
		}
	}

	// Apply known renames: modify the FROM realm so Atlas sees the objects
	// with their new names, then emit RENAME statements separately.
	var renameStmts []string
	if len(cmd.Renames) > 0 {
		renameStmts = applyKnownRenames(fromRealm, cmd.Renames, cmd.Dialect)
	}

	// Detect rename candidates before diffing (drop+add pairs with same type).
	candidates := detectRenameCandidates(fromRealm, toRealm, cmd.Renames)

	changes, err := drv.RealmDiff(fromRealm, toRealm)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}

	// Build structured changes from the schema diff + known renames.
	structured := extractRenameChanges(cmd.Renames)
	structured = append(structured, extractChanges(changes)...)

	if len(changes) == 0 && len(renameStmts) == 0 {
		return &Result{RenameCandidates: candidates, Changes: structured}, nil
	}

	var diffStmts []string
	if len(changes) > 0 {
		plan, err := drv.PlanChanges(ctx, "migration", changes)
		if err != nil {
			return nil, fmt.Errorf("plan: %w", err)
		}
		for _, c := range plan.Changes {
			diffStmts = append(diffStmts, c.Cmd)
		}
	}

	// Prepend RENAME statements before the diff output.
	stmts := append(renameStmts, diffStmts...)
	return &Result{Statements: stmts, Changes: structured, RenameCandidates: candidates}, nil
}

// extractChanges converts Atlas schema.Change objects into flat Change structs.
func extractChanges(changes []schema.Change) []Change {
	var result []Change
	for _, c := range changes {
		switch v := c.(type) {
		case *schema.AddTable:
			// Include column names in detail
			var cols []string
			for _, c := range v.T.Columns {
				cols = append(cols, c.Name)
			}
			detail := ""
			if len(cols) > 0 {
				detail = strings.Join(cols, ", ")
			}
			result = append(result, Change{Type: "add_table", Table: v.T.Name, Detail: detail})
			for _, idx := range v.T.Indexes {
				result = append(result, Change{Type: "add_index", Table: v.T.Name, Name: idx.Name})
			}
		case *schema.DropTable:
			result = append(result, Change{Type: "drop_table", Table: v.T.Name})
		case *schema.RenameTable:
			result = append(result, Change{Type: "rename_table", Table: v.To.Name, Detail: v.From.Name + " -> " + v.To.Name})
		case *schema.ModifyTable:
			result = append(result, extractTableChanges(v.T.Name, v.Changes)...)
		case *schema.AddView:
			result = append(result, Change{Type: "add_view", Table: v.V.Name})
		case *schema.DropView:
			result = append(result, Change{Type: "drop_view", Table: v.V.Name})
		case *schema.ModifyView:
			result = append(result, Change{Type: "modify_view", Table: v.To.Name})
		case *schema.AddFunc:
			result = append(result, Change{Type: "add_function", Table: v.F.Name})
		case *schema.DropFunc:
			result = append(result, Change{Type: "drop_function", Table: v.F.Name})
		case *schema.ModifyFunc:
			result = append(result, Change{Type: "modify_function", Table: v.To.Name})
		case *schema.ModifySchema:
			// Schema-level changes may contain nested table/view/func changes
			result = append(result, extractChanges(v.Changes)...)
		}
	}
	return result
}

// extractTableChanges converts sub-changes of a ModifyTable into flat Change structs.
func extractTableChanges(tableName string, changes []schema.Change) []Change {
	var result []Change
	for _, c := range changes {
		switch v := c.(type) {
		case *schema.AddColumn:
			detail := ""
			if v.C.Type != nil && v.C.Type.Type != nil {
				detail = typeString(v.C.Type.Type)
			}
			result = append(result, Change{Type: "add_column", Table: tableName, Name: v.C.Name, Detail: detail})
		case *schema.DropColumn:
			result = append(result, Change{Type: "drop_column", Table: tableName, Name: v.C.Name})
		case *schema.ModifyColumn:
			detail := describeColumnModification(v)
			result = append(result, Change{Type: "modify_column", Table: tableName, Name: v.To.Name, Detail: detail})
		case *schema.RenameColumn:
			result = append(result, Change{Type: "rename_column", Table: tableName, Name: v.To.Name, Detail: v.From.Name + " -> " + v.To.Name})
		case *schema.AddIndex:
			result = append(result, Change{Type: "add_index", Table: tableName, Name: v.I.Name})
		case *schema.DropIndex:
			result = append(result, Change{Type: "drop_index", Table: tableName, Name: v.I.Name})
		}
	}
	return result
}

// describeColumnModification returns a human-readable description of a column modification.
func describeColumnModification(m *schema.ModifyColumn) string {
	var parts []string
	if m.Change.Is(schema.ChangeType) {
		from := ""
		to := ""
		if m.From.Type != nil && m.From.Type.Type != nil {
			from = typeString(m.From.Type.Type)
		}
		if m.To.Type != nil && m.To.Type.Type != nil {
			to = typeString(m.To.Type.Type)
		}
		if from != "" && to != "" && from != to {
			parts = append(parts, from+" -> "+to)
		}
	}
	if m.Change.Is(schema.ChangeNull) {
		if m.To.Type != nil && m.To.Type.Null {
			parts = append(parts, "set nullable")
		} else {
			parts = append(parts, "set not null")
		}
	}
	if m.Change.Is(schema.ChangeDefault) {
		parts = append(parts, "default changed")
	}
	if len(parts) == 0 {
		return "modified"
	}
	return strings.Join(parts, ", ")
}

// extractRenameChanges converts known renames (from @docs.previously) into Change structs.
func extractRenameChanges(renames []Rename) []Change {
	var result []Change
	for _, r := range renames {
		switch r.Type {
		case "column":
			result = append(result, Change{
				Type:   "rename_column",
				Table:  r.Table,
				Name:   r.NewName,
				Detail: r.OldName + " -> " + r.NewName,
			})
		case "table":
			result = append(result, Change{
				Type:   "rename_table",
				Table:  r.NewName,
				Detail: r.OldName + " -> " + r.NewName,
			})
		}
	}
	return result
}

// applyKnownRenames modifies the FROM realm in-place to reflect known renames.
// For each rename, it renames the column/table in the FROM realm to its new name
// (so Atlas sees no drop+add) and returns the corresponding RENAME SQL statements.
func applyKnownRenames(fromRealm *schema.Realm, renames []Rename, dialect string) []string {
	var stmts []string
	// Build table rename map (newName -> oldName) so column renames
	// can look up the old table name when the table was also renamed.
	tableRenameMap := make(map[string]string)
	for _, r := range renames {
		if r.Type == "table" {
			tableRenameMap[r.NewName] = r.OldName
		}
	}

	for _, r := range renames {
		switch r.Type {
		case "column":
			// Try the table name as-is first, then try the old table name
			// (if the table itself was renamed)
			tableName := r.Table
			col := findColumn(fromRealm, tableName, r.OldName)
			if col == nil {
				if oldTable, ok := tableRenameMap[tableName]; ok {
					tableName = oldTable
					col = findColumn(fromRealm, tableName, r.OldName)
				}
			}
			if col != nil {
				col.Name = r.NewName
				stmts = append(stmts, fmtRenameColumn(r.Table, r.OldName, r.NewName, dialect))
				// Also update any index references to this column
				tbl := findTable(fromRealm, r.Table)
				if tbl != nil {
					for _, idx := range tbl.Indexes {
						for _, part := range idx.Parts {
							if part.C != nil && part.C == col {
								// Column pointer already updated — nothing else needed
							}
						}
					}
				}
			}
		case "table":
			tbl := findTable(fromRealm, r.OldName)
			if tbl != nil {
				tbl.Name = r.NewName
				stmts = append(stmts, fmtRenameTable(r.OldName, r.NewName, dialect))
			}
		}
	}
	return stmts
}

// detectRenameCandidates compares FROM and TO realms to find potential renames.
// A candidate is a dropped column + added column on the same table with the same type,
// not already covered by a known rename.
func detectRenameCandidates(fromRealm, toRealm *schema.Realm, knownRenames []Rename) []RenameCandidate {
	// Build set of known renames for quick lookup
	knownSet := make(map[string]bool)
	for _, r := range knownRenames {
		if r.Type == "column" {
			knownSet[r.Table+"."+r.OldName+"->"+r.NewName] = true
		} else {
			knownSet[r.OldName+"->"+r.NewName] = true
		}
	}

	var candidates []RenameCandidate

	// Compare tables across schemas
	for _, fromSchema := range fromRealm.Schemas {
		toSchema := findSchema(toRealm, fromSchema.Name)
		if toSchema == nil {
			continue
		}

		// ── Column rename candidates ──
		for _, fromTable := range fromSchema.Tables {
			toTable := findTableInSchema(toSchema, fromTable.Name)
			if toTable == nil {
				continue
			}

			// Find columns in FROM that are missing in TO (dropped)
			var droppedCols []*schema.Column
			for _, fc := range fromTable.Columns {
				if findColumnInTable(toTable, fc.Name) == nil {
					droppedCols = append(droppedCols, fc)
				}
			}

			// Find columns in TO that are missing in FROM (added)
			var addedCols []*schema.Column
			for _, tc := range toTable.Columns {
				if findColumnInTable(fromTable, tc.Name) == nil {
					addedCols = append(addedCols, tc)
				}
			}

			// Match dropped+added pairs with same type
			usedDropped := make(map[int]bool)
			usedAdded := make(map[int]bool)

			for di, dc := range droppedCols {
				for ai, ac := range addedCols {
					if usedDropped[di] || usedAdded[ai] {
						continue
					}
					if typesMatch(dc, ac) {
						key := fromTable.Name + "." + dc.Name + "->" + ac.Name
						if knownSet[key] {
							continue
						}
						candidates = append(candidates, RenameCandidate{
							Type:    "column",
							Table:   fromTable.Name,
							OldName: dc.Name,
							NewName: ac.Name,
							ColType: typeString(dc.Type.Type),
						})
						usedDropped[di] = true
						usedAdded[ai] = true
					}
				}
			}
		}

		// ── Table rename candidates ──
		// Find tables in FROM missing from TO (dropped)
		var droppedTables []*schema.Table
		for _, ft := range fromSchema.Tables {
			if findTableInSchema(toSchema, ft.Name) == nil {
				droppedTables = append(droppedTables, ft)
			}
		}

		// Find tables in TO missing from FROM (added)
		var addedTables []*schema.Table
		for _, tt := range toSchema.Tables {
			if findTableInSchema(fromSchema, tt.Name) == nil {
				addedTables = append(addedTables, tt)
			}
		}

		// Only suggest table renames when there's exactly one drop and one add
		// (otherwise it's ambiguous which table maps to which).
		if len(droppedTables) == 1 && len(addedTables) == 1 {
			dt := droppedTables[0]
			at := addedTables[0]
			key := dt.Name + "->" + at.Name
			if !knownSet[key] {
				candidates = append(candidates, RenameCandidate{
					Type:    "table",
					Table:   dt.Name,
					OldName: dt.Name,
					NewName: at.Name,
				})
			}
		}
	}

	return candidates
}

// ── Realm lookup helpers ────────────────────────────────────────────────

func findSchema(realm *schema.Realm, name string) *schema.Schema {
	for _, s := range realm.Schemas {
		if s.Name == name {
			return s
		}
	}
	return nil
}

func findTable(realm *schema.Realm, name string) *schema.Table {
	for _, s := range realm.Schemas {
		for _, t := range s.Tables {
			if t.Name == name {
				return t
			}
		}
	}
	return nil
}

func findTableInSchema(s *schema.Schema, name string) *schema.Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func findColumn(realm *schema.Realm, tableName, colName string) *schema.Column {
	t := findTable(realm, tableName)
	if t == nil {
		return nil
	}
	return findColumnInTable(t, colName)
}

func findColumnInTable(t *schema.Table, name string) *schema.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// typesMatch checks if two columns have the same type for rename candidate detection.
func typesMatch(a, b *schema.Column) bool {
	if a.Type == nil || b.Type == nil {
		return false
	}
	if a.Type.Type == nil || b.Type.Type == nil {
		return false
	}
	return typeString(a.Type.Type) == typeString(b.Type.Type)
}

// fmtRenameColumn returns a dialect-appropriate RENAME COLUMN statement.
func fmtRenameColumn(table, oldCol, newCol, dialect string) string {
	q := quoteIdent(dialect)
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", q(table), q(oldCol), q(newCol))
}

// fmtRenameTable returns a dialect-appropriate RENAME TABLE statement.
func fmtRenameTable(oldTable, newTable, dialect string) string {
	q := quoteIdent(dialect)
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", q(oldTable), q(newTable))
}

// quoteIdent returns a function that quotes an identifier for the given dialect.
func quoteIdent(dialect string) func(string) string {
	switch dialect {
	case "mysql":
		return func(s string) string { return "`" + s + "`" }
	default: // postgres, sqlite
		return func(s string) string { return `"` + s + `"` }
	}
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
	return openDriverWithConnection(dialect, "dev")
}

func openDriverWithConnection(dialect, connection string) (migrate.Driver, error) {
	db, err := sql.Open("wasi", connection)
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

// inspectConnection opens a driver on a named connection and inspects the current DB state.
func inspectConnection(ctx context.Context, dialect, connection, schemaName string) (*schema.Realm, error) {
	drv, err := openDriverWithConnection(dialect, connection)
	if err != nil {
		return nil, err
	}
	if schemaName != "" {
		s, err := drv.InspectSchema(ctx, schemaName, nil)
		if err != nil {
			return nil, err
		}
		return schema.NewRealm(s), nil
	}
	return drv.InspectRealm(ctx, nil)
}

func fatal(format string, args ...any) {
	result := Result{Error: fmt.Sprintf(format, args...)}
	json.NewEncoder(os.Stdout).Encode(result)
	os.Exit(1)
}
