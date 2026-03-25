// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package sqlite

import (
	"context"
	"fmt"
	"strings"

	"ariga.io/atlas/schemahcl"
	"ariga.io/atlas/sql/internal/specutil"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlspec"
)

// InspectRealm returns schema descriptions of all resources in the given realm.
func (i *inspect) InspectRealm(ctx context.Context, opts *schema.InspectRealmOption) (*schema.Realm, error) {
	schemas, err := i.databases(ctx, opts)
	if err != nil {
		return nil, err
	}
	if len(schemas) > 1 {
		return nil, fmt.Errorf("sqlite: multiple database files are not supported by the driver. got: %d", len(schemas))
	}
	if opts == nil {
		opts = &schema.InspectRealmOption{}
	}
	var (
		r    = schema.NewRealm(schemas...)
		mode = sqlx.ModeInspectRealm(opts)
	)
	if mode.Is(schema.InspectTables) {
		for _, s := range schemas {
			tables, err := i.tables(ctx, nil)
			if err != nil {
				return nil, err
			}
			s.AddTables(tables...)
			for _, t := range tables {
				if err := i.inspectTable(ctx, t); err != nil {
					return nil, err
				}
			}
		}
		sqlx.LinkSchemaTables(r.Schemas)
	}
	if mode.Is(schema.InspectViews) {
		if err := i.inspectViews(ctx, r, nil); err != nil {
			return nil, err
		}
	}
	if mode.Is(schema.InspectTriggers) {
		if err := i.inspectTriggers(ctx, r, nil); err != nil {
			return nil, err
		}
	}
	return schema.ExcludeRealm(r, opts.Exclude)
}

// InspectSchema returns schema descriptions of the tables in the given schema.
// If the schema name is empty, the "main" database is used.
func (i *inspect) InspectSchema(ctx context.Context, name string, opts *schema.InspectOptions) (*schema.Schema, error) {
	if name == "" {
		name = mainFile
	}
	schemas, err := i.databases(ctx, &schema.InspectRealmOption{
		Schemas: []string{name},
	})
	if err != nil {
		return nil, err
	}
	if len(schemas) == 0 {
		return nil, &schema.NotExistError{
			Err: fmt.Errorf("sqlite: schema %q was not found", name),
		}
	}
	if opts == nil {
		opts = &schema.InspectOptions{}
	}
	var (
		r    = schema.NewRealm(schemas...)
		mode = sqlx.ModeInspectSchema(opts)
	)
	if mode.Is(schema.InspectTables) {
		tables, err := i.tables(ctx, opts)
		if err != nil {
			return nil, err
		}
		r.Schemas[0].AddTables(tables...)
		for _, t := range tables {
			if err := i.inspectTable(ctx, t); err != nil {
				return nil, err
			}
		}
		sqlx.LinkSchemaTables(schemas)
	}
	if mode.Is(schema.InspectViews) {
		if err := i.inspectViews(ctx, r, opts); err != nil {
			return nil, err
		}
	}
	if mode.Is(schema.InspectTriggers) {
		if err := i.inspectTriggers(ctx, r, nil); err != nil {
			return nil, err
		}
	}
	return schema.ExcludeSchema(r.Schemas[0], opts.Exclude)
}

var (
	specOptions []schemahcl.Option
	scanFuncs   = &specutil.ScanFuncs{
		Table: convertTable,
		View:  convertView,
	}
)

func triggersSpec(triggers []*schema.Trigger, _ *specutil.Doc) ([]*sqlspec.Trigger, error) {
	specs := make([]*sqlspec.Trigger, 0, len(triggers))
	for _, t := range triggers {
		spec := &sqlspec.Trigger{Name: t.Name}
		switch {
		case t.Table != nil:
			spec.On = schemahcl.BuildRef([]schemahcl.PathIndex{{T: "table", V: []string{t.Table.Name}}})
		case t.View != nil:
			spec.On = schemahcl.BuildRef([]schemahcl.PathIndex{{T: "view", V: []string{t.View.Name}}})
		}
		if t.Body != "" {
			spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("as", t.Body))
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

func (i *inspect) inspectViews(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	if len(r.Schemas) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, viewsQuery)
	if err != nil {
		return fmt.Errorf("sqlite: querying views: %w", err)
	}
	defer rows.Close()
	s := r.Schemas[0]
	for rows.Next() {
		var name, stmt string
		if err := rows.Scan(&name, &stmt); err != nil {
			return fmt.Errorf("sqlite: scanning view: %w", err)
		}
		v := schema.NewView(name, viewDef(stmt)).SetSchema(s)
		s.Views = append(s.Views, v)
	}
	return rows.Err()
}

func (i *inspect) inspectTriggers(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	if len(r.Schemas) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, triggersQuery)
	if err != nil {
		return fmt.Errorf("sqlite: querying triggers: %w", err)
	}
	defer rows.Close()
	s := r.Schemas[0]
	for rows.Next() {
		var name, tblName, stmt string
		if err := rows.Scan(&name, &tblName, &stmt); err != nil {
			return fmt.Errorf("sqlite: scanning trigger: %w", err)
		}
		t := &schema.Trigger{
			Name: name,
			Body: stmt,
		}
		parseTriggerMeta(t, stmt)
		// Attach to the owning table.
		for _, tbl := range s.Tables {
			if tbl.Name == tblName {
				t.Table = tbl
				t.Deps = append(t.Deps, tbl)
				tbl.Triggers = append(tbl.Triggers, t)
				break
			}
		}
		// If not on a table, check views.
		if t.Table == nil {
			for _, v := range s.Views {
				if v.Name == tblName {
					t.View = v
					t.Deps = append(t.Deps, v)
					v.Triggers = append(v.Triggers, t)
					break
				}
			}
		}
	}
	return rows.Err()
}

func (s *state) addView(add *schema.AddView) error {
	v := add.V
	b := s.Build("CREATE VIEW")
	if sqlx.Has(add.Extra, &schema.IfNotExists{}) {
		b.P("IF NOT EXISTS")
	}
	b.View(v).P("AS").P(v.Def)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Comment: fmt.Sprintf("create view %q", v.Name),
		Reverse: s.Build("DROP VIEW").View(v).String(),
	})
	return nil
}

func (s *state) dropView(drop *schema.DropView) error {
	v := drop.V
	b := s.Build("DROP VIEW")
	if sqlx.Has(drop.Extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	b.View(v)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop view %q", v.Name),
	})
	return nil
}

func (s *state) modifyView(modify *schema.ModifyView) error {
	// SQLite does not support ALTER VIEW; drop and recreate.
	if err := s.dropView(&schema.DropView{V: modify.From}); err != nil {
		return err
	}
	return s.addView(&schema.AddView{V: modify.To})
}

func (s *state) renameView(rename *schema.RenameView) error {
	// SQLite does not support RENAME VIEW; drop old and create with new name.
	if err := s.dropView(&schema.DropView{V: rename.From}); err != nil {
		return err
	}
	return s.addView(&schema.AddView{V: rename.To})
}

func (s *state) addTrigger(add *schema.AddTrigger) error {
	t := add.T
	s.append(&migrate.Change{
		Cmd:     t.Body,
		Source:  add,
		Comment: fmt.Sprintf("create trigger %q", t.Name),
		Reverse: s.Build("DROP TRIGGER IF EXISTS").Ident(t.Name).String(),
	})
	return nil
}

func (s *state) dropTrigger(drop *schema.DropTrigger) error {
	t := drop.T
	b := s.Build("DROP TRIGGER")
	if sqlx.Has(drop.Extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	b.Ident(t.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop trigger %q", t.Name),
	})
	return nil
}

func verifyChanges(context.Context, []schema.Change) error {
	return nil // unimplemented.
}

// SupportChange reports if the change is supported by the differ.
func (*diff) SupportChange(c schema.Change) bool {
	switch c.(type) {
	case *schema.RenameConstraint:
		return false
	}
	return true
}

const (
	// Query to list database views.
	viewsQuery = "SELECT name, sql FROM sqlite_master WHERE type = 'view' ORDER BY name"
	// Query to list database triggers.
	triggersQuery = "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
)

// viewDef extracts the SELECT definition from a SQLite CREATE VIEW statement.
// The sql column in sqlite_master is: CREATE [TEMP] VIEW [IF NOT EXISTS] name AS def
func viewDef(stmt string) string {
	upper := strings.ToUpper(stmt)
	idx := strings.Index(upper, " AS ")
	if idx == -1 {
		return stmt
	}
	return strings.TrimSpace(stmt[idx+4:])
}

// parseTriggerMeta extracts timing, event, and orientation from a SQLite
// CREATE TRIGGER statement and sets them on the given Trigger.
func parseTriggerMeta(t *schema.Trigger, stmt string) {
	upper := strings.ToUpper(stmt)
	// Isolate the header (everything before BEGIN).
	header := upper
	for _, sep := range []string{"\nBEGIN", " BEGIN"} {
		if idx := strings.Index(upper, sep); idx != -1 {
			header = upper[:idx]
			break
		}
	}
	// Parse timing.
	switch {
	case strings.Contains(header, " INSTEAD OF "):
		t.ActionTime = schema.TriggerTimeInstead
	case strings.Contains(header, " BEFORE "):
		t.ActionTime = schema.TriggerTimeBefore
	case strings.Contains(header, " AFTER "):
		t.ActionTime = schema.TriggerTimeAfter
	}
	// SQLite supports exactly one event per trigger.
	switch {
	case strings.Contains(header, " INSERT"):
		t.Events = append(t.Events, schema.TriggerEventInsert)
	case strings.Contains(header, " DELETE"):
		t.Events = append(t.Events, schema.TriggerEventDelete)
	case strings.Contains(header, " UPDATE"):
		t.Events = append(t.Events, schema.TriggerEventUpdate)
	}
	// SQLite only supports row-level triggers.
	t.For = schema.TriggerForRow
}
