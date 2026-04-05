// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.
//
// Modifications Copyright 2026 Elliot Shepherd

//go:build !ent

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"ariga.io/atlas/schemahcl"
	"ariga.io/atlas/sql/internal/specutil"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/postgres/internal/postgresop"
	"ariga.io/atlas/sql/schema"
)

// A diff provides a PostgreSQL implementation for schema.Inspector.
type inspect struct{ *conn }

var _ schema.Inspector = (*inspect)(nil)

// InspectRealm returns schema descriptions of all resources in the given realm.
func (i *inspect) InspectRealm(ctx context.Context, opts *schema.InspectRealmOption) (_ *schema.Realm, rerr error) {
	undo, err := i.noSearchPath(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { rerr = errors.Join(rerr, undo()) }()
	schemas, err := i.schemas(ctx, opts)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &schema.InspectRealmOption{}
	}
	var (
		r    = schema.NewRealm(schemas...)
		mode = sqlx.ModeInspectRealm(opts)
	)
	if len(schemas) > 0 {
		if mode.Is(schema.InspectTypes) {
			if err := i.inspectEnums(ctx, r); err != nil {
				return nil, err
			}
			if err := i.inspectTypes(ctx, r, nil); err != nil {
				return nil, err
			}
		}
		if mode.Is(schema.InspectTables) {
			if err := i.inspectTables(ctx, r, nil); err != nil {
				return nil, err
			}
			sqlx.LinkSchemaTables(schemas)
		}
		if mode.Is(schema.InspectViews) {
			if err := i.inspectViews(ctx, r, nil); err != nil {
				return nil, err
			}
		}
		if mode.Is(schema.InspectFuncs) {
			if err := i.inspectFuncs(ctx, r, nil); err != nil {
				return nil, err
			}
		}
		if mode.Is(schema.InspectObjects) {
			if err := i.inspectObjects(ctx, r, nil); err != nil {
				return nil, err
			}
			if err := i.inspectRealmObjects(ctx, r, nil); err != nil {
				return nil, err
			}
		}
		if mode.Is(schema.InspectTriggers) {
			if err := i.inspectTriggers(ctx, r, nil); err != nil {
				return nil, err
			}
			if err := i.inspectPolicies(ctx, r, nil); err != nil {
				return nil, err
			}
		}
		if err := i.inspectDeps(ctx, r, nil); err != nil {
			return nil, err
		}
		resolveColumnTypes(r)
	}
	return schema.ExcludeRealm(r, opts.Exclude)
}

// noSearchPath ensures the session search_path is clean when inspecting realms to ensures all
// referenced objects in the public schema (or any other default search_path) are returned
// qualified in the inspection.
func (i *inspect) noSearchPath(ctx context.Context) (func() error, error) {
	if i.crdb {
		// Skip logic for CockroachDB.
		return func() error { return nil }, nil
	}
	rows, err := i.QueryContext(ctx, "SELECT current_setting('search_path'), set_config('search_path', '', false)")
	if err != nil {
		return nil, err
	}
	var prev sql.NullString
	if err := sqlx.ScanOne(rows, &prev, &sql.NullString{}); err != nil {
		return nil, err
	}
	return func() error {
		if sqlx.ValidString(prev) {
			rows, err := i.QueryContext(ctx, "SELECT set_config('search_path', $1, false)", prev.String)
			if err != nil {
				return err
			}
			return rows.Close()
		}
		return nil
	}, nil
}

// InspectSchema returns schema descriptions of the tables in the given schema.
// If the schema name is empty, the result will be the attached schema.
func (i *inspect) InspectSchema(ctx context.Context, name string, opts *schema.InspectOptions) (s *schema.Schema, err error) {
	if name == "" && i.schema != "" {
		name = i.schema // Otherwise, the "current_schema()" is used.
	}
	schemas, err := i.schemas(ctx, &schema.InspectRealmOption{Schemas: []string{name}})
	if err != nil {
		return nil, err
	}
	switch n := len(schemas); {
	case n == 0:
		// Empty string indicates current connected schema.
		if name == "" {
			return nil, &schema.NotExistError{Err: errors.New("postgres: current_schema() defined in search_path was not found")}
		}
		return nil, &schema.NotExistError{Err: fmt.Errorf("postgres: schema %q was not found", name)}
	case n > 1:
		return nil, fmt.Errorf("postgres: %d schemas were found for %q", n, name)
	}
	if opts == nil {
		opts = &schema.InspectOptions{}
	}
	var (
		r    = schema.NewRealm(schemas...)
		mode = sqlx.ModeInspectSchema(opts)
	)
	if mode.Is(schema.InspectTypes) {
		if err := i.inspectEnums(ctx, r); err != nil {
			return nil, err
		}
		if err := i.inspectTypes(ctx, r, opts); err != nil {
			return nil, err
		}
	}
	if mode.Is(schema.InspectTables) {
		if err := i.inspectTables(ctx, r, opts); err != nil {
			return nil, err
		}
		sqlx.LinkSchemaTables(schemas)
	}
	if mode.Is(schema.InspectViews) {
		if err := i.inspectViews(ctx, r, opts); err != nil {
			return nil, err
		}
	}
	if mode.Is(schema.InspectFuncs) {
		if err := i.inspectFuncs(ctx, r, opts); err != nil {
			return nil, err
		}
	}
	if mode.Is(schema.InspectObjects) {
		if err := i.inspectObjects(ctx, r, opts); err != nil {
			return nil, err
		}
	}
	if mode.Is(schema.InspectTriggers) {
		if err := i.inspectTriggers(ctx, r, nil); err != nil {
			return nil, err
		}
		if err := i.inspectPolicies(ctx, r, nil); err != nil {
			return nil, err
		}
	}
	if err := i.inspectDeps(ctx, r, opts); err != nil {
		return nil, err
	}
	resolveColumnTypes(r)
	return schema.ExcludeSchema(r.Schemas[0], opts.Exclude)
}

func (i *inspect) inspectTables(ctx context.Context, r *schema.Realm, opts *schema.InspectOptions) error {
	if err := i.tables(ctx, r, opts); err != nil {
		return err
	}
	// Collect schemas with tables for batch querying.
	var schemas []*schema.Schema
	for _, s := range r.Schemas {
		if len(s.Tables) > 0 {
			schemas = append(schemas, s)
		}
	}
	if len(schemas) == 0 {
		return nil
	}
	// Query columns, indexes, checks, and FKs across all schemas at once
	// (one round trip per resource type instead of one per schema).
	if err := i.columns(ctx, schemas); err != nil {
		return err
	}
	if err := i.indexes(ctx, schemas); err != nil {
		return err
	}
	if err := i.checks(ctx, schemas); err != nil {
		return err
	}
	if err := i.fks(ctx, schemas); err != nil {
		return err
	}
	for _, s := range schemas {
		if err := i.partitions(s); err != nil {
			return err
		}
	}
	return nil
}

// table returns the table from the database, or a NotExistError if the table was not found.
func (i *inspect) tables(ctx context.Context, realm *schema.Realm, opts *schema.InspectOptions) error {
	var (
		args  []any
		query = fmt.Sprintf(tablesQuery, nArgs(0, len(realm.Schemas)))
	)
	for _, s := range realm.Schemas {
		args = append(args, s.Name)
	}
	if opts != nil && len(opts.Tables) > 0 {
		for _, t := range opts.Tables {
			args = append(args, t)
		}
		query = fmt.Sprintf(tablesQueryArgs, nArgs(0, len(realm.Schemas)), nArgs(len(realm.Schemas), len(opts.Tables)))
	}
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			oid                                                            sql.NullInt64
			tSchema, name, comment, partattrs, partstart, partexprs, extra sql.NullString
			rlsEnabled, rlsForced                                          sql.NullBool
		)
		if err := rows.Scan(&oid, &tSchema, &name, &comment, &partattrs, &partstart, &partexprs, &extra, &rlsEnabled, &rlsForced); err != nil {
			return fmt.Errorf("scan table information: %w", err)
		}
		if !sqlx.ValidString(tSchema) || !sqlx.ValidString(name) {
			return fmt.Errorf("invalid schema or table name: %q.%q", tSchema.String, name.String)
		}
		s, ok := realm.Schema(tSchema.String)
		if !ok {
			return fmt.Errorf("schema %q was not found in realm", tSchema.String)
		}
		t := schema.NewTable(name.String)
		s.AddTables(t)
		if oid.Valid {
			t.AddAttrs(&OID{V: oid.Int64})
		}
		if sqlx.ValidString(comment) {
			t.SetComment(comment.String)
		}
		if sqlx.ValidString(partattrs) {
			t.AddAttrs(&Partition{
				start: partstart.String,
				attrs: partattrs.String,
				exprs: partexprs.String,
			})
		}
		if (rlsEnabled.Valid && rlsEnabled.Bool) || (rlsForced.Valid && rlsForced.Bool) {
			t.AddAttrs(&RowLevelSecurity{
				Enabled: rlsEnabled.Valid && rlsEnabled.Bool,
				Forced:  rlsForced.Valid && rlsForced.Bool,
			})
		}
	}
	return rows.Err()
}

// columns queries and appends the columns for all given schemas in one round trip.
func (i *inspect) columns(ctx context.Context, schemas []*schema.Schema) error {
	query := columnsQuery
	if i.crdb {
		query = crdbColumnsQuery
	}
	rows, err := i.querySchemas(ctx, query, schemas)
	if err != nil {
		return fmt.Errorf("postgres: querying columns: %w", err)
	}
	defer rows.Close()
	sm := schemaMap(schemas)
	for rows.Next() {
		var schemaName string
		if err := i.addColumn(sm, rows, &schemaName); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
	}
	return rows.Err()
}

// addColumn scans the current row and adds a new column from it to the scope (table or view).
// sm is a schema lookup map; schemaName is scanned from the first column of the row.
func (i *inspect) addColumn(sm map[string]*schema.Schema, rows *sql.Rows, schemaName *string) (err error) {
	var (
		typid, typelem, maxlen, precision, timeprecision, scale, seqstart, seqinc, seqlast, attnum                                 sql.NullInt64
		table, name, typ, fmtype, nullable, defaults, identity, genidentity, genexpr, charset, collate, comment, typtype, interval sql.NullString
	)
	if err = rows.Scan(
		schemaName,
		&table, &name, &typ, &fmtype, &nullable, &defaults, &maxlen, &precision, &timeprecision, &scale, &interval, &charset,
		&collate, &identity, &seqstart, &seqinc, &seqlast, &genidentity, &genexpr, &comment, &typtype, &typelem, &typid, &attnum,
	); err != nil {
		return err
	}
	s, ok := sm[*schemaName]
	if !ok {
		return nil // schema not in scope — skip
	}
	t, ok := s.Table(table.String)
	if !ok {
		return nil // table not in scope — skip
	}
	c := &schema.Column{
		Name: name.String,
		Type: &schema.ColumnType{
			Null: nullable.String == "YES",
			Raw: func() string {
				// For domains, use the domain type instead of the base type.
				if typtype.String == "d" {
					return fmtype.String
				}
				return typ.String
			}(),
		},
	}
	c.Type.Type, err = columnType(&columnDesc{
		typ:           typ.String,
		fmtype:        fmtype.String,
		size:          maxlen.Int64,
		scale:         scale.Int64,
		typtype:       typtype.String,
		typelem:       typelem.Int64,
		typid:         typid.Int64,
		interval:      interval.String,
		precision:     precision.Int64,
		timePrecision: &timeprecision.Int64,
	})
	switch tt := c.Type.Type.(type) {
	case *ArrayType:
		if u, ok := tt.Underlying().(*UserDefinedType); ok {
			tt.Type = i.underlyingType(s, u)
		}
	case *UserDefinedType:
		ut := i.underlyingType(s, tt)
		if ut != tt {
			c.Type.Raw = tt.T
			c.Type.Type = ut
		}
	}
	if defaults.Valid {
		columnDefault(c, defaults.String)
	}
	if identity.String == "YES" {
		c.Attrs = append(c.Attrs, &Identity{
			Generation: genidentity.String,
			Sequence: &Sequence{
				Last:      seqlast.Int64,
				Start:     seqstart.Int64,
				Increment: seqinc.Int64,
			},
		})
	}
	if sqlx.ValidString(genexpr) {
		c.Attrs = append(c.Attrs, &schema.GeneratedExpr{
			Expr: genexpr.String,
		})
	}
	if sqlx.ValidString(comment) {
		c.SetComment(comment.String)
	}
	if sqlx.ValidString(charset) {
		c.SetCharset(charset.String)
	}
	if sqlx.ValidString(collate) {
		c.SetCollation(collate.String)
	}
	t.AddColumns(c)
	return nil
}

// parseType is like ParseType, but aware of the Realm state.
func (i *inspect) parseType(ns *schema.Schema, s string) (schema.Type, error) {
	t, err := ParseType(s)
	if err != nil {
		return nil, err
	}
	switch tt := t.(type) {
	case *ArrayType:
		if u, ok := tt.Underlying().(*UserDefinedType); ok {
			tt.Type = i.underlyingType(ns, u)
		}
	case *UserDefinedType:
		t = i.underlyingType(ns, tt)
	}
	return t, nil
}

// underlyingType returns the underlying type of the given user-defined
// type by searching the realm.
func (i *inspect) underlyingType(s *schema.Schema, u *UserDefinedType) schema.Type {
	var (
		sr       []*schema.Schema
		ns, name = parseFmtType(u.T)
	)
	switch nsScope := i.schema != ""; {
	// If the scope is one schema, the namespace defined
	// on the type because it resides on a different schema.
	case ns == "":
		if !nsScope && s.Realm != nil {
			// Search in the "public" schema first, because in the default
			// configuration unqualified types refer to the public schema.
			if s1, ok := s.Realm.Schema("public"); ok {
				sr = append(sr, s1)
			}
		}
		sr = append(sr, s)
	// Allow searching in other schemas, only if
	// we are not in a schema scope.
	case ns != "" && !nsScope && s.Realm != nil:
		if s1, ok := s.Realm.Schema(ns); ok {
			sr = []*schema.Schema{s1}
		}
	}
	for _, s := range sr {
		for _, o := range s.Objects {
			if e, ok := o.(*schema.EnumType); ok && e.T == name {
				return e
			} else if d, ok := o.(*DomainType); ok && d.T == name {
				return d
			} else if c, ok := o.(*CompositeType); ok && c.T == name {
				return c
			}
		}
	}
	// No match.
	return u
}

// resolveColumnTypes links UserDefinedType columns to actual schema objects
// (EnumType, DomainType) that were inspected separately. This enables the
// dependency sorter to correctly order type creation before table creation.
func resolveColumnTypes(r *schema.Realm) {
	// Build realm-wide lookup of type names → objects.
	// Keys include both unqualified ("color") and qualified ("b.color") forms.
	enums := make(map[string]*schema.EnumType)
	domains := make(map[string]*DomainType)
	composites := make(map[string]*CompositeType)
	for _, s := range r.Schemas {
		for _, o := range s.Objects {
			switch v := o.(type) {
			case *schema.EnumType:
				enums[v.T] = v
				if s.Name != "" {
					enums[s.Name+"."+v.T] = v
				}
			case *DomainType:
				domains[v.T] = v
				if s.Name != "" {
					domains[s.Name+"."+v.T] = v
				}
			case *CompositeType:
				composites[v.T] = v
				if s.Name != "" {
					composites[s.Name+"."+v.T] = v
				}
			}
		}
	}
	// resolveType resolves a column type to its actual schema object.
	// Handles both UserDefinedType (from table columns with typtype info)
	// and UnsupportedType (from composite fields without typtype info).
	resolveType := func(ct *schema.ColumnType) {
		if ct == nil || ct.Type == nil {
			return
		}
		switch t := ct.Type.(type) {
		case *UserDefinedType:
			switch t.C {
			case "e":
				if e, ok := enums[t.T]; ok {
					ct.Type = e
				}
			case "d":
				if d, ok := domains[t.T]; ok {
					ct.Type = d
				}
			case "":
				// Composite fields from ParseType have no typtype info.
				// Try matching by name against known enums and domains.
				if e, ok := enums[t.T]; ok {
					ct.Type = e
				} else if d, ok := domains[t.T]; ok {
					ct.Type = d
				}
			}
		case *schema.UnsupportedType:
			// Composite type fields use UnsupportedType for cross-schema refs.
			if e, ok := enums[t.T]; ok {
				ct.Type = e
			} else if d, ok := domains[t.T]; ok {
				ct.Type = d
			} else {
				// Try stripping "public." prefix as a last resort.
				name := t.T
				if idx := strings.LastIndex(name, "."); idx >= 0 {
					name = name[idx+1:]
				}
				if e, ok := enums[name]; ok {
					ct.Type = e
				} else if d, ok := domains[name]; ok {
					ct.Type = d
				}
			}
		}
	}
	// Also add DepsOf support for RangeObj.
	// (RangeObj.Deps is populated below alongside composites.)

	// Resolve column types across all schemas.
	for _, s := range r.Schemas {
		for _, t := range s.Tables {
			for _, c := range t.Columns {
				resolveType(c.Type)
			}
		}
		// Resolve range type subtype dependencies.
		for _, o := range s.Objects {
			if ro, ok := o.(*RangeObj); ok {
				var typeName string
				switch t := ro.Subtype.(type) {
				case *UserDefinedType:
					typeName = t.T
				case *schema.UnsupportedType:
					typeName = t.T
				}
				if typeName != "" {
					if e, ok := enums[typeName]; ok {
						ro.Deps = append(ro.Deps, e)
					} else if d, ok := domains[typeName]; ok {
						ro.Deps = append(ro.Deps, d)
					} else if c, ok := composites[typeName]; ok {
						ro.Deps = append(ro.Deps, c)
					}
				}
			}
		}
		// Resolve composite type field dependencies WITHOUT replacing the field type.
		// We only add deps for sorting — the field type stays as-is (e.g., "b.color")
		// so that FormatType produces the schema-qualified name correctly.
		for _, o := range s.Objects {
			if ct, ok := o.(*CompositeType); ok {
				for _, f := range ct.Fields {
					if f.Type == nil {
						continue
					}
					// Check if the field type matches a known enum/domain by name.
					var typeName string
					switch t := f.Type.Type.(type) {
					case *UserDefinedType:
						typeName = t.T
					case *schema.UnsupportedType:
						typeName = t.T
					}
					if typeName == "" {
						continue
					}
					if e, ok := enums[typeName]; ok {
						ct.Deps = append(ct.Deps, e)
					} else if d, ok := domains[typeName]; ok {
						ct.Deps = append(ct.Deps, d)
					} else if c2, ok := composites[typeName]; ok && c2 != ct {
						ct.Deps = append(ct.Deps, c2)
					}
				}
			}
		}
	}
}

// enumValues fills enum columns with their values from the database.
func (i *inspect) inspectEnums(ctx context.Context, r *schema.Realm) error {
	var (
		ids  = make(map[int64]*schema.EnumType)
		args = make([]any, 0, len(r.Schemas))
	)
	for _, s := range r.Schemas {
		args = append(args, s.Name)
	}
	if len(args) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, fmt.Sprintf(enumsQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying enum values: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id       int64
			ns, n, v string
		)
		if err := rows.Scan(&ns, &id, &n, &v); err != nil {
			return fmt.Errorf("postgres: scanning enum label: %w", err)
		}
		e, ok := ids[id]
		if !ok {
			e = &schema.EnumType{T: n}
			ids[id] = e
		}
		if e.Schema == nil {
			s, ok := r.Schema(ns)
			if !ok {
				return fmt.Errorf("postgres: schema %q for enum %q was not found in inspection", ns, e.T)
			}
			e.Schema = s
			s.Objects = append(s.Objects, e)
		}
		e.Values = append(e.Values, v)
	}
	return nil
}

// indexes queries and appends the indexes for all given schemas in one round trip.
func (i *inspect) indexes(ctx context.Context, schemas []*schema.Schema) error {
	if i.crdb {
		for _, s := range schemas {
			if err := i.crdbIndexes(ctx, s); err != nil {
				return err
			}
		}
		return nil
	}
	rows, err := i.querySchemas(ctx, i.indexesQuery(), schemas)
	if err != nil {
		return fmt.Errorf("postgres: querying indexes: %w", err)
	}
	defer rows.Close()
	sm := schemaMap(schemas)
	if err := i.addIndexes(rows, queryScope{
		hasT: func(sv, tv string) bool {
			s := sm[sv]
			if s == nil { return false }
			_, ok := s.Table(tv)
			return ok
		},
		setPK: func(sv, tv string, idx *schema.Index) error {
			s := sm[sv]
			if s == nil { return nil }
			t, ok := s.Table(tv)
			if !ok { return nil }
			t.SetPrimaryKey(idx)
			return nil
		},
		addIndex: func(sv, tv string, idx *schema.Index) error {
			s := sm[sv]
			if s == nil { return nil }
			t, ok := s.Table(tv)
			if !ok { return nil }
			t.AddIndexes(idx)
			return nil
		},
		column: func(sv, tv, name string) (*schema.Column, bool) {
			s := sm[sv]
			if s == nil { return nil, false }
			t, ok := s.Table(tv)
			if !ok { return nil, false }
			return t.Column(name)
		},
	}); err != nil {
		return err
	}
	return rows.Err()
}

func (i *inspect) indexesQuery() (q string) {
	switch {
	case i.supportsIndexNullsDistinct():
		q = indexesAbove15
	case i.supportsIndexInclude():
		q = indexesAbove11
	default:
		q = indexesBelow11
	}
	return
}

type queryScope struct {
	hasT     func(sv, tv string) bool
	setPK    func(sv, tv string, idx *schema.Index) error
	addIndex func(sv, tv string, idx *schema.Index) error
	column   func(sv, tv, name string) (*schema.Column, bool)
}

// addIndexes scans the rows and adds the indexes to the table.
// The first scanned column must be schema_name.
func (i *inspect) addIndexes(rows *sql.Rows, scope queryScope) error {
	names := make(map[string]*schema.Index)
	for rows.Next() {
		var (
			schemaName, table, name, typ                                                             string
			uniq, primary, included, nullsnotdistinct                                                bool
			desc, nullsfirst, nullslast, opcdefault                                                  sql.NullBool
			column, constraints, pred, expr, comment, options, opcname, opcschema, opcparams, exoper sql.NullString
		)
		if err := rows.Scan(
			&schemaName, &table, &name, &typ, &column, &included, &primary, &uniq, &exoper, &constraints, &pred, &expr, &desc,
			&nullsfirst, &nullslast, &comment, &options, &opcname, &opcschema, &opcdefault, &opcparams, &nullsnotdistinct,
		); err != nil {
			return fmt.Errorf("postgres: scanning indexes: %w", err)
		}
		if !scope.hasT(schemaName, table) {
			continue // not in scope — skip
		}
		key := schemaName + "." + name
		idx, ok := names[key]
		if !ok {
			idx = &schema.Index{
				Name:   name,
				Unique: uniq,
				Attrs: []schema.Attr{
					&IndexType{T: typ},
				},
			}
			if sqlx.ValidString(comment) {
				idx.Attrs = append(idx.Attrs, &schema.Comment{Text: comment.String})
			}
			if sqlx.ValidString(constraints) {
				var m map[string]string
				if err := json.Unmarshal([]byte(constraints.String), &m); err != nil {
					return fmt.Errorf("postgres: unmarshaling index constraints: %w", err)
				}
				for n, t := range m {
					idx.AddAttrs(&Constraint{N: n, T: t})
				}
			}
			if sqlx.ValidString(pred) {
				idx.AddAttrs(&IndexPredicate{P: pred.String})
			}
			if sqlx.ValidString(options) {
				p, err := newIndexStorage(options.String)
				if err != nil {
					return err
				}
				idx.AddAttrs(p)
			}
			if nullsnotdistinct {
				idx.AddAttrs(&IndexNullsDistinct{V: false})
			}
			names[key] = idx
			var err error
			if primary {
				err = scope.setPK(schemaName, table, idx)
			} else {
				err = scope.addIndex(schemaName, table, idx)
			}
			if err != nil {
				return err
			}
		}
		part := &schema.IndexPart{SeqNo: len(idx.Parts) + 1, Desc: desc.Bool}
		if nullsfirst.Bool || nullslast.Bool {
			part.Attrs = append(part.Attrs, &IndexColumnProperty{
				NullsFirst: nullsfirst.Bool,
				NullsLast:  nullslast.Bool,
			})
		}
		if sqlx.ValidString(exoper) {
			part.AddAttrs(NewOperator(i.schema, exoper.String))
		}
		switch {
		case included:
			c, ok := scope.column(schemaName, table, column.String)
			if !ok {
				return fmt.Errorf("postgres: INCLUDE column %q was not found for index %q", column.String, idx.Name)
			}
			var include IndexInclude
			sqlx.Has(idx.Attrs, &include)
			include.Columns = append(include.Columns, c)
			schema.ReplaceOrAppend(&idx.Attrs, &include)
		case sqlx.ValidString(column):
			part.C, ok = scope.column(schemaName, table, column.String)
			if !ok {
				return fmt.Errorf("postgres: column %q was not found for index %q", column.String, idx.Name)
			}
			part.C.Indexes = append(part.C.Indexes, idx)
			idx.Parts = append(idx.Parts, part)
		case sqlx.ValidString(expr):
			part.X = &schema.RawExpr{
				X: expr.String,
			}
			idx.Parts = append(idx.Parts, part)
		default:
			return fmt.Errorf("postgres: invalid part for index %q", idx.Name)
		}
		if err := i.mayAppendOps(part, opcschema.String, opcname.String, opcparams.String, opcdefault.Bool); err != nil {
			return err
		}
	}
	return nil
}

// mayAppendOps appends an operator_class attribute to the part in case it is not the default.
func (i *inspect) mayAppendOps(part *schema.IndexPart, ns, name, params string, defaults bool) error {
	if name == "" || defaults && params == "" {
		return nil
	}
	op := &IndexOpClass{Name: name, Default: defaults}
	if err := op.parseParams(params); err != nil {
		return err
	}
	part.Attrs = append(part.Attrs, op)
	// Detect if the operator class reside in an external schema
	// and should be handled accordingly in other stages.
	switch {
	case
		// Schema is not defined.
		ns == "",
		// Operator class is defined in current scope.
		ns == i.schema,
		// Operator class is defined in the default search_path.
		ns == "pg_catalog", ns == "public":
	default:
		op.Name = fmt.Sprintf("%s.%s", ns, name)
	}
	return nil
}

// partitions builds the partition each table in the schema.
func (i *inspect) partitions(s *schema.Schema) error {
	for _, t := range s.Tables {
		var d Partition
		if !sqlx.Has(t.Attrs, &d) {
			continue
		}
		switch s := strings.ToLower(d.start); s {
		case "r":
			d.T = PartitionTypeRange
		case "l":
			d.T = PartitionTypeList
		case "h":
			d.T = PartitionTypeHash
		default:
			return fmt.Errorf("postgres: unexpected partition strategy %q", s)
		}
		idxs := strings.Split(strings.TrimSpace(d.attrs), " ")
		if len(idxs) == 0 {
			return fmt.Errorf("postgres: no columns/expressions were found in partition key for column %q", t.Name)
		}
		for i := range idxs {
			switch idx, err := strconv.Atoi(idxs[i]); {
			case err != nil:
				return fmt.Errorf("postgres: faild parsing partition key index %q", idxs[i])
			// An expression.
			case idx == 0:
				j := sqlx.ExprLastIndex(d.exprs)
				if j == -1 {
					return fmt.Errorf("postgres: no expression found in partition key: %q", d.exprs)
				}
				d.Parts = append(d.Parts, &PartitionPart{
					X: &schema.RawExpr{X: d.exprs[:j+1]},
				})
				d.exprs = strings.TrimPrefix(d.exprs[j+1:], ", ")
			// A column at index idx-1.
			default:
				if idx > len(t.Columns) {
					return fmt.Errorf("postgres: unexpected column index %d", idx)
				}
				d.Parts = append(d.Parts, &PartitionPart{
					C: t.Columns[idx-1],
				})
			}
		}
		schema.ReplaceOrAppend(&t.Attrs, &d)
	}
	return nil
}

// fks queries and appends the foreign keys of the given table.
func (i *inspect) fks(ctx context.Context, schemas []*schema.Schema) error {
	r := schema.NewRealm(schemas...)
	rows, err := i.querySchemas(ctx, fksQuery, schemas)
	if err != nil {
		return fmt.Errorf("postgres: querying foreign keys: %w", err)
	}
	defer rows.Close()
	if err := sqlx.TypedRealmFKs[*ReferenceOption](r, rows); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return rows.Err()
}

// checks queries and appends the check constraints for all given schemas in one round trip.
func (i *inspect) checks(ctx context.Context, schemas []*schema.Schema) error {
	rows, err := i.querySchemas(ctx, checksQuery, schemas)
	if err != nil {
		return fmt.Errorf("postgres: querying check constraints: %w", err)
	}
	defer rows.Close()
	sm := schemaMap(schemas)
	if err := i.addChecks(sm, rows); err != nil {
		return err
	}
	return rows.Err()
}

// addChecks scans the rows and adds the checks to the table.
func (i *inspect) addChecks(sm map[string]*schema.Schema, rows *sql.Rows) error {
	type tc struct{ s, t, n string }
	names := make(map[tc]*schema.Check)
	for rows.Next() {
		var (
			noInherit                                        bool
			schemaName, table, name, column, clause, indexes string
		)
		if err := rows.Scan(&schemaName, &table, &name, &clause, &column, &indexes, &noInherit); err != nil {
			return fmt.Errorf("postgres: scanning check: %w", err)
		}
		s, ok := sm[schemaName]
		if !ok {
			continue // schema not in scope
		}
		t, ok := s.Table(table)
		if !ok {
			continue // table not in scope
		}
		c, ok := t.Column(column)
		if !ok {
			return fmt.Errorf("postgres: column %q was not found for check %q", column, name)
		}
		ck, ok := names[tc{s: schemaName, t: table, n: name}]
		if !ok {
			ck = &schema.Check{Name: name, Expr: clause, Attrs: []schema.Attr{&CheckColumns{}}}
			if noInherit {
				ck.AddAttrs(&NoInherit{})
			}
			names[tc{s: schemaName, t: table, n: name}] = ck
			t.AddAttrs(ck)
		}
		c.AddAttrs(ck)
		attr := ck.Attrs[0].(*CheckColumns)
		attr.Columns = append(attr.Columns, column)
	}
	return nil
}

// schemas returns the list of the schemas in the database.
func (i *inspect) schemas(ctx context.Context, opts *schema.InspectRealmOption) ([]*schema.Schema, error) {
	var (
		args  []any
		query = schemasQuery
	)
	if opts != nil {
		switch n := len(opts.Schemas); {
		case n == 1 && opts.Schemas[0] == "":
			query = fmt.Sprintf(schemasQueryArgs, "= CURRENT_SCHEMA()")
		case n == 1 && opts.Schemas[0] != "":
			query = fmt.Sprintf(schemasQueryArgs, "= $1")
			args = append(args, opts.Schemas[0])
		case n > 0:
			query = fmt.Sprintf(schemasQueryArgs, "IN ("+nArgs(0, len(opts.Schemas))+")")
			for _, s := range opts.Schemas {
				args = append(args, s)
			}
		}
	}
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: querying schemas: %w", err)
	}
	defer rows.Close()
	var schemas []*schema.Schema
	for rows.Next() {
		var (
			name    string
			comment sql.NullString
		)
		if err := rows.Scan(&name, &comment); err != nil {
			return nil, err
		}
		s := schema.New(name)
		if comment.Valid {
			s.SetComment(comment.String)
		}
		schemas = append(schemas, s)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return schemas, nil
}

// querySchemas runs a query across multiple schemas. The query must have two %s
// placeholders: first for schema names IN (...), second for table names IN (...).
func (i *inspect) querySchemas(ctx context.Context, query string, schemas []*schema.Schema) (*sql.Rows, error) {
	var args []any
	for _, s := range schemas {
		args = append(args, s.Name)
	}
	nSchemas := len(args)
	for _, s := range schemas {
		for _, t := range s.Tables {
			args = append(args, t.Name)
		}
	}
	nTables := len(args) - nSchemas
	q := fmt.Sprintf(query, nArgs(0, nSchemas), nArgs(nSchemas, nTables))
	return i.QueryContext(ctx, q, args...)
}

// schemaMap builds a lookup map from schema name -> *schema.Schema for fast dispatch.
func schemaMap(schemas []*schema.Schema) map[string]*schema.Schema {
	m := make(map[string]*schema.Schema, len(schemas))
	for _, s := range schemas {
		m[s.Name] = s
	}
	return m
}

func nArgs(start, n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteString(", ")
		}
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(start + i))
	}
	return b.String()
}

func columnDefault(c *schema.Column, s string) {
	// Note: we do NOT convert nextval() defaults to SerialType here.
	// Serial conversion is done later in inspectObjects, only when
	// the sequence is confirmed to be auto-owned (deptype='a').
	c.Default = defaultExpr(c.Type.Type, s)
}

func defaultExpr(t schema.Type, s string) schema.Expr {
	switch {
	case sqlx.IsLiteralBool(s), sqlx.IsLiteralNumber(s), sqlx.IsQuoted(s, '\''):
		return &schema.Literal{V: s}
	default:
		var x schema.Expr = &schema.RawExpr{X: s}
		// Try casting or fallback to raw expressions (e.g. column text[] has the default of '{}':text[]).
		if v, ok := canConvert(t, s); ok {
			x = &schema.Literal{V: v}
		}
		return x
	}
}

func canConvert(t schema.Type, x string) (string, bool) {
	i := strings.LastIndex(x, "::")
	if i == -1 || !sqlx.IsQuoted(x[:i], '\'') {
		return "", false
	}
	q := x[0:i]
	x = x[1 : i-1]
	switch t.(type) {
	case *schema.EnumType:
		return q, true
	case *schema.BoolType:
		if sqlx.IsLiteralBool(x) {
			return x, true
		}
	case *schema.DecimalType, *schema.IntegerType, *schema.FloatType:
		if sqlx.IsLiteralNumber(x) {
			return x, true
		}
	case *ArrayType, *schema.BinaryType, *schema.JSONType, *NetworkType, *schema.SpatialType, *schema.StringType, *schema.TimeType, *schema.UUIDType, *XMLType:
		return q, true
	}
	return "", false
}

type (
	// UserDefinedType defines a user-defined type attribute.
	UserDefinedType struct {
		schema.Type
		T string
		C string // Optional type class.

	}

	// RowType defines a composite type that represents a table row.
	RowType struct {
		schema.Type
		T *schema.Table // Table that this row type represents.
	}

	// PseudoType defines a non-column pseudo-type, such as function arguments and return types.
	// https://www.postgresql.org/docs/current/datatype-pseudo.html
	PseudoType struct {
		schema.Type
		T string // e.g., void, any, cstring, etc.
	}

	// OID is the object identifier as defined in the Postgres catalog.
	OID struct {
		schema.Attr
		V int64
	}

	// RowLevelSecurity indicates whether row-level security is enabled on a table.
	RowLevelSecurity struct {
		schema.Attr
		Enabled bool
		Forced  bool // FORCE ROW LEVEL SECURITY (applies to table owner too).
	}

	// ArrayType defines an array type.
	// https://postgresql.org/docs/current/arrays.html
	ArrayType struct {
		schema.Type        // Underlying items type (e.g. varchar(255)).
		T           string // Formatted type (e.g. int[]).
	}

	// BitType defines a bit type.
	// https://postgresql.org/docs/current/datatype-bit.html
	BitType struct {
		schema.Type
		T   string
		Len int64
	}

	// DomainType represents a domain type.
	// https://www.postgresql.org/docs/current/domains.html
	DomainType struct {
		schema.Type
		schema.Object
		T       string          // Type name.
		Schema  *schema.Schema  // Optional schema.
		Null    bool            // Nullability.
		Default schema.Expr     // Default value.
		Checks  []*schema.Check // Check constraints.
		Attrs   []schema.Attr   // Extra attributes, such as OID.
		Deps    []schema.Object // Objects this domain depends on.
	}

	// CompositeType defines a composite type.
	// https://www.postgresql.org/docs/current/rowtypes.html
	CompositeType struct {
		schema.Type
		schema.Object
		T      string           // Type name.
		Schema *schema.Schema   // Optional schema.
		Fields []*schema.Column // Type fields, also known as attributes/columns.
		Attrs  []schema.Attr    // Extra attributes, such as OID.
		Deps   []schema.Object  // Objects this domain depends on.
	}

	// CollationObj defines a collation as a schema object.
	// https://www.postgresql.org/docs/current/sql-createcollation.html
	CollationObj struct {
		schema.Object
		T             string         // Collation name.
		Schema        *schema.Schema // Schema where defined.
		Locale        string         // LOCALE value.
		Provider      string         // "icu", "libc", or "builtin".
		Deterministic *bool          // DETERMINISTIC flag (nil = default true).
		LcCollate     string         // LC_COLLATE value.
		LcCtype       string         // LC_CTYPE value.
		Version       string         // Collation version.
		Attrs         []schema.Attr
		Deps          []schema.Object
	}

	// IntervalType defines an interval type.
	// https://postgresql.org/docs/current/datatype-datetime.html
	IntervalType struct {
		schema.Type
		T         string // Type name.
		F         string // Optional field. YEAR, MONTH, ..., MINUTE TO SECOND.
		Precision *int   // Optional precision.
	}

	// A NetworkType defines a network type.
	// https://postgresql.org/docs/current/datatype-net-types.html
	NetworkType struct {
		schema.Type
		T   string
		Len int64
	}

	// A CurrencyType defines a currency type.
	CurrencyType struct {
		schema.Type
		T string
	}

	// A RangeType defines a range type.
	// https://www.postgresql.org/docs/current/rangetypes.html
	RangeType struct {
		schema.Type
		T string
	}

	// A SerialType defines a serial type.
	// https://postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL
	SerialType struct {
		schema.Type
		T         string
		Precision int
		// SequenceName holds the inspected sequence name attached to the column.
		// It defaults to <Table>_<Column>_seq when the column is created, but may
		// be different in case the table or the column was renamed.
		SequenceName string
	}

	// A TextSearchType defines full text search types.
	// https://www.postgresql.org/docs/current/datatype-textsearch.html
	TextSearchType struct {
		schema.Type
		T string
	}

	// UUIDType is alias to schema.UUIDType.
	// Defined here for backward compatibility reasons.
	UUIDType = schema.UUIDType

	// OIDType defines an object identifier type.
	OIDType struct {
		schema.Type
		T string
	}

	// A XMLType defines an XML type.
	XMLType struct {
		schema.Type
		T string
	}

	// ConvertUsing describes the USING clause to convert
	// one type to another.
	ConvertUsing struct {
		schema.Clause
		X string // Conversion expression.
	}

	// Constraint describes a postgres constraint.
	// https://postgresql.org/docs/current/catalog-pg-constraint.html
	Constraint struct {
		schema.Attr
		N string // constraint name
		T string // c, f, p, u, t, x.
	}

	// Operator describes an operator.
	// https://www.postgresql.org/docs/current/sql-createoperator.html
	Operator struct {
		schema.Attr
		schema.Object
		// Schema where the operator is defined. If nil, the operator
		// is not managed by the current scope.
		Schema *schema.Schema
		// Operator name. Might include the schema name if the schema
		// is not managed by the current scope or extension based.
		// e.g., "public.&&".
		Name  string
		Attrs []schema.Attr
	}

	// Sequence defines (the supported) sequence options.
	// https://postgresql.org/docs/current/sql-createsequence.html
	Sequence struct {
		schema.Object
		// Fields used by the Identity schema attribute.
		Start     int64
		Increment int64
		// Last sequence value written to disk.
		// https://postgresql.org/docs/current/view-pg-sequences.html.
		Last int64

		// Field used when defining and managing independent
		// sequences (not part of IDENTITY or serial columns).
		Name     string         // Sequence name.
		Schema   *schema.Schema // Optional schema.
		Type     schema.Type    // Sequence type.
		Cache    int64          // Cache size.
		Min, Max *int64         // Min and max values.
		Cycle    bool           // Whether the sequence cycles.
		Attrs    []schema.Attr  // Additional attributes (e.g., comments),
		Owner    struct {       // Optional owner of the sequence.
			T *schema.Table
			C *schema.Column
		}
	}

	// Extension defines a PostgreSQL extension.
	Extension struct {
		schema.Object
		T       string // Extension name.
		Schema  string // Schema the extension is installed in.
		Version string // Extension version.
	}

	// Policy defines a row-level security policy.
	// https://www.postgresql.org/docs/current/sql-createpolicy.html
	Policy struct {
		schema.Object
		Name  string         // Policy name.
		Table *schema.Table  // Table the policy applies to.
		As    string         // PERMISSIVE or RESTRICTIVE.
		For   string         // ALL, SELECT, INSERT, UPDATE, DELETE.
		To    []string       // Target roles.
		Using string         // USING expression (qual).
		Check string         // WITH CHECK expression.
		Attrs []schema.Attr
		Deps  []schema.Object
	}

	// EventTrigger defines a database-level event trigger.
	// https://www.postgresql.org/docs/current/sql-createeventtrigger.html
	EventTrigger struct {
		schema.Object
		Name    string    // Trigger name.
		Event   string    // Event type: ddl_command_start, ddl_command_end, table_rewrite, sql_drop.
		Tags    []string  // Command tag filters (WHEN TAG IN ...).
		FuncRef string    // Function to execute.
		Attrs   []schema.Attr
		Deps    []schema.Object
	}

	// Cast defines a type cast.
	Cast struct {
		schema.Object
		Source  string
		Target  string
		Method  string
		FuncRef string
		Context string
		Attrs   []schema.Attr
		Deps    []schema.Object
	}

	// Aggregate defines a PostgreSQL aggregate function.
	// https://www.postgresql.org/docs/current/sql-createaggregate.html
	Aggregate struct {
		schema.Object
		Name         string         // Aggregate name.
		Schema       *schema.Schema // Schema the aggregate belongs to.
		Args         []*schema.FuncArg // Argument types.
		StateFunc    string         // State transition function name.
		StateType    schema.Type    // State data type.
		FinalFunc    string         // Final calculation function (optional).
		InitVal      string         // Initial value of the state (optional).
		SortOp       string         // Sort operator for min/max aggregates (optional).
		Parallel     string         // Parallel mode: SAFE, UNSAFE, RESTRICTED (optional).
		Attrs        []schema.Attr
		Deps         []schema.Object
	}

	// RangeObj defines a range type as a schema object.
	RangeObj struct {
		schema.Type
		schema.Object
		T              string
		Schema         *schema.Schema
		Subtype        schema.Type
		SubtypeDiff    string
		MultirangeName string
		Attrs          []schema.Attr
		Deps           []schema.Object
	}

	// Role defines a database role.
	Role struct {
		schema.Object
		Name        string
		Superuser   bool
		CreateDB    bool
		CreateRole  bool
		Login       bool
		Inherit     bool
		Replication bool
		BypassRLS   bool
		ConnLimit   int
		MemberOf    []string
		Attrs       []schema.Attr
		Deps        []schema.Object
	}

	// Identity defines an identity column.
	Identity struct {
		schema.Attr
		Generation string // ALWAYS, BY DEFAULT.
		Sequence   *Sequence
	}

	// IndexType represents an index type.
	// https://postgresql.org/docs/current/indexes-types.html
	IndexType struct {
		schema.Attr
		T string // BTREE, BRIN, HASH, GiST, SP-GiST, GIN.
	}

	// IndexPredicate describes a partial index predicate.
	// https://postgresql.org/docs/current/catalog-pg-index.html
	IndexPredicate struct {
		schema.Attr
		P string
	}

	// IndexColumnProperty describes an index column property.
	// https://postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-INDEX-COLUMN-PROPS
	IndexColumnProperty struct {
		schema.Attr
		// NullsFirst defaults to true for DESC indexes.
		NullsFirst bool
		// NullsLast defaults to true for ASC indexes.
		NullsLast bool
	}

	// IndexStorageParams describes index storage parameters add with the WITH clause.
	// https://postgresql.org/docs/current/sql-createindex.html#SQL-CREATEINDEX-STORAGE-PARAMETERS
	IndexStorageParams struct {
		schema.Attr
		// AutoSummarize defines the authsummarize storage parameter.
		AutoSummarize bool
		// PagesPerRange defines pages_per_range storage
		// parameter for BRIN indexes. Defaults to 128.
		PagesPerRange int64
	}

	// IndexInclude describes the INCLUDE clause allows specifying
	// a list of column which added to the index as non-key columns.
	// https://www.postgresql.org/docs/current/sql-createindex.html
	IndexInclude struct {
		schema.Attr
		Columns []*schema.Column
	}

	// IndexOpClass describers operator class of the index part.
	// https://www.postgresql.org/docs/current/indexes-opclass.html.
	IndexOpClass struct {
		schema.Attr
		Name    string                  // Name of the operator class. Qualified if schema is not the default, and required.
		Default bool                    // If it is the default operator class.
		Params  []struct{ N, V string } // Optional parameters.
	}

	// IndexNullsDistinct describes the NULLS [NOT] DISTINCT clause.
	IndexNullsDistinct struct {
		schema.Attr
		V bool // NULLS [NOT] DISTINCT. Defaults to true.
	}

	// Concurrently describes the CONCURRENTLY clause to instruct Postgres to
	// build or drop the index concurrently without blocking the current table.
	// https://www.postgresql.org/docs/current/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY
	Concurrently struct {
		schema.Clause
	}

	// NotValid describes the NOT VALID clause for the creation
	// of check and foreign-key constraints.
	NotValid struct {
		schema.Clause
	}

	// NoInherit attribute defines the NO INHERIT flag for CHECK constraint.
	// https://postgresql.org/docs/current/catalog-pg-constraint.html
	NoInherit struct {
		schema.Attr
	}

	// CheckColumns attribute hold the column named used by the CHECK constraints.
	// This attribute is added on inspection for internal usage and has no meaning
	// on migration.
	CheckColumns struct {
		schema.Attr
		Columns []string
	}

	// Partition defines the spec of a partitioned table.
	Partition struct {
		schema.Attr
		// T defines the type/strategy of the partition.
		// Can be one of: RANGE, LIST, HASH.
		T string
		// Partition parts. The additional attributes
		// on each part can be used to control collation.
		Parts []*PartitionPart

		// Internal info returned from pg_partitioned_table.
		start, attrs, exprs string
	}

	// An PartitionPart represents an index part that
	// can be either an expression or a column.
	PartitionPart struct {
		X     schema.Expr
		C     *schema.Column
		Attrs []schema.Attr
	}

	// Cascade describes that a CASCADE clause should be added to the DROP [TABLE|SCHEMA]
	// operation. Note, this clause is automatically added to DROP SCHEMA by the planner.
	Cascade struct {
		schema.Clause
	}

	// ReferenceOption describes the ON DELETE and ON UPDATE options for foreign keys.
	ReferenceOption schema.ReferenceOption
)

var _ specutil.RefNamer = (*DomainType)(nil)

// Ref returns a reference to the domain type.
func (d *DomainType) Ref() *schemahcl.Ref {
	return specutil.ObjectRef(d.Schema, d)
}

// SpecType returns the type of the domain.
func (d *DomainType) SpecType() string {
	return "domain"
}

// SpecName returns the name of the domain.
func (d *DomainType) SpecName() string {
	return d.T
}

// Underlying returns the underlying type of the domain.
func (d *DomainType) Underlying() schema.Type {
	return d.Type
}

// SpecType returns the type of the composite type.
func (c *CompositeType) SpecType() string {
	return "composite"
}

// SpecName returns the name of the composite type.
func (c *CompositeType) SpecName() string {
	return c.T
}

// SpecType returns the HCL block type for a collation object.
func (c *CollationObj) SpecType() string {
	return "collation"
}

// SpecName returns the name of the collation object.
func (c *CollationObj) SpecName() string {
	return c.T
}

// DepsOf returns the dependencies of this aggregate (implements the interface
// used by the topological sort for AddObject ordering).
func (a *Aggregate) DepsOf() []schema.Object {
	return a.Deps
}

// DepsOf returns the dependencies of this composite type.
func (c *CompositeType) DepsOf() []schema.Object {
	return c.Deps
}

// DepsOf returns the dependencies of this range type.
func (r *RangeObj) DepsOf() []schema.Object {
	return r.Deps
}

// DepsOf returns the dependencies of this policy.
func (p *Policy) DepsOf() []schema.Object {
	return p.Deps
}

// DepsOf returns the dependencies of this event trigger.
func (e *EventTrigger) DepsOf() []schema.Object {
	return e.Deps
}

// DepsOf returns the dependencies of this domain type.
func (d *DomainType) DepsOf() []schema.Object {
	return d.Deps
}

// DepsOf returns the dependencies of this collation.
func (c *CollationObj) DepsOf() []schema.Object {
	return c.Deps
}

// DepsOf returns the dependencies of this cast.
func (c *Cast) DepsOf() []schema.Object {
	return c.Deps
}

// DepsOf returns the dependencies of this role.
func (r *Role) DepsOf() []schema.Object {
	return r.Deps
}

// SpecType returns the HCL block type for a range object.
func (r *RangeObj) SpecType() string {
	return "range"
}

// SpecName returns the name of the range object.
func (r *RangeObj) SpecName() string {
	return r.T
}

// Underlying returns the underlying type of the array.
func (a *ArrayType) Underlying() schema.Type {
	return a.Type
}

// String implements fmt.Stringer interface.
func (o ReferenceOption) String() string {
	return string(o)
}

// Scan implements sql.Scanner interface.
func (o *ReferenceOption) Scan(v any) error {
	var s sql.NullString
	if err := s.Scan(v); err != nil {
		return err
	}
	switch strings.ToLower(s.String) {
	case "a":
		*o = ReferenceOption(schema.NoAction)
	case "r":
		*o = ReferenceOption(schema.Restrict)
	case "c":
		*o = ReferenceOption(schema.Cascade)
	case "n":
		*o = ReferenceOption(schema.SetNull)
	case "d":
		*o = ReferenceOption(schema.SetDefault)
	default:
		return fmt.Errorf("unknown reference option: %q", s.String)
	}
	return nil
}

// IsUnique reports if the type is a unique constraint.
func (c Constraint) IsUnique() bool { return strings.ToLower(c.T) == "u" }

// IsExclude reports if the type is an exclude constraint.
func (c Constraint) IsExclude() bool { return strings.ToLower(c.T) == "x" }

// UniqueConstraint returns constraint with type "u".
func UniqueConstraint(name string) *Constraint {
	return &Constraint{T: "u", N: name}
}

// ExcludeConstraint returns constraint with type "x".
func ExcludeConstraint(name string) *Constraint {
	return &Constraint{T: "x", N: name}
}

// NewOperator returns the string representation of the operator.
func NewOperator(scope string, name string) *Operator {
	// When scanned from the database, the operator is returned as: "<schema>.<operator>".
	// The common case is that operators are the default and defined in pg_catalog, or are
	// installed by extensions.
	if parts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '.'
	}); len(parts) == 2 && (scope == "" || parts[0] == "pg_catalog" || parts[0] == scope) {
		return &Operator{Name: parts[1]}
	}
	return &Operator{Name: name}
}

// IntegerType returns the underlying integer type this serial type represents.
func (s *SerialType) IntegerType() *schema.IntegerType {
	t := &schema.IntegerType{T: TypeInteger}
	switch s.T {
	case TypeSerial2, TypeSmallSerial:
		t.T = TypeSmallInt
	case TypeSerial8, TypeBigSerial:
		t.T = TypeBigInt
	}
	return t
}

// SetType sets the serial type from the given integer type.
func (s *SerialType) SetType(t *schema.IntegerType) {
	switch t.T {
	case TypeSmallInt, TypeInt2:
		s.T = TypeSmallSerial
	case TypeInteger, TypeInt4, TypeInt:
		s.T = TypeSerial
	case TypeBigInt, TypeInt8:
		s.T = TypeBigSerial
	}
}

// sequence returns the inspected name of the sequence
// or the standard name defined by postgres.
func (s *SerialType) sequence(t *schema.Table, c *schema.Column) string {
	if s.SequenceName != "" {
		return s.SequenceName
	}
	return fmt.Sprintf("%s_%s_seq", t.Name, c.Name)
}

var (
	opsOnce    sync.Once
	defaultOps map[postgresop.Class]bool
)

// DefaultFor reports if the operator_class is the default for the index part.
func (o *IndexOpClass) DefaultFor(idx *schema.Index, part *schema.IndexPart) (bool, error) {
	// Explicitly defined as the default (Usually, it comes from the inspection).
	if o.Default && len(o.Params) == 0 {
		return true, nil
	}
	it := &IndexType{T: IndexTypeBTree}
	if sqlx.Has(idx.Attrs, it) {
		it.T = strings.ToUpper(it.T)
	}
	// The key type must be known to check if it is the default op_class.
	if part.X != nil || len(o.Params) > 0 {
		return false, nil
	}
	opsOnce.Do(func() {
		defaultOps = make(map[postgresop.Class]bool, len(postgresop.Classes))
		for _, op := range postgresop.Classes {
			if op.Default {
				defaultOps[postgresop.Class{Name: op.Name, Method: op.Method, Type: op.Type}] = true
			}
		}
	})
	var (
		t   string
		err error
	)
	switch typ := part.C.Type.Type.(type) {
	case *schema.EnumType:
		t = "anyenum"
	case *ArrayType:
		t = "anyarray"
	default:
		t, err = FormatType(typ)
		if err != nil {
			return false, fmt.Errorf("postgres: format operator-class type %T: %w", typ, err)
		}
	}
	return defaultOps[postgresop.Class{Name: o.Name, Method: it.T, Type: t}], nil
}

// Equal reports whether o and x are the same operator class.
func (o *IndexOpClass) Equal(x *IndexOpClass) bool {
	if o.Name != x.Name || o.Default != x.Default || len(o.Params) != len(x.Params) {
		return false
	}
	for i := range o.Params {
		if o.Params[i].N != x.Params[i].N || o.Params[i].V != x.Params[i].V {
			return false
		}
	}
	return true
}

// String returns the string representation of the operator class.
func (o *IndexOpClass) String() string {
	if len(o.Params) == 0 {
		return o.Name
	}
	var b strings.Builder
	b.WriteString(o.Name)
	b.WriteString("(")
	for i, p := range o.Params {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(p.N)
		b.WriteString("=")
		b.WriteString(p.V)
	}
	b.WriteString(")")
	return b.String()
}

// UnmarshalText parses the operator class from its string representation.
func (o *IndexOpClass) UnmarshalText(text []byte) error {
	i := bytes.IndexByte(text, '(')
	if i == -1 {
		o.Name = string(text)
		return nil
	}
	o.Name = string(text[:i])
	return o.parseParams(string(text[i:]))
}

// parseParams parses index class parameters defined in HCL or returned
// from the database. For example: '{k=v}', '(k1=v1,k2=v2)'.
func (o *IndexOpClass) parseParams(kv string) error {
	switch {
	case kv == "":
	case strings.HasPrefix(kv, "(") && strings.HasSuffix(kv, ")"), strings.HasPrefix(kv, "{") && strings.HasSuffix(kv, "}"):
		for _, e := range strings.Split(kv[1:len(kv)-1], ",") {
			if kv := strings.Split(strings.TrimSpace(e), "="); len(kv) == 2 {
				o.Params = append(o.Params, struct{ N, V string }{N: kv[0], V: kv[1]})
			}
		}
	default:
		return fmt.Errorf("postgres: unexpected operator class parameters format: %q", kv)
	}
	return nil
}

// newIndexStorage parses and returns the index storage parameters.
func newIndexStorage(opts string) (*IndexStorageParams, error) {
	params := &IndexStorageParams{}
	for _, p := range strings.Split(strings.Trim(opts, "{}"), ",") {
		kv := strings.Split(p, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid index storage parameter: %s", p)
		}
		switch kv[0] {
		case "autosummarize":
			b, err := strconv.ParseBool(kv[1])
			if err != nil {
				return nil, fmt.Errorf("failed parsing autosummarize %q: %w", kv[1], err)
			}
			params.AutoSummarize = b
		case "pages_per_range":
			i, err := strconv.ParseInt(kv[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed parsing pages_per_range %q: %w", kv[1], err)
			}
			params.PagesPerRange = i
		}
	}
	return params, nil
}

// reFmtType extracts the formatted type and an option schema qualifier.
var reFmtType = regexp.MustCompile(`^(?:(".+"|\w+)\.)?(".+"|\w+)$`)

// parseFmtType parses the formatted type returned from pg_catalog.format_type
// and extract the schema and type name.
func parseFmtType(t string) (s, n string) {
	n = t
	parts := reFmtType.FindStringSubmatch(t)
	r := func(s string) string {
		s = strings.ReplaceAll(s, `""`, `"`)
		if len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
			s = s[1 : len(s)-1]
		}
		return s
	}
	if len(parts) > 1 {
		s = r(parts[1])
	}
	if len(parts) > 2 {
		n = r(parts[2])
	}
	return s, n
}

const (
	// Query to list runtime parameters.
	paramsQuery = `SELECT current_setting('server_version_num'), current_setting('default_table_access_method', true), current_setting('crdb_version', true)`

	// Query to list database schemas.
	schemasQuery = `
SELECT
	nspname AS schema_name,
	pg_catalog.obj_description(ns.oid) AS comment
FROM
	pg_catalog.pg_namespace ns
	LEFT JOIN pg_depend AS dep ON dep.classid = 'pg_catalog.pg_namespace'::regclass::oid AND dep.objid = ns.oid AND dep.deptype = 'e'
WHERE
	nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'crdb_internal', 'pg_extension')
	AND nspname NOT LIKE 'pg_%temp_%'
	AND dep.objid IS NULL
ORDER BY
    nspname`

	// Query to list database schemas.
	schemasQueryArgs = `
SELECT
	nspname AS schema_name,
	pg_catalog.obj_description(ns.oid) AS comment
FROM
	pg_catalog.pg_namespace ns
	LEFT JOIN pg_depend AS dep ON dep.classid = 'pg_catalog.pg_namespace'::regclass::oid AND dep.objid = ns.oid AND dep.deptype = 'e'
WHERE
	nspname %s
	AND dep.objid IS NULL
ORDER BY
    nspname`

	// Query to list table columns.
	columnsQuery = `
SELECT
	t1.table_schema,
	t1.table_name,
	t1.column_name,
	t1.data_type,
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS format_type,
	t1.is_nullable,
	t1.column_default,
	t1.character_maximum_length,
	t1.numeric_precision,
	t1.datetime_precision,
	t1.numeric_scale,
	t1.interval_type,
	t1.character_set_name,
	t1.collation_name,
	t1.is_identity,
	t1.identity_start,
	t1.identity_increment,
	(CASE WHEN t1.is_identity = 'YES' THEN (SELECT last_value FROM pg_sequences WHERE quote_ident(schemaname) || '.' || quote_ident(sequencename) = pg_get_serial_sequence(quote_ident(t1.table_schema) || '.' || quote_ident(t1.table_name), t1.column_name)) END) AS identity_last,
	t1.identity_generation,
	t1.generation_expression,
	col_description(t3.oid, "ordinal_position") AS comment,
	t4.typtype,
	t4.typelem,
	t4.oid,
	a.attnum
FROM
	"information_schema"."columns" AS t1
	JOIN pg_catalog.pg_namespace AS t2 ON t2.nspname = t1.table_schema
	JOIN pg_catalog.pg_class AS t3 ON t3.relnamespace = t2.oid AND t3.relname = t1.table_name
	JOIN pg_catalog.pg_attribute AS a ON a.attrelid = t3.oid AND a.attname = t1.column_name
	LEFT JOIN pg_catalog.pg_type AS t4 ON t4.oid = a.atttypid
WHERE
	t1.table_schema IN (%s) AND t1.table_name IN (%s)
ORDER BY
	t1.table_schema, t1.table_name, t1.ordinal_position
`
	// Query to list enum values.
	enumsQuery = `
SELECT
	n.nspname AS schema_name,
	e.enumtypid AS enum_id,
	t.typname AS enum_name,
	e.enumlabel AS enum_value
FROM
	pg_enum e
	JOIN pg_type t ON e.enumtypid = t.oid
	JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE
    n.nspname IN (%s)
ORDER BY
    n.nspname, e.enumtypid, e.enumsortorder
`
	// Query to list foreign-keys.
	fksQuery = `
SELECT 
    fk.constraint_name,
    fk.table_name,
    a1.attname AS column_name,
    fk.schema_name,
    fk.referenced_table_name,
    a2.attname AS referenced_column_name,
    fk.referenced_schema_name,
    fk.confupdtype,
    fk.confdeltype
	FROM 
	    (
	    	SELECT
	      		con.conname AS constraint_name,
	      		con.conrelid,
	      		con.confrelid,
	      		t1.relname AS table_name,
	      		ns1.nspname AS schema_name,
      			t2.relname AS referenced_table_name,
	      		ns2.nspname AS referenced_schema_name,
	      		generate_series(1,array_length(con.conkey,1)) as ord,
	      		unnest(con.conkey) AS conkey,
	      		unnest(con.confkey) AS confkey,
	      		con.confupdtype,
	      		con.confdeltype
	    	FROM pg_constraint con
	    	JOIN pg_class t1 ON t1.oid = con.conrelid
	    	JOIN pg_class t2 ON t2.oid = con.confrelid
	    	JOIN pg_namespace ns1 on t1.relnamespace = ns1.oid
	    	JOIN pg_namespace ns2 on t2.relnamespace = ns2.oid
	    	WHERE ns1.nspname IN (%s)
	    	AND t1.relname IN (%s)
	    	AND con.contype = 'f'
	) AS fk
	JOIN pg_attribute a1 ON a1.attnum = fk.conkey AND a1.attrelid = fk.conrelid
	JOIN pg_attribute a2 ON a2.attnum = fk.confkey AND a2.attrelid = fk.confrelid
	ORDER BY
	    fk.conrelid, fk.constraint_name, fk.ord
`

	// Query to list table check constraints.
	checksQuery = `
SELECT
	nsp.nspname AS schema_name,
	rel.relname AS table_name,
	t1.conname AS constraint_name,
	pg_get_expr(t1.conbin, t1.conrelid) as expression,
	t2.attname as column_name,
	t1.conkey as column_indexes,
	t1.connoinherit as no_inherit
FROM
	pg_constraint t1
	JOIN pg_attribute t2
	ON t2.attrelid = t1.conrelid
	AND t2.attnum = ANY (t1.conkey)
	JOIN pg_class rel
	ON rel.oid = t1.conrelid
	JOIN pg_namespace nsp
	ON nsp.oid = t1.connamespace
WHERE
	t1.contype = 'c'
	AND nsp.nspname IN (%s)
	AND rel.relname IN (%s)
ORDER BY
	t1.conname, array_position(t1.conkey, t2.attnum)
`
)

var (
	indexesBelow11   = fmt.Sprintf(indexesQueryTmpl, "false", "false", "%s", "%s")
	indexesAbove11   = fmt.Sprintf(indexesQueryTmpl, "(a.attname <> '' AND idx.indnatts > idx.indnkeyatts AND idx.ord > idx.indnkeyatts)", "false", "%s", "%s")
	indexesAbove15   = fmt.Sprintf(indexesQueryTmpl, "(a.attname <> '' AND idx.indnatts > idx.indnkeyatts AND idx.ord > idx.indnkeyatts)", "idx.indnullsnotdistinct", "%s", "%s")
	indexesQueryTmpl = `
SELECT
	n.nspname AS schema_name,
	t.relname AS table_name,
	i.relname AS index_name,
	am.amname AS index_type,
	a.attname AS column_name,
	%s AS included,
	idx.indisprimary AS primary,
	idx.indisunique AS unique,
	(CASE WHEN idx.indisexclusion THEN (SELECT conexclop[idx.ord]::regoper FROM pg_constraint WHERE conindid = idx.indexrelid) END) AS excoper,
	con.nametypes AS constraints,
	pg_get_expr(idx.indpred, idx.indrelid) AS predicate,
	pg_get_indexdef(idx.indexrelid, idx.ord, false) AS expression,
	pg_index_column_has_property(idx.indexrelid, idx.ord, 'desc') AS isdesc,
	pg_index_column_has_property(idx.indexrelid, idx.ord, 'nulls_first') AS nulls_first,
	pg_index_column_has_property(idx.indexrelid, idx.ord, 'nulls_last') AS nulls_last,
	obj_description(i.oid, 'pg_class') AS comment,
	i.reloptions AS options,
	op.opcname AS opclass_name,
	op.opcnamespace::regnamespace::text AS opclass_schema,
	op.opcdefault AS opclass_default,
	a2.attoptions AS opclass_params,
    %s AS indnullsnotdistinct
FROM
	(
		select
			*,
			generate_series(1,array_length(i.indkey,1)) as ord,
			unnest(i.indkey) AS key
		from pg_index i
	) idx
	JOIN pg_class i ON i.oid = idx.indexrelid
	JOIN pg_class t ON t.oid = idx.indrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	LEFT JOIN (
	    select conindid, jsonb_object_agg(conname, contype) AS nametypes
	    from pg_constraint
	    group by conindid
	) con ON con.conindid = idx.indexrelid
	LEFT JOIN pg_attribute a ON (a.attrelid, a.attnum) = (idx.indrelid, idx.key)
	JOIN pg_am am ON am.oid = i.relam
	LEFT JOIN pg_opclass op ON op.oid = idx.indclass[idx.ord-1]
	LEFT JOIN pg_attribute a2 ON (a2.attrelid, a2.attnum) = (idx.indexrelid, idx.ord)
WHERE
	n.nspname IN (%s)
	AND t.relname IN (%s)
ORDER BY
	n.nspname, table_name, index_name, idx.ord
`
)
