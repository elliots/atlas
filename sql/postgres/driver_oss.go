// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.
//
// Modifications Copyright 2026 Elliot Shepherd

//go:build !ent

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"time"

	"ariga.io/atlas/schemahcl"
	"ariga.io/atlas/sql/internal/specutil"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlclient"
	"ariga.io/atlas/sql/sqlspec"

	"github.com/zclconf/go-cty/cty"
)

type (
	// Driver represents a PostgreSQL driver for introspecting database schemas,
	// generating diff between schema elements and apply migrations changes.
	Driver struct {
		*conn
		schema.Differ
		schema.Inspector
		migrate.PlanApplier
	}

	// database connection and its information.
	conn struct {
		schema.ExecQuerier
		// The schema in the `search_path` parameter (if given).
		schema string
		// Maps to the connection default_table_access_method parameter.
		accessMethod string
		// System variables that are set on `Open`.
		version int
		crdb    bool
	}
)

var _ interface {
	migrate.StmtScanner
	schema.TypeParseFormatter
} = (*Driver)(nil)

// DriverName holds the name used for registration.
const DriverName = "postgres"

func init() {
	sqlclient.Register(
		DriverName,
		sqlclient.OpenerFunc(opener),
		sqlclient.RegisterDriverOpener(Open),
		sqlclient.RegisterFlavours("postgresql"),
		sqlclient.RegisterCodec(codec, codec),
		sqlclient.RegisterURLParser(parser{}),
	)
}

func opener(_ context.Context, u *url.URL) (*sqlclient.Client, error) {
	ur := parser{}.ParseURL(u)
	db, err := sql.Open(DriverName, ur.DSN)
	if err != nil {
		return nil, err
	}
	drv, err := Open(db)
	if err != nil {
		if cerr := db.Close(); cerr != nil {
			err = fmt.Errorf("%w: %v", err, cerr)
		}
		return nil, err
	}
	switch drv := drv.(type) {
	case *Driver:
		drv.schema = ur.Schema
	case noLockDriver:
		drv.noLocker.(*Driver).schema = ur.Schema
	}
	return &sqlclient.Client{
		Name:   DriverName,
		DB:     db,
		URL:    ur,
		Driver: drv,
	}, nil
}

// Open opens a new PostgreSQL driver.
func Open(db schema.ExecQuerier) (migrate.Driver, error) {
	c := &conn{ExecQuerier: db}
	rows, err := db.QueryContext(context.Background(), paramsQuery)
	if err != nil {
		return nil, fmt.Errorf("postgres: scanning system variables: %w", err)
	}
	var ver, am, crdb sql.NullString
	if err := sqlx.ScanOne(rows, &ver, &am, &crdb); err != nil {
		return nil, fmt.Errorf("postgres: scanning system variables: %w", err)
	}
	if c.version, err = strconv.Atoi(ver.String); err != nil {
		return nil, fmt.Errorf("postgres: malformed version: %s: %w", ver.String, err)
	}
	if c.version < 10_00_00 {
		return nil, fmt.Errorf("postgres: unsupported postgres version: %d", c.version)
	}
	c.accessMethod = am.String
	if c.crdb = sqlx.ValidString(crdb); c.crdb {
		return noLockDriver{
			&Driver{
				conn:        c,
				Differ:      &sqlx.Diff{DiffDriver: &crdbDiff{diff{c}}},
				Inspector:   &crdbInspect{inspect{c}},
				PlanApplier: &planApply{c},
			},
		}, nil
	}
	return &Driver{
		conn:        c,
		Differ:      &sqlx.Diff{DiffDriver: &diff{c}},
		Inspector:   &inspect{c},
		PlanApplier: &planApply{c},
	}, nil
}

func (d *Driver) dev() *sqlx.DevDriver {
	return &sqlx.DevDriver{
		Driver: d,
		PatchObject: func(s *schema.Schema, o schema.Object) {
			if e, ok := o.(*schema.EnumType); ok {
				e.Schema = s
			}
		},
	}
}

// NormalizeRealm returns the normal representation of the given database.
func (d *Driver) NormalizeRealm(ctx context.Context, r *schema.Realm) (*schema.Realm, error) {
	return d.dev().NormalizeRealm(ctx, r)
}

// NormalizeSchema returns the normal representation of the given database.
func (d *Driver) NormalizeSchema(ctx context.Context, s *schema.Schema) (*schema.Schema, error) {
	return d.dev().NormalizeSchema(ctx, s)
}

// Lock implements the schema.Locker interface.
func (d *Driver) Lock(ctx context.Context, name string, timeout time.Duration) (schema.UnlockFunc, error) {
	conn, err := sqlx.SingleConn(ctx, d.ExecQuerier)
	if err != nil {
		return nil, err
	}
	h := fnv.New32()
	h.Write([]byte(name))
	id := h.Sum32()
	if err := acquire(ctx, conn, id, timeout); err != nil {
		conn.Close()
		return nil, err
	}
	return func() error {
		defer conn.Close()
		rows, err := conn.QueryContext(ctx, "SELECT pg_advisory_unlock($1)", id)
		if err != nil {
			return err
		}
		switch released, err := sqlx.ScanNullBool(rows); {
		case err != nil:
			return err
		case !released.Valid || !released.Bool:
			return fmt.Errorf("sql/postgres: failed releasing lock %d", id)
		}
		return nil
	}, nil
}

// Snapshot implements migrate.Snapshoter.
func (d *Driver) Snapshot(ctx context.Context) (migrate.RestoreFunc, error) {
	// Postgres will only then be considered bound to a schema if the `search_path` was given.
	// In all other cases, the connection is considered bound to the realm.
	if d.schema != "" {
		s, err := d.InspectSchema(ctx, d.schema, nil)
		if err != nil {
			return nil, err
		}
		if len(s.Tables) > 0 {
			return nil, &migrate.NotCleanError{
				State:  schema.NewRealm(s),
				Reason: fmt.Sprintf("found table %q in connected schema", s.Tables[0].Name),
			}
		}
		return d.SchemaRestoreFunc(s), nil
	}
	// Not bound to a schema.
	realm, err := d.InspectRealm(ctx, nil)
	if err != nil {
		return nil, err
	}
	restore := d.RealmRestoreFunc(realm)
	// Postgres is considered clean, if there are no schemas or the public schema has no tables.
	if len(realm.Schemas) == 0 {
		return restore, nil
	}
	if s, ok := realm.Schema("public"); len(realm.Schemas) == 1 && ok {
		if len(s.Tables) > 0 {
			return nil, &migrate.NotCleanError{
				State:  realm,
				Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0].Name, s.Name),
			}
		}
		return restore, nil
	}
	return nil, &migrate.NotCleanError{
		State:  realm,
		Reason: fmt.Sprintf("found schema %q", realm.Schemas[0].Name),
	}
}

// SchemaRestoreFunc returns a function that restores the given schema to its desired state.
func (d *Driver) SchemaRestoreFunc(desired *schema.Schema) migrate.RestoreFunc {
	return func(ctx context.Context) error {
		current, err := d.InspectSchema(ctx, desired.Name, nil)
		if err != nil {
			return err
		}
		changes, err := d.SchemaDiff(current, desired)
		if err != nil {
			return err
		}
		return d.ApplyChanges(ctx, withCascade(changes))
	}
}

// RealmRestoreFunc returns a function that restores the given realm to its desired state.
func (d *Driver) RealmRestoreFunc(desired *schema.Realm) migrate.RestoreFunc {
	// Default behavior for Postgres is to have a single "public" schema.
	// In that case, all other schemas are dropped, but this one is cleared
	// object by object. To keep process faster, we drop the schema and recreate it.
	if !d.crdb && len(desired.Schemas) == 1 && desired.Schemas[0].Name == "public" {
		if pb := desired.Schemas[0]; len(pb.Tables)+len(pb.Views)+len(pb.Funcs)+len(pb.Procs)+len(pb.Objects) == 0 {
			return func(ctx context.Context) error {
				current, err := d.InspectRealm(ctx, nil)
				if err != nil {
					return err
				}
				changes, err := d.RealmDiff(current, desired)
				if err != nil {
					return err
				}
				// If there is no diff, do nothing.
				if len(changes) == 0 {
					return nil
				}
				// Else, prefer to drop the public schema and apply
				// database changes instead of executing changes one by one.
				if changes, err = d.RealmDiff(current, &schema.Realm{Attrs: desired.Attrs, Objects: desired.Objects}); err != nil {
					return err
				}
				if err := d.ApplyChanges(ctx, withCascade(changes)); err != nil {
					return err
				}
				// Recreate the public schema.
				return d.ApplyChanges(ctx, []schema.Change{
					&schema.AddSchema{S: pb, Extra: []schema.Clause{&schema.IfExists{}}},
				})
			}
		}
	}
	return func(ctx context.Context) (err error) {
		current, err := d.InspectRealm(ctx, nil)
		if err != nil {
			return err
		}
		changes, err := d.RealmDiff(current, desired)
		if err != nil {
			return err
		}
		return d.ApplyChanges(ctx, withCascade(changes))
	}
}

func withCascade(changes schema.Changes) schema.Changes {
	for _, c := range changes {
		switch c := c.(type) {
		case *schema.DropTable:
			c.Extra = append(c.Extra, &schema.IfExists{}, &Cascade{})
		case *schema.DropView:
			c.Extra = append(c.Extra, &schema.IfExists{}, &Cascade{})
		case *schema.DropProc:
			c.Extra = append(c.Extra, &schema.IfExists{}, &Cascade{})
		case *schema.DropFunc:
			c.Extra = append(c.Extra, &schema.IfExists{}, &Cascade{})
		case *schema.DropObject:
			c.Extra = append(c.Extra, &schema.IfExists{}, &Cascade{})
		}
	}
	return changes
}

// CheckClean implements migrate.CleanChecker.
func (d *Driver) CheckClean(ctx context.Context, revT *migrate.TableIdent) error {
	if revT == nil { // accept nil values
		revT = &migrate.TableIdent{}
	}
	if d.schema != "" {
		switch s, err := d.InspectSchema(ctx, d.schema, nil); {
		case err != nil:
			return err
		case len(s.Tables) == 0, (revT.Schema == "" || s.Name == revT.Schema) && len(s.Tables) == 1 && s.Tables[0].Name == revT.Name:
			return nil
		default:
			return &migrate.NotCleanError{State: schema.NewRealm(s), Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0].Name, s.Name)}
		}
	}
	r, err := d.InspectRealm(ctx, nil)
	if err != nil {
		return err
	}
	for _, s := range r.Schemas {
		switch {
		case len(s.Tables) == 0 && s.Name == "public":
		case len(s.Tables) == 0 || s.Name != revT.Schema:
			return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found schema %q", s.Name)}
		case len(s.Tables) > 1:
			return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found %d tables in schema %q", len(s.Tables), s.Name)}
		case len(s.Tables) == 1 && s.Tables[0].Name != revT.Name:
			return &migrate.NotCleanError{State: r, Reason: fmt.Sprintf("found table %q in schema %q", s.Tables[0].Name, s.Name)}
		}
	}
	return nil
}

// Version returns the version of the connected database.
func (d *Driver) Version() string {
	return strconv.Itoa(d.conn.version)
}

// FormatType converts schema type to its column form in the database.
func (*Driver) FormatType(t schema.Type) (string, error) {
	return FormatType(t)
}

// ParseType returns the schema.Type value represented by the given string.
func (*Driver) ParseType(s string) (schema.Type, error) {
	return ParseType(s)
}

// StmtBuilder is a helper method used to build statements with PostgreSQL formatting.
func (*Driver) StmtBuilder(opts migrate.PlanOptions) *sqlx.Builder {
	return &sqlx.Builder{
		QuoteOpening: '"',
		QuoteClosing: '"',
		Schema:       opts.SchemaQualifier,
		Indent:       opts.Indent,
	}
}

// ScanStmts implements migrate.StmtScanner.
func (*Driver) ScanStmts(input string) ([]*migrate.Stmt, error) {
	return (&migrate.Scanner{
		ScannerOptions: migrate.ScannerOptions{
			MatchBegin:       true,
			MatchBeginAtomic: true,
			MatchDollarQuote: true,
			EscapedStringExt: true,
		},
	}).Scan(input)
}

// Use pg_try_advisory_lock to avoid deadlocks between multiple executions of Atlas (commonly tests).
// The common case is as follows: a process (P1) of Atlas takes a lock, and another process (P2) of
// Atlas waits for the lock. Now if P1 execute "CREATE INDEX CONCURRENTLY" (either in apply or diff),
// the command waits all active transactions that can potentially changed the index to be finished.
// P2 can be executed in a transaction block (opened explicitly by Atlas), or a single statement tx
// also known as "autocommit mode". Read more: https://www.postgresql.org/docs/current/sql-begin.html.
func acquire(ctx context.Context, conn schema.ExecQuerier, id uint32, timeout time.Duration) error {
	var (
		inter = 25
		start = time.Now()
	)
	for {
		rows, err := conn.QueryContext(ctx, "SELECT pg_try_advisory_lock($1)", id)
		if err != nil {
			return err
		}
		switch acquired, err := sqlx.ScanNullBool(rows); {
		case err != nil:
			return err
		case acquired.Bool:
			return nil
		case time.Since(start) > timeout:
			return schema.ErrLocked
		default:
			if err := rows.Close(); err != nil {
				return err
			}
			// 25ms~50ms, 50ms~100ms, ..., 800ms~1.6s, 1s~2s.
			d := min(time.Duration(inter)*time.Millisecond, time.Second)
			time.Sleep(d + time.Duration(rand.Intn(int(d))))
			inter += inter
		}
	}
}

// supportsIndexInclude reports if the server supports the INCLUDE clause.
func (c *conn) supportsIndexInclude() bool {
	return c.version >= 11_00_00
}

// supportsIndexNullsDistinct reports if the server supports the NULLS [NOT] DISTINCT clause.
func (c *conn) supportsIndexNullsDistinct() bool {
	return c.version >= 15_00_00
}

type parser struct{}

// ParseURL implements the sqlclient.URLParser interface.
func (parser) ParseURL(u *url.URL) *sqlclient.URL {
	return &sqlclient.URL{URL: u, DSN: u.String(), Schema: u.Query().Get("search_path")}
}

// ChangeSchema implements the sqlclient.SchemaChanger interface.
func (parser) ChangeSchema(u *url.URL, s string) *url.URL {
	nu := *u
	q := nu.Query()
	q.Set("search_path", s)
	nu.RawQuery = q.Encode()
	return &nu
}

// Standard column types (and their aliases) as defined in
// PostgreSQL codebase/website.
const (
	TypeBit     = "bit"
	TypeBitVar  = "bit varying"
	TypeBoolean = "boolean"
	TypeBool    = "bool" // boolean.
	TypeBytea   = "bytea"

	TypeCharacter = "character"
	TypeChar      = "char" // character
	TypeCharVar   = "character varying"
	TypeVarChar   = "varchar" // character varying
	TypeText      = "text"
	TypeBPChar    = "bpchar" // blank-padded character.
	typeName      = "name"   // internal type for object names

	TypeSmallInt = "smallint"
	TypeInteger  = "integer"
	TypeBigInt   = "bigint"
	TypeInt      = "int"  // integer.
	TypeInt2     = "int2" // smallint.
	TypeInt4     = "int4" // integer.
	TypeInt8     = "int8" // bigint.

	TypeXID  = "xid"  // transaction identifier.
	TypeXID8 = "xid8" // 64-bit transaction identifier.

	TypeCIDR     = "cidr"
	TypeInet     = "inet"
	TypeMACAddr  = "macaddr"
	TypeMACAddr8 = "macaddr8"

	TypeCircle  = "circle"
	TypeLine    = "line"
	TypeLseg    = "lseg"
	TypeBox     = "box"
	TypePath    = "path"
	TypePolygon = "polygon"
	TypePoint   = "point"

	TypeDate          = "date"
	TypeTime          = "time"   // time without time zone
	TypeTimeTZ        = "timetz" // time with time zone
	TypeTimeWTZ       = "time with time zone"
	TypeTimeWOTZ      = "time without time zone"
	TypeTimestamp     = "timestamp" // timestamp without time zone
	TypeTimestampTZ   = "timestamptz"
	TypeTimestampWTZ  = "timestamp with time zone"
	TypeTimestampWOTZ = "timestamp without time zone"

	TypeDouble = "double precision"
	TypeReal   = "real"
	TypeFloat8 = "float8" // double precision
	TypeFloat4 = "float4" // real
	TypeFloat  = "float"  // float(p).

	TypeNumeric = "numeric"
	TypeDecimal = "decimal" // numeric

	TypeSmallSerial = "smallserial" // smallint with auto_increment.
	TypeSerial      = "serial"      // integer with auto_increment.
	TypeBigSerial   = "bigserial"   // bigint with auto_increment.
	TypeSerial2     = "serial2"     // smallserial
	TypeSerial4     = "serial4"     // serial
	TypeSerial8     = "serial8"     // bigserial

	TypeArray       = "array"
	TypeXML         = "xml"
	TypeJSON        = "json"
	TypeJSONB       = "jsonb"
	TypeUUID        = "uuid"
	TypeMoney       = "money"
	TypeInterval    = "interval"
	TypeTSQuery     = "tsquery"
	TypeTSVector    = "tsvector"
	TypeUserDefined = "user-defined"

	TypeInt4Range      = "int4range"
	TypeInt4MultiRange = "int4multirange"
	TypeInt8Range      = "int8range"
	TypeInt8MultiRange = "int8multirange"
	TypeNumRange       = "numrange"
	TypeNumMultiRange  = "nummultirange"
	TypeTSRange        = "tsrange"
	TypeTSMultiRange   = "tsmultirange"
	TypeTSTZRange      = "tstzrange"
	TypeTSTZMultiRange = "tstzmultirange"
	TypeDateRange      = "daterange"
	TypeDateMultiRange = "datemultirange"

	// PostgreSQL internal object types and their aliases.
	typeOID           = "oid"
	typeRegClass      = "regclass"
	typeRegCollation  = "regcollation"
	typeRegConfig     = "regconfig"
	typeRegDictionary = "regdictionary"
	typeRegNamespace  = "regnamespace"
	typeRegOper       = "regoper"
	typeRegOperator   = "regoperator"
	typeRegProc       = "regproc"
	typeRegProcedure  = "regprocedure"
	typeRegRole       = "regrole"
	typeRegType       = "regtype"

	// PostgreSQL of supported pseudo-types.
	typeAny          = "any"
	typeAnyElement   = "anyelement"
	typeAnyArray     = "anyarray"
	typeAnyNonArray  = "anynonarray"
	typeAnyEnum      = "anyenum"
	typeInternal     = "internal"
	typeRecord       = "record"
	typeTrigger      = "trigger"
	typeEventTrigger = "event_trigger"
	typeVoid         = "void"
	typeUnknown      = "unknown"
)

// List of supported index types.
const (
	IndexTypeBTree       = "BTREE"
	IndexTypeBRIN        = "BRIN"
	IndexTypeHash        = "HASH"
	IndexTypeGIN         = "GIN"
	IndexTypeGiST        = "GIST"
	IndexTypeSPGiST      = "SPGIST"
	defaultPagesPerRange = 128
	defaultListLimit     = 4 * 1024
	defaultBtreeFill     = 90
)

const (
	storageParamFillFactor = "fillfactor"
	storageParamDedup      = "deduplicate_items"
	storageParamBuffering  = "buffering"
	storageParamFastUpdate = "fastupdate"
	storageParamListLimit  = "gin_pending_list_limit"
	storageParamPagesRange = "pages_per_range"
	storageParamAutoSum    = "autosummarize"
)

const (
	bufferingOff    = "OFF"
	bufferingOn     = "ON"
	bufferingAuto   = "AUTO"
	storageParamOn  = "ON"
	storageParamOff = "OFF"
)

// List of "GENERATED" types.
const (
	GeneratedTypeAlways    = "ALWAYS"
	GeneratedTypeByDefault = "BY_DEFAULT" // BY DEFAULT.
)

// List of PARTITION KEY types.
const (
	PartitionTypeRange = "RANGE"
	PartitionTypeList  = "LIST"
	PartitionTypeHash  = "HASH"
)

var (
	specOptions []schemahcl.Option
	specFuncs   = &specutil.SchemaFuncs{
		Table: tableSpec,
		View:  viewSpec,
		Func:  funcSpec,
		Proc:  procSpec,
	}
	scanFuncs = &specutil.ScanFuncs{
		Table:    convertTable,
		View:     convertView,
		Func:     convertFunc,
		Proc:     convertProc,
		Triggers: convertTriggers,
	}
)

func tableAttrsSpec(*schema.Table, *sqlspec.Table) {
	// unimplemented.
}

func convertTableAttrs(*sqlspec.Table, *schema.Table) error {
	return nil // unimplemented.
}

// tableAttrDiff allows extending table attributes diffing with build-specific logic.
func (*diff) tableAttrDiff(_, _ *schema.Table) ([]schema.Change, error) {
	return nil, nil // unimplemented.
}

// addTableAttrs allows extending table attributes creation with build-specific logic.
func (*state) addTableAttrs(_ *schema.AddTable) {
	// unimplemented.
}

// alterTableAttr allows extending table attributes alteration with build-specific logic.
func (s *state) alterTableAttr(*sqlx.Builder, *schema.ModifyAttr) {
	// unimplemented.
}

func realmObjectsSpec(*doc, *schema.Realm) error {
	return nil // unimplemented.
}

func triggersSpec(triggers []*schema.Trigger, d *doc) error {
	for _, t := range triggers {
		spec := &sqlspec.Trigger{
			Name: t.Name,
		}
		// Set the "on" reference to the owning table or view.
		switch {
		case t.Table != nil:
			spec.On = schemahcl.BuildRef([]schemahcl.PathIndex{{T: "table", V: []string{t.Table.Name}}})
		case t.View != nil:
			spec.On = schemahcl.BuildRef([]schemahcl.PathIndex{{T: "view", V: []string{t.View.Name}}})
		}
		// Build the timing block (before/after/instead_of) with event flags.
		if t.ActionTime != "" && len(t.Events) > 0 {
			var blockType string
			switch t.ActionTime {
			case schema.TriggerTimeBefore:
				blockType = "before"
			case schema.TriggerTimeAfter:
				blockType = "after"
			case schema.TriggerTimeInstead:
				blockType = "instead_of"
			}
			if blockType != "" {
				block := &schemahcl.Resource{Type: blockType}
				for _, e := range t.Events {
					switch strings.ToUpper(e.Name) {
					case "INSERT":
						block.Attrs = append(block.Attrs, schemahcl.BoolAttr("insert", true))
					case "UPDATE":
						block.Attrs = append(block.Attrs, schemahcl.BoolAttr("update", true))
					case "DELETE":
						block.Attrs = append(block.Attrs, schemahcl.BoolAttr("delete", true))
					case "TRUNCATE":
						block.Attrs = append(block.Attrs, schemahcl.BoolAttr("truncate", true))
					}
				}
				spec.Extra.Children = append(spec.Extra.Children, block)
			}
		}
		// Use execute block with function reference if the body references a function.
		// Otherwise, use inline "as" attribute.
		if t.Body != "" {
			// Try to extract the function name from the trigger body for the execute block.
			if fn := extractExecuteFunc(t.Body); fn != "" {
				block := &schemahcl.Resource{
					Type: "execute",
					Attrs: []*schemahcl.Attr{
						schemahcl.RefAttr("function", schemahcl.BuildRef([]schemahcl.PathIndex{{T: "function", V: []string{fn}}})),
					},
				}
				spec.Extra.Children = append(spec.Extra.Children, block)
			} else {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("as", sqlspec.MightHeredoc(t.Body)))
			}
		}
		d.Triggers = append(d.Triggers, spec)
	}
	return nil
}

// extractExecuteFunc extracts the function name from a trigger body like
// "CREATE TRIGGER ... EXECUTE FUNCTION schema.funcname()".
func extractExecuteFunc(body string) string {
	upper := strings.ToUpper(body)
	idx := strings.Index(upper, "EXECUTE FUNCTION ")
	if idx == -1 {
		idx = strings.Index(upper, "EXECUTE PROCEDURE ")
	}
	if idx == -1 {
		return ""
	}
	// Find the start of the function name after "EXECUTE FUNCTION ".
	keyword := "EXECUTE FUNCTION "
	if strings.Contains(upper[idx:], "EXECUTE PROCEDURE ") {
		keyword = "EXECUTE PROCEDURE "
	}
	rest := body[idx+len(keyword):]
	// Strip trailing parens and args: "public.update_updated_at()" → "public.update_updated_at"
	if paren := strings.Index(rest, "("); paren != -1 {
		rest = rest[:paren]
	}
	rest = strings.TrimSpace(rest)
	// Strip schema qualifier: "public.update_updated_at" → "update_updated_at"
	if dot := strings.LastIndex(rest, "."); dot != -1 {
		rest = rest[dot+1:]
	}
	return rest
}

// parseFuncArgs parses the argument string from pg_get_function_arguments into schema.FuncArgs.
// Format: "arg1 type1, arg2 type2" or "IN arg1 type1, OUT arg2 type2".
func parseFuncArgs(argsStr string) []*schema.FuncArg {
	if argsStr == "" {
		return nil
	}
	var args []*schema.FuncArg
	for _, part := range strings.Split(argsStr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		arg := &schema.FuncArg{}
		// Check for mode prefix.
		upper := strings.ToUpper(part)
		switch {
		case strings.HasPrefix(upper, "INOUT "):
			arg.Mode = schema.FuncArgModeInOut
			part = strings.TrimSpace(part[6:])
		case strings.HasPrefix(upper, "IN "):
			arg.Mode = schema.FuncArgModeIn
			part = strings.TrimSpace(part[3:])
		case strings.HasPrefix(upper, "OUT "):
			arg.Mode = schema.FuncArgModeOut
			part = strings.TrimSpace(part[4:])
		case strings.HasPrefix(upper, "VARIADIC "):
			arg.Mode = schema.FuncArgModeVariadic
			part = strings.TrimSpace(part[9:])
		}
		// Split into name and type. The last word(s) are the type.
		// But arg might be unnamed: just "integer".
		// If there are 2+ tokens and the first is not a known type, treat it as name.
		tokens := strings.Fields(part)
		if len(tokens) == 1 {
			// Unnamed arg, just type.
			if t, err := ParseType(tokens[0]); err == nil {
				arg.Type = t
			}
		} else {
			// Try to parse everything after the first token as a type.
			typStr := strings.Join(tokens[1:], " ")
			if t, err := ParseType(typStr); err == nil {
				arg.Name = tokens[0]
				arg.Type = t
			} else if t, err := ParseType(part); err == nil {
				// Entire string is a type (e.g., "double precision").
				arg.Type = t
			}
		}
		if arg.Type != nil {
			args = append(args, arg)
		}
	}
	return args
}

// extractFuncBody extracts just the function body from a pg_get_functiondef DDL string.
// It looks for dollar-quoted strings ($tag$...$tag$) or single-quoted strings.
func extractFuncBody(def string) string {
	// Find dollar-quoted body: $tag$...$tag$
	idx := strings.Index(def, "$")
	if idx == -1 {
		return ""
	}
	// Find the end of the opening tag.
	endTag := strings.Index(def[idx+1:], "$")
	if endTag == -1 {
		return ""
	}
	tag := def[idx : idx+endTag+2] // e.g., "$function$"
	bodyStart := idx + len(tag)
	bodyEnd := strings.LastIndex(def, tag)
	if bodyEnd <= bodyStart {
		return ""
	}
	body := strings.TrimSpace(def[bodyStart:bodyEnd])
	return body
}

// langVar maps postgres language names to their HCL enum-style values.
func langVar(lang string) string {
	switch strings.ToLower(lang) {
	case "plpgsql":
		return "PLpgSQL"
	case "sql":
		return "SQL"
	case "c":
		return "C"
	default:
		return lang
	}
}

// funcSpec converts a schema.Func to a sqlspec.Func for HCL marshaling.
func funcSpec(f *schema.Func) (*sqlspec.Func, error) {
	spec := &sqlspec.Func{
		Name: f.Name,
	}
	if f.Lang != "" {
		spec.Lang = schemahcl.RefValue(langVar(f.Lang))
	}
	for _, a := range f.Args {
		arg := &sqlspec.FuncArg{
			Name: a.Name,
		}
		if a.Type != nil {
			c, err := columnTypeSpec(a.Type)
			if err != nil {
				return nil, fmt.Errorf("function %q arg %q type: %w", f.Name, a.Name, err)
			}
			arg.Type = c.Type
		}
		if a.Mode != "" {
			arg.Extra.Attrs = append(arg.Extra.Attrs, schemahcl.StringAttr("mode", string(a.Mode)))
		}
		if a.Default != nil {
			arg.Default = cty.StringVal(a.Default.(*schema.RawExpr).X)
		}
		spec.Args = append(spec.Args, arg)
	}
	// Return type as additional attribute.
	if f.Ret != nil {
		if u, ok := f.Ret.(*schema.UnsupportedType); ok {
			// Complex return types (e.g., TABLE(...)) are stored as raw strings.
			spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.RawAttr("return", u.T))
		} else {
			c, err := columnTypeSpec(f.Ret)
			if err != nil {
				return nil, fmt.Errorf("function %q return type: %w", f.Name, err)
			}
			spec.Extra.Attrs = append(spec.Extra.Attrs, specutil.TypeAttr("return", c.Type))
		}
	}
	// Extract just the body from the full DDL for HCL output.
	if f.Body != "" {
		body := extractFuncBody(f.Body)
		if body == "" {
			body = f.Body // Fallback to full body if extraction fails.
		}
		spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("as", sqlspec.MightHeredoc(body)))
	}
	return spec, nil
}

// procSpec converts a schema.Proc to a sqlspec.Func for HCL marshaling.
func procSpec(p *schema.Proc) (*sqlspec.Func, error) {
	spec := &sqlspec.Func{
		Name: p.Name,
	}
	if p.Lang != "" {
		spec.Lang = schemahcl.RefValue(langVar(p.Lang))
	}
	for _, a := range p.Args {
		arg := &sqlspec.FuncArg{
			Name: a.Name,
		}
		if a.Type != nil {
			c, err := columnTypeSpec(a.Type)
			if err != nil {
				return nil, fmt.Errorf("procedure %q arg %q type: %w", p.Name, a.Name, err)
			}
			arg.Type = c.Type
		}
		if a.Mode != "" {
			arg.Extra.Attrs = append(arg.Extra.Attrs, schemahcl.StringAttr("mode", string(a.Mode)))
		}
		if a.Default != nil {
			arg.Default = cty.StringVal(a.Default.(*schema.RawExpr).X)
		}
		spec.Args = append(spec.Args, arg)
	}
	if p.Body != "" {
		body := extractFuncBody(p.Body)
		if body == "" {
			body = p.Body
		}
		spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("as", sqlspec.MightHeredoc(body)))
	}
	return spec, nil
}

// convertFunc converts a sqlspec.Func (HCL) to a schema.Func.
func convertFunc(spec *sqlspec.Func, parent *schema.Schema) (*schema.Func, error) {
	f := &schema.Func{
		Name:   spec.Name,
		Schema: parent,
	}
	// Read lang from struct field or from Extra attrs (enum ref).
	if spec.Lang.Type() == cty.String {
		f.Lang = spec.Lang.AsString()
	} else if a, ok := spec.Attr("lang"); ok {
		if s, err := a.String(); err == nil {
			f.Lang = s
		} else if ref, err := a.Ref(); err == nil {
			f.Lang = ref
		}
	}
	for _, sa := range spec.Args {
		arg := &schema.FuncArg{
			Name: sa.Name,
		}
		if sa.Type != nil {
			t, err := TypeRegistry.Type(sa.Type, sa.Extra.Attrs)
			if err != nil {
				return nil, fmt.Errorf("function %q arg %q: %w", f.Name, sa.Name, err)
			}
			arg.Type = t
		}
		if m, ok := sa.Attr("mode"); ok {
			s, err := m.String()
			if err != nil {
				return nil, err
			}
			arg.Mode = schema.FuncArgMode(s)
		}
		if sa.Default.Type() == cty.String {
			arg.Default = &schema.RawExpr{X: sa.Default.AsString()}
		}
		f.Args = append(f.Args, arg)
	}
	if rt, ok := spec.Attr("return"); ok {
		typ, err := rt.Type()
		if err != nil {
			// Might be a raw expression (e.g., TABLE(...)).
			if rt.IsRawExpr() {
				if raw, err := rt.RawExpr(); err == nil {
					f.Ret = &schema.UnsupportedType{T: raw.X}
				}
			}
		} else {
			t, err := TypeRegistry.Type(typ, nil)
			if err != nil {
				return nil, fmt.Errorf("function %q return type: %w", f.Name, err)
			}
			f.Ret = t
		}
	}
	if a, ok := spec.Attr("as"); ok {
		s, err := a.String()
		if err != nil {
			return nil, err
		}
		// Reconstruct full DDL for migration (addFunc uses Body as the CREATE FUNCTION statement).
		f.Body = buildFuncDDL(f, s)
	}
	return f, nil
}

// convertProc converts a sqlspec.Func (HCL) to a schema.Proc.
func convertProc(spec *sqlspec.Func, parent *schema.Schema) (*schema.Proc, error) {
	p := &schema.Proc{
		Name:   spec.Name,
		Schema: parent,
	}
	if spec.Lang.Type() == cty.String {
		p.Lang = spec.Lang.AsString()
	} else if a, ok := spec.Attr("lang"); ok {
		if s, err := a.String(); err == nil {
			p.Lang = s
		} else if ref, err := a.Ref(); err == nil {
			p.Lang = ref
		}
	}
	for _, sa := range spec.Args {
		arg := &schema.FuncArg{
			Name: sa.Name,
		}
		if sa.Type != nil {
			t, err := TypeRegistry.Type(sa.Type, sa.Extra.Attrs)
			if err != nil {
				return nil, fmt.Errorf("procedure %q arg %q: %w", p.Name, sa.Name, err)
			}
			arg.Type = t
		}
		if m, ok := sa.Attr("mode"); ok {
			s, err := m.String()
			if err != nil {
				return nil, err
			}
			arg.Mode = schema.FuncArgMode(s)
		}
		if sa.Default.Type() == cty.String {
			arg.Default = &schema.RawExpr{X: sa.Default.AsString()}
		}
		p.Args = append(p.Args, arg)
	}
	if a, ok := spec.Attr("as"); ok {
		s, err := a.String()
		if err != nil {
			return nil, err
		}
		p.Body = buildProcDDL(p, s)
	}
	return p, nil
}

// convertTriggers converts sqlspec.Trigger specs and attaches them to their owning tables/views.
func convertTriggers(r *schema.Realm, triggers []*sqlspec.Trigger) error {
	for _, spec := range triggers {
		t := &schema.Trigger{
			Name: spec.Name,
		}
		// Parse timing blocks (before/after/instead_of).
		for _, blockType := range []string{"before", "after", "instead_of"} {
			if block, ok := spec.Extra.Resource(blockType); ok {
				switch blockType {
				case "before":
					t.ActionTime = schema.TriggerTimeBefore
				case "after":
					t.ActionTime = schema.TriggerTimeAfter
				case "instead_of":
					t.ActionTime = schema.TriggerTimeInstead
				}
				if a, ok := block.Attr("insert"); ok {
					if b, _ := a.Bool(); b {
						t.Events = append(t.Events, schema.TriggerEventInsert)
					}
				}
				if a, ok := block.Attr("update"); ok {
					if b, _ := a.Bool(); b {
						t.Events = append(t.Events, schema.TriggerEventUpdate)
					}
				}
				if a, ok := block.Attr("delete"); ok {
					if b, _ := a.Bool(); b {
						t.Events = append(t.Events, schema.TriggerEventDelete)
					}
				}
				if a, ok := block.Attr("truncate"); ok {
					if b, _ := a.Bool(); b {
						t.Events = append(t.Events, schema.TriggerEventTruncate)
					}
				}
				}
		}
		// Default to FOR EACH ROW if not explicitly set.
		if t.For == "" {
			t.For = schema.TriggerForRow
		}
		// Parse execute block for function reference and reconstruct body.
		var execFunc string
		if block, ok := spec.Extra.Resource("execute"); ok {
			if a, ok := block.Attr("function"); ok {
				if ref, err := a.Ref(); err == nil {
					r := &schemahcl.Ref{V: ref}
					if names, err := r.ByType("function"); err == nil && len(names) > 0 {
						execFunc = names[0]
					}
				}
			}
		}
		if a, ok := spec.Attr("as"); ok {
			s, err := a.String()
			if err != nil {
				return err
			}
			t.Body = s
		}
		// Resolve the "on" reference to find the table or view.
		if spec.On != nil {
			found := false
			for _, s := range r.Schemas {
				for _, tbl := range s.Tables {
					ref := schemahcl.BuildRef([]schemahcl.PathIndex{{T: "table", V: []string{tbl.Name}}})
					if ref.V == spec.On.V {
						t.Table = tbl
						tbl.Triggers = append(tbl.Triggers, t)
						found = true
						break
					}
				}
				if found {
					break
				}
				for _, v := range s.Views {
					ref := schemahcl.BuildRef([]schemahcl.PathIndex{{T: "view", V: []string{v.Name}}})
					if ref.V == spec.On.V {
						t.View = v
						v.Triggers = append(v.Triggers, t)
						found = true
						break
					}
				}
				if found {
					break
				}
			}
		}
		// Populate Deps for ordering: trigger depends on its table/view and function.
		if t.Table != nil {
			t.Deps = append(t.Deps, t.Table)
		}
		if t.View != nil {
			t.Deps = append(t.Deps, t.View)
		}
		// Find the referenced function in the realm and add as dependency.
		if execFunc != "" {
			for _, s := range r.Schemas {
				for _, f := range s.Funcs {
					if f.Name == execFunc {
						t.Deps = append(t.Deps, f)
					}
				}
			}
		}
		// If the body was not set from an "as" attribute, reconstruct it from components.
		if t.Body == "" && execFunc != "" {
			t.Body = buildTriggerBody(t, execFunc)
		}
	}
	return nil
}

// buildTriggerBody reconstructs a CREATE TRIGGER DDL statement from parsed HCL components.
func buildTriggerBody(t *schema.Trigger, funcName string) string {
	var b strings.Builder
	b.WriteString("CREATE TRIGGER ")
	b.WriteString(t.Name)
	if t.ActionTime != "" {
		b.WriteString(" ")
		b.WriteString(strings.ToUpper(string(t.ActionTime)))
	}
	for i, e := range t.Events {
		if i > 0 {
			b.WriteString(" OR")
		}
		b.WriteString(" ")
		b.WriteString(strings.ToUpper(e.Name))
	}
	b.WriteString(" ON ")
	if t.Table != nil {
		if t.Table.Schema != nil {
			b.WriteString(t.Table.Schema.Name)
			b.WriteString(".")
		}
		b.WriteString(t.Table.Name)
	} else if t.View != nil {
		if t.View.Schema != nil {
			b.WriteString(t.View.Schema.Name)
			b.WriteString(".")
		}
		b.WriteString(t.View.Name)
	}
	if t.For != "" {
		b.WriteString(" FOR EACH ")
		b.WriteString(string(t.For))
	}
	b.WriteString(" EXECUTE FUNCTION ")
	b.WriteString(funcName)
	b.WriteString("()")
	return b.String()
}

// buildFuncDDL reconstructs a CREATE FUNCTION DDL from parsed HCL components.
func buildFuncDDL(f *schema.Func, body string) string {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE FUNCTION ")
	if f.Schema != nil {
		b.WriteString(f.Schema.Name)
		b.WriteString(".")
	}
	b.WriteString(f.Name)
	b.WriteString("(")
	for i, a := range f.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		if a.Mode != "" && a.Mode != schema.FuncArgModeIn {
			b.WriteString(string(a.Mode))
			b.WriteString(" ")
		}
		if a.Name != "" {
			b.WriteString(a.Name)
			b.WriteString(" ")
		}
		if a.Type != nil {
			if s, err := FormatType(a.Type); err == nil {
				b.WriteString(s)
			}
		}
		if a.Default != nil {
			b.WriteString(" DEFAULT ")
			b.WriteString(a.Default.(*schema.RawExpr).X)
		}
	}
	b.WriteString(")\n")
	if f.Ret != nil {
		b.WriteString(" RETURNS ")
		if u, ok := f.Ret.(*schema.UnsupportedType); ok {
			b.WriteString(u.T)
		} else if s, err := FormatType(f.Ret); err == nil {
			b.WriteString(s)
		}
		b.WriteString("\n")
	}
	b.WriteString(" LANGUAGE ")
	b.WriteString(f.Lang)
	b.WriteString("\nAS $function$\n")
	b.WriteString(body)
	b.WriteString("\n$function$\n")
	return b.String()
}

// buildProcDDL reconstructs a CREATE PROCEDURE DDL from parsed HCL components.
func buildProcDDL(p *schema.Proc, body string) string {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE PROCEDURE ")
	if p.Schema != nil {
		b.WriteString(p.Schema.Name)
		b.WriteString(".")
	}
	b.WriteString(p.Name)
	b.WriteString("(")
	for i, a := range p.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		if a.Mode != "" && a.Mode != schema.FuncArgModeIn {
			b.WriteString(string(a.Mode))
			b.WriteString(" ")
		}
		if a.Name != "" {
			b.WriteString(a.Name)
			b.WriteString(" ")
		}
		if a.Type != nil {
			if s, err := FormatType(a.Type); err == nil {
				b.WriteString(s)
			}
		}
		if a.Default != nil {
			b.WriteString(" DEFAULT ")
			b.WriteString(a.Default.(*schema.RawExpr).X)
		}
	}
	b.WriteString(")\n")
	b.WriteString(" LANGUAGE ")
	b.WriteString(p.Lang)
	b.WriteString("\nAS $procedure$\n")
	b.WriteString(body)
	b.WriteString("\n$procedure$\n")
	return b.String()
}

func (i *inspect) inspectViews(ctx context.Context, r *schema.Realm, opts *schema.InspectOptions) error {
	schemas := make(map[string]*schema.Schema)
	for _, s := range r.Schemas {
		schemas[s.Name] = s
	}
	query := viewsQuery
	args := make([]any, 0, len(schemas))
	idx := 0
	placeholders := make([]string, 0, len(schemas))
	for name := range schemas {
		idx++
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, name)
	}
	query = fmt.Sprintf(query, strings.Join(placeholders, ", "))
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: inspecting views: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			schemaName, viewName, def string
			isMaterialized             bool
		)
		if err := rows.Scan(&schemaName, &viewName, &def, &isMaterialized); err != nil {
			return fmt.Errorf("postgres: scanning view: %w", err)
		}
		s, ok := schemas[schemaName]
		if !ok {
			continue
		}
		v := schema.NewView(viewName, def).SetSchema(s)
		if isMaterialized {
			v.SetMaterialized(true)
		}
		s.Views = append(s.Views, v)
	}
	return rows.Err()
}

func (i *inspect) inspectFuncs(ctx context.Context, r *schema.Realm, opts *schema.InspectOptions) error {
	schemas := make(map[string]*schema.Schema)
	for _, s := range r.Schemas {
		schemas[s.Name] = s
	}
	placeholders := make([]string, 0, len(schemas))
	args := make([]any, 0, len(schemas))
	idx := 0
	for name := range schemas {
		idx++
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, name)
	}
	query := fmt.Sprintf(funcsQuery, strings.Join(placeholders, ", "))
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: inspecting functions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			schemaName, funcName, kind, def, lang, retType, funcArgs string
		)
		if err := rows.Scan(&schemaName, &funcName, &kind, &def, &lang, &retType, &funcArgs); err != nil {
			return fmt.Errorf("postgres: scanning function: %w", err)
		}
		s, ok := schemas[schemaName]
		if !ok {
			continue
		}
		args := parseFuncArgs(funcArgs)
		switch kind {
		case "f": // normal function
			f := &schema.Func{Name: funcName, Schema: s, Body: def, Lang: lang, Args: args}
			if retType != "" {
				if t, err := ParseType(retType); err == nil {
					f.Ret = t
				} else {
					// For complex return types like TABLE(...), store as-is.
					f.Ret = &schema.UnsupportedType{T: retType}
				}
			}
			s.Funcs = append(s.Funcs, f)
		case "p": // procedure
			p := &schema.Proc{Name: funcName, Schema: s, Body: def, Lang: lang, Args: args}
			s.Procs = append(s.Procs, p)
		}
	}
	return rows.Err()
}

func (*inspect) inspectTypes(context.Context, *schema.Realm, *schema.InspectOptions) error {
	return nil // unimplemented.
}

func (*inspect) inspectObjects(context.Context, *schema.Realm, *schema.InspectOptions) error {
	return nil // unimplemented.
}

func (i *inspect) inspectTriggers(ctx context.Context, r *schema.Realm, opts *schema.InspectOptions) error {
	schemas := make(map[string]*schema.Schema)
	for _, s := range r.Schemas {
		schemas[s.Name] = s
	}
	placeholders := make([]string, 0, len(schemas))
	args := make([]any, 0, len(schemas))
	idx := 0
	for name := range schemas {
		idx++
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, name)
	}
	query := fmt.Sprintf(triggersQuery, strings.Join(placeholders, ", "))
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: inspecting triggers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			schemaName, trigName, tableName, actionTiming, eventManip, orientation, trigDef string
		)
		if err := rows.Scan(&schemaName, &trigName, &tableName, &actionTiming, &eventManip, &orientation, &trigDef); err != nil {
			return fmt.Errorf("postgres: scanning trigger: %w", err)
		}
		s, ok := schemas[schemaName]
		if !ok {
			continue
		}
		t := &schema.Trigger{
			Name: trigName,
			Body: trigDef,
		}
		switch strings.ToUpper(actionTiming) {
		case "BEFORE":
			t.ActionTime = schema.TriggerTimeBefore
		case "AFTER":
			t.ActionTime = schema.TriggerTimeAfter
		case "INSTEAD OF":
			t.ActionTime = schema.TriggerTimeInstead
		}
		for _, ev := range strings.Split(eventManip, " OR ") {
			switch strings.TrimSpace(strings.ToUpper(ev)) {
			case "INSERT":
				t.Events = append(t.Events, schema.TriggerEventInsert)
			case "UPDATE":
				t.Events = append(t.Events, schema.TriggerEventUpdate)
			case "DELETE":
				t.Events = append(t.Events, schema.TriggerEventDelete)
			case "TRUNCATE":
				t.Events = append(t.Events, schema.TriggerEventTruncate)
			}
		}
		switch strings.ToUpper(orientation) {
		case "ROW":
			t.For = schema.TriggerForRow
		case "STATEMENT":
			t.For = schema.TriggerForStmt
		}
		// Attach to the table and set dependencies.
		for _, tbl := range s.Tables {
			if tbl.Name == tableName {
				t.Table = tbl
				t.Deps = append(t.Deps, tbl)
				tbl.Triggers = append(tbl.Triggers, t)
				break
			}
		}
		// If not found on tables, check views.
		if t.Table == nil {
			for _, v := range s.Views {
				if v.Name == tableName {
					t.View = v
					t.Deps = append(t.Deps, v)
					v.Triggers = append(v.Triggers, t)
					break
				}
			}
		}
		// Add dependency on the function referenced by the trigger.
		for _, f := range s.Funcs {
			if strings.Contains(t.Body, f.Name) {
				t.Deps = append(t.Deps, f)
			}
		}
	}
	return rows.Err()
}

func (*inspect) inspectDeps(context.Context, *schema.Realm, *schema.InspectOptions) error {
	return nil // unimplemented.
}

func (*inspect) inspectRealmObjects(context.Context, *schema.Realm, *schema.InspectOptions) error {
	return nil // unimplemented.
}

func (s *state) addView(add *schema.AddView) error {
	v := add.V
	cmd := "CREATE"
	if v.Materialized() {
		cmd += " MATERIALIZED"
	}
	cmd += " VIEW"
	b := s.Build(cmd)
	if sqlx.Has(add.Extra, &schema.IfNotExists{}) {
		b.P("IF NOT EXISTS")
	}
	b.View(v).P("AS").P(v.Def)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  add,
		Comment: fmt.Sprintf("create view %q", v.Name),
		Reverse: s.dropViewCmd(v),
	})
	return nil
}

func (s *state) dropView(drop *schema.DropView) error {
	v := drop.V
	cmd := s.dropViewCmd(v)
	s.append(&migrate.Change{
		Cmd:     cmd,
		Source:  drop,
		Comment: fmt.Sprintf("drop view %q", v.Name),
	})
	return nil
}

func (s *state) dropViewCmd(v *schema.View) string {
	b := s.Build("DROP")
	if v.Materialized() {
		b.P("MATERIALIZED")
	}
	b.P("VIEW")
	b.View(v)
	return b.String()
}

func (s *state) modifyView(modify *schema.ModifyView) error {
	// Always drop and recreate to handle column removals/renames safely.
	// Postgres CREATE OR REPLACE VIEW can't drop columns.
	if err := s.dropView(&schema.DropView{V: modify.From}); err != nil {
		return err
	}
	return s.addView(&schema.AddView{V: modify.To})
}

func (s *state) renameView(rename *schema.RenameView) {
	b := s.Build("ALTER")
	if rename.From.Materialized() {
		b.P("MATERIALIZED")
	}
	b.P("VIEW")
	b.View(rename.From).P("RENAME TO").Ident(rename.To.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  rename,
		Comment: fmt.Sprintf("rename view %q to %q", rename.From.Name, rename.To.Name),
	})
}

func (s *state) addFunc(add *schema.AddFunc) error {
	// Body contains the full CREATE FUNCTION statement from pg_get_functiondef.
	s.append(&migrate.Change{
		Cmd:     add.F.Body,
		Source:  add,
		Comment: fmt.Sprintf("create function %q", add.F.Name),
		Reverse: s.Build("DROP FUNCTION IF EXISTS").Func(add.F).String(),
	})
	return nil
}

func (s *state) dropFunc(drop *schema.DropFunc) error {
	b := s.Build("DROP FUNCTION")
	if sqlx.Has(drop.Extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	b.Func(drop.F)
	if sqlx.Has(drop.Extra, &Cascade{}) {
		b.P("CASCADE")
	}
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop function %q", drop.F.Name),
	})
	return nil
}

func (s *state) modifyFunc(modify *schema.ModifyFunc) error {
	// For functions, we just re-create using CREATE OR REPLACE
	// which is embedded in the Body (full definition from pg_get_functiondef).
	s.append(&migrate.Change{
		Cmd:     modify.To.Body,
		Source:  modify,
		Comment: fmt.Sprintf("modify function %q", modify.To.Name),
	})
	return nil
}

func (s *state) renameFunc(rename *schema.RenameFunc) error {
	b := s.Build("ALTER FUNCTION")
	b.Func(rename.From).P("RENAME TO").Ident(rename.To.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  rename,
		Comment: fmt.Sprintf("rename function %q to %q", rename.From.Name, rename.To.Name),
	})
	return nil
}

func (s *state) addProc(add *schema.AddProc) error {
	s.append(&migrate.Change{
		Cmd:     add.P.Body,
		Source:  add,
		Comment: fmt.Sprintf("create procedure %q", add.P.Name),
		Reverse: s.Build("DROP PROCEDURE IF EXISTS").Proc(add.P).String(),
	})
	return nil
}

func (s *state) dropProc(drop *schema.DropProc) error {
	b := s.Build("DROP PROCEDURE")
	if sqlx.Has(drop.Extra, &schema.IfExists{}) {
		b.P("IF EXISTS")
	}
	b.Proc(drop.P)
	if sqlx.Has(drop.Extra, &Cascade{}) {
		b.P("CASCADE")
	}
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  drop,
		Comment: fmt.Sprintf("drop procedure %q", drop.P.Name),
	})
	return nil
}

func (s *state) modifyProc(modify *schema.ModifyProc) error {
	s.append(&migrate.Change{
		Cmd:     modify.To.Body,
		Source:  modify,
		Comment: fmt.Sprintf("modify procedure %q", modify.To.Name),
	})
	return nil
}

func (s *state) renameProc(rename *schema.RenameProc) error {
	b := s.Build("ALTER PROCEDURE")
	b.Proc(rename.From).P("RENAME TO").Ident(rename.To.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  rename,
		Comment: fmt.Sprintf("rename procedure %q to %q", rename.From.Name, rename.To.Name),
	})
	return nil
}

func (s *state) addObject(add *schema.AddObject) error {
	switch o := add.O.(type) {
	case *schema.EnumType:
		create, drop := s.createDropEnum(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create enum type %q", o.T),
		})
	default:
		// unsupported object type.
	}
	return nil
}

func (s *state) dropObject(drop *schema.DropObject) error {
	switch o := drop.O.(type) {
	case *schema.EnumType:
		create, dropE := s.createDropEnum(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     dropE,
			Reverse: create,
			Comment: fmt.Sprintf("drop enum type %q", o.T),
		})
	default:
		// unsupported object type.
	}
	return nil
}

func (s *state) modifyObject(modify *schema.ModifyObject) error {
	if _, ok := modify.From.(*schema.EnumType); ok {
		return s.alterEnum(modify)
	}
	return nil // unimplemented.
}

func (s *state) addTrigger(add *schema.AddTrigger) error {
	t := add.T
	// Body contains the full CREATE TRIGGER statement from pg_get_triggerdef.
	s.append(&migrate.Change{
		Cmd:     t.Body,
		Source:  add,
		Comment: fmt.Sprintf("create trigger %q", t.Name),
		Reverse: s.dropTriggerCmd(t),
	})
	return nil
}

func (s *state) dropTrigger(drop *schema.DropTrigger) error {
	s.append(&migrate.Change{
		Cmd:     s.dropTriggerCmd(drop.T),
		Source:  drop,
		Comment: fmt.Sprintf("drop trigger %q", drop.T.Name),
	})
	return nil
}

func (s *state) dropTriggerCmd(t *schema.Trigger) string {
	b := s.Build("DROP TRIGGER IF EXISTS")
	b.Ident(t.Name).P("ON")
	if t.Table != nil {
		b.Table(t.Table)
	} else if t.View != nil {
		b.View(t.View)
	}
	return b.String()
}

func (s *state) renameTrigger(rename *schema.RenameTrigger) error {
	b := s.Build("ALTER TRIGGER")
	b.Ident(rename.From.Name).P("ON")
	if rename.From.Table != nil {
		b.Table(rename.From.Table)
	} else if rename.From.View != nil {
		b.View(rename.From.View)
	}
	b.P("RENAME TO").Ident(rename.To.Name)
	s.append(&migrate.Change{
		Cmd:     b.String(),
		Source:  rename,
		Comment: fmt.Sprintf("rename trigger %q to %q", rename.From.Name, rename.To.Name),
	})
	return nil
}

func (s *state) modifyTrigger(modify *schema.ModifyTrigger) error {
	// No ALTER TRIGGER for body changes — drop and recreate.
	if err := s.dropTrigger(&schema.DropTrigger{T: modify.From}); err != nil {
		return err
	}
	return s.addTrigger(&schema.AddTrigger{T: modify.To})
}

func (*diff) ViewAttrChanges(from, to *schema.View) []schema.Change {
	var changes []schema.Change
	if change := sqlx.CommentDiff(from.Attrs, to.Attrs); change != nil {
		changes = append(changes, change)
	}
	return changes
}

// RealmObjectDiff returns a changeset for migrating realm (database) objects
// from one state to the other. For example, adding extensions or users.
func (*diff) RealmObjectDiff(_, _ *schema.Realm) ([]schema.Change, error) {
	return nil, nil // unimplemented.
}

// SchemaObjectDiff returns a changeset for migrating schema objects from
// one state to the other.
func (d *diff) SchemaObjectDiff(from, to *schema.Schema, opts *schema.DiffOptions) ([]schema.Change, error) {
	var changes []schema.Change
	// Drop or modify enums.
	for _, o1 := range from.Objects {
		e1, ok := o1.(*schema.EnumType)
		if !ok {
			continue // Unsupported object type.
		}
		o2, ok := to.Object(func(o schema.Object) bool {
			e2, ok := o.(*schema.EnumType)
			return ok && e1.T == e2.T
		})
		if !ok {
			changes = append(changes, &schema.DropObject{O: o1})
			continue
		}
		if e2 := o2.(*schema.EnumType); !sqlx.ValuesEqual(e1.Values, e2.Values) {
			changes = append(changes, &schema.ModifyObject{From: e1, To: e2})
		}
	}
	// Add new enums.
	for _, o1 := range to.Objects {
		e1, ok := o1.(*schema.EnumType)
		if !ok {
			continue // Unsupported object type.
		}
		if _, ok := from.Object(func(o schema.Object) bool {
			e2, ok := o.(*schema.EnumType)
			return ok && e1.T == e2.T
		}); !ok {
			changes = append(changes, &schema.AddObject{O: e1})
		}
	}
	return changes, nil
}

// ProcFuncsDiff implements the sqlx.ProcFuncsDiffer interface.
func (d *diff) ProcFuncsDiff(from, to *schema.Schema, opts *schema.DiffOptions) ([]schema.Change, error) {
	var changes []schema.Change
	// Drop or modify functions.
	for _, f1 := range from.Funcs {
		f2 := findFunc(to.Funcs, f1)
		if f2 == nil {
			changes = append(changes, &schema.DropFunc{F: f1})
			continue
		}
		if sqlx.BodyDefChanged(f1.Body, f2.Body) {
			changes = append(changes, &schema.ModifyFunc{From: f1, To: f2})
		}
	}
	// Add functions.
	for _, f1 := range to.Funcs {
		if findFunc(from.Funcs, f1) == nil {
			changes = append(changes, &schema.AddFunc{F: f1})
		}
	}
	// Drop or modify procedures.
	for _, p1 := range from.Procs {
		p2 := findProc(to.Procs, p1)
		if p2 == nil {
			changes = append(changes, &schema.DropProc{P: p1})
			continue
		}
		if sqlx.BodyDefChanged(p1.Body, p2.Body) {
			changes = append(changes, &schema.ModifyProc{From: p1, To: p2})
		}
	}
	// Add procedures.
	for _, p1 := range to.Procs {
		if findProc(from.Procs, p1) == nil {
			changes = append(changes, &schema.AddProc{P: p1})
		}
	}
	return changes, nil
}

func findFunc(funcs []*schema.Func, f *schema.Func) *schema.Func {
	for _, fn := range funcs {
		if fn.Name == f.Name {
			return fn
		}
	}
	return nil
}

func findProc(procs []*schema.Proc, p *schema.Proc) *schema.Proc {
	for _, pr := range procs {
		if pr.Name == p.Name {
			return pr
		}
	}
	return nil
}

// verifyChanges validates that all planned changes can be executed by the driver.
// It returns an error for unsupported operations that would fail during planning.
func verifyChanges(_ context.Context, changes []schema.Change) error {
	return nil
}

func convertDomains(_ []*sqlspec.Table, domains []*domain, _ *schema.Realm) error {
	if len(domains) > 0 {
		return fmt.Errorf("postgres: domains are not supported by this version. Use: https://atlasgo.io/getting-started")
	}
	return nil
}

func convertAggregate(d *doc, _ *schema.Realm) error {
	if len(d.Aggregates) > 0 {
		return fmt.Errorf("postgres: aggregates are not supported by this version. Use: https://atlasgo.io/getting-started")
	}
	return nil
}

func convertSequences(_ []*sqlspec.Table, seqs []*sqlspec.Sequence, _ *schema.Realm) error {
	if len(seqs) > 0 {
		return fmt.Errorf("postgres: sequences are not supported by this version. Use: https://atlasgo.io/getting-started")
	}
	return nil
}

func convertPolicies(_ []*sqlspec.Table, ps []*policy, _ *schema.Realm) error {
	if len(ps) > 0 {
		return fmt.Errorf("postgres: policies are not supported by this version. Use: https://atlasgo.io/getting-started")
	}
	return nil
}

func convertExtensions(exs []*extension, _ *schema.Realm) error {
	if len(exs) > 0 {
		return fmt.Errorf("postgres: extensions are not supported by this version. Use: https://atlasgo.io/getting-started")
	}
	return nil
}

func convertEventTriggers(evs []*eventTrigger, _ *schema.Realm) error {
	if len(evs) > 0 {
		return fmt.Errorf("postgres: event triggers are not supported by this version. Use: https://atlasgo.io/getting-started")
	}
	return nil
}

func normalizeRealm(*schema.Realm) error {
	return nil
}

func schemasObjectSpec(*doc, ...*schema.Schema) error {
	return nil // unimplemented.
}

// objectSpec converts from a concrete schema objects into specs.
func objectSpec(d *doc, spec *specutil.SchemaSpec, s *schema.Schema) error {
	for _, o := range s.Objects {
		if e, ok := o.(*schema.EnumType); ok {
			d.Enums = append(d.Enums, &enum{
				Name:   e.T,
				Values: e.Values,
				Schema: specutil.SchemaRef(spec.Schema.Name),
			})
		}
	}
	return nil
}

// convertEnums converts possibly referenced column types (like enums) to
// an actual schema.Type and sets it on the correct schema.Column.
func convertTypes(d *doc, r *schema.Realm) error {
	if len(d.Enums) == 0 {
		return nil
	}
	byName := make(map[string]*schema.EnumType)
	for _, e := range d.Enums {
		if byName[e.Name] != nil {
			return fmt.Errorf("duplicate enum %q", e.Name)
		}
		ns, err := specutil.SchemaName(e.Schema)
		if err != nil {
			return fmt.Errorf("extract schema name from enum reference: %w", err)
		}
		es, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("schema %q defined on enum %q was not found in realm", ns, e.Name)
		}
		e1 := &schema.EnumType{T: e.Name, Schema: es, Values: e.Values}
		es.AddObjects(e1)
		byName[e.Name] = e1
	}
	for _, t := range d.Tables {
		for _, c := range t.Columns {
			var enum *schema.EnumType
			switch {
			case c.Type.IsRefTo("enum"):
				n, err := enumName(c.Type)
				if err != nil {
					return err
				}
				e, ok := byName[n]
				if !ok {
					return fmt.Errorf("enum %q was not found in realm", n)
				}
				enum = e
			default:
				if n, ok := arrayType(c.Type.T); ok {
					enum = byName[n]
				}
			}
			if enum == nil {
				continue
			}
			schemaT, err := specutil.SchemaName(t.Schema)
			if err != nil {
				return fmt.Errorf("extract schema name from table reference: %w", err)
			}
			ts, ok := r.Schema(schemaT)
			if !ok {
				return fmt.Errorf("schema %q not found in realm for table %q", schemaT, t.Name)
			}
			tt, ok := ts.Table(t.Name)
			if !ok {
				return fmt.Errorf("table %q not found in schema %q", t.Name, ts.Name)
			}
			cc, ok := tt.Column(c.Name)
			if !ok {
				return fmt.Errorf("column %q not found in table %q", c.Name, t.Name)
			}
			switch t := cc.Type.Type.(type) {
			case *ArrayType:
				t.Type = enum
			default:
				cc.Type.Type = enum
			}
		}
	}
	return nil
}

func indexToUnique(*schema.ModifyIndex) (*AddUniqueConstraint, bool) {
	return nil, false // unimplemented.
}

func uniqueConstChanged(_, _ []schema.Attr) bool {
	// Unsupported change in package mode (ariga.io/sql/postgres)
	// to keep BC with old versions.
	return false
}

func excludeConstChanged(_, _ []schema.Attr) bool {
	// Unsupported change in package mode (ariga.io/sql/postgres)
	// to keep BC with old versions.
	return false
}

func convertExclude(schemahcl.Resource, *schema.Table) error {
	return nil // unimplemented.
}

func (*state) sortChanges(changes []schema.Change) []schema.Change {
	return sqlx.SortChanges(changes, nil)
}

func (*state) detachCycles(changes []schema.Change) ([]schema.Change, error) {
	return sqlx.DetachCycles(changes)
}

func excludeSpec(*sqlspec.Table, *sqlspec.Index, *schema.Index, *Constraint) error {
	return nil // unimplemented.
}

const (
	// Query to list tables information.
	// Note, 'attrs' are not supported in this version.
	tablesQuery = `
SELECT
	t3.oid,
	t1.table_schema,
	t1.table_name,
	pg_catalog.obj_description(t3.oid, 'pg_class') AS comment,
	t4.partattrs AS partition_attrs,
	t4.partstrat AS partition_strategy,
	pg_get_expr(t4.partexprs, t4.partrelid) AS partition_exprs,
	'{}' AS attrs
FROM
	INFORMATION_SCHEMA.TABLES AS t1
	JOIN pg_catalog.pg_namespace AS t2 ON t2.nspname = t1.table_schema
	JOIN pg_catalog.pg_class AS t3 ON t3.relnamespace = t2.oid AND t3.relname = t1.table_name
	LEFT JOIN pg_catalog.pg_partitioned_table AS t4 ON t4.partrelid = t3.oid
	LEFT JOIN pg_depend AS t5 ON t5.classid = 'pg_catalog.pg_class'::regclass::oid AND t5.objid = t3.oid AND t5.deptype = 'e'
WHERE
	t1.table_type = 'BASE TABLE'
	AND NOT COALESCE(t3.relispartition, false)
	AND t1.table_schema IN (%s)
	AND t5.objid IS NULL
ORDER BY
	t1.table_schema, t1.table_name
`
	// Query to list tables by their names.
	// Note, 'attrs' are not supported in this version.
	tablesQueryArgs = `
SELECT
	t3.oid,
	t1.table_schema,
	t1.table_name,
	pg_catalog.obj_description(t3.oid, 'pg_class') AS comment,
	t4.partattrs AS partition_attrs,
	t4.partstrat AS partition_strategy,
	pg_get_expr(t4.partexprs, t4.partrelid) AS partition_exprs,
	'{}' AS attrs
FROM
	INFORMATION_SCHEMA.TABLES AS t1
	JOIN pg_catalog.pg_namespace AS t2 ON t2.nspname = t1.table_schema
	JOIN pg_catalog.pg_class AS t3 ON t3.relnamespace = t2.oid AND t3.relname = t1.table_name
	LEFT JOIN pg_catalog.pg_partitioned_table AS t4 ON t4.partrelid = t3.oid
	LEFT JOIN pg_depend AS t5 ON t5.classid = 'pg_catalog.pg_class'::regclass::oid AND t5.objid = t3.oid AND t5.deptype = 'e'
WHERE
	t1.table_type = 'BASE TABLE'
	AND NOT COALESCE(t3.relispartition, false)
	AND t1.table_schema IN (%s)
	AND t1.table_name IN (%s)
	AND t5.objid IS NULL
ORDER BY
	t1.table_schema, t1.table_name
`
	// Query to list views (regular and materialized).
	viewsQuery = `
SELECT
	n.nspname AS schema_name,
	c.relname AS view_name,
	pg_get_viewdef(c.oid, true) AS definition,
	c.relkind = 'm' AS is_materialized
FROM
	pg_catalog.pg_class c
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_depend d ON d.classid = 'pg_catalog.pg_class'::regclass::oid AND d.objid = c.oid AND d.deptype = 'e'
WHERE
	c.relkind IN ('v', 'm')
	AND n.nspname IN (%s)
	AND d.objid IS NULL
ORDER BY
	n.nspname, c.relname
`
	// Query to list functions and procedures.
	funcsQuery = `
SELECT
	n.nspname AS schema_name,
	p.proname AS func_name,
	p.prokind AS kind,
	pg_get_functiondef(p.oid) AS definition,
	l.lanname AS lang,
	CASE WHEN p.prokind = 'f' THEN pg_catalog.pg_get_function_result(p.oid) ELSE '' END AS return_type,
	COALESCE(pg_catalog.pg_get_function_arguments(p.oid), '') AS func_args
FROM
	pg_catalog.pg_proc p
	JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
	JOIN pg_catalog.pg_language l ON l.oid = p.prolang
	LEFT JOIN pg_depend d ON d.classid = 'pg_catalog.pg_proc'::regclass::oid AND d.objid = p.oid AND d.deptype = 'e'
WHERE
	p.prokind IN ('f', 'p')
	AND n.nspname IN (%s)
	AND d.objid IS NULL
ORDER BY
	n.nspname, p.proname
`
	// Query to list triggers using pg_get_triggerdef for full DDL.
	triggersQuery = `
SELECT
	n.nspname AS schema_name,
	t.tgname AS trigger_name,
	c.relname AS table_name,
	CASE t.tgtype::int & 66
		WHEN 2  THEN 'BEFORE'
		WHEN 64 THEN 'INSTEAD OF'
		ELSE 'AFTER'
	END AS action_timing,
	array_to_string(ARRAY[
		CASE WHEN t.tgtype::int & 4  != 0 THEN 'INSERT'   END,
		CASE WHEN t.tgtype::int & 8  != 0 THEN 'DELETE'   END,
		CASE WHEN t.tgtype::int & 16 != 0 THEN 'UPDATE'   END,
		CASE WHEN t.tgtype::int & 32 != 0 THEN 'TRUNCATE' END
	], ' OR ') AS event_manipulation,
	CASE WHEN t.tgtype::int & 1 != 0 THEN 'ROW' ELSE 'STATEMENT' END AS orientation,
	pg_get_triggerdef(t.oid) AS definition
FROM
	pg_catalog.pg_trigger t
	JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
	NOT t.tgisinternal
	AND n.nspname IN (%s)
ORDER BY
	n.nspname, c.relname, t.tgname
`
)
