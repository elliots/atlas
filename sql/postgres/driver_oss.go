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
				// Fast path: if already clean, do nothing.
				if len(current.Schemas) <= 1 && len(current.Objects) == 0 {
					if len(current.Schemas) == 0 {
						return nil
					}
					if s := current.Schemas[0]; s.Name == "public" && len(s.Tables)+len(s.Views)+len(s.Funcs)+len(s.Procs)+len(s.Objects) == 0 {
						return nil
					}
				}
				// Batch all cleanup into a single exec to minimize round trips.
				var stmts []string
				for _, s := range current.Schemas {
					stmts = append(stmts, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", s.Name))
				}
				for _, o := range current.Objects {
					if ext, ok := o.(*Extension); ok {
						stmts = append(stmts, fmt.Sprintf("DROP EXTENSION IF EXISTS %q CASCADE", ext.T))
					}
				}
				stmts = append(stmts, `CREATE SCHEMA IF NOT EXISTS "public"`)
				_, err = d.ExecContext(ctx, strings.Join(stmts, ";\n"))
				return err
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
		Schema:       opts.StripDefaultSchema,
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
func (*diff) tableAttrDiff(from, to *schema.Table) ([]schema.Change, error) {
	var changes []schema.Change
	var fromRLS, toRLS RowLevelSecurity
	sqlx.Has(from.Attrs, &fromRLS)
	sqlx.Has(to.Attrs, &toRLS)
	if fromRLS.Enabled != toRLS.Enabled || fromRLS.Forced != toRLS.Forced {
		changes = append(changes, &schema.ModifyAttr{
			From: &fromRLS,
			To:   &toRLS,
		})
	}
	return changes, nil
}

// addTableAttrs allows extending table attributes creation with build-specific logic.
func (s *state) addTableAttrs(add *schema.AddTable) {
	var rls RowLevelSecurity
	if sqlx.Has(add.T.Attrs, &rls) && rls.Enabled {
		enable := s.Build("ALTER TABLE").Table(add.T).P("ENABLE ROW LEVEL SECURITY").String()
		disable := s.Build("ALTER TABLE").Table(add.T).P("DISABLE ROW LEVEL SECURITY").String()
		s.append(&migrate.Change{
			Cmd:     enable,
			Reverse: disable,
			Comment: fmt.Sprintf("enable row-level security for table %q", add.T.Name),
		})
		if rls.Forced {
			force := s.Build("ALTER TABLE").Table(add.T).P("FORCE ROW LEVEL SECURITY").String()
			noForce := s.Build("ALTER TABLE").Table(add.T).P("NO FORCE ROW LEVEL SECURITY").String()
			s.append(&migrate.Change{
				Cmd:     force,
				Reverse: noForce,
				Comment: fmt.Sprintf("force row-level security for table %q", add.T.Name),
			})
		}
	}
}

// alterTableAttr allows extending table attributes alteration with build-specific logic.
func (s *state) alterTableAttr(b *sqlx.Builder, modify *schema.ModifyAttr) {
	if to, ok := modify.To.(*RowLevelSecurity); ok {
		if to.Enabled {
			b.P("ENABLE ROW LEVEL SECURITY")
		} else {
			b.P("DISABLE ROW LEVEL SECURITY")
		}
	}
}

// realmObjectsSpec converts realm-level objects (extensions, event triggers) to HCL spec.
func realmObjectsSpec(d *doc, r *schema.Realm) error {
	for _, o := range r.Objects {
		switch o := o.(type) {
		case *Extension:
			spec := &extension{Name: o.T}
			if o.Version != "" {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("version", o.Version))
			}
			if o.Schema != "" {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("schema", o.Schema))
			}
			d.Extensions = append(d.Extensions, spec)
		case *EventTrigger:
			spec := &eventTrigger{Name: o.Name}
			if o.Event != "" {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringAttr("on", o.Event))
			}
			if len(o.Tags) > 0 {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringsAttr("tags", o.Tags...))
			}
			if o.FuncRef != "" {
				spec.Extra.Children = append(spec.Extra.Children, &schemahcl.Resource{
					Type: "execute",
					Attrs: []*schemahcl.Attr{
						schemahcl.RefAttr("function", schemahcl.BuildRef([]schemahcl.PathIndex{{T: "function", V: []string{o.FuncRef}}})),
					},
				})
			}
			d.EventTriggers = append(d.EventTriggers, spec)
		case *Role:
			spec := &role{Name: o.Name}
			if o.Superuser {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("superuser", true))
			}
			if o.CreateDB {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("create_db", true))
			}
			if o.CreateRole {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("create_role", true))
			}
			if o.Login {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("login", true))
			}
			if !o.Inherit {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("inherit", false))
			}
			if o.Replication {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("replication", true))
			}
			if o.BypassRLS {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("bypass_rls", true))
			}
			if o.ConnLimit != -1 {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.IntAttr("conn_limit", o.ConnLimit))
			}
			if len(o.MemberOf) > 0 {
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.StringsAttr("member_of", o.MemberOf...))
			}
			d.Roles = append(d.Roles, spec)
		}
	}
	return nil
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
		// Strip DEFAULT clauses — pg_get_function_arguments includes them but they're not part of the type.
		if idx := strings.Index(strings.ToUpper(part), " DEFAULT "); idx != -1 {
			part = strings.TrimSpace(part[:idx])
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
				arg.Name = strings.Trim(tokens[0], `"`)
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

// stripOwnSchemaFromType removes the function's own schema prefix from type strings.
// e.g. "SETOF public.posts" -> "SETOF posts" when ownSchema is "public".
// Cross-schema references (e.g. "SETOF auth.users" when ownSchema is "public") are preserved.
func stripOwnSchemaFromType(t string, ownSchema string) string {
	prefix := ownSchema + "."
	lower := strings.ToLower(t)
	lowerPrefix := strings.ToLower(prefix)
	if strings.HasPrefix(lower, "setof ") {
		rest := t[6:]
		if strings.HasPrefix(strings.ToLower(rest), lowerPrefix) {
			return t[:6] + rest[len(prefix):]
		}
		return t
	}
	if strings.HasPrefix(lower, lowerPrefix) {
		return t[len(prefix):]
	}
	return t
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
	if err := rows.Err(); err != nil {
		return err
	}
	// Skip view columns query when no views were found.
	hasViews := false
	for _, s := range r.Schemas {
		if len(s.Views) > 0 {
			hasViews = true
			break
		}
	}
	if !hasViews {
		return nil
	}
	// Populate view columns from pg_attribute.
	return i.inspectViewColumns(ctx, r)
}

func (i *inspect) inspectViewColumns(ctx context.Context, r *schema.Realm) error {
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
	query := fmt.Sprintf(viewColumnsQuery, strings.Join(placeholders, ", "))
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: inspecting view columns: %w", err)
	}
	defer rows.Close()
	// Build view lookup.
	viewMap := make(map[string]*schema.View)
	for _, s := range r.Schemas {
		for _, v := range s.Views {
			viewMap[s.Name+"."+v.Name] = v
		}
	}
	for rows.Next() {
		var (
			schemaName, viewName, colName, colType string
			isNullable                             bool
		)
		if err := rows.Scan(&schemaName, &viewName, &colName, &colType, &isNullable); err != nil {
			return fmt.Errorf("postgres: scanning view column: %w", err)
		}
		v, ok := viewMap[schemaName+"."+viewName]
		if !ok {
			continue
		}
		t, err := ParseType(colType)
		if err != nil {
			t = &schema.UnsupportedType{T: colType}
		}
		// "?column?" is Postgres's default for unnamed expressions (e.g. SELECT 1).
		// Treat as unnamed so downstream consumers can handle it appropriately.
		if colName == "?column?" {
			colName = ""
		}
		col := schema.NewColumn(colName).SetType(t)
		if isNullable {
			col.Type.Null = true
		}
		v.Columns = append(v.Columns, col)
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
		// Skip internal functions auto-generated by PostgreSQL (e.g., range type
		// constructors, type I/O functions). These are implementation details
		// managed by PostgreSQL itself, not user-created objects.
		if lang == "internal" {
			continue
		}
		args := parseFuncArgs(funcArgs)
		switch kind {
		case "f": // normal function
			f := &schema.Func{Name: funcName, Schema: s, Body: def, Lang: lang, Args: args}
			if retType != "" {
				// Strip own-schema qualification from return types (e.g. "SETOF public.posts" -> "SETOF posts"
				// when the function is in "public" schema). Cross-schema refs are preserved.
				cleanRet := stripOwnSchemaFromType(retType, schemaName)
				if t, err := ParseType(cleanRet); err == nil {
					f.Ret = t
				} else {
					// For complex return types like TABLE(...), store as-is.
					f.Ret = &schema.UnsupportedType{T: cleanRet}
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

// inspectTypes queries pg_type for domain and composite types and attaches them to schemas.
func (i *inspect) inspectTypes(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	args := make([]any, 0, len(r.Schemas))
	for _, s := range r.Schemas {
		args = append(args, s.Name)
	}
	if len(args) == 0 {
		return nil
	}
	// Inspect domain types.
	rows, err := i.QueryContext(ctx, fmt.Sprintf(domainsQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying domain types: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			ns, name, baseType string
			notNull            bool
			dflt               sql.NullString
		)
		if err := rows.Scan(&ns, &name, &baseType, &notNull, &dflt); err != nil {
			return fmt.Errorf("postgres: scanning domain type: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("postgres: schema %q for domain %q not found", ns, name)
		}
		typ, err := ParseType(baseType)
		if err != nil {
			typ = &schema.UnsupportedType{T: baseType}
		}
		d := &DomainType{
			T:      name,
			Schema: s,
			Null:   !notNull,
			Type:   typ,
		}
		if dflt.Valid {
			d.Default = &schema.RawExpr{X: dflt.String}
		}
		s.Objects = append(s.Objects, d)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Inspect domain check constraints.
	chkRows, err := i.QueryContext(ctx, fmt.Sprintf(domainChecksQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying domain checks: %w", err)
	}
	defer chkRows.Close()
	for chkRows.Next() {
		var ns, typName, conName, expr string
		if err := chkRows.Scan(&ns, &typName, &conName, &expr); err != nil {
			return fmt.Errorf("postgres: scanning domain check: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			continue
		}
		for _, o := range s.Objects {
			if d, ok := o.(*DomainType); ok && d.T == typName {
				d.Checks = append(d.Checks, &schema.Check{
					Name: conName,
					Expr: expr,
				})
				break
			}
		}
	}
	if err := chkRows.Err(); err != nil {
		return err
	}
	// Inspect composite types.
	rows2, err := i.QueryContext(ctx, fmt.Sprintf(compositesQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying composite types: %w", err)
	}
	defer rows2.Close()
	composites := make(map[string]*CompositeType)
	for rows2.Next() {
		var (
			ns, typName, fieldName, fieldType string
		)
		if err := rows2.Scan(&ns, &typName, &fieldName, &fieldType); err != nil {
			return fmt.Errorf("postgres: scanning composite type: %w", err)
		}
		key := ns + "." + typName
		ct, ok := composites[key]
		if !ok {
			s, ok := r.Schema(ns)
			if !ok {
				return fmt.Errorf("postgres: schema %q for composite %q not found", ns, typName)
			}
			ct = &CompositeType{T: typName, Schema: s}
			composites[key] = ct
			s.Objects = append(s.Objects, ct)
		}
		ft, err := ParseType(fieldType)
		if err != nil {
			ft = &schema.UnsupportedType{T: fieldType}
		}
		ct.Fields = append(ct.Fields, &schema.Column{
			Name: fieldName,
			Type: &schema.ColumnType{Raw: fieldType, Type: ft},
		})
	}
	return rows2.Err()
}

// inspectObjects queries pg_sequences for independent sequences and collations, and attaches them to schemas.
func (i *inspect) inspectObjects(ctx context.Context, r *schema.Realm, opts *schema.InspectOptions) error {
	args := make([]any, 0, len(r.Schemas))
	for _, s := range r.Schemas {
		args = append(args, s.Name)
	}
	if len(args) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, fmt.Sprintf(sequencesQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying sequences: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			ns, name, seqType string
			start, increment  int64
			cache             int64
			minV, maxV        sql.NullInt64
			cycle             bool
			ownerTable        sql.NullString
			ownerColumn       sql.NullString
			depType           sql.NullString
		)
		if err := rows.Scan(&ns, &name, &seqType, &start, &increment, &cache, &minV, &maxV, &cycle, &ownerTable, &ownerColumn, &depType); err != nil {
			return fmt.Errorf("postgres: scanning sequence: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("postgres: schema %q for sequence %q not found", ns, name)
		}
		// Auto-owned sequences are managed by the column — skip standalone emission.
		if ownerTable.Valid && ownerColumn.Valid {
			if depType.Valid && depType.String == "i" {
				// Identity columns: sequence is internal, handled by GENERATED AS IDENTITY.
				continue
			}
			// Serial columns: convert to SerialType.
			convertToSerial(s, ownerTable.String, ownerColumn.String, name)
			continue
		}
		typ, err := ParseType(seqType)
		if err != nil {
			typ = &schema.UnsupportedType{T: seqType}
		}
		seq := &Sequence{
			Name:      name,
			Schema:    s,
			Type:      typ,
			Start:     start,
			Increment: increment,
			Cache:     cache,
			Cycle:     cycle,
		}
		if minV.Valid {
			v := minV.Int64
			seq.Min = &v
		}
		if maxV.Valid {
			v := maxV.Int64
			seq.Max = &v
		}
		s.Objects = append(s.Objects, seq)
		// Add sequence as a dependency of the table that uses it via nextval() default.
		// This ensures the topological sort creates the sequence before the table.
		for _, t := range s.Tables {
			for _, c := range t.Columns {
				if c.Default == nil {
					continue
				}
				if raw, ok := c.Default.(*schema.RawExpr); ok && strings.Contains(raw.X, name) {
					t.Deps = append(t.Deps, seq)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := i.inspectCollations(ctx, r, opts); err != nil {
		return err
	}
	if err := i.inspectRangeTypes(ctx, r, opts); err != nil {
		return err
	}
	return i.inspectAggregates(ctx, r, opts)
}

// inspectCollations queries pg_collation for user-defined collations and attaches them to schemas.
func (i *inspect) inspectCollations(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	args := make([]any, 0, len(r.Schemas))
	for _, s := range r.Schemas {
		args = append(args, s.Name)
	}
	if len(args) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, fmt.Sprintf(collationsQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying collations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			ns, name, provider, lcCollate, lcCtype string
			deterministic                           bool
		)
		if err := rows.Scan(&ns, &name, &provider, &lcCollate, &lcCtype, &deterministic); err != nil {
			return fmt.Errorf("postgres: scanning collation: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("postgres: schema %q for collation %q not found", ns, name)
		}
		c := &CollationObj{
			T:         name,
			Schema:    s,
			Provider:  provider,
			LcCollate: lcCollate,
			LcCtype:   lcCtype,
		}
		if !deterministic {
			f := false
			c.Deterministic = &f
		}
		s.Objects = append(s.Objects, c)
	}
	return rows.Err()
}

func (i *inspect) inspectRangeTypes(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	args := make([]any, 0, len(r.Schemas))
	for _, s := range r.Schemas {
		args = append(args, s.Name)
	}
	if len(args) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, fmt.Sprintf(rangeTypesQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying range types: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ns, typName, subtype, subtypeDiff, multirangeName string
		if err := rows.Scan(&ns, &typName, &subtype, &subtypeDiff, &multirangeName); err != nil {
			return fmt.Errorf("postgres: scanning range type: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("postgres: schema %q for range type %q not found", ns, typName)
		}
		st, err := ParseType(subtype)
		if err != nil {
			st = &schema.UnsupportedType{T: subtype}
		}
		ro := &RangeObj{
			T:              typName,
			Schema:         s,
			Subtype:        st,
			SubtypeDiff:    subtypeDiff,
			MultirangeName: multirangeName,
		}
		s.Objects = append(s.Objects, ro)
	}
	return rows.Err()
}

// inspectAggregates queries pg_aggregate for user-defined aggregates and attaches them to schemas.
func (i *inspect) inspectAggregates(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	args := make([]any, 0, len(r.Schemas))
	for _, s := range r.Schemas {
		args = append(args, s.Name)
	}
	if len(args) == 0 {
		return nil
	}
	rows, err := i.QueryContext(ctx, fmt.Sprintf(aggregatesQuery, nArgs(0, len(args))), args...)
	if err != nil {
		return fmt.Errorf("postgres: querying aggregates: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			ns, name, stateFunc, stateType string
			finalFunc, initVal, sortOp     sql.NullString
			parallel                       sql.NullString
			argTypes                       string
		)
		if err := rows.Scan(&ns, &name, &stateFunc, &stateType, &finalFunc, &initVal, &sortOp, &parallel, &argTypes); err != nil {
			return fmt.Errorf("postgres: scanning aggregate: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("postgres: schema %q for aggregate %q not found", ns, name)
		}
		st, err := ParseType(stateType)
		if err != nil {
			st = &schema.UnsupportedType{T: stateType}
		}
		agg := &Aggregate{
			Name:      name,
			Schema:    s,
			StateFunc: stateFunc,
			StateType: st,
		}
		if finalFunc.Valid && finalFunc.String != "-" {
			agg.FinalFunc = finalFunc.String
		}
		if initVal.Valid {
			agg.InitVal = initVal.String
		}
		if sortOp.Valid && sortOp.String != "0" {
			agg.SortOp = sortOp.String
		}
		if parallel.Valid {
			agg.Parallel = parallel.String
		}
		// Parse argument types.
		if argTypes != "" {
			for _, a := range strings.Split(argTypes, ", ") {
				a = strings.TrimSpace(a)
				if a == "" {
					continue
				}
				at, err := ParseType(a)
				if err != nil {
					at = &schema.UnsupportedType{T: a}
				}
				agg.Args = append(agg.Args, &schema.FuncArg{
					Type: at,
				})
			}
		}
		// Set dependencies on the state/final functions.
		// Match by name, stripping any schema qualifier (e.g., "public._group_concat" → "_group_concat").
		matchFunc := func(fname, target string) bool {
			if target == "" {
				return false
			}
			if fname == target {
				return true
			}
			// Strip schema qualifier from target.
			if i := strings.LastIndex(target, "."); i >= 0 {
				return fname == target[i+1:]
			}
			return false
		}
		for _, f := range s.Funcs {
			if matchFunc(f.Name, agg.StateFunc) || matchFunc(f.Name, agg.FinalFunc) {
				agg.Deps = append(agg.Deps, f)
			}
		}
		s.Objects = append(s.Objects, agg)
	}
	return rows.Err()
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

// inspectDeps populates Deps fields on functions, procedures, views and triggers
// by querying pg_depend for actual catalog-tracked dependencies between objects.
func (i *inspect) inspectDeps(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	// Build lookup maps: pg_class OID name → schema object.
	type objKey struct {
		schema, name string
	}
	tables := make(map[objKey]*schema.Table)
	views := make(map[objKey]*schema.View)
	funcs := make(map[objKey]*schema.Func)
	procs := make(map[objKey]*schema.Proc)
	enums := make(map[objKey]*schema.EnumType)
	domains := make(map[objKey]*DomainType)
	composites := make(map[objKey]*CompositeType)
	for _, s := range r.Schemas {
		for _, t := range s.Tables {
			tables[objKey{s.Name, t.Name}] = t
		}
		for _, v := range s.Views {
			views[objKey{s.Name, v.Name}] = v
		}
		for _, f := range s.Funcs {
			funcs[objKey{s.Name, f.Name}] = f
		}
		for _, p := range s.Procs {
			procs[objKey{s.Name, p.Name}] = p
		}
		for _, o := range s.Objects {
			switch v := o.(type) {
			case *schema.EnumType:
				enums[objKey{s.Name, v.T}] = v
			case *DomainType:
				domains[objKey{s.Name, v.T}] = v
			case *CompositeType:
				composites[objKey{s.Name, v.T}] = v
			}
		}
	}

	// resolve returns the schema object for a given (schema, name, kind) triple.
	resolve := func(ns, name, kind string) schema.Object {
		key := objKey{ns, name}
		switch kind {
		case "table":
			if t, ok := tables[key]; ok {
				return t
			}
		case "view", "matview":
			if v, ok := views[key]; ok {
				return v
			}
		case "function":
			if f, ok := funcs[key]; ok {
				return f
			}
		case "procedure":
			if p, ok := procs[key]; ok {
				return p
			}
		case "enum":
			if e, ok := enums[key]; ok {
				return e
			}
		case "domain":
			if d, ok := domains[key]; ok {
				return d
			}
		case "composite":
			if c, ok := composites[key]; ok {
				return c
			}
			// Every table has an implicit composite type with the same name.
			// pg_depend reports these as "composite" kind, so fall through to tables.
			if t, ok := tables[key]; ok {
				return t
			}
		}
		return nil
	}

	// Query pg_depend to find dependencies between functions/views and other objects.
	rows, err := i.QueryContext(ctx, depsQuery)
	if err != nil {
		return fmt.Errorf("postgres: querying dependencies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var srcSchema, srcName, srcKind, depSchema, depName, depKind string
		if err := rows.Scan(&srcSchema, &srcName, &srcKind, &depSchema, &depName, &depKind); err != nil {
			return fmt.Errorf("postgres: scanning dependency: %w", err)
		}
		dep := resolve(depSchema, depName, depKind)
		if dep == nil {
			continue
		}
		key := objKey{srcSchema, srcName}
		switch srcKind {
		case "function":
			if f, ok := funcs[key]; ok {
				f.Deps = append(f.Deps, dep)
			}
		case "procedure":
			if p, ok := procs[key]; ok {
				p.Deps = append(p.Deps, dep)
			}
		case "view", "matview":
			if v, ok := views[key]; ok {
				v.Deps = append(v.Deps, dep)
			}
		case "table":
			if t, ok := tables[key]; ok {
				t.Deps = append(t.Deps, dep)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// pg_depend doesn't track runtime references in function/procedure bodies
	// (e.g., SELECT FROM table_name in SQL functions). Use word-boundary matching
	// on function bodies to catch these.
	for _, s := range r.Schemas {
		for _, f := range s.Funcs {
			bodyWords := tokenize(f.Body)
			for _, t := range s.Tables {
				if bodyWords[strings.ToLower(t.Name)] && !sliceContains(f.Deps, t) {
					f.Deps = append(f.Deps, t)
				}
			}
			for _, f2 := range s.Funcs {
				if f2 != f && bodyWords[strings.ToLower(f2.Name)] && !sliceContains(f.Deps, f2) {
					f.Deps = append(f.Deps, f2)
				}
			}
		}
		for _, p := range s.Procs {
			bodyWords := tokenize(p.Body)
			for _, t := range s.Tables {
				if bodyWords[strings.ToLower(t.Name)] && !sliceContains(p.Deps, t) {
					p.Deps = append(p.Deps, t)
				}
			}
		}
		// Views: pg_depend tracks view deps at the pg_rewrite level, not on the
		// view's pg_class entry. Analyze the view definition for table/func/view refs.
		for _, v := range s.Views {
			defWords := tokenize(v.Def)
			for _, t := range s.Tables {
				if defWords[strings.ToLower(t.Name)] && !sliceContains(v.Deps, t) {
					v.Deps = append(v.Deps, t)
				}
			}
			for _, f := range s.Funcs {
				if defWords[strings.ToLower(f.Name)] && !sliceContains(v.Deps, f) {
					v.Deps = append(v.Deps, f)
				}
			}
			for _, v2 := range s.Views {
				if v2 != v && defWords[strings.ToLower(v2.Name)] && !sliceContains(v.Deps, v2) {
					v.Deps = append(v.Deps, v2)
				}
			}
		}
	}
	// Tables: column DEFAULT expressions may reference functions from any schema.
	// Build a realm-wide function lookup for cross-schema default dependencies.
	allFuncs := make(map[string]*schema.Func) // "schema.func" → *Func
	for _, s := range r.Schemas {
		for _, f := range s.Funcs {
			allFuncs[strings.ToLower(s.Name+"."+f.Name)] = f
			allFuncs[strings.ToLower(f.Name)] = f
		}
	}
	for _, s := range r.Schemas {
		for _, t := range s.Tables {
			for _, c := range t.Columns {
				if c.Default == nil {
					continue
				}
				raw, ok := c.Default.(*schema.RawExpr)
				if !ok {
					continue
				}
				defWords := tokenize(raw.X)
				for fname, f := range allFuncs {
					if defWords[fname] && !sliceContains(t.Deps, f) {
						t.Deps = append(t.Deps, f)
					}
				}
			}
		}
	}
	return nil
}

// tokenize splits a function body into lowercase word tokens for dependency matching.
func tokenize(body string) map[string]bool {
	words := make(map[string]bool)
	var buf strings.Builder
	for _, r := range strings.ToLower(body) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			buf.WriteRune(r)
		} else {
			if buf.Len() > 0 {
				words[buf.String()] = true
				buf.Reset()
			}
		}
	}
	if buf.Len() > 0 {
		words[buf.String()] = true
	}
	return words
}

func sliceContains(deps []schema.Object, obj schema.Object) bool {
	for _, d := range deps {
		if d == obj {
			return true
		}
	}
	return false
}

// inspectRealmObjects queries pg_extension for installed extensions and pg_event_trigger for event triggers.
func (i *inspect) inspectRealmObjects(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
	rows, err := i.QueryContext(ctx, extensionsQuery)
	if err != nil {
		return fmt.Errorf("postgres: querying extensions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, ns, version string
		if err := rows.Scan(&name, &ns, &version); err != nil {
			return fmt.Errorf("postgres: scanning extension: %w", err)
		}
		r.Objects = append(r.Objects, &Extension{
			T:       name,
			Schema:  ns,
			Version: version,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Query event triggers (realm-scoped, not schema-scoped).
	etRows, err := i.QueryContext(ctx, eventTriggersQuery)
	if err != nil {
		return fmt.Errorf("postgres: querying event triggers: %w", err)
	}
	defer etRows.Close()
	for etRows.Next() {
		var name, event, funcName, tags string
		if err := etRows.Scan(&name, &event, &funcName, &tags); err != nil {
			return fmt.Errorf("postgres: scanning event trigger: %w", err)
		}
		et := &EventTrigger{Name: name, Event: event, FuncRef: funcName}
		if tags != "" {
			et.Tags = strings.Split(tags, ",")
		}
		r.Objects = append(r.Objects, et)
	}
	if err := etRows.Err(); err != nil {
		return err
	}
	// Query roles (realm-scoped, not schema-scoped).
	roleRows, err := i.QueryContext(ctx, rolesQuery)
	if err != nil {
		return fmt.Errorf("postgres: querying roles: %w", err)
	}
	defer roleRows.Close()
	roles := make(map[string]*Role)
	for roleRows.Next() {
		var name string
		var comment sql.NullString
		var super, createDB, createRole, canLogin, inherit, replication, bypassRLS bool
		var connLimit int
		if err := roleRows.Scan(&name, &super, &createDB, &createRole, &canLogin, &inherit, &replication, &bypassRLS, &connLimit, &comment); err != nil {
			return fmt.Errorf("postgres: scanning role: %w", err)
		}
		role := &Role{
			Name:        name,
			Superuser:   super,
			CreateDB:    createDB,
			CreateRole:  createRole,
			Login:       canLogin,
			Inherit:     inherit,
			Replication: replication,
			BypassRLS:   bypassRLS,
			ConnLimit:   connLimit,
		}
		if comment.String != "" {
			role.Attrs = append(role.Attrs, &schema.Comment{Text: comment.String})
		}
		roles[name] = role
		r.Objects = append(r.Objects, role)
	}
	if err := roleRows.Err(); err != nil {
		return err
	}
	// Query role memberships and attach to roles.
	memberRows, err := i.QueryContext(ctx, roleMembersQuery)
	if err != nil {
		return fmt.Errorf("postgres: querying role members: %w", err)
	}
	defer memberRows.Close()
	for memberRows.Next() {
		var roleName, memberOf string
		if err := memberRows.Scan(&roleName, &memberOf); err != nil {
			return fmt.Errorf("postgres: scanning role member: %w", err)
		}
		if role, ok := roles[roleName]; ok {
			role.MemberOf = append(role.MemberOf, memberOf)
		}
	}
	return memberRows.Err()
}

// inspectPolicies queries pg_policies and attaches RLS policies to their tables.
func (i *inspect) inspectPolicies(ctx context.Context, r *schema.Realm, _ *schema.InspectOptions) error {
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
	query := fmt.Sprintf(policiesQuery, strings.Join(placeholders, ", "))
	rows, err := i.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: inspecting policies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schemaName, tableName, policyName, permissive, cmd, roles, using, check string
		if err := rows.Scan(&schemaName, &tableName, &policyName, &permissive, &cmd, &roles, &using, &check); err != nil {
			return fmt.Errorf("postgres: scanning policy: %w", err)
		}
		s, ok := schemas[schemaName]
		if !ok {
			continue
		}
		p := &Policy{
			Name:  policyName,
			As:    permissive,
			For:   cmd,
			Using: using,
			Check: check,
		}
		if roles != "" {
			p.To = strings.Split(roles, ",")
		}
		for _, tbl := range s.Tables {
			if tbl.Name == tableName {
				p.Table = tbl
				p.Deps = append(p.Deps, tbl)
				break
			}
		}
		s.Objects = append(s.Objects, p)
	}
	return rows.Err()
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
	case *Sequence:
		create, drop := s.createDropSequence(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create sequence %q", o.Name),
		})
	case *DomainType:
		create, drop := s.createDropDomain(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create domain type %q", o.T),
		})
	case *CompositeType:
		create, drop := s.createDropComposite(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create composite type %q", o.T),
		})
	case *CollationObj:
		create, drop := s.createDropCollation(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create collation %q", o.T),
		})
	case *RangeObj:
		create, drop := s.createDropRange(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create range type %q", o.T),
		})
	case *Extension:
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %q", o.T),
			Reverse: fmt.Sprintf("DROP EXTENSION IF EXISTS %q", o.T),
			Comment: fmt.Sprintf("create extension %q", o.T),
		})
	case *Policy:
		create, drop := s.createDropPolicy(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create policy %q on table %q", o.Name, o.Table.Name),
		})
	case *EventTrigger:
		create, drop := s.createDropEventTrigger(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create event trigger %q", o.Name),
		})
	case *Role:
		create, drop := s.createDropRole(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create role %q", o.Name),
		})
	case *Cast:
		create, drop := s.createDropCast(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create cast (%s AS %s)", o.Source, o.Target),
		})
	case *Aggregate:
		create, drop := s.createDropAggregate(o)
		s.append(&migrate.Change{
			Source:  add,
			Cmd:     create,
			Reverse: drop,
			Comment: fmt.Sprintf("create aggregate %q", o.Name),
		})
	}
	return nil
}

func (s *state) dropObject(drop *schema.DropObject) error {
	cascade := sqlx.Has(drop.Extra, &Cascade{})
	switch o := drop.O.(type) {
	case *schema.EnumType:
		create, dropE := s.createDropEnum(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropE, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop enum type %q", o.T),
		})
	case *Sequence:
		create, dropS := s.createDropSequence(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropS, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop sequence %q", o.Name),
		})
	case *DomainType:
		create, dropD := s.createDropDomain(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropD, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop domain type %q", o.T),
		})
	case *CompositeType:
		create, dropC := s.createDropComposite(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropC, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop composite type %q", o.T),
		})
	case *CollationObj:
		create, dropCo := s.createDropCollation(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropCo, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop collation %q", o.T),
		})
	case *RangeObj:
		create, dropR := s.createDropRange(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropR, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop range type %q", o.T),
		})
	case *Extension:
		cmd := fmt.Sprintf("DROP EXTENSION IF EXISTS %q", o.T)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(cmd, cascade),
			Reverse: fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %q", o.T),
			Comment: fmt.Sprintf("drop extension %q", o.T),
		})
	case *Policy:
		create, dropP := s.createDropPolicy(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropP, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop policy %q on table %q", o.Name, o.Table.Name),
		})
	case *EventTrigger:
		create, dropET := s.createDropEventTrigger(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropET, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop event trigger %q", o.Name),
		})
	case *Role:
		create, dropR := s.createDropRole(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropR, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop role %q", o.Name),
		})
	case *Cast:
		create, dropC := s.createDropCast(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropC, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop cast (%s AS %s)", o.Source, o.Target),
		})
	case *Aggregate:
		create, dropA := s.createDropAggregate(o)
		s.append(&migrate.Change{
			Source:  drop,
			Cmd:     withCascadeStr(dropA, cascade),
			Reverse: create,
			Comment: fmt.Sprintf("drop aggregate %q", o.Name),
		})
	}
	return nil
}

// withCascadeStr appends " CASCADE" to a SQL statement if cascade is true.
func withCascadeStr(stmt string, cascade bool) string {
	if cascade {
		return stmt + " CASCADE"
	}
	return stmt
}

func (s *state) modifyObject(modify *schema.ModifyObject) error {
	if _, ok := modify.From.(*schema.EnumType); ok {
		return s.alterEnum(modify)
	}
	// Policies: drop and recreate (ALTER POLICY can't change AS or FOR).
	if p, ok := modify.From.(*Policy); ok {
		_, drop := s.createDropPolicy(p)
		to := modify.To.(*Policy)
		create, _ := s.createDropPolicy(to)
		s.append(&migrate.Change{
			Source:  modify,
			Cmd:     drop,
			Comment: fmt.Sprintf("drop policy %q on table %q", p.Name, p.Table.Name),
		})
		s.append(&migrate.Change{
			Source:  modify,
			Cmd:     create,
			Comment: fmt.Sprintf("create policy %q on table %q", to.Name, to.Table.Name),
		})
		return nil
	}
	// Event triggers: drop and recreate.
	if et, ok := modify.From.(*EventTrigger); ok {
		_, drop := s.createDropEventTrigger(et)
		to := modify.To.(*EventTrigger)
		create, _ := s.createDropEventTrigger(to)
		s.append(&migrate.Change{
			Source:  modify,
			Cmd:     drop,
			Comment: fmt.Sprintf("drop event trigger %q", et.Name),
		})
		s.append(&migrate.Change{
			Source:  modify,
			Cmd:     create,
			Comment: fmt.Sprintf("create event trigger %q", to.Name),
		})
		return nil
	}
	return nil
}

// convertToSerial finds the column on the given table that uses nextval() for the
// given sequence, and converts it from integer + DEFAULT nextval() to SerialType.
// This is only called for auto-owned sequences (deptype='a' in pg_depend).
func convertToSerial(s *schema.Schema, tableName, columnName, seqName string) {
	t, ok := s.Table(tableName)
	if !ok {
		return
	}
	c, ok := t.Column(columnName)
	if !ok {
		return
	}
	tt, ok := c.Type.Type.(*schema.IntegerType)
	if !ok {
		return
	}
	st := &SerialType{SequenceName: seqName}
	st.SetType(tt)
	c.Type.Raw = st.T
	c.Type.Type = st
	c.Default = nil // serial columns don't need an explicit DEFAULT
}

// mustFormat formats a schema.Type to its SQL string, returning an empty string on error.
func mustFormat(t schema.Type) string {
	s, _ := FormatType(t)
	return s
}

// createDropSequence returns CREATE and DROP statements for a sequence.
func (s *state) createDropSequence(seq *Sequence) (string, string) {
	b := s.Build("CREATE SEQUENCE")
	b.SchemaResource(seq.Schema, seq.Name)
	if seq.Type != nil {
		b.P("AS", mustFormat(seq.Type))
	}
	b.P("START WITH", strconv.FormatInt(seq.Start, 10))
	b.P("INCREMENT BY", strconv.FormatInt(seq.Increment, 10))
	if seq.Min != nil {
		b.P("MINVALUE", strconv.FormatInt(*seq.Min, 10))
	} else {
		b.P("NO MINVALUE")
	}
	if seq.Max != nil {
		b.P("MAXVALUE", strconv.FormatInt(*seq.Max, 10))
	} else {
		b.P("NO MAXVALUE")
	}
	if seq.Cache > 1 {
		b.P("CACHE", strconv.FormatInt(seq.Cache, 10))
	}
	if seq.Cycle {
		b.P("CYCLE")
	}
	create := b.String()
	drop := s.Build("DROP SEQUENCE IF EXISTS").SchemaResource(seq.Schema, seq.Name).String()
	return create, drop
}

// createDropDomain returns CREATE and DROP statements for a domain type.
func (s *state) createDropDomain(d *DomainType) (string, string) {
	b := s.Build("CREATE DOMAIN")
	b.SchemaResource(d.Schema, d.T)
	b.P("AS", mustFormat(d.Type))
	if !d.Null {
		b.P("NOT NULL")
	}
	if d.Default != nil {
		if raw, ok := d.Default.(*schema.RawExpr); ok {
			b.P("DEFAULT", raw.X)
		}
	}
	for _, ck := range d.Checks {
		expr := ck.Expr
		// pg_get_constraintdef returns "CHECK (...)" — use it directly.
		if strings.HasPrefix(strings.ToUpper(expr), "CHECK") {
			b.P("CONSTRAINT").Ident(ck.Name).P(expr)
		} else {
			b.P("CONSTRAINT").Ident(ck.Name).P("CHECK (").P(expr).P(")")
		}
	}
	create := b.String()
	drop := s.Build("DROP DOMAIN IF EXISTS").SchemaResource(d.Schema, d.T).String()
	return create, drop
}

// createDropPolicy returns CREATE and DROP statements for an RLS policy.
func (s *state) createDropPolicy(p *Policy) (string, string) {
	b := s.Build("CREATE POLICY")
	b.Ident(p.Name).P("ON")
	if p.Table != nil {
		b.SchemaResource(p.Table.Schema, p.Table.Name)
	}
	if p.As != "" {
		b.P("AS", strings.ToUpper(p.As))
	}
	if p.For != "" {
		b.P("FOR", strings.ToUpper(p.For))
	}
	if len(p.To) > 0 {
		b.P("TO", strings.Join(p.To, ", "))
	}
	if p.Using != "" {
		b.P("USING (" + p.Using + ")")
	}
	if p.Check != "" {
		b.P("WITH CHECK (" + p.Check + ")")
	}
	create := b.String()
	drop := s.Build("DROP POLICY IF EXISTS").Ident(p.Name)
	if p.Table != nil {
		drop.P("ON").SchemaResource(p.Table.Schema, p.Table.Name)
	}
	return create, drop.String()
}

// createDropRole returns CREATE and DROP statements for a database role.
func (s *state) createDropRole(r *Role) (string, string) {
	b := s.Build("CREATE ROLE")
	b.Ident(r.Name)
	var opts []string
	if r.Superuser {
		opts = append(opts, "SUPERUSER")
	}
	if r.CreateDB {
		opts = append(opts, "CREATEDB")
	}
	if r.CreateRole {
		opts = append(opts, "CREATEROLE")
	}
	if r.Login {
		opts = append(opts, "LOGIN")
	}
	if !r.Inherit {
		opts = append(opts, "NOINHERIT")
	}
	if r.Replication {
		opts = append(opts, "REPLICATION")
	}
	if r.BypassRLS {
		opts = append(opts, "BYPASSRLS")
	}
	if r.ConnLimit >= 0 {
		opts = append(opts, fmt.Sprintf("CONNECTION LIMIT %d", r.ConnLimit))
	}
	if len(opts) > 0 {
		b.P(strings.Join(opts, " "))
	}
	if len(r.MemberOf) > 0 {
		b.P("IN ROLE")
		for i, m := range r.MemberOf {
			if i > 0 {
				b.Comma()
			}
			b.Ident(m)
		}
	}
	drop := s.Build("DROP ROLE IF EXISTS").Ident(r.Name).String()
	return b.String(), drop
}

// createDropEventTrigger returns CREATE and DROP statements for an event trigger.
func (s *state) createDropEventTrigger(et *EventTrigger) (string, string) {
	b := s.Build("CREATE EVENT TRIGGER")
	b.Ident(et.Name).P("ON", et.Event)
	if len(et.Tags) > 0 {
		quoted := make([]string, len(et.Tags))
		for i, t := range et.Tags {
			quoted[i] = "'" + t + "'"
		}
		b.P("WHEN TAG IN (" + strings.Join(quoted, ", ") + ")")
	}
	if et.FuncRef != "" {
		b.P("EXECUTE FUNCTION").Ident(et.FuncRef).P("()")
	}
	create := b.String()
	drop := s.Build("DROP EVENT TRIGGER IF EXISTS").Ident(et.Name).String()
	return create, drop
}

// createDropAggregate returns CREATE and DROP statements for an aggregate function.
func (s *state) createDropAggregate(a *Aggregate) (string, string) {
	// Build argument type list for both CREATE and DROP.
	argTypes := make([]string, len(a.Args))
	for i, arg := range a.Args {
		argTypes[i] = mustFormat(arg.Type)
	}
	argList := strings.Join(argTypes, ", ")

	b := s.Build("CREATE AGGREGATE")
	b.SchemaResource(a.Schema, a.Name)
	b.P("(" + argList + ") (")
	// Schema-qualify function references so they resolve regardless of search_path.
	sfunc := a.StateFunc
	if a.Schema != nil && a.Schema.Name != "" && !strings.Contains(sfunc, ".") {
		sfunc = fmt.Sprintf("%q.%q", a.Schema.Name, sfunc)
	} else {
		sfunc = fmt.Sprintf("%q", sfunc)
	}
	b.P("SFUNC =", sfunc)
	b.P(", STYPE =", mustFormat(a.StateType))
	if a.FinalFunc != "" {
		ffunc := a.FinalFunc
		if a.Schema != nil && a.Schema.Name != "" && !strings.Contains(ffunc, ".") {
			ffunc = fmt.Sprintf("%q.%q", a.Schema.Name, ffunc)
		} else {
			ffunc = fmt.Sprintf("%q", ffunc)
		}
		b.P(", FINALFUNC =", ffunc)
	}
	if a.InitVal != "" {
		b.P(", INITCOND = '" + a.InitVal + "'")
	}
	if a.SortOp != "" {
		b.P(", SORTOP =", a.SortOp)
	}
	if a.Parallel != "" && a.Parallel != "UNSAFE" {
		b.P(", PARALLEL =", a.Parallel)
	}
	b.P(")")
	create := b.String()
	db := s.Build("DROP AGGREGATE IF EXISTS")
	db.SchemaResource(a.Schema, a.Name)
	db.P("(" + argList + ")")
	drop := db.String()
	return create, drop
}

// createDropCast returns CREATE and DROP statements for a type cast.
func (s *state) createDropCast(c *Cast) (string, string) {
	create := fmt.Sprintf("CREATE CAST (%s AS %s)", c.Source, c.Target)
	switch c.Method {
	case "function":
		create += " WITH FUNCTION " + c.FuncRef
	case "inout":
		create += " WITH INOUT"
	default:
		create += " WITHOUT FUNCTION"
	}
	if c.Context == "assignment" {
		create += " AS ASSIGNMENT"
	} else if c.Context == "implicit" {
		create += " AS IMPLICIT"
	}
	drop := fmt.Sprintf("DROP CAST IF EXISTS (%s AS %s)", c.Source, c.Target)
	return create, drop
}

// createDropComposite returns CREATE and DROP statements for a composite type.
func (s *state) createDropComposite(c *CompositeType) (string, string) {
	b := s.Build("CREATE TYPE")
	b.SchemaResource(c.Schema, c.T)
	b.P("AS (")
	for i, f := range c.Fields {
		if i > 0 {
			b.Comma()
		}
		b.Ident(f.Name).P(mustFormat(f.Type.Type))
	}
	b.P(")")
	create := b.String()
	drop := s.Build("DROP TYPE IF EXISTS").SchemaResource(c.Schema, c.T).String()
	return create, drop
}

// createDropCollation returns CREATE and DROP statements for a collation.
func (s *state) createDropCollation(c *CollationObj) (string, string) {
	b := s.Build("CREATE COLLATION")
	b.SchemaResource(c.Schema, c.T)
	var parts []string
	if c.Provider != "" {
		parts = append(parts, "PROVIDER = "+c.Provider)
	}
	if c.Locale != "" {
		parts = append(parts, "LOCALE = '"+c.Locale+"'")
	}
	if c.LcCollate != "" {
		parts = append(parts, "LC_COLLATE = '"+c.LcCollate+"'")
	}
	if c.LcCtype != "" {
		parts = append(parts, "LC_CTYPE = '"+c.LcCtype+"'")
	}
	if c.Deterministic != nil && !*c.Deterministic {
		parts = append(parts, "DETERMINISTIC = false")
	}
	if len(parts) > 0 {
		b.P("(" + strings.Join(parts, ", ") + ")")
	}
	create := b.String()
	drop := s.Build("DROP COLLATION IF EXISTS").SchemaResource(c.Schema, c.T).String()
	return create, drop
}

func (s *state) createDropRange(r *RangeObj) (string, string) {
	b := s.Build("CREATE TYPE")
	b.SchemaResource(r.Schema, r.T)
	b.P("AS RANGE (SUBTYPE =", mustFormat(r.Subtype))
	if r.SubtypeDiff != "" {
		b.P(", SUBTYPE_DIFF =", r.SubtypeDiff)
	}
	if r.MultirangeName != "" {
		b.P(", MULTIRANGE_TYPE_NAME =", r.MultirangeName)
	}
	b.P(")")
	drop := s.Build("DROP TYPE IF EXISTS").SchemaResource(r.Schema, r.T).String()
	return b.String(), drop
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
func (*diff) RealmObjectDiff(from, to *schema.Realm) ([]schema.Change, error) {
	var changes []schema.Change
	// Drop realm-level objects.
	for _, o1 := range from.Objects {
		switch v1 := o1.(type) {
		case *Extension:
			if _, ok := to.Object(func(o schema.Object) bool {
				e2, ok := o.(*Extension)
				return ok && v1.T == e2.T
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *EventTrigger:
			if _, ok := to.Object(func(o schema.Object) bool {
				et2, ok := o.(*EventTrigger)
				return ok && v1.Name == et2.Name
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *Role:
			if _, ok := to.Object(func(o schema.Object) bool {
				r2, ok := o.(*Role)
				return ok && v1.Name == r2.Name
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *Cast:
			if _, ok := to.Object(func(o schema.Object) bool {
				c2, ok := o.(*Cast)
				return ok && v1.Source == c2.Source && v1.Target == c2.Target
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		}
	}
	// Add realm-level objects.
	for _, o1 := range to.Objects {
		switch v1 := o1.(type) {
		case *Extension:
			if _, ok := from.Object(func(o schema.Object) bool {
				e2, ok := o.(*Extension)
				return ok && v1.T == e2.T
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *EventTrigger:
			if _, ok := from.Object(func(o schema.Object) bool {
				et2, ok := o.(*EventTrigger)
				return ok && v1.Name == et2.Name
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *Role:
			if _, ok := from.Object(func(o schema.Object) bool {
				r2, ok := o.(*Role)
				return ok && v1.Name == r2.Name
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *Cast:
			if _, ok := from.Object(func(o schema.Object) bool {
				c2, ok := o.(*Cast)
				return ok && v1.Source == c2.Source && v1.Target == c2.Target
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		}
	}
	return changes, nil
}

// SchemaObjectDiff returns a changeset for migrating schema objects from
// one state to the other.
func (d *diff) SchemaObjectDiff(from, to *schema.Schema, opts *schema.DiffOptions) ([]schema.Change, error) {
	var changes []schema.Change
	// Drop or modify existing objects.
	for _, o1 := range from.Objects {
		switch v1 := o1.(type) {
		case *schema.EnumType:
			o2, ok := to.Object(func(o schema.Object) bool {
				e2, ok := o.(*schema.EnumType)
				return ok && v1.T == e2.T
			})
			if !ok {
				changes = append(changes, &schema.DropObject{O: o1})
				continue
			}
			if e2 := o2.(*schema.EnumType); !sqlx.ValuesEqual(v1.Values, e2.Values) {
				changes = append(changes, &schema.ModifyObject{From: v1, To: e2})
			}
		case *DomainType:
			if _, ok := to.Object(func(o schema.Object) bool {
				d2, ok := o.(*DomainType)
				return ok && v1.T == d2.T
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *CompositeType:
			if _, ok := to.Object(func(o schema.Object) bool {
				c2, ok := o.(*CompositeType)
				return ok && v1.T == c2.T
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *CollationObj:
			if _, ok := to.Object(func(o schema.Object) bool {
				c2, ok := o.(*CollationObj)
				return ok && v1.T == c2.T
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *RangeObj:
			if _, ok := to.Object(func(o schema.Object) bool {
				r2, ok := o.(*RangeObj)
				return ok && v1.T == r2.T
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *Sequence:
			if _, ok := to.Object(func(o schema.Object) bool {
				s2, ok := o.(*Sequence)
				return ok && v1.Name == s2.Name
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		case *Policy:
			o2, ok := to.Object(func(o schema.Object) bool {
				p2, ok := o.(*Policy)
				return ok && v1.Name == p2.Name && v1.Table != nil && p2.Table != nil && v1.Table.Name == p2.Table.Name
			})
			if !ok {
				changes = append(changes, &schema.DropObject{O: o1})
				continue
			}
			p2 := o2.(*Policy)
			if v1.As != p2.As || v1.For != p2.For || v1.Using != p2.Using || v1.Check != p2.Check || !sqlx.ValuesEqual(v1.To, p2.To) {
				changes = append(changes, &schema.ModifyObject{From: v1, To: p2})
			}
		case *Aggregate:
			if _, ok := to.Object(func(o schema.Object) bool {
				a2, ok := o.(*Aggregate)
				return ok && v1.Name == a2.Name
			}); !ok {
				changes = append(changes, &schema.DropObject{O: o1})
			}
		}
	}
	// Add new objects.
	for _, o1 := range to.Objects {
		switch v1 := o1.(type) {
		case *schema.EnumType:
			if _, ok := from.Object(func(o schema.Object) bool {
				e2, ok := o.(*schema.EnumType)
				return ok && v1.T == e2.T
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *DomainType:
			if _, ok := from.Object(func(o schema.Object) bool {
				d2, ok := o.(*DomainType)
				return ok && v1.T == d2.T
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *CompositeType:
			if _, ok := from.Object(func(o schema.Object) bool {
				c2, ok := o.(*CompositeType)
				return ok && v1.T == c2.T
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *CollationObj:
			if _, ok := from.Object(func(o schema.Object) bool {
				c2, ok := o.(*CollationObj)
				return ok && v1.T == c2.T
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *RangeObj:
			if _, ok := from.Object(func(o schema.Object) bool {
				r2, ok := o.(*RangeObj)
				return ok && v1.T == r2.T
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *Sequence:
			if _, ok := from.Object(func(o schema.Object) bool {
				s2, ok := o.(*Sequence)
				return ok && v1.Name == s2.Name
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *Policy:
			if _, ok := from.Object(func(o schema.Object) bool {
				p2, ok := o.(*Policy)
				return ok && v1.Name == p2.Name && v1.Table != nil && p2.Table != nil && v1.Table.Name == p2.Table.Name
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
		case *Aggregate:
			if _, ok := from.Object(func(o schema.Object) bool {
				a2, ok := o.(*Aggregate)
				return ok && v1.Name == a2.Name
			}); !ok {
				changes = append(changes, &schema.AddObject{O: v1})
			}
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

// convertDomains converts domain HCL specs to schema.DomainType objects and
// resolves column type references pointing to domains.
func convertDomains(tables []*sqlspec.Table, domains []*domain, r *schema.Realm) error {
	if len(domains) == 0 {
		return nil
	}
	byName := make(map[string]*DomainType)
	for _, d := range domains {
		ns, err := specutil.SchemaName(d.Schema)
		if err != nil {
			return fmt.Errorf("extract schema name from domain reference: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("schema %q defined on domain %q was not found in realm", ns, d.Name)
		}
		dt := &DomainType{T: d.Name, Schema: s, Null: d.Null}
		if d.Type != nil {
			t, err := TypeRegistry.Type(d.Type, nil)
			if err != nil {
				return fmt.Errorf("resolve domain %q base type: %w", d.Name, err)
			}
			dt.Type = t
		}
		if d.Default != (cty.Value{}) && !d.Default.IsNull() {
			expr, err := specutil.Default(d.Default)
			if err != nil {
				return fmt.Errorf("resolve domain %q default: %w", d.Name, err)
			}
			dt.Default = expr
		}
		for _, ck := range d.Checks {
			dt.Checks = append(dt.Checks, &schema.Check{Name: ck.Name, Expr: ck.Expr})
		}
		s.AddObjects(dt)
		byName[d.Name] = dt
	}
	// Resolve column type references to domains.
	for _, t := range tables {
		for _, c := range t.Columns {
			if !c.Type.IsRefTo("domain") {
				continue
			}
			name, err := domainName(c.Type)
			if err != nil {
				return err
			}
			dt, ok := byName[name]
			if !ok {
				return fmt.Errorf("domain %q was not found in realm", name)
			}
			schemaT, err := specutil.SchemaName(t.Schema)
			if err != nil {
				continue
			}
			ts, ok := r.Schema(schemaT)
			if !ok {
				continue
			}
			tt, ok := ts.Table(t.Name)
			if !ok {
				continue
			}
			cc, ok := tt.Column(c.Name)
			if !ok {
				continue
			}
			cc.Type.Type = dt
		}
	}
	return nil
}

// domainName extracts the domain name from a type reference.
func domainName(t *schemahcl.Type) (string, error) {
	ref := &schemahcl.Ref{V: t.T}
	parts, err := ref.ByType("domain")
	if err != nil {
		return "", err
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("empty domain reference")
	}
	return parts[len(parts)-1], nil
}

func convertAggregate(d *doc, r *schema.Realm) error {
	for _, a := range d.Aggregates {
		ns, err := specutil.SchemaName(a.Schema)
		if err != nil {
			return fmt.Errorf("postgres: aggregate %q: %w", a.Name, err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("postgres: schema %q for aggregate %q not found", ns, a.Name)
		}
		agg := &Aggregate{
			Name:   a.Name,
			Schema: s,
		}
		if v, ok := a.Attr("state_func"); ok {
			str, err := v.String()
			if err == nil {
				agg.StateFunc = str
			} else if ref, err2 := v.Ref(); err2 == nil {
				// e.g., function.my_func → extract "my_func"
				parts := strings.Split(ref, ".")
				agg.StateFunc = parts[len(parts)-1]
			}
		}
		if v, ok := a.Attr("state_type"); ok {
			str, err := v.String()
			if err != nil {
				return fmt.Errorf("postgres: aggregate %q state_type: %w", a.Name, err)
			}
			t, err := ParseType(str)
			if err != nil {
				t = &schema.UnsupportedType{T: str}
			}
			agg.StateType = t
		}
		if v, ok := a.Attr("initial_value"); ok {
			str, _ := v.String()
			agg.InitVal = str
		}
		if v, ok := a.Attr("parallel"); ok {
			str, _ := v.String()
			agg.Parallel = str
		}
		// Parse arguments.
		for _, arg := range a.Args {
			fa := &schema.FuncArg{Name: arg.Name}
			if arg.Type != nil {
				t, err := TypeRegistry.Type(arg.Type, nil)
				if err != nil {
					return fmt.Errorf("postgres: aggregate %q arg type: %w", a.Name, err)
				}
				fa.Type = t
			}
			agg.Args = append(agg.Args, fa)
		}
		s.Objects = append(s.Objects, agg)
	}
	return nil
}

// convertSequences converts sequence HCL specs to postgres Sequence objects.
func convertSequences(_ []*sqlspec.Table, seqs []*sqlspec.Sequence, r *schema.Realm) error {
	for _, seq := range seqs {
		ns, err := specutil.SchemaName(seq.Schema)
		if err != nil {
			return fmt.Errorf("extract schema name from sequence reference: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("schema %q defined on sequence %q was not found in realm", ns, seq.Name)
		}
		ps := &Sequence{Name: seq.Name, Schema: s}
		if a, ok := seq.Attr("start"); ok {
			v, _ := a.Int64()
			ps.Start = v
		}
		if a, ok := seq.Attr("increment"); ok {
			v, _ := a.Int64()
			ps.Increment = v
		}
		if a, ok := seq.Attr("cache"); ok {
			v, _ := a.Int64()
			ps.Cache = v
		}
		if a, ok := seq.Attr("cycle"); ok {
			v, _ := a.Bool()
			ps.Cycle = v
		}
		if a, ok := seq.Attr("min"); ok {
			v, _ := a.Int64()
			ps.Min = &v
		}
		if a, ok := seq.Attr("max"); ok {
			v, _ := a.Int64()
			ps.Max = &v
		}
		if a, ok := seq.Attr("type"); ok {
			typ, err := a.Type()
			if err == nil {
				t, err := TypeRegistry.Type(typ, nil)
				if err == nil {
					ps.Type = t
				}
			}
		}
		s.AddObjects(ps)
	}
	return nil
}

// convertPolicies converts policy HCL specs to Policy objects attached to their tables.
func convertPolicies(_ []*sqlspec.Table, ps []*policy, r *schema.Realm) error {
	for _, p := range ps {
		if p.On == nil {
			return fmt.Errorf("postgres: policy %q missing required 'on' table reference", p.Name)
		}
		// Find the table matching the On reference.
		var targetTable *schema.Table
		var targetSchema *schema.Schema
		for _, s := range r.Schemas {
			for _, tbl := range s.Tables {
				ref := schemahcl.BuildRef([]schemahcl.PathIndex{{T: "table", V: []string{tbl.Name}}})
				if ref.V == p.On.V {
					targetTable = tbl
					targetSchema = s
					break
				}
			}
			if targetTable != nil {
				break
			}
		}
		if targetTable == nil {
			return fmt.Errorf("postgres: policy %q: table not found for reference %q", p.Name, p.On.V)
		}
		pol := &Policy{
			Name:  p.Name,
			Table: targetTable,
		}
		pol.Deps = append(pol.Deps, targetTable)
		if a, ok := p.Attr("as"); ok {
			pol.As, _ = a.String()
		}
		if a, ok := p.Attr("for"); ok {
			pol.For, _ = a.String()
		}
		if a, ok := p.Attr("to"); ok {
			if lst, err := a.Strings(); err == nil {
				pol.To = lst
			} else if s, err := a.String(); err == nil {
				pol.To = []string{s}
			}
		}
		if a, ok := p.Attr("using"); ok {
			pol.Using, _ = a.String()
		}
		if a, ok := p.Attr("with_check"); ok {
			pol.Check, _ = a.String()
		}
		targetSchema.AddObjects(pol)
	}
	return nil
}

// convertComposites converts composite HCL specs to CompositeType objects.
func convertComposites(composites []*composite, r *schema.Realm) error {
	for _, c := range composites {
		ns, err := specutil.SchemaName(c.Schema)
		if err != nil {
			return fmt.Errorf("extract schema name from composite reference: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("schema %q defined on composite %q was not found in realm", ns, c.Name)
		}
		ct := &CompositeType{T: c.Name, Schema: s}
		for _, f := range c.Fields {
			if f.Type == nil {
				continue
			}
			t, err := TypeRegistry.Type(f.Type, nil)
			if err != nil {
				return fmt.Errorf("resolve composite %q field %q type: %w", c.Name, f.Name, err)
			}
			ct.Fields = append(ct.Fields, &schema.Column{
				Name: f.Name,
				Type: &schema.ColumnType{Type: t},
			})
		}
		s.AddObjects(ct)
	}
	return nil
}

// convertCollations converts collation HCL specs to CollationObj objects on the schema.
func convertCollations(collations []*collation, r *schema.Realm) error {
	for _, c := range collations {
		ns, err := specutil.SchemaName(c.Schema)
		if err != nil {
			return fmt.Errorf("extract schema name from collation reference: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("schema %q defined on collation %q was not found in realm", ns, c.Name)
		}
		co := &CollationObj{T: c.Name, Schema: s}
		if a, ok := c.Attr("provider"); ok {
			co.Provider, _ = a.String()
		}
		if a, ok := c.Attr("locale"); ok {
			co.Locale, _ = a.String()
		}
		if a, ok := c.Attr("lc_collate"); ok {
			co.LcCollate, _ = a.String()
		}
		if a, ok := c.Attr("lc_ctype"); ok {
			co.LcCtype, _ = a.String()
		}
		if a, ok := c.Attr("deterministic"); ok {
			v, err := a.Bool()
			if err == nil {
				co.Deterministic = &v
			}
		}
		s.AddObjects(co)
	}
	return nil
}

// convertRanges converts range HCL specs to RangeObj objects on the schema.
func convertRanges(ranges []*rangeType, r *schema.Realm) error {
	for _, rng := range ranges {
		ns, err := specutil.SchemaName(rng.Schema)
		if err != nil {
			return fmt.Errorf("extract schema name from range reference: %w", err)
		}
		s, ok := r.Schema(ns)
		if !ok {
			return fmt.Errorf("schema %q defined on range %q was not found in realm", ns, rng.Name)
		}
		ro := &RangeObj{T: rng.Name, Schema: s}
		if a, ok := rng.Attr("subtype"); ok {
			typ, err := a.Type()
			if err == nil {
				t, err := TypeRegistry.Type(typ, nil)
				if err == nil {
					ro.Subtype = t
				}
			}
		}
		if a, ok := rng.Attr("subtype_diff"); ok {
			ro.SubtypeDiff, _ = a.String()
		}
		if a, ok := rng.Attr("multirange_type_name"); ok {
			ro.MultirangeName, _ = a.String()
		}
		s.AddObjects(ro)
	}
	return nil
}

// convertExtensions converts extension HCL specs to Extension objects on the realm.
func convertExtensions(exs []*extension, r *schema.Realm) error {
	for _, e := range exs {
		ext := &Extension{T: e.Name}
		if a, ok := e.Attr("version"); ok {
			ext.Version, _ = a.String()
		}
		if a, ok := e.Attr("schema"); ok {
			ext.Schema, _ = a.String()
		}
		r.Objects = append(r.Objects, ext)
	}
	return nil
}

// convertEventTriggers converts event trigger HCL specs to EventTrigger realm objects.
func convertEventTriggers(evs []*eventTrigger, realm *schema.Realm) error {
	for _, ev := range evs {
		et := &EventTrigger{Name: ev.Name}
		if a, ok := ev.Attr("on"); ok {
			et.Event, _ = a.String()
		}
		if a, ok := ev.Attr("tags"); ok {
			et.Tags, _ = a.Strings()
		}
		if block, ok := ev.Extra.Resource("execute"); ok {
			if a, ok := block.Attr("function"); ok {
				if ref, err := a.Ref(); err == nil {
					hclRef := &schemahcl.Ref{V: ref}
					if names, err := hclRef.ByType("function"); err == nil && len(names) > 0 {
						et.FuncRef = names[0]
					}
				}
			}
		}
		realm.Objects = append(realm.Objects, et)
	}
	return nil
}

// convertRoles converts role HCL specs to Role realm objects.
func convertRoles(roles []*role, realm *schema.Realm) error {
	for _, r := range roles {
		obj := &Role{Name: r.Name, ConnLimit: -1, Inherit: true}
		if a, ok := r.Attr("superuser"); ok {
			obj.Superuser, _ = a.Bool()
		}
		if a, ok := r.Attr("create_db"); ok {
			obj.CreateDB, _ = a.Bool()
		}
		if a, ok := r.Attr("create_role"); ok {
			obj.CreateRole, _ = a.Bool()
		}
		if a, ok := r.Attr("login"); ok {
			obj.Login, _ = a.Bool()
		}
		if a, ok := r.Attr("inherit"); ok {
			obj.Inherit, _ = a.Bool()
		}
		if a, ok := r.Attr("replication"); ok {
			obj.Replication, _ = a.Bool()
		}
		if a, ok := r.Attr("bypass_rls"); ok {
			obj.BypassRLS, _ = a.Bool()
		}
		if a, ok := r.Attr("conn_limit"); ok {
			v, _ := a.Int()
			obj.ConnLimit = v
		}
		if a, ok := r.Attr("member_of"); ok {
			obj.MemberOf, _ = a.Strings()
		}
		realm.Objects = append(realm.Objects, obj)
	}
	return nil
}

// convertCasts converts cast HCL specs to Cast realm objects.
func convertCasts(casts []*castSpec, realm *schema.Realm) error {
	for _, c := range casts {
		obj := &Cast{Context: "explicit"}
		if a, ok := c.Attr("source"); ok {
			obj.Source, _ = a.String()
		}
		if a, ok := c.Attr("target"); ok {
			obj.Target, _ = a.String()
		}
		if a, ok := c.Attr("with"); ok {
			v, _ := a.String()
			if strings.EqualFold(v, "INOUT") {
				obj.Method = "inout"
			} else {
				obj.Method = "function"
				obj.FuncRef = v
			}
		} else {
			obj.Method = "without"
		}
		if a, ok := c.Attr("as"); ok {
			v, _ := a.String()
			obj.Context = strings.ToLower(v)
		}
		realm.Objects = append(realm.Objects, obj)
	}
	return nil
}

func normalizeRealm(*schema.Realm) error {
	return nil
}

func schemasObjectSpec(d *doc, schemas ...*schema.Schema) error {
	for _, s := range schemas {
		ref := specutil.SchemaRef(s.Name)
		for _, o := range s.Objects {
			switch o := o.(type) {
			case *Sequence:
				spec := &sqlspec.Sequence{
					Name:   o.Name,
					Schema: ref,
				}
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.Int64Attr("start", o.Start))
				spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.Int64Attr("increment", o.Increment))
				if o.Cache > 1 {
					spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.Int64Attr("cache", o.Cache))
				}
				if o.Cycle {
					spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.BoolAttr("cycle", true))
				}
				if o.Min != nil {
					spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.Int64Attr("min", *o.Min))
				}
				if o.Max != nil {
					spec.Extra.Attrs = append(spec.Extra.Attrs, schemahcl.Int64Attr("max", *o.Max))
				}
				if o.Type != nil {
					c, err := columnTypeSpec(o.Type)
					if err == nil {
						spec.Extra.Attrs = append(spec.Extra.Attrs, specutil.TypeAttr("type", c.Type))
					}
				}
				d.Sequences = append(d.Sequences, spec)
			}
		}
	}
	return nil
}

// objectSpec converts from a concrete schema objects into specs.
func objectSpec(d *doc, spec *specutil.SchemaSpec, s *schema.Schema) error {
	ref := specutil.SchemaRef(spec.Schema.Name)
	for _, o := range s.Objects {
		switch o := o.(type) {
		case *schema.EnumType:
			d.Enums = append(d.Enums, &enum{
				Name:   o.T,
				Values: o.Values,
				Schema: ref,
			})
		case *DomainType:
			ds := &domain{
				Name:   o.T,
				Schema: ref,
				Null:   o.Null,
			}
			if o.Type != nil {
				c, err := columnTypeSpec(o.Type)
				if err != nil {
					return err
				}
				ds.Type = c.Type
			}
			if o.Default != nil {
				if raw, ok := o.Default.(*schema.RawExpr); ok {
					ds.Default = schemahcl.RawExprValue(&schemahcl.RawExpr{X: raw.X})
				}
			}
			for _, ck := range o.Checks {
				ds.Checks = append(ds.Checks, &sqlspec.Check{
					Name: ck.Name,
					Expr: ck.Expr,
				})
			}
			d.Domains = append(d.Domains, ds)
		case *CompositeType:
			cs := &composite{
				Name:   o.T,
				Schema: ref,
			}
			for _, f := range o.Fields {
				if f.Type == nil {
					continue
				}
				c, err := columnTypeSpec(f.Type.Type)
				if err != nil {
					return err
				}
				cs.Fields = append(cs.Fields, &compositeField{
					Name: f.Name,
					Type: c.Type,
				})
			}
			d.Composites = append(d.Composites, cs)
		case *Policy:
			if o.Table == nil {
				continue
			}
			ps := &policy{
				Name: o.Name,
				On:   schemahcl.BuildRef([]schemahcl.PathIndex{{T: "table", V: []string{o.Table.Name}}}),
			}
			if o.As != "" {
				ps.Extra.Attrs = append(ps.Extra.Attrs, schemahcl.StringAttr("as", o.As))
			}
			if o.For != "" {
				ps.Extra.Attrs = append(ps.Extra.Attrs, schemahcl.StringAttr("for", o.For))
			}
			if len(o.To) > 0 {
				ps.Extra.Attrs = append(ps.Extra.Attrs, schemahcl.StringsAttr("to", o.To...))
			}
			if o.Using != "" {
				ps.Extra.Attrs = append(ps.Extra.Attrs, schemahcl.StringAttr("using", o.Using))
			}
			if o.Check != "" {
				ps.Extra.Attrs = append(ps.Extra.Attrs, schemahcl.StringAttr("with_check", o.Check))
			}
			d.Policies = append(d.Policies, ps)
		case *CollationObj:
			co := &collation{
				Name:   o.T,
				Schema: ref,
			}
			if o.Provider != "" {
				co.Extra.Attrs = append(co.Extra.Attrs, schemahcl.StringAttr("provider", o.Provider))
			}
			if o.Locale != "" {
				co.Extra.Attrs = append(co.Extra.Attrs, schemahcl.StringAttr("locale", o.Locale))
			}
			if o.LcCollate != "" {
				co.Extra.Attrs = append(co.Extra.Attrs, schemahcl.StringAttr("lc_collate", o.LcCollate))
			}
			if o.LcCtype != "" {
				co.Extra.Attrs = append(co.Extra.Attrs, schemahcl.StringAttr("lc_ctype", o.LcCtype))
			}
			if o.Deterministic != nil {
				co.Extra.Attrs = append(co.Extra.Attrs, schemahcl.BoolAttr("deterministic", *o.Deterministic))
			}
			d.Collations = append(d.Collations, co)
		case *RangeObj:
			rs := &rangeType{
				Name:   o.T,
				Schema: ref,
			}
			if o.Subtype != nil {
				c, err := columnTypeSpec(o.Subtype)
				if err != nil {
					return err
				}
				rs.Extra.Attrs = append(rs.Extra.Attrs, specutil.TypeAttr("subtype", c.Type))
			}
			if o.SubtypeDiff != "" {
				rs.Extra.Attrs = append(rs.Extra.Attrs, schemahcl.StringAttr("subtype_diff", o.SubtypeDiff))
			}
			if o.MultirangeName != "" {
				rs.Extra.Attrs = append(rs.Extra.Attrs, schemahcl.StringAttr("multirange_type_name", o.MultirangeName))
			}
			d.Ranges = append(d.Ranges, rs)
		case *Aggregate:
			as := &aggregate{
				Name:   o.Name,
				Schema: ref,
			}
			as.Extra.Attrs = append(as.Extra.Attrs, schemahcl.StringAttr("state_func", o.StateFunc))
			if o.StateType != nil {
				as.Extra.Attrs = append(as.Extra.Attrs, schemahcl.StringAttr("state_type", mustFormat(o.StateType)))
			}
			if o.FinalFunc != "" {
				as.Extra.Attrs = append(as.Extra.Attrs, schemahcl.StringAttr("final_func", o.FinalFunc))
			}
			if o.InitVal != "" {
				as.Extra.Attrs = append(as.Extra.Attrs, schemahcl.StringAttr("initial_value", o.InitVal))
			}
			if o.Parallel != "" && o.Parallel != "UNSAFE" {
				as.Extra.Attrs = append(as.Extra.Attrs, schemahcl.StringAttr("parallel", o.Parallel))
			}
			for _, arg := range o.Args {
				fa := &sqlspec.FuncArg{Name: arg.Name}
				if arg.Type != nil {
					c, err := columnTypeSpec(arg.Type)
					if err != nil {
						return err
					}
					fa.Type = c.Type
				}
				as.Args = append(as.Args, fa)
			}
			d.Aggregates = append(d.Aggregates, as)
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
	'{}' AS attrs,
	t3.relrowsecurity,
	t3.relforcerowsecurity
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
	'{}' AS attrs,
	t3.relrowsecurity,
	t3.relforcerowsecurity
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
	// Query to list view columns via pg_attribute.
	viewColumnsQuery = `
SELECT
	n.nspname AS schema_name,
	c.relname AS view_name,
	a.attname AS column_name,
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
	NOT a.attnotnull AS is_nullable
FROM
	pg_catalog.pg_class c
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE
	c.relkind IN ('v', 'm')
	AND n.nspname IN (%s)
ORDER BY
	n.nspname, c.relname, a.attnum
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
	// Query to list domain types.
	domainsQuery = `
SELECT
	n.nspname AS schema_name,
	t.typname AS type_name,
	pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type,
	t.typnotnull AS not_null,
	pg_catalog.pg_get_expr(t.typdefaultbin, 0) AS default_value
FROM
	pg_catalog.pg_type t
	JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE
	t.typtype = 'd'
	AND n.nspname IN (%s)
ORDER BY
	n.nspname, t.typname
`
	// Query to list check constraints on domain types.
	domainChecksQuery = `
SELECT
	n.nspname AS schema_name,
	t.typname AS type_name,
	c.conname AS constraint_name,
	pg_catalog.pg_get_constraintdef(c.oid) AS check_expr
FROM
	pg_catalog.pg_constraint c
	JOIN pg_catalog.pg_type t ON t.oid = c.contypid
	JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE
	c.contype = 'c'
	AND t.typtype = 'd'
	AND n.nspname IN (%s)
ORDER BY
	n.nspname, t.typname, c.conname
`
	// Query to list composite types and their fields.
	compositesQuery = `
SELECT
	n.nspname AS schema_name,
	t.typname AS type_name,
	a.attname AS field_name,
	pg_catalog.format_type(a.atttypid, a.atttypmod) AS field_type
FROM
	pg_catalog.pg_type t
	JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
	JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
	JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE
	t.typtype = 'c'
	AND n.nspname IN (%s)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_class r
		WHERE r.oid = t.typrelid AND r.relkind IN ('r', 'v', 'm', 'p')
	)
	-- Exclude extension-owned types (e.g., tablefunc's crosstab types).
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_depend d
		WHERE d.objid = t.oid AND d.deptype = 'e'
	)
ORDER BY
	n.nspname, t.typname, a.attnum
`
	// Query to list independent sequences (not owned by any column).
	sequencesQuery = `
SELECT
	n.nspname AS schema_name,
	c.relname AS sequence_name,
	pg_catalog.format_type(s.seqtypid, NULL) AS data_type,
	s.seqstart AS start_value,
	s.seqincrement AS increment,
	s.seqcache AS cache_size,
	s.seqmin AS min_value,
	s.seqmax AS max_value,
	s.seqcycle AS cycle,
	d_table.relname AS owner_table,
	d_col.attname AS owner_column,
	dep.deptype AS dep_type
FROM
	pg_catalog.pg_sequence s
	JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = c.oid AND dep.deptype IN ('a', 'i') AND dep.classid = 'pg_class'::regclass
	LEFT JOIN pg_catalog.pg_class d_table ON d_table.oid = dep.refobjid AND dep.refclassid = 'pg_class'::regclass
	LEFT JOIN pg_catalog.pg_attribute d_col ON d_col.attrelid = dep.refobjid AND d_col.attnum = dep.refobjsubid
WHERE
	n.nspname IN (%s)
ORDER BY
	n.nspname, c.relname
`
	// Query to list user-defined aggregates.
	aggregatesQuery = `
SELECT
	n.nspname AS schema_name,
	p.proname AS agg_name,
	sf.proname AS state_func,
	pg_catalog.format_type(a.aggtranstype, NULL) AS state_type,
	ff.proname AS final_func,
	a.agginitval AS init_val,
	COALESCE(so.oprname, '') AS sort_op,
	CASE p.proparallel
		WHEN 's' THEN 'SAFE'
		WHEN 'u' THEN 'UNSAFE'
		WHEN 'r' THEN 'RESTRICTED'
		ELSE ''
	END AS parallel,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS arg_types
FROM
	pg_catalog.pg_aggregate a
	JOIN pg_catalog.pg_proc p ON p.oid = a.aggfnoid
	JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
	JOIN pg_catalog.pg_proc sf ON sf.oid = a.aggtransfn
	LEFT JOIN pg_catalog.pg_proc ff ON ff.oid = a.aggfinalfn
	LEFT JOIN pg_catalog.pg_operator so ON so.oid = a.aggsortop
WHERE
	n.nspname IN (%s)
ORDER BY
	n.nspname, p.proname
`
	// Query to find dependencies between objects using pg_depend.
	// Returns (source_schema, source_name, source_kind, dep_schema, dep_name, dep_kind) rows.
	depsQuery = `
SELECT DISTINCT
	src_ns.nspname  AS src_schema,
	COALESCE(src_proc.proname, src_rel.relname) AS src_name,
	CASE
		WHEN src_proc.oid IS NOT NULL AND src_proc.prokind = 'p' THEN 'procedure'
		WHEN src_proc.oid IS NOT NULL THEN 'function'
		WHEN src_rel.relkind = 'r' THEN 'table'
		WHEN src_rel.relkind = 'v' THEN 'view'
		WHEN src_rel.relkind = 'm' THEN 'matview'
		ELSE 'unknown'
	END AS src_kind,
	dep_ns.nspname  AS dep_schema,
	COALESCE(dep_proc.proname, dep_rel.relname, dep_typ.typname) AS dep_name,
	CASE
		WHEN dep_proc.oid IS NOT NULL AND dep_proc.prokind = 'p' THEN 'procedure'
		WHEN dep_proc.oid IS NOT NULL THEN 'function'
		WHEN dep_rel.relkind = 'r' THEN 'table'
		WHEN dep_rel.relkind = 'v' THEN 'view'
		WHEN dep_rel.relkind = 'm' THEN 'matview'
		WHEN dep_typ.typtype = 'e' THEN 'enum'
		WHEN dep_typ.typtype = 'd' THEN 'domain'
		WHEN dep_typ.typtype = 'c' THEN 'composite'
		ELSE 'unknown'
	END AS dep_kind
FROM pg_catalog.pg_depend d
-- Source: function/procedure
LEFT JOIN pg_catalog.pg_proc src_proc ON d.classid = 'pg_proc'::regclass AND src_proc.oid = d.objid
LEFT JOIN pg_catalog.pg_namespace src_proc_ns ON src_proc.pronamespace = src_proc_ns.oid
-- Source: table/view
LEFT JOIN pg_catalog.pg_class src_rel ON d.classid = 'pg_class'::regclass AND src_rel.oid = d.objid AND src_rel.relkind IN ('r', 'v', 'm')
LEFT JOIN pg_catalog.pg_namespace src_rel_ns ON src_rel.relnamespace = src_rel_ns.oid
-- Combined source namespace
CROSS JOIN LATERAL (SELECT COALESCE(src_proc_ns.nspname, src_rel_ns.nspname) AS nspname) src_ns
-- Dependency target: table/view
LEFT JOIN pg_catalog.pg_class dep_rel ON d.refclassid = 'pg_class'::regclass AND dep_rel.oid = d.refobjid AND dep_rel.relkind IN ('r', 'v', 'm')
LEFT JOIN pg_catalog.pg_namespace dep_rel_ns ON dep_rel.relnamespace = dep_rel_ns.oid
-- Dependency target: function/procedure
LEFT JOIN pg_catalog.pg_proc dep_proc ON d.refclassid = 'pg_proc'::regclass AND dep_proc.oid = d.refobjid
LEFT JOIN pg_catalog.pg_namespace dep_proc_ns ON dep_proc.pronamespace = dep_proc_ns.oid
-- Dependency target: type (enum, domain, composite)
LEFT JOIN pg_catalog.pg_type dep_typ ON d.refclassid = 'pg_type'::regclass AND dep_typ.oid = d.refobjid AND dep_typ.typtype IN ('e', 'd', 'c')
LEFT JOIN pg_catalog.pg_namespace dep_typ_ns ON dep_typ.typnamespace = dep_typ_ns.oid
-- Combined dep namespace
CROSS JOIN LATERAL (SELECT COALESCE(dep_rel_ns.nspname, dep_proc_ns.nspname, dep_typ_ns.nspname) AS nspname) dep_ns
WHERE
	d.deptype IN ('n', 'a')
	AND (src_proc.oid IS NOT NULL OR src_rel.oid IS NOT NULL)
	AND (dep_rel.oid IS NOT NULL OR dep_proc.oid IS NOT NULL OR dep_typ.oid IS NOT NULL)
	-- Exclude self-references
	AND NOT (d.classid = d.refclassid AND d.objid = d.refobjid)
	-- Only user schemas
	AND src_ns.nspname NOT IN ('pg_catalog', 'information_schema')
	AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY src_ns.nspname, src_name, dep_ns.nspname, dep_name
`
	// Query to list collations defined in the schemas.
	collationsQuery = `
SELECT
	n.nspname AS schema_name,
	c.collname AS collation_name,
	CASE c.collprovider WHEN 'd' THEN 'libc' WHEN 'c' THEN 'libc' WHEN 'i' THEN 'icu' WHEN 'b' THEN 'builtin' ELSE 'libc' END AS provider,
	COALESCE(c.collcollate, '') AS lc_collate,
	COALESCE(c.collctype, '') AS lc_ctype,
	c.collisdeterministic AS deterministic
FROM pg_catalog.pg_collation c
JOIN pg_catalog.pg_namespace n ON n.oid = c.collnamespace
WHERE n.nspname IN (%s)
ORDER BY n.nspname, c.collname
`
	// Query to list range types defined in the schemas.
	rangeTypesQuery = `
SELECT
	n.nspname AS schema_name,
	t.typname AS type_name,
	format_type(r.rngsubtype, NULL) AS subtype,
	COALESCE(p.proname, '') AS subtype_diff,
	COALESCE(mt.typname, '') AS multirange_name
FROM pg_catalog.pg_range r
JOIN pg_catalog.pg_type t ON t.oid = r.rngtypid
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
LEFT JOIN pg_catalog.pg_proc p ON p.oid = r.rngsubdiff
LEFT JOIN pg_catalog.pg_type mt ON mt.oid = r.rngmultitypid
WHERE n.nspname IN (%s)
ORDER BY n.nspname, t.typname
`
	// Query to list extensions.
	extensionsQuery = `
SELECT
	e.extname AS name,
	n.nspname AS schema_name,
	e.extversion AS version
FROM
	pg_catalog.pg_extension e
	JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
WHERE
	e.extname != 'plpgsql'
ORDER BY
	e.extname
`
	// Query to list row-level security policies.
	policiesQuery = `
SELECT
	p.schemaname,
	p.tablename,
	p.policyname,
	LOWER(p.permissive) AS permissive,
	LOWER(p.cmd) AS cmd,
	ARRAY_TO_STRING(p.roles, ',') AS roles,
	COALESCE(p.qual, '') AS using_expr,
	COALESCE(p.with_check, '') AS check_expr
FROM
	pg_catalog.pg_policies p
WHERE
	p.schemaname IN (%s)
ORDER BY
	p.schemaname, p.tablename, p.policyname
`
	// Query to list event triggers.
	eventTriggersQuery = `
SELECT
	e.evtname,
	e.evtevent,
	p.proname AS func_name,
	COALESCE(ARRAY_TO_STRING(e.evttags, ','), '') AS tags
FROM
	pg_catalog.pg_event_trigger e
	JOIN pg_catalog.pg_proc p ON p.oid = e.evtfoid
ORDER BY
	e.evtname
`
	// Query to list non-system roles (excludes pg_* roles and the postgres superuser).
	rolesQuery = `
SELECT
    r.rolname,
    r.rolsuper,
    r.rolcreatedb,
    r.rolcreaterole,
    r.rolcanlogin,
    r.rolinherit,
    r.rolreplication,
    r.rolbypassrls,
    r.rolconnlimit,
    pg_catalog.shobj_description(r.oid, 'pg_authid') AS comment
FROM pg_catalog.pg_roles r
WHERE r.rolname NOT LIKE 'pg_%'
  AND r.rolname != 'postgres'
  AND r.oid >= 16384
ORDER BY r.rolname
`
	// Query to list role membership for non-system roles.
	roleMembersQuery = `
SELECT
    r.rolname AS role_name,
    m.rolname AS member_of
FROM pg_catalog.pg_auth_members a
JOIN pg_catalog.pg_roles r ON r.oid = a.member
JOIN pg_catalog.pg_roles m ON m.oid = a.roleid
WHERE r.rolname NOT LIKE 'pg_%'
  AND r.oid >= 16384
ORDER BY r.rolname, m.rolname
`
)
