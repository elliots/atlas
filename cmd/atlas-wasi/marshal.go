// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package main

import (
	"reflect"
	"strings"

	"ariga.io/atlas/sql/schema"
)

// Flat, cycle-free types for JSON serialization.
// The schema.* types have back-pointers (Table→Schema, Schema→Realm, etc.)
// that cause cycles in json.Marshal. These flat types omit them.

type flatResult struct {
	Schema     *flatRealm        `json:"schema,omitempty"`
	Statements []string          `json:"statements,omitempty"`
	Changes    []Change          `json:"changes,omitempty"`
	RenameCandidates []RenameCandidate `json:"renameCandidates,omitempty"`
	Error      string            `json:"error,omitempty"`
}

type flatRealm struct {
	Schemas []flatSchema `json:"schemas,omitempty"`
	Attrs   []any        `json:"attrs,omitempty"`
}

type flatSchema struct {
	Name   string      `json:"name"`
	Tables []flatTable `json:"tables,omitempty"`
	Views  []flatView  `json:"views,omitempty"`
	Funcs  []flatFunc  `json:"funcs,omitempty"`
	Procs  []flatProc  `json:"procs,omitempty"`
	Attrs  []any       `json:"attrs,omitempty"`
}

type flatTable struct {
	Name        string           `json:"name"`
	Columns     []flatColumn     `json:"columns,omitempty"`
	Indexes     []flatIndex      `json:"indexes,omitempty"`
	PrimaryKey  *flatIndex       `json:"primary_key,omitempty"`
	ForeignKeys []flatForeignKey `json:"foreign_keys,omitempty"`
	Triggers    []flatTrigger    `json:"triggers,omitempty"`
	Attrs       []any            `json:"attrs,omitempty"`
}

type flatColumn struct {
	Name    string          `json:"name"`
	Type    *flatColumnType `json:"type,omitempty"`
	Default any             `json:"default,omitempty"`
	Attrs   []any           `json:"attrs,omitempty"`
}

type flatColumnType struct {
	Type     string   `json:"T,omitempty"`
	Raw      string   `json:"raw,omitempty"`
	Null     bool     `json:"null,omitempty"`
	Category string   `json:"category,omitempty"` // string, integer, float, decimal, boolean, time, binary, json, uuid, spatial, enum, unknown
	IsCustom bool     `json:"is_custom,omitempty"`
	// Enum-specific fields
	EnumValues []string `json:"enum_values,omitempty"`
	// Composite type fields
	CompositeFields []flatCompositeField `json:"composite_fields,omitempty"`
}

type flatCompositeField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type flatIndex struct {
	Name   string          `json:"name,omitempty"`
	Unique bool            `json:"unique,omitempty"`
	Parts  []flatIndexPart `json:"parts,omitempty"`
	Attrs  []any           `json:"attrs,omitempty"`
}

type flatIndexPart struct {
	Column string `json:"column,omitempty"`
	Desc   bool   `json:"desc,omitempty"`
	Attrs  []any  `json:"attrs,omitempty"`
}

type flatForeignKey struct {
	Symbol     string   `json:"symbol,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	RefTable   string   `json:"ref_table,omitempty"`
	RefColumns []string `json:"ref_columns,omitempty"`
	OnUpdate   string   `json:"on_update,omitempty"`
	OnDelete   string   `json:"on_delete,omitempty"`
}

type flatView struct {
	Name    string       `json:"name"`
	Def     string       `json:"def,omitempty"`
	Columns []flatColumn `json:"columns,omitempty"`
	Attrs   []any        `json:"attrs,omitempty"`
}

type flatFunc struct {
	Name   string         `json:"name"`
	Args   []flatFuncArg  `json:"args,omitempty"`
	Ret    *flatColumnType `json:"ret,omitempty"`
	Lang   string         `json:"lang,omitempty"`
	Attrs  []any          `json:"attrs,omitempty"`
}

type flatFuncArg struct {
	Name string         `json:"name,omitempty"`
	Type *flatColumnType `json:"type,omitempty"`
	Mode string         `json:"mode,omitempty"`
}

type flatProc struct {
	Name  string `json:"name"`
	Attrs []any  `json:"attrs,omitempty"`
}

type flatTrigger struct {
	Name  string `json:"name"`
	Attrs []any  `json:"attrs,omitempty"`
}

// flattenResult converts a Result with cyclic schema.Realm into a flat, JSON-safe structure.
func flattenResult(r *Result) *flatResult {
	fr := &flatResult{
		Statements: r.Statements,
		Changes:    r.Changes,
		RenameCandidates: r.RenameCandidates,
		Error:      r.Error,
	}
	if r.Schema != nil {
		fr.Schema = flattenRealm(r.Schema)
	}
	return fr
}

func flattenRealm(r *schema.Realm) *flatRealm {
	fr := &flatRealm{
		Attrs: flattenAttrs(r.Attrs),
	}
	for _, s := range r.Schemas {
		fr.Schemas = append(fr.Schemas, flattenSchema(s))
	}
	return fr
}

func flattenSchema(s *schema.Schema) flatSchema {
	fs := flatSchema{
		Name:  s.Name,
		Attrs: flattenAttrs(s.Attrs),
	}
	for _, t := range s.Tables {
		fs.Tables = append(fs.Tables, flattenTable(t))
	}
	for _, v := range s.Views {
		fs.Views = append(fs.Views, flattenView(v))
	}
	for _, f := range s.Funcs {
		ff := flatFunc{Name: f.Name, Lang: f.Lang, Attrs: flattenAttrs(f.Attrs)}
		for _, a := range f.Args {
			fa := flatFuncArg{Name: a.Name, Mode: string(a.Mode)}
			if a.Type != nil {
				cat, isCust, enumVals, compFields := typeCategory(a.Type)
				fa.Type = &flatColumnType{
					Type:            typeString(a.Type),
					Category:        cat,
					IsCustom:        isCust,
					EnumValues:      enumVals,
					CompositeFields: compFields,
				}
			}
			ff.Args = append(ff.Args, fa)
		}
		if f.Ret != nil {
			cat, isCust, enumVals, compFields := typeCategory(f.Ret)
			ff.Ret = &flatColumnType{
				Type:            typeString(f.Ret),
				Category:        cat,
				IsCustom:        isCust,
				EnumValues:      enumVals,
				CompositeFields: compFields,
			}
		}
		fs.Funcs = append(fs.Funcs, ff)
	}
	for _, p := range s.Procs {
		fs.Procs = append(fs.Procs, flatProc{Name: p.Name, Attrs: flattenAttrs(p.Attrs)})
	}
	return fs
}

func flattenTable(t *schema.Table) flatTable {
	ft := flatTable{
		Name:  t.Name,
		Attrs: flattenAttrs(t.Attrs),
	}
	for _, c := range t.Columns {
		ft.Columns = append(ft.Columns, flattenColumn(c))
	}
	for _, idx := range t.Indexes {
		fi := flattenIndex(idx)
		ft.Indexes = append(ft.Indexes, fi)
	}
	if t.PrimaryKey != nil {
		pk := flattenIndex(t.PrimaryKey)
		ft.PrimaryKey = &pk
	}
	for _, fk := range t.ForeignKeys {
		ft.ForeignKeys = append(ft.ForeignKeys, flattenForeignKey(fk))
	}
	for _, tr := range t.Triggers {
		ft.Triggers = append(ft.Triggers, flatTrigger{Name: tr.Name, Attrs: flattenAttrs(tr.Attrs)})
	}
	return ft
}

func flattenColumn(c *schema.Column) flatColumn {
	fc := flatColumn{
		Name:  c.Name,
		Attrs: flattenAttrs(c.Attrs),
	}
	if c.Type != nil {
		fct := &flatColumnType{
			Null: c.Type.Null,
			Raw:  c.Type.Raw,
		}
		if c.Type.Type != nil {
			fct.Type = typeString(c.Type.Type)
			fct.Category, fct.IsCustom, fct.EnumValues, fct.CompositeFields = typeCategory(c.Type.Type)
		}
		fc.Type = fct
	}
	if c.Default != nil {
		fc.Default = exprValue(c.Default)
	}
	return fc
}

func flattenIndex(idx *schema.Index) flatIndex {
	fi := flatIndex{
		Name:   idx.Name,
		Unique: idx.Unique,
		Attrs:  flattenAttrs(idx.Attrs),
	}
	for _, p := range idx.Parts {
		fp := flatIndexPart{
			Desc:  p.Desc,
			Attrs: flattenAttrs(p.Attrs),
		}
		if p.C != nil {
			fp.Column = p.C.Name
		}
		fi.Parts = append(fi.Parts, fp)
	}
	return fi
}

func flattenForeignKey(fk *schema.ForeignKey) flatForeignKey {
	ffk := flatForeignKey{
		Symbol:   fk.Symbol,
		OnUpdate: string(fk.OnUpdate),
		OnDelete: string(fk.OnDelete),
	}
	for _, c := range fk.Columns {
		ffk.Columns = append(ffk.Columns, c.Name)
	}
	if fk.RefTable != nil {
		ffk.RefTable = fk.RefTable.Name
	}
	for _, c := range fk.RefColumns {
		ffk.RefColumns = append(ffk.RefColumns, c.Name)
	}
	return ffk
}

func flattenView(v *schema.View) flatView {
	fv := flatView{
		Name:  v.Name,
		Def:   v.Def,
		Attrs: flattenAttrs(v.Attrs),
	}
	for _, c := range v.Columns {
		fv.Columns = append(fv.Columns, flattenColumn(c))
	}
	return fv
}

// flattenAttrs converts []schema.Attr to []any for JSON serialization.
// Recognizes Tag, Comment, Check types by their interface methods.
func flattenAttrs(attrs []schema.Attr) []any {
	if len(attrs) == 0 {
		return nil
	}
	var result []any
	for _, a := range attrs {
		switch v := a.(type) {
		case *schema.Tag:
			result = append(result, map[string]string{
				"Name": v.Name,
				"Args": v.Args,
			})
		case *schema.Comment:
			result = append(result, map[string]string{
				"Text": v.Text,
			})
		case *schema.Check:
			m := map[string]string{
				"Expr": v.Expr,
			}
			if v.Name != "" {
				m["Name"] = v.Name
			}
			result = append(result, m)
		default:
			// Skip driver-specific attrs that may have cycles
			// (e.g., postgres.Identity, which references Sequence)
		}
	}
	return result
}

// typeString extracts a readable type name from a schema.Type interface.
// typeCategory returns the category, whether it's a custom/user-defined type,
// and enum values (if applicable) for a schema.Type.
func typeCategory(t schema.Type) (category string, isCustom bool, enumValues []string, compositeFields []flatCompositeField) {
	switch v := t.(type) {
	case *schema.StringType:
		return "string", false, nil, nil
	case *schema.IntegerType:
		return "integer", false, nil, nil
	case *schema.FloatType:
		return "float", false, nil, nil
	case *schema.DecimalType:
		return "decimal", false, nil, nil
	case *schema.BoolType:
		return "boolean", false, nil, nil
	case *schema.TimeType:
		return "time", false, nil, nil
	case *schema.BinaryType:
		return "binary", false, nil, nil
	case *schema.JSONType:
		return "json", false, nil, nil
	case *schema.UUIDType:
		return "uuid", false, nil, nil
	case *schema.SpatialType:
		return "spatial", false, nil, nil
	case *schema.EnumType:
		return "enum", true, v.Values, nil
	case *schema.UnsupportedType:
		return "unknown", true, nil, nil
	default:
		// Check for postgres CompositeType — extract fields
		typeName := reflect.TypeOf(t).String()
		if strings.Contains(typeName, "Composite") {
			// Use reflection to get Fields []*schema.Column
			val := reflect.ValueOf(t).Elem()
			fieldsVal := val.FieldByName("Fields")
			var fields []flatCompositeField
			if fieldsVal.IsValid() && fieldsVal.Kind() == reflect.Slice {
				for i := 0; i < fieldsVal.Len(); i++ {
					col := fieldsVal.Index(i).Interface().(*schema.Column)
					fields = append(fields, flatCompositeField{
						Name: col.Name,
						Type: typeString(col.Type.Type),
					})
				}
			}
			return "composite", true, nil, fields
		}
		// Handle driver-specific types by checking the T field via reflection.
		// This covers postgres SerialType, ArrayType, NetworkType, CurrencyType, etc.
		return typeCategoryFromName(typeString(t)), isCustomType(t), nil, nil
	}
}

// typeCategoryFromName infers a category from the type's T string.
// Used as fallback for driver-specific types not in the schema.* hierarchy.
func typeCategoryFromName(t string) string {
	switch strings.ToLower(t) {
	case "smallserial", "serial", "bigserial", "serial2", "serial4", "serial8":
		return "integer"
	case "money":
		return "decimal"
	case "inet", "cidr", "macaddr", "macaddr8":
		return "string"
	case "tsvector", "tsquery":
		return "string"
	case "xml":
		return "string"
	case "oid":
		return "integer"
	case "interval":
		return "time"
	case "bit", "varbit", "bit varying":
		return "string"
	case "citext":
		return "string"
	case "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange":
		return "string"
	}
	// Array types
	if strings.HasSuffix(t, "[]") || strings.HasPrefix(t, "_") {
		return "array"
	}
	return "unknown"
}

// isCustomType checks if a type is user-defined (not a driver built-in).
func isCustomType(t schema.Type) bool {
	typeName := reflect.TypeOf(t).String()
	// Driver-specific types like *postgres.SerialType, *postgres.ArrayType are NOT custom.
	// User-defined types would be *postgres.UserDefinedType, *postgres.DomainType, *postgres.CompositeType
	return strings.Contains(typeName, "UserDefined") ||
		strings.Contains(typeName, "Domain") ||
		strings.Contains(typeName, "Composite")
}

func typeString(t schema.Type) string {
	// Most driver types have a T string field or implement fmt.Stringer.
	// Use reflection to find a T field.
	if ts, ok := t.(interface{ String() string }); ok {
		return ts.String()
	}
	// Try to find a T field via reflection.
	// This covers postgres.IntegerType{T: "bigint"}, etc.
	v := reflect.ValueOf(t)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		f := v.FieldByName("T")
		if f.IsValid() && f.Kind() == reflect.String {
			return f.String()
		}
	}
	return ""
}

// exprValue extracts a serializable value from an Expr.
func exprValue(e schema.Expr) any {
	switch v := e.(type) {
	case *schema.RawExpr:
		return map[string]string{"X": v.X}
	case *schema.Literal:
		return map[string]string{"V": v.V}
	default:
		return nil
	}
}
