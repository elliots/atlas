// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package migrate

import (
	"testing"

	"ariga.io/atlas/sql/schema"

	"github.com/stretchr/testify/require"
)

func TestParseTags(t *testing.T) {
	tests := []struct {
		name     string
		comments []string
		want     []struct{ name, args string }
	}{
		{
			name:     "empty",
			comments: nil,
			want:     nil,
		},
		{
			name:     "no tags",
			comments: []string{"-- just a comment\n"},
			want:     nil,
		},
		{
			name:     "simple tag",
			comments: []string{"-- @audit.tracked\n"},
			want:     []struct{ name, args string }{{name: "audit.tracked"}},
		},
		{
			name:     "tag with args",
			comments: []string{"-- @audit.log(on: [update, delete])\n"},
			want: []struct{ name, args string }{
				{name: "audit.log", args: "on: [update, delete]"},
			},
		},
		{
			name:     "multiple tags one line",
			comments: []string{"-- @gql.filter @gql.order\n"},
			want: []struct{ name, args string }{
				{name: "gql.filter"},
				{name: "gql.order"},
			},
		},
		{
			name: "multiple comment lines",
			comments: []string{
				"-- @tenant.global\n",
				"-- @audit.tracked\n",
				"-- @gql.expose(\"Tenant\", behaviors: [select, single], simpleCollections: true)\n",
			},
			want: []struct{ name, args string }{
				{name: "tenant.global"},
				{name: "audit.tracked"},
				{name: "gql.expose", args: "\"Tenant\", behaviors: [select, single], simpleCollections: true"},
			},
		},
		{
			name:     "tag without namespace",
			comments: []string{"-- @deprecated\n"},
			want:     []struct{ name, args string }{{name: "deprecated"}},
		},
		{
			name:     "deeply nested namespace",
			comments: []string{"-- @a.b.c.d\n"},
			want:     []struct{ name, args string }{{name: "a.b.c.d"}},
		},
		{
			name:     "block comment",
			comments: []string{"/* @audit.tracked */"},
			want:     []struct{ name, args string }{{name: "audit.tracked"}},
		},
		{
			name:     "hash comment",
			comments: []string{"# @audit.tracked\n"},
			want:     []struct{ name, args string }{{name: "audit.tracked"}},
		},
		{
			name: "rbac with named args",
			comments: []string{
				"-- @rbac(admin: all, installer: select, customer: select, readonly: select)\n",
			},
			want: []struct{ name, args string }{
				{name: "rbac", args: "admin: all, installer: select, customer: select, readonly: select"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tags := ParseTags(tt.comments)
			if tt.want == nil {
				require.Nil(t, tags)
				return
			}
			require.Len(t, tags, len(tt.want))
			for i, w := range tt.want {
				require.Equal(t, w.name, tags[i].Name)
				require.Equal(t, w.args, tags[i].Args)
			}
		})
	}
}

func TestStmtTags(t *testing.T) {
	stmt := &Stmt{
		Text: "CREATE TABLE tenant ();",
		Comments: []string{
			"-- @tenant.global\n",
			"-- @audit.tracked\n",
		},
	}
	tags := stmt.Tags()
	require.Len(t, tags, 2)
	require.Equal(t, "tenant.global", tags[0].Name)
	require.Equal(t, "audit.tracked", tags[1].Name)
}

func TestParseStmtTags(t *testing.T) {
	stmt := `CREATE TABLE public.tenant (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    -- @gql.filter @gql.order
    name text NOT NULL UNIQUE,

    slug citext NOT NULL UNIQUE,

    -- @gql.omit
    stripe_customer_id text,

    -- @gql.omit
    subscription_tier text NOT NULL DEFAULT 'free'
        CHECK (subscription_tier IN ('free', 'starter', 'professional', 'enterprise')),

    is_active boolean NOT NULL DEFAULT true
);`

	colTags, tableTags := ParseStmtTags(stmt)

	require.Empty(t, tableTags)

	require.Len(t, colTags["name"], 2)
	require.Equal(t, "gql.filter", colTags["name"][0].Name)
	require.Equal(t, "gql.order", colTags["name"][1].Name)

	require.Len(t, colTags["stripe_customer_id"], 1)
	require.Equal(t, "gql.omit", colTags["stripe_customer_id"][0].Name)

	require.Len(t, colTags["subscription_tier"], 1)
	require.Equal(t, "gql.omit", colTags["subscription_tier"][0].Name)

	// Columns without tags should not appear.
	require.Nil(t, colTags["id"])
	require.Nil(t, colTags["slug"])
	require.Nil(t, colTags["is_active"])
}

func TestParseStmtTags_UserExample(t *testing.T) {
	// The user's example with all tag placement patterns.
	stmt := `-- @for("one")
create table one ( -- @for("one again")

    -- @for("two")
    two text, -- @for("two again")
    three text, -- @for("three")

    -- @for("four")


four numeric

    -- @for("one yet again")

)`

	// Note: "-- @for("one")" before the statement would normally be in
	// Stmt.Comments, not in stmt.Text. We're testing ParseStmtTags which
	// only sees the statement text. Let's include it in the text to test
	// the full scenario.
	colTags, tableTags := ParseStmtTags(stmt)

	// "@for("one")" is a preceding comment before CREATE TABLE → table
	// "@for("one again")" is inline on the CREATE TABLE line → table
	// "@for("one yet again")" is after last column, before ) → table
	tableTagNames := tagNames(tableTags)
	require.Contains(t, tableTagNames, "for")
	// Count table tags: "one", "one again", "one yet again" = 3
	require.Len(t, tableTags, 3)
	require.Equal(t, `"one"`, tableTags[0].Args)
	require.Equal(t, `"one again"`, tableTags[1].Args)
	require.Equal(t, `"one yet again"`, tableTags[2].Args)

	// "@for("two")" is preceding comment for column two
	// "@for("two again")" is inline on column two's line
	require.Len(t, colTags["two"], 2)
	require.Equal(t, `"two"`, colTags["two"][0].Args)
	require.Equal(t, `"two again"`, colTags["two"][1].Args)

	// "@for("three")" is inline on column three's line
	require.Len(t, colTags["three"], 1)
	require.Equal(t, `"three"`, colTags["three"][0].Args)

	// "@for("four")" is preceding comment for column four
	require.Len(t, colTags["four"], 1)
	require.Equal(t, `"four"`, colTags["four"][0].Args)
}

func TestParseStmtTags_QuotedColumns(t *testing.T) {
	stmt := `CREATE TABLE test (
    -- @pii
    "user name" text NOT NULL
);`
	colTags, tableTags := ParseStmtTags(stmt)
	require.Empty(t, tableTags)
	require.Len(t, colTags["user name"], 1)
	require.Equal(t, "pii", colTags["user name"][0].Name)
}

func TestParseStmtTags_NoTags(t *testing.T) {
	stmt := `CREATE TABLE test (
    id bigserial PRIMARY KEY,
    name text NOT NULL
);`
	colTags, tableTags := ParseStmtTags(stmt)
	require.Empty(t, tableTags)
	require.Empty(t, colTags)
}

func TestParseStmtTags_View(t *testing.T) {
	stmt := `CREATE VIEW active_users AS -- @gql.expose
    SELECT id, name FROM users WHERE is_active = true;`
	_, tableTags := ParseStmtTags(stmt)
	require.Len(t, tableTags, 1)
	require.Equal(t, "gql.expose", tableTags[0].Name)
}

func TestParseTableName(t *testing.T) {
	tests := []struct {
		stmt       string
		wantSchema string
		wantTable  string
	}{
		{"CREATE TABLE tenant (id int);", "", "tenant"},
		{"CREATE TABLE public.tenant (id int);", "public", "tenant"},
		{"create table IF NOT EXISTS public.users (id int);", "public", "users"},
		{`CREATE TABLE "my schema"."my table" (id int);`, "my schema", "my table"},
		{`CREATE TABLE "users" (id int);`, "", "users"},
		{"ALTER TABLE tenant ADD COLUMN x int;", "", ""},
		{"SELECT 1;", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			s, tn := parseTableName(tt.stmt)
			require.Equal(t, tt.wantSchema, s)
			require.Equal(t, tt.wantTable, tn)
		})
	}
}

func TestApplyTags(t *testing.T) {
	idx := &TagIndex{
		TableTags: map[string][]*schema.Tag{
			"public.tenant": {{Name: "audit.tracked"}, {Name: "tenant.global"}},
		},
		ColumnTags: map[string][]*schema.Tag{
			"public.tenant.name": {{Name: "gql.filter"}, {Name: "gql.order"}},
			"public.tenant.slug": {{Name: "gql.omit"}},
		},
	}
	realm := schema.NewRealm(
		schema.New("public").AddTables(
			schema.NewTable("tenant").
				AddColumns(
					schema.NewColumn("id"),
					schema.NewColumn("name"),
					schema.NewColumn("slug"),
				),
		),
	)
	idx.ApplyTags(realm)

	tenant := realm.Schemas[0].Tables[0]
	tags := schema.Tags(tenant.Attrs)
	require.Len(t, tags, 2)
	require.Equal(t, "audit.tracked", tags[0].Name)
	require.Equal(t, "tenant.global", tags[1].Name)

	nameTags := schema.Tags(tenant.Columns[1].Attrs)
	require.Len(t, nameTags, 2)
	require.Equal(t, "gql.filter", nameTags[0].Name)
	require.Equal(t, "gql.order", nameTags[1].Name)

	slugTags := schema.Tags(tenant.Columns[2].Attrs)
	require.Len(t, slugTags, 1)
	require.Equal(t, "gql.omit", slugTags[0].Name)

	// id column should have no tags.
	require.Empty(t, schema.Tags(tenant.Columns[0].Attrs))
}

func tagNames(tags []*schema.Tag) []string {
	names := make([]string, len(tags))
	for i, t := range tags {
		names[i] = t.Name
	}
	return names
}
