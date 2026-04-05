// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package mysql

import (
	"context"
	"fmt"
	"testing"

	"ariga.io/atlas/sql/internal/sqltest"
	"ariga.io/atlas/sql/internal/sqlx"
	"ariga.io/atlas/sql/schema"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestInspectViews(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	mk := mock{m}
	mk.version("8.0.13")
	mk.ExpectQuery(sqltest.Escape(fmt.Sprintf(schemasQueryArgs, "= ?"))).
		WithArgs("public").
		WillReturnRows(sqltest.Rows(`
+-------------+----------------------------+------------------------+
| SCHEMA_NAME | DEFAULT_CHARACTER_SET_NAME | DEFAULT_COLLATION_NAME |
+-------------+----------------------------+------------------------+
| public      | utf8mb4                    | utf8mb4_unicode_ci     |
+-------------+----------------------------+------------------------+
`))
	mk.ExpectQuery(sqltest.Escape(fmt.Sprintf(viewsQuery, "?"))).
		WithArgs("public").
		WillReturnRows(sqltest.Rows(`
+-------------+-------------+------------------------------------------+--------------+
| TABLE_SCHEMA | TABLE_NAME | VIEW_DEFINITION                          | CHECK_OPTION |
+-------------+-------------+------------------------------------------+--------------+
| public      | active_users | SELECT * FROM users WHERE active = 1    | NONE         |
| public      | admin_users  | SELECT * FROM users WHERE role = 'admin'| CASCADED     |
+-------------+-------------+------------------------------------------+--------------+
`))
	drv, err := Open(db)
	require.NoError(t, err)
	s, err := drv.InspectSchema(context.Background(), "public", &schema.InspectOptions{
		Mode: schema.InspectViews,
	})
	require.NoError(t, err)
	require.Len(t, s.Views, 2)
	require.Equal(t, "active_users", s.Views[0].Name)
	require.Equal(t, "SELECT * FROM users WHERE active = 1", s.Views[0].Def)
	require.Equal(t, "admin_users", s.Views[1].Name)
	require.Equal(t, "SELECT * FROM users WHERE role = 'admin'", s.Views[1].Def)
	// Check that CASCADED check option is set on admin_users view.
	var co schema.ViewCheckOption
	require.True(t, sqlx.Has(s.Views[1].Attrs, &co))
	require.Equal(t, schema.ViewCheckOptionCascaded, co.V)
}

func TestInspectTriggers(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	mk := mock{m}
	mk.version("8.0.13")
	mk.ExpectQuery(sqltest.Escape(fmt.Sprintf(schemasQueryArgs, "= ?"))).
		WithArgs("public").
		WillReturnRows(sqltest.Rows(`
+-------------+----------------------------+------------------------+
| SCHEMA_NAME | DEFAULT_CHARACTER_SET_NAME | DEFAULT_COLLATION_NAME |
+-------------+----------------------------+------------------------+
| public      | utf8mb4                    | utf8mb4_unicode_ci     |
+-------------+----------------------------+------------------------+
`))
	mk.ExpectQuery(sqltest.Escape(fmt.Sprintf(triggersQuery, "?"))).
		WithArgs("public").
		WillReturnRows(sqltest.Rows(`
+-----------------+---------------------------+--------------------+---------------+--------------------+------------------------------+
| TRIGGER_SCHEMA  | TRIGGER_NAME              | EVENT_OBJECT_TABLE | ACTION_TIMING | EVENT_MANIPULATION | ACTION_STATEMENT             |
+-----------------+---------------------------+--------------------+---------------+--------------------+------------------------------+
| public          | users_before_insert       | users              | BEFORE        | INSERT             | BEGIN SELECT 1; END          |
| public          | orders_after_delete       | orders             | AFTER         | DELETE             | BEGIN SELECT 1; END          |
+-----------------+---------------------------+--------------------+---------------+--------------------+------------------------------+
`))
	drv, err := Open(db)
	require.NoError(t, err)
	// InspectTriggers mode only — triggers are parsed but not linked (no tables loaded).
	s, err := drv.InspectSchema(context.Background(), "public", &schema.InspectOptions{
		Mode: schema.InspectTriggers,
	})
	require.NoError(t, err)
	require.Empty(t, s.Tables) // no tables loaded
	require.Empty(t, s.Views)
}

func TestPlanViewChanges(t *testing.T) {
	tests := []struct {
		name    string
		changes []schema.Change
		wantCmd []string
		wantRev []any
	}{
		{
			name: "add view",
			changes: []schema.Change{
				&schema.AddView{V: schema.NewView("active_users", "SELECT * FROM users WHERE active = 1")},
			},
			wantCmd: []string{"CREATE VIEW `active_users` AS SELECT * FROM users WHERE active = 1"},
			wantRev: []any{"DROP VIEW `active_users`"},
		},
		{
			name: "drop view",
			changes: []schema.Change{
				&schema.DropView{V: schema.NewView("active_users", "SELECT * FROM users WHERE active = 1")},
			},
			wantCmd: []string{"DROP VIEW `active_users`"},
			wantRev: []any{nil},
		},
		{
			name: "modify view",
			changes: []schema.Change{
				&schema.ModifyView{
					From: schema.NewView("v", "SELECT * FROM t"),
					To:   schema.NewView("v", "SELECT id FROM t"),
				},
			},
			// MySQL uses CREATE OR REPLACE VIEW for in-place modification.
			wantCmd: []string{"CREATE OR REPLACE VIEW `v` AS SELECT id FROM t"},
			wantRev: []any{nil},
		},
		{
			name: "rename view",
			changes: []schema.Change{
				&schema.RenameView{From: schema.NewView("v1", "SELECT 1"), To: schema.NewView("v2", "SELECT 1")},
			},
			// MySQL uses RENAME TABLE which works on views.
			wantCmd: []string{"RENAME TABLE `v1` TO `v2`"},
			wantRev: []any{"RENAME TABLE `v2` TO `v1`"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drv, _, err := newMigrate("8.0.13")
			require.NoError(t, err)
			plan, err := drv.PlanChanges(context.Background(), "plan", tt.changes)
			require.NoError(t, err)
			require.Len(t, plan.Changes, len(tt.wantCmd))
			for i, c := range plan.Changes {
				require.Equal(t, tt.wantCmd[i], c.Cmd, "change %d cmd", i)
				require.Equal(t, tt.wantRev[i], c.Reverse, "change %d reverse", i)
			}
		})
	}
}

func TestPlanTriggerChanges(t *testing.T) {
	body := "CREATE TRIGGER `users_after_insert` AFTER INSERT ON `users` FOR EACH ROW BEGIN SELECT 1; END"
	tests := []struct {
		name    string
		changes []schema.Change
		wantCmd []string
		wantRev []any
	}{
		{
			name: "add trigger",
			changes: []schema.Change{
				&schema.AddTrigger{T: &schema.Trigger{Name: "users_after_insert", Body: body}},
			},
			wantCmd: []string{body},
			wantRev: []any{"DROP TRIGGER IF EXISTS `users_after_insert`"},
		},
		{
			name: "drop trigger",
			changes: []schema.Change{
				&schema.DropTrigger{T: &schema.Trigger{Name: "users_after_insert"}},
			},
			wantCmd: []string{"DROP TRIGGER `users_after_insert`"},
			wantRev: []any{nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drv, _, err := newMigrate("8.0.13")
			require.NoError(t, err)
			plan, err := drv.PlanChanges(context.Background(), "plan", tt.changes)
			require.NoError(t, err)
			require.Len(t, plan.Changes, len(tt.wantCmd))
			for i, c := range plan.Changes {
				require.Equal(t, tt.wantCmd[i], c.Cmd, "change %d cmd", i)
				require.Equal(t, tt.wantRev[i], c.Reverse, "change %d reverse", i)
			}
		})
	}
}

func TestBuildTriggerBody(t *testing.T) {
	tests := []struct {
		name, table, timing, event, stmt, want string
	}{
		{
			name:   "users_before_insert",
			table:  "users",
			timing: "before",
			event:  "insert",
			stmt:   "BEGIN SELECT 1; END",
			want:   "CREATE TRIGGER `users_before_insert` BEFORE INSERT ON `users` FOR EACH ROW BEGIN SELECT 1; END",
		},
		{
			name:   "orders_after_delete",
			table:  "orders",
			timing: "AFTER",
			event:  "DELETE",
			stmt:   "BEGIN DELETE FROM audit; END",
			want:   "CREATE TRIGGER `orders_after_delete` AFTER DELETE ON `orders` FOR EACH ROW BEGIN DELETE FROM audit; END",
		},
	}
	for _, tt := range tests {
		got := buildTriggerBody(tt.name, tt.table, tt.timing, tt.event, tt.stmt)
		require.Equal(t, tt.want, got, "name: %s", tt.name)
	}
}
