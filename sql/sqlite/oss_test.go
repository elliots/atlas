// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build !ent

package sqlite

import (
	"context"
	"fmt"
	"testing"

	"ariga.io/atlas/sql/internal/sqltest"
	"ariga.io/atlas/sql/schema"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestInspectViews(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	mk := mock{m}
	mk.ExpectQuery(sqltest.Escape(fmt.Sprintf(databasesQueryArgs, "?"))).
		WithArgs("main").
		WillReturnRows(sqltest.Rows(`
 name | file
------+------
 main |
`))
	mk.ExpectQuery(sqltest.Escape(viewsQuery)).
		WillReturnRows(sqltest.Rows(`
 name         | sql
--------------+------------------------------------------------------------------
 active_users | CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1
`))
	drv, err := Open(db)
	require.NoError(t, err)
	s, err := drv.InspectSchema(context.Background(), "", &schema.InspectOptions{
		Mode: schema.InspectViews,
	})
	require.NoError(t, err)
	require.Len(t, s.Views, 1)
	require.Equal(t, "active_users", s.Views[0].Name)
	require.Equal(t, "SELECT * FROM users WHERE active = 1", s.Views[0].Def)
}

func TestInspectTriggers(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	mk := mock{m}
	mk.ExpectQuery(sqltest.Escape(fmt.Sprintf(databasesQueryArgs, "?"))).
		WithArgs("main").
		WillReturnRows(sqltest.Rows(`
 name | file
------+------
 main |
`))
	mk.ExpectQuery(sqltest.Escape(triggersQuery)).
		WillReturnRows(sqltest.Rows(`
 name                | tbl_name | sql
---------------------+----------+--------------------------------------------------------------------------------------------
 users_before_insert | users    | CREATE TRIGGER users_before_insert BEFORE INSERT ON users BEGIN SELECT 1; END
 orders_after_delete | orders   | CREATE TRIGGER orders_after_delete AFTER DELETE ON orders BEGIN SELECT 1; END
`))
	drv, err := Open(db)
	require.NoError(t, err)
	// InspectTriggers mode only — triggers are parsed but not linked (no tables loaded).
	s, err := drv.InspectSchema(context.Background(), "", &schema.InspectOptions{
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
			wantCmd: []string{"DROP VIEW `v`", "CREATE VIEW `v` AS SELECT id FROM t"},
			wantRev: []any{nil, "DROP VIEW `v`"},
		},
		{
			name: "rename view",
			changes: []schema.Change{
				&schema.RenameView{From: schema.NewView("v1", "SELECT 1"), To: schema.NewView("v2", "SELECT 1")},
			},
			wantCmd: []string{"DROP VIEW `v1`", "CREATE VIEW `v2` AS SELECT 1"},
			wantRev: []any{nil, "DROP VIEW `v2`"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := DefaultPlan.PlanChanges(context.Background(), "plan", tt.changes)
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
	body := "CREATE TRIGGER users_after_insert AFTER INSERT ON users FOR EACH ROW BEGIN SELECT 1; END"
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
			plan, err := DefaultPlan.PlanChanges(context.Background(), "plan", tt.changes)
			require.NoError(t, err)
			require.Len(t, plan.Changes, len(tt.wantCmd))
			for i, c := range plan.Changes {
				require.Equal(t, tt.wantCmd[i], c.Cmd, "change %d cmd", i)
				require.Equal(t, tt.wantRev[i], c.Reverse, "change %d reverse", i)
			}
		})
	}
}

func TestViewDef(t *testing.T) {
	tests := []struct{ stmt, want string }{
		{"CREATE VIEW v AS SELECT 1", "SELECT 1"},
		{"CREATE TEMP VIEW v AS SELECT * FROM t", "SELECT * FROM t"},
		{"CREATE VIEW IF NOT EXISTS v AS SELECT id FROM users", "SELECT id FROM users"},
		{"create view v as select 1, 2", "select 1, 2"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, viewDef(tt.stmt), "stmt: %s", tt.stmt)
	}
}

func TestParseTriggerMeta(t *testing.T) {
	tests := []struct {
		stmt   string
		timing schema.TriggerTime
		event  schema.TriggerEvent
	}{
		{
			stmt:   "CREATE TRIGGER t BEFORE INSERT ON users BEGIN SELECT 1; END",
			timing: schema.TriggerTimeBefore,
			event:  schema.TriggerEventInsert,
		},
		{
			stmt:   "CREATE TRIGGER t AFTER UPDATE ON users FOR EACH ROW BEGIN SELECT 1; END",
			timing: schema.TriggerTimeAfter,
			event:  schema.TriggerEventUpdate,
		},
		{
			stmt:   "CREATE TRIGGER t AFTER DELETE ON users\nBEGIN SELECT 1; END",
			timing: schema.TriggerTimeAfter,
			event:  schema.TriggerEventDelete,
		},
		{
			stmt:   "CREATE TRIGGER t INSTEAD OF INSERT ON my_view BEGIN SELECT 1; END",
			timing: schema.TriggerTimeInstead,
			event:  schema.TriggerEventInsert,
		},
	}
	for _, tt := range tests {
		trig := &schema.Trigger{}
		parseTriggerMeta(trig, tt.stmt)
		require.Equal(t, tt.timing, trig.ActionTime, "stmt: %s", tt.stmt)
		require.Equal(t, []schema.TriggerEvent{tt.event}, trig.Events, "stmt: %s", tt.stmt)
		require.Equal(t, schema.TriggerForRow, trig.For, "stmt: %s", tt.stmt)
	}
}
