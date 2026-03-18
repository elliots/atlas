// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package schema

import (
	"testing"
)

func TestTags(t *testing.T) {
	attrs := []Attr{
		&Comment{Text: "hello"},
		&Tag{Name: "audit.tracked"},
		&Tag{Name: "gql.expose", Args: `"Tenant"`},
		&Check{Expr: "x > 0"},
	}
	tags := Tags(attrs)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tags))
	}
	if tags[0].Name != "audit.tracked" {
		t.Errorf("expected audit.tracked, got %s", tags[0].Name)
	}
	if tags[1].Name != "gql.expose" {
		t.Errorf("expected gql.expose, got %s", tags[1].Name)
	}
	if tags[1].Args != `"Tenant"` {
		t.Errorf("expected \"Tenant\", got %s", tags[1].Args)
	}
}

func TestHasTag(t *testing.T) {
	attrs := []Attr{
		&Tag{Name: "audit.tracked"},
		&Tag{Name: "gql.expose", Args: `"Tenant"`},
	}
	if !HasTag(attrs, "audit.tracked") {
		t.Error("expected HasTag to return true for audit.tracked")
	}
	if !HasTag(attrs, "gql.expose") {
		t.Error("expected HasTag to return true for gql.expose")
	}
	if HasTag(attrs, "nonexistent") {
		t.Error("expected HasTag to return false for nonexistent")
	}
}

func TestFindTag(t *testing.T) {
	attrs := []Attr{
		&Tag{Name: "gql.expose", Args: `"Tenant", behaviors: [select]`},
	}
	tag := FindTag(attrs, "gql.expose")
	if tag == nil {
		t.Fatal("expected to find tag")
	}
	if tag.Args != `"Tenant", behaviors: [select]` {
		t.Errorf("unexpected args: %s", tag.Args)
	}
	if FindTag(attrs, "nonexistent") != nil {
		t.Error("expected nil for nonexistent tag")
	}
}

func TestTableAddTags(t *testing.T) {
	tbl := NewTable("test").
		AddTags(
			&Tag{Name: "audit.tracked"},
			&Tag{Name: "tenant.global"},
		)
	tags := Tags(tbl.Attrs)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tags))
	}
}

func TestColumnAddTags(t *testing.T) {
	col := NewColumn("name").
		AddTags(&Tag{Name: "gql.filter"}, &Tag{Name: "gql.order"})
	tags := Tags(col.Attrs)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tags))
	}
}
