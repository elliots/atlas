// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package schema

// Tag represents a structured annotation on a schema element,
// parsed from SQL comments with the @-prefix syntax.
//
// Examples:
//
//	-- @audit.tracked
//	-- @gql.expose("Tenant", behaviors: [select, single])
//	-- @rbac(admin: all, installer: select)
type Tag struct {
	Name string // Dotted name, e.g. "gql.expose", "audit.tracked"
	Args string // Raw argument string (contents of parentheses), empty if no args
}

func (*Tag) attr() {}

// Tags returns all Tag attributes from the given attribute list.
func Tags(attrs []Attr) []*Tag {
	var tags []*Tag
	for _, a := range attrs {
		if t, ok := a.(*Tag); ok {
			tags = append(tags, t)
		}
	}
	return tags
}

// HasTag reports whether the attribute list contains a tag with the given name.
func HasTag(attrs []Attr, name string) bool {
	for _, a := range attrs {
		if t, ok := a.(*Tag); ok && t.Name == name {
			return true
		}
	}
	return false
}

// FindTag returns the first tag with the given name, or nil.
func FindTag(attrs []Attr, name string) *Tag {
	for _, a := range attrs {
		if t, ok := a.(*Tag); ok && t.Name == name {
			return t
		}
	}
	return nil
}

// AddTags appends the given tags to the table.
func (t *Table) AddTags(tags ...*Tag) *Table {
	for _, tag := range tags {
		t.Attrs = append(t.Attrs, tag)
	}
	return t
}

// AddTags appends the given tags to the column.
func (c *Column) AddTags(tags ...*Tag) *Column {
	for _, tag := range tags {
		c.Attrs = append(c.Attrs, tag)
	}
	return c
}

// AddTags appends the given tags to the view.
func (v *View) AddTags(tags ...*Tag) *View {
	for _, tag := range tags {
		v.Attrs = append(v.Attrs, tag)
	}
	return v
}
