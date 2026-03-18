// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package migrate

import (
	"regexp"
	"strings"

	"ariga.io/atlas/sql/schema"
)

// reTag matches @-prefixed annotation tags in SQL comments.
// Supports dotted names like @ns.key and optional parenthesized arguments.
var reTag = regexp.MustCompile(`@([\w]+(?:\.[\w]+)*)(?:\(([^)]*)\))?`)

// ParseTags extracts structured tags from SQL comment strings.
// Each comment string may contain one or more @-prefixed tags.
//
// Examples:
//
//	"-- @audit.tracked\n"         → [Tag{Name: "audit.tracked"}]
//	"-- @gql.filter @gql.order\n" → [Tag{Name: "gql.filter"}, Tag{Name: "gql.order"}]
//	"-- @rbac(admin: all)\n"      → [Tag{Name: "rbac", Args: "admin: all"}]
func ParseTags(comments []string) []*schema.Tag {
	var tags []*schema.Tag
	for _, c := range comments {
		tags = append(tags, parseLine(c)...)
	}
	return tags
}

// parseLine extracts tags from a single comment line or block.
func parseLine(c string) []*schema.Tag {
	// Handle multi-line block comments by splitting into lines.
	if strings.HasPrefix(strings.TrimSpace(c), "/*") {
		c = strings.TrimSpace(c)
		c = strings.TrimPrefix(c, "/*")
		c = strings.TrimSuffix(c, "*/")
	}
	var tags []*schema.Tag
	for _, line := range strings.Split(c, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "--")
		line = strings.TrimPrefix(line, "#")
		line = strings.TrimPrefix(line, "*") // block comment continuation
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "@") {
			continue
		}
		matches := reTag.FindAllStringSubmatch(line, -1)
		for _, m := range matches {
			tag := &schema.Tag{Name: m[1]}
			if len(m) > 2 {
				tag.Args = strings.TrimSpace(m[2])
			}
			tags = append(tags, tag)
		}
	}
	return tags
}

// Tags returns all annotation tags from the statement's associated comments.
// These are table/object-level tags that appear before the SQL statement.
func (s *Stmt) Tags() []*schema.Tag {
	return ParseTags(s.Comments)
}

// ParseInlineTags extracts column-level tags from within a CREATE TABLE
// statement body. It returns a map from column name to its associated tags.
//
// Inline tags are SQL comments containing @-annotations that appear on the
// line(s) immediately before a column definition:
//
//	CREATE TABLE tenant (
//	    id uuid PRIMARY KEY,
//	    -- @gql.filter @gql.order
//	    name text NOT NULL,
//	    -- @gql.omit
//	    stripe_customer_id text
//	);
func ParseInlineTags(stmtText string) map[string][]*schema.Tag {
	result := make(map[string][]*schema.Tag)
	lines := strings.Split(stmtText, "\n")
	var pendingTags []*schema.Tag
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Accumulate tags from comment-only lines.
		if isCommentLine(trimmed) && strings.Contains(trimmed, "@") {
			pendingTags = append(pendingTags, parseLine(trimmed)...)
			continue
		}
		// If we have pending tags and hit a column definition, associate them.
		if len(pendingTags) > 0 && trimmed != "" {
			if name := extractColumnName(trimmed); name != "" {
				result[name] = append(result[name], pendingTags...)
			}
			pendingTags = nil
			continue
		}
		// Reset pending tags on non-comment, non-column lines.
		pendingTags = nil
	}
	return result
}

// reCreateTable matches CREATE TABLE statements and extracts the schema-qualified table name.
var reCreateTable = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:"([^"]+)"|(\w+))\.)?(?:"([^"]+)"|(\w+))`)

// extractTableName returns the (schema, table) name from a CREATE TABLE statement.
// Returns empty strings if the statement is not a CREATE TABLE.
func extractTableName(stmtText string) (schemaName, tableName string) {
	m := reCreateTable.FindStringSubmatch(stmtText)
	if m == nil {
		return "", ""
	}
	// Schema name: quoted (m[1]) or unquoted (m[2])
	if m[1] != "" {
		schemaName = m[1]
	} else {
		schemaName = m[2]
	}
	// Table name: quoted (m[3]) or unquoted (m[4])
	if m[3] != "" {
		tableName = m[3]
	} else {
		tableName = m[4]
	}
	return schemaName, tableName
}

// TagIndex holds extracted tags keyed by table and column names.
// Table-level tags use the key "schema.table" or just "table".
// Column-level tags use the key "schema.table.column" or "table.column".
type TagIndex struct {
	// TableTags maps table names to their tags.
	// Keys are "table" or "schema.table".
	TableTags map[string][]*schema.Tag
	// ColumnTags maps "table.column" or "schema.table.column" to column tags.
	ColumnTags map[string][]*schema.Tag
}

// ExtractTags scans SQL files from a directory and extracts all @-annotation tags.
// It returns a TagIndex that can be used to apply tags to schema objects after inspection.
func ExtractTags(drv Driver, dir Dir) (*TagIndex, error) {
	idx := &TagIndex{
		TableTags:  make(map[string][]*schema.Tag),
		ColumnTags: make(map[string][]*schema.Tag),
	}
	files, err := dir.Files()
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		stmts, err := FileStmtDecls(drv, f)
		if err != nil {
			return nil, err
		}
		for _, stmt := range stmts {
			sn, tn := extractTableName(stmt.Text)
			if tn == "" {
				continue
			}
			// Table-level tags from comments before the statement.
			tableTags := stmt.Tags()
			if len(tableTags) > 0 {
				key := tableKey(sn, tn)
				idx.TableTags[key] = append(idx.TableTags[key], tableTags...)
			}
			// Column-level tags from inline comments.
			colTags := ParseInlineTags(stmt.Text)
			for col, tags := range colTags {
				key := columnKey(sn, tn, col)
				idx.ColumnTags[key] = append(idx.ColumnTags[key], tags...)
			}
		}
	}
	return idx, nil
}

// ApplyTags applies the extracted tags from a TagIndex to a schema.Realm.
func (idx *TagIndex) ApplyTags(realm *schema.Realm) {
	for _, s := range realm.Schemas {
		for _, t := range s.Tables {
			// Try both "schema.table" and "table" keys.
			for _, key := range []string{tableKey(s.Name, t.Name), tableKey("", t.Name)} {
				if tags, ok := idx.TableTags[key]; ok {
					t.AddTags(tags...)
					break
				}
			}
			for _, c := range t.Columns {
				for _, key := range []string{columnKey(s.Name, t.Name, c.Name), columnKey("", t.Name, c.Name)} {
					if tags, ok := idx.ColumnTags[key]; ok {
						c.AddTags(tags...)
						break
					}
				}
			}
		}
	}
}

func tableKey(schemaName, tableName string) string {
	if schemaName != "" {
		return schemaName + "." + tableName
	}
	return tableName
}

func columnKey(schemaName, tableName, columnName string) string {
	if schemaName != "" {
		return schemaName + "." + tableName + "." + columnName
	}
	return tableName + "." + columnName
}

// isCommentLine reports whether the line is a SQL comment.
func isCommentLine(line string) bool {
	return strings.HasPrefix(line, "--") || strings.HasPrefix(line, "#")
}

// extractColumnName extracts the column name from a line that looks like
// a column definition. Returns empty string if the line doesn't appear
// to be a column definition.
func extractColumnName(line string) string {
	// Skip lines that are clearly not column definitions.
	upper := strings.ToUpper(strings.TrimSpace(line))
	for _, prefix := range []string{
		"CREATE", "PRIMARY", "UNIQUE", "CHECK", "CONSTRAINT",
		"FOREIGN", "INDEX", ")", "(", "LIKE", "EXCLUDE",
	} {
		if strings.HasPrefix(upper, prefix) {
			return ""
		}
	}
	// The column name is the first identifier on the line.
	// Handle quoted identifiers.
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "\"") {
		if i := strings.Index(line[1:], "\""); i >= 0 {
			return line[1 : i+1]
		}
		return ""
	}
	// Unquoted identifier: take chars until whitespace or punctuation.
	var name strings.Builder
	for _, r := range line {
		if r == ' ' || r == '\t' || r == ',' || r == '(' {
			break
		}
		name.WriteRune(r)
	}
	n := name.String()
	// Sanity check: column names should not be empty or SQL keywords.
	if n == "" {
		return ""
	}
	return n
}
