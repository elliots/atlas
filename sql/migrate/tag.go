// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package migrate

import (
	"strings"

	"ariga.io/atlas/sql/schema"
)

// parseTagsFromComment extracts @-prefixed tags from a single comment string.
// Handles --, #, and /* */ comment styles. A comment may contain multiple tags.
//
// Tag syntax: @name or @name(args) or @ns.name(args)
// where name parts are dot-separated identifiers and args is everything
// inside balanced parentheses.
func parseTagsFromComment(c string) []*schema.Tag {
	c = strings.TrimSpace(c)
	// Strip block comment delimiters.
	if strings.HasPrefix(c, "/*") {
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
		tags = append(tags, scanTags(line)...)
	}
	return tags
}

// scanTags scans a string for @-prefixed tags and returns them.
func scanTags(s string) []*schema.Tag {
	var tags []*schema.Tag
	i := 0
	for i < len(s) {
		// Find next @.
		at := strings.IndexByte(s[i:], '@')
		if at < 0 {
			break
		}
		at += i
		// Read the tag name (word chars and dots).
		nameStart := at + 1
		nameEnd := nameStart
		for nameEnd < len(s) && (isIdent(s[nameEnd]) || s[nameEnd] == '.') {
			nameEnd++
		}
		name := s[nameStart:nameEnd]
		if name == "" {
			i = at + 1
			continue
		}
		// Check for parenthesized arguments.
		var args string
		end := nameEnd
		if end < len(s) && s[end] == '(' {
			// Scan for balanced parens.
			depth := 0
			argStart := end + 1
			j := end
			for j < len(s) {
				switch s[j] {
				case '(':
					depth++
				case ')':
					depth--
					if depth == 0 {
						args = strings.TrimSpace(s[argStart:j])
						j++
						end = j
						goto done
					}
				}
				j++
			}
			// Unbalanced parens — treat as no args.
			end = nameEnd
		}
	done:
		tags = append(tags, &schema.Tag{Name: name, Args: args})
		i = end
	}
	return tags
}

func isIdent(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

// ParseTags extracts structured tags from SQL comment strings.
// Each comment string may contain one or more @-prefixed tags.
func ParseTags(comments []string) []*schema.Tag {
	var tags []*schema.Tag
	for _, c := range comments {
		tags = append(tags, parseTagsFromComment(c)...)
	}
	return tags
}

// Tags returns all annotation tags from the statement's associated comments.
func (s *Stmt) Tags() []*schema.Tag {
	return ParseTags(s.Comments)
}

// ParseStmtTags parses a complete SQL statement (including any inline comments)
// and returns tags associated with the table and with each column.
//
// Tag association rules:
//   - A comment before CREATE TABLE attaches to the table.
//   - An inline comment on the CREATE TABLE line attaches to the table.
//   - A comment on the same line as a column definition attaches to that column.
//   - A comment on a line before a column definition attaches to the next column.
//   - A comment after the last column (before the closing paren) attaches to the table.
//
// The returned tableTags are for Stmt.Comments (before the statement).
// Use the second return (columnTags) for per-column tags, and the third (inlineTableTags)
// for tags found inside the statement that belong to the table.
func ParseStmtTags(stmtText string) (columnTags map[string][]*schema.Tag, tableTags []*schema.Tag) {
	columnTags = make(map[string][]*schema.Tag)
	p := newStmtParser(stmtText)
	p.parse()
	columnTags = p.columnTags
	tableTags = p.tableTags
	return
}

// stmtParser is a state machine that walks a CREATE TABLE statement character
// by character, tracking paren depth to distinguish the column-list body from
// the rest of the statement. It collects comments and associates them with the
// table or with individual columns.
type stmtParser struct {
	src  string
	pos  int
	depth int // paren depth

	// Are we inside the column-list body (depth == 1 after the first '(')?
	inBody    bool
	bodyStart int // position of the first '(' + 1

	// Current line tracking.
	lineStart  int
	lineHasCol bool   // does the current line have a column definition?
	lineCol    string // column name on the current line

	// Pending tags from comment-only lines (preceding-comment rule).
	pending []*schema.Tag

	// Results.
	tableTags  []*schema.Tag
	columnTags map[string][]*schema.Tag

	// Track the last column we saw, and the names of all columns.
	lastCol string
	columns []string
}

func newStmtParser(src string) *stmtParser {
	return &stmtParser{
		src:        src,
		columnTags: make(map[string][]*schema.Tag),
	}
}

func (p *stmtParser) parse() {
	for p.pos < len(p.src) {
		ch := p.src[p.pos]
		switch {
		case ch == '-' && p.pos+1 < len(p.src) && p.src[p.pos+1] == '-':
			p.handleLineComment()
		case ch == '/' && p.pos+1 < len(p.src) && p.src[p.pos+1] == '*':
			p.handleBlockComment()
		case ch == '\'':
			p.skipString('\'')
		case ch == '"':
			// If we're in the body at depth 1 and haven't seen a column on this line,
			// this might be a quoted column name.
			if p.inBody && p.depth == 1 && !p.lineHasCol && strings.TrimSpace(p.src[p.lineStart:p.pos]) == "" {
				// Read the quoted identifier.
				end := strings.IndexByte(p.src[p.pos+1:], '"')
				if end >= 0 {
					name := p.src[p.pos+1 : p.pos+1+end]
					p.pos += end + 2 // skip past closing quote
					p.lineHasCol = true
					p.lineCol = name
					if len(p.pending) > 0 {
						p.columnTags[name] = append(p.columnTags[name], p.pending...)
						p.pending = nil
					}
					continue
				}
			}
			p.skipString('"')
		case ch == '(':
			p.depth++
			if p.depth == 1 && !p.inBody {
				p.inBody = true
				p.bodyStart = p.pos + 1
				// Any pending tags from before the opening paren belong to the table.
				p.tableTags = append(p.tableTags, p.pending...)
				p.pending = nil
			}
			p.pos++
		case ch == ')':
			if p.depth == 1 && p.inBody {
				// Closing the column list. Any pending tags go to the table.
				p.tableTags = append(p.tableTags, p.pending...)
				p.pending = nil
				p.inBody = false
			}
			p.depth--
			p.pos++
		case ch == ',' && p.depth == 1 && p.inBody:
			// End of a column definition at top level of body.
			p.finishColumn()
			p.pos++
		case ch == '\n':
			p.endLine()
			p.pos++
			p.lineStart = p.pos
			p.lineHasCol = false
			p.lineCol = ""
		default:
			// If we're in the body at depth 1, try to detect a column name
			// at the start of meaningful content on a line.
			if p.inBody && p.depth == 1 && !p.lineHasCol && !isSpace(ch) {
				name := p.tryColumnName()
				if name != "" {
					p.lineHasCol = true
					p.lineCol = name
					// Pending preceding-line tags attach to this column.
					if len(p.pending) > 0 {
						p.columnTags[name] = append(p.columnTags[name], p.pending...)
						p.pending = nil
					}
				}
			}
			p.pos++
		}
	}
	// Handle any trailing pending tags.
	p.tableTags = append(p.tableTags, p.pending...)
	p.pending = nil
}

// handleLineComment processes a -- comment starting at p.pos.
func (p *stmtParser) handleLineComment() {
	start := p.pos
	// Advance past the comment to end of line.
	end := strings.IndexByte(p.src[p.pos:], '\n')
	if end < 0 {
		end = len(p.src)
	} else {
		end += p.pos
	}
	comment := p.src[start:end]
	p.pos = end // will hit '\n' on next iteration or EOF

	tags := scanTags(strings.TrimPrefix(strings.TrimSpace(comment), "--"))
	if len(tags) == 0 {
		return
	}

	// Determine context: is there non-whitespace before this comment on the same line?
	beforeComment := strings.TrimSpace(p.src[p.lineStart:start])
	if beforeComment != "" {
		// Inline comment — tags attach to whatever is on this line.
		if p.inBody && p.lineHasCol {
			// Same line as a column definition.
			p.columnTags[p.lineCol] = append(p.columnTags[p.lineCol], tags...)
		} else {
			// Same line as CREATE TABLE or other non-column content.
			p.tableTags = append(p.tableTags, tags...)
		}
	} else {
		// Comment-only line — pending for the next definition.
		p.pending = append(p.pending, tags...)
	}
}

// handleBlockComment processes a /* */ comment starting at p.pos.
func (p *stmtParser) handleBlockComment() {
	start := p.pos
	p.pos += 2
	end := strings.Index(p.src[p.pos:], "*/")
	if end < 0 {
		p.pos = len(p.src)
	} else {
		p.pos += end + 2
	}
	comment := p.src[start:p.pos]
	tags := parseTagsFromComment(comment)
	if len(tags) == 0 {
		return
	}
	beforeComment := strings.TrimSpace(p.src[p.lineStart:start])
	if beforeComment != "" {
		if p.inBody && p.lineHasCol {
			p.columnTags[p.lineCol] = append(p.columnTags[p.lineCol], tags...)
		} else {
			p.tableTags = append(p.tableTags, tags...)
		}
	} else {
		p.pending = append(p.pending, tags...)
	}
}

// endLine is called when we hit a newline. Nothing to do here currently;
// the actual line processing happens as we encounter comments and identifiers.
func (p *stmtParser) endLine() {}

// finishColumn is called when we hit a comma at depth 1 (end of column def).
func (p *stmtParser) finishColumn() {
	if p.lineCol != "" {
		p.lastCol = p.lineCol
		p.columns = append(p.columns, p.lineCol)
	}
}

// tryColumnName tries to read a column name starting at p.pos.
// Returns the name without advancing p.pos (the caller does that).
// Returns "" if this doesn't look like a column definition.
func (p *stmtParser) tryColumnName() string {
	// Skip constraint keywords.
	rest := p.src[p.pos:]
	upper := strings.ToUpper(trimLeadingSpace(rest))
	for _, kw := range []string{
		"PRIMARY ", "UNIQUE ", "CHECK ", "CHECK(",
		"CONSTRAINT ", "FOREIGN ", "INDEX ",
		"LIKE ", "EXCLUDE ",
	} {
		if strings.HasPrefix(upper, kw) {
			return ""
		}
	}
	// Read an identifier.
	if len(rest) > 0 && rest[0] == '"' {
		// Quoted identifier.
		end := strings.IndexByte(rest[1:], '"')
		if end >= 0 {
			return rest[1 : end+1]
		}
		return ""
	}
	// Unquoted identifier.
	var name strings.Builder
	for _, c := range rest {
		if isIdent(byte(c)) {
			name.WriteByte(byte(c))
		} else {
			break
		}
	}
	return name.String()
}

func (p *stmtParser) skipString(quote byte) {
	p.pos++ // skip opening quote
	for p.pos < len(p.src) {
		if p.src[p.pos] == quote {
			p.pos++
			// Handle escaped quote (double quote).
			if p.pos < len(p.src) && p.src[p.pos] == quote {
				p.pos++
				continue
			}
			return
		}
		p.pos++
	}
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

func trimLeadingSpace(s string) string {
	i := 0
	for i < len(s) && isSpace(s[i]) {
		i++
	}
	return s[i:]
}

// TagIndex holds extracted tags keyed by table and column names.
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
			sn, tn := parseTableName(stmt.Text)
			if tn == "" {
				continue
			}
			// Tags from comments before the statement.
			tableTags := stmt.Tags()
			// Tags from inside the statement (inline table tags + column tags).
			colTags, inlineTT := ParseStmtTags(stmt.Text)
			tableTags = append(tableTags, inlineTT...)
			if len(tableTags) > 0 {
				key := tableKey(sn, tn)
				idx.TableTags[key] = append(idx.TableTags[key], tableTags...)
			}
			for col, tags := range colTags {
				key := columnKey(sn, tn, col)
				idx.ColumnTags[key] = append(idx.ColumnTags[key], tags...)
			}
		}
	}
	return idx, nil
}

// parseTableName extracts (schema, table) from a CREATE TABLE statement.
// Uses a simple scanner rather than regex.
func parseTableName(s string) (schemaName, tableName string) {
	p := strings.TrimSpace(s)
	// Match CREATE TABLE (case-insensitive).
	if len(p) < 12 {
		return "", ""
	}
	if !strings.EqualFold(p[:6], "CREATE") {
		return "", ""
	}
	p = trimLeadingSpace(p[6:])
	if !strings.EqualFold(p[:5], "TABLE") {
		return "", ""
	}
	p = trimLeadingSpace(p[5:])
	// Optional IF NOT EXISTS.
	if len(p) > 15 && strings.EqualFold(p[:2], "IF") {
		rest := trimLeadingSpace(p[2:])
		if strings.EqualFold(rest[:3], "NOT") {
			rest = trimLeadingSpace(rest[3:])
			if strings.EqualFold(rest[:6], "EXISTS") {
				p = trimLeadingSpace(rest[6:])
			}
		}
	}
	// Read first identifier (could be schema or table).
	first, rest := readIdent(p)
	if first == "" {
		return "", ""
	}
	rest = trimLeadingSpace(rest)
	// If followed by '.', it's schema.table.
	if len(rest) > 0 && rest[0] == '.' {
		rest = trimLeadingSpace(rest[1:])
		second, _ := readIdent(rest)
		if second == "" {
			return "", first
		}
		return first, second
	}
	return "", first
}

// readIdent reads a quoted or unquoted SQL identifier from the start of s.
func readIdent(s string) (name, rest string) {
	if len(s) == 0 {
		return "", s
	}
	if s[0] == '"' {
		end := strings.IndexByte(s[1:], '"')
		if end < 0 {
			return "", s
		}
		return s[1 : end+1], s[end+2:]
	}
	i := 0
	for i < len(s) && isIdent(s[i]) {
		i++
	}
	return s[:i], s[i:]
}

// ApplyTags applies the extracted tags from a TagIndex to a schema.Realm.
func (idx *TagIndex) ApplyTags(realm *schema.Realm) {
	for _, s := range realm.Schemas {
		for _, t := range s.Tables {
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
