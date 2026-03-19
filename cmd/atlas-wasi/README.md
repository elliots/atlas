# Atlas WASI Module — Host Integration API

## Overview

A WebAssembly (WASI) binary that provides database schema inspection, diffing, and migration planning. Supports Postgres, MySQL, and SQLite. You provide the database — the module calls out to you to run SQL.

## Two interfaces to implement

### 1. Host function: `atlas_sql`

Provide this when instantiating the WASM module. The module calls it whenever it needs to run SQL on your database.

```typescript
// Imported as "env" "atlas_sql"
function atlas_sql(
  reqPtr: number,   // pointer to JSON request in WASM memory
  reqLen: number,   // length of request
  respPtr: number,  // pointer to response buffer in WASM memory
  respCap: number   // capacity of response buffer
): bigint           // actual response length (negative = buffer too small, retry with abs value)
```

**Request** (JSON, read from WASM memory):

```json
{
  "type": "query",
  "sql": "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
  "args": ["users"]
}
```

`type` is `"query"` for SELECT-style queries or `"exec"` for DDL/DML.

**Response** (JSON, write into WASM memory):

```json
// For "query":
{ "columns": ["column_name"], "rows": [["id"], ["email"], ["username"]] }

// For "exec":
{ "rows_affected": 1, "last_insert_id": 0 }

// On error:
{ "error": "relation \"users\" does not exist" }
```

The module calls this many times per command — it runs DDL, then queries system catalogs (`pg_catalog`, `information_schema`, etc.) to inspect the resulting schema.

### 2. Command interface: stdin/stdout

Send a JSON command to the module's stdin. Read the JSON result from stdout. Each invocation handles one command, then the module exits.

---

## Commands

### `inspect` — Execute SQL, return parsed schema with tags

**Input** (stdin):

```json
{
  "type": "inspect",
  "dialect": "postgres",
  "schema": "public",
  "files": [
    "-- @audit.track(on: [delete, update])\nCREATE TABLE users (\n    id BIGSERIAL PRIMARY KEY,\n    -- @pii.mask\n    email TEXT NOT NULL UNIQUE,\n    -- @gql.filter @gql.order(asc)\n    username TEXT NOT NULL\n);",
    "CREATE VIEW active_users AS SELECT * FROM users;"
  ]
}
```

| Field     | Type       | Required | Description |
|-----------|------------|----------|-------------|
| `type`    | `string`   | yes      | `"inspect"` |
| `dialect` | `string`   | yes      | `"postgres"`, `"mysql"`, or `"sqlite"` |
| `schema`  | `string`   | no       | Filter to a specific schema (e.g. `"public"`) |
| `files`   | `string[]` | yes      | Raw SQL file contents to execute |

**Output** (stdout):

```json
{
  "schema": {
    "schemas": [{
      "name": "public",
      "tables": [{
        "name": "users",
        "attrs": [
          { "Name": "audit.track", "Args": "on: [delete, update]" }
        ],
        "columns": [
          { "name": "id", "type": { "T": "bigint" } },
          {
            "name": "email",
            "type": { "T": "text" },
            "attrs": [{ "Name": "pii.mask", "Args": "" }]
          },
          {
            "name": "username",
            "type": { "T": "text" },
            "attrs": [
              { "Name": "gql.filter", "Args": "" },
              { "Name": "gql.order", "Args": "asc" }
            ]
          }
        ],
        "indexes": [],
        "primary_key": {}
      }],
      "views": [{ "name": "active_users" }]
    }]
  }
}
```

### `diff` — Compare two schemas, return migration SQL

**Input**:

```json
{
  "type": "diff",
  "dialect": "postgres",
  "from": [
    "CREATE TABLE users (id BIGSERIAL PRIMARY KEY);"
  ],
  "to": [
    "CREATE TABLE users (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL);",
    "CREATE INDEX idx_email ON users (email);"
  ]
}
```

| Field     | Type       | Required | Description |
|-----------|------------|----------|-------------|
| `type`    | `string`   | yes      | `"diff"` |
| `dialect` | `string`   | yes      | Database dialect |
| `from`    | `string[]` | yes      | SQL files representing the current state |
| `to`      | `string[]` | yes      | SQL files representing the desired state |
| `schema`  | `string`   | no       | Filter to a specific schema |

**Output**:

```json
{
  "statements": [
    "ALTER TABLE \"users\" ADD COLUMN \"email\" text NOT NULL",
    "CREATE INDEX \"idx_email\" ON \"users\" (\"email\")"
  ]
}
```

**Important**: The module executes `from` SQL, inspects, then executes `to` SQL and inspects again. The host is responsible for providing a clean database state for each phase — use separate connections, snapshots, or reset the database between phases.

### `apply` — Execute SQL statements

**Input**:

```json
{
  "type": "apply",
  "dialect": "postgres",
  "files": ["CREATE TABLE t (id int);"]
}
```

**Output**:

```json
{}
```

---

## Tag syntax

Tags are `@`-prefixed annotations in SQL comments. The module extracts them from the SQL text before execution (since comments are lost when SQL is run on the database), then reattaches them to the inspected schema objects by matching table and column names.

```sql
-- Before a statement -> attaches to the table/view
-- @audit.tracked
CREATE TABLE users (
    id int,

    -- Before a column -> attaches to the next column
    -- @pii.mask
    email text,

    name text, -- Inline -> attaches to this column (name)

    -- After last column, before ) -> attaches to the table
    -- @gql.expose
);
```

Tag structure: `{ Name: string, Args: string }`

- `@foo` -> `{ Name: "foo", Args: "" }`
- `@foo.bar` -> `{ Name: "foo.bar", Args: "" }`
- `@foo(x, y)` -> `{ Name: "foo", Args: "x, y" }`
- `@gql.expose("Tenant", behaviors: [select])` -> `{ Name: "gql.expose", Args: "\"Tenant\", behaviors: [select]" }`

Multiple tags on one line: `-- @gql.filter @gql.order(asc)` produces two separate tags.

---

## Building

```bash
cd atlas/cmd/atlas-wasi
GOOS=wasip1 GOARCH=wasm go build -o atlas.wasm .
```

Produces a ~14MB WASM binary containing Postgres, MySQL, and SQLite dialect support.

## Error handling

All commands return an `error` field on failure:

```json
{ "error": "exec \"CREATE TABLE ...\": relation already exists" }
```

The module writes valid JSON to stdout even on fatal errors, then exits with code 1.

## Notes

- Each command is one stdin/stdout cycle — the module exits after responding
- All SQL execution happens through the `atlas_sql` host function
- The module never opens network connections or touches the filesystem
- Schema JSON follows Atlas's `schema.Realm` Go struct serialization
- The module handles dialect-specific catalog queries internally (e.g. `pg_catalog` for Postgres, `information_schema` for MySQL, `sqlite_master` for SQLite)
