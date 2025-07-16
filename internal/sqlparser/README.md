# SQL Parser

This package provides functionality to parse SQL CREATE TABLE statements and convert them to YAML format that matches the structure used by the data generator.

## Features

- Parse CREATE TABLE statements with various formats:
  - Simple table names: `CREATE TABLE users (...)`
  - Schema-qualified names: `CREATE TABLE schema.users (...)`
  - Catalog-qualified names: `CREATE TABLE catalog.schema.users (...)`
- Extract column definitions including:
  - Column names and data types
  - NULL/NOT NULL constraints
  - Comments
- Convert to YAML format matching the TableSchema structure
- Save parsed results to YAML files

## Usage

### Basic Parsing

```go
import "data-generator/internal/sqlparser"

// Parse SQL to YAML string
sql := `CREATE TABLE users (
    id BIGINT NOT NULL COMMENT 'Primary key',
    name VARCHAR(100) NOT NULL COMMENT 'User name',
    email VARCHAR(255) COMMENT 'Email address',
    created_at TIMESTAMP NOT NULL
);`

yaml, err := sqlparser.ParseSQLToYAML(sql)
if err != nil {
    log.Fatal(err)
}
fmt.Println(yaml)
```

### Save to File

```go
// Convert SQL to YAML file
err := sqlparser.ConvertSQLToYAMLFile(sql, "output/users_table.yaml")
if err != nil {
    log.Fatal(err)
}

// Convert SQL file to YAML file
err = sqlparser.ConvertSQLFileToYAMLFile("input.sql", "output/table.yaml")
if err != nil {
    log.Fatal(err)
}
```

### Advanced Usage

```go
// Extract just the table name
tableName, err := sqlparser.ExtractTableNameFromSQL(sql)
if err != nil {
    log.Fatal(err)
}

// Run examples
sqlparser.ExampleUsage()
```

## Supported SQL Features

### Table Names
- `CREATE TABLE table_name`
- `CREATE TABLE schema.table_name`
- `CREATE TABLE catalog.schema.table_name`
- `CREATE TABLE IF NOT EXISTS ...`

### Data Types
The parser normalizes various SQL data types to standard formats:
- `VARCHAR(n)`, `CHAR(n)`, `TEXT` → `STRING`
- `INTEGER`, `INT` → `INT`
- `BIGINT` → `BIGINT`
- `DECIMAL(p,s)`, `NUMERIC(p,s)` → `DECIMAL(p,s)`
- `BOOLEAN`, `BOOL` → `BOOLEAN`
- `TIMESTAMP` → `TIMESTAMP`
- `DATE` → `DATE`
- `TIME` → `TIME`
- `DOUBLE` → `DOUBLE`
- `FLOAT` → `FLOAT`

### Column Constraints
- `NOT NULL` / `NULL`
- `COMMENT 'comment text'`

## Output Format

The parser generates YAML in the following format:

```yaml
table_name: "users"
catalog: "main"
schema: "public"
rows: 1000
columns:
  - name: "id"
    type: "BIGINT"
    nullable: false
    comment: "Primary key"
  - name: "name"
    type: "STRING"
    nullable: false
    comment: "User name"
  - name: "email"
    type: "STRING"
    nullable: true
    comment: "Email address"
```

This format matches the TableSchema structure used by the data generator and can be used directly with the existing YAML-based table generation functionality.

## Examples

See `example_usage.go` for comprehensive examples of different SQL table formats and how to parse them. 