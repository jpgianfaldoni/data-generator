# Table Schema to SQL Generator

A well-structured Go project that reads YAML table schemas and generates Databricks SQL CREATE TABLE and INSERT statements with random test data.

## Project Structure

```
go-project/
├── main.go                     # CLI interface
├── internal/                   # Internal packages (not importable by external projects)
│   └── sqlgen/                # SQL generation package
│       ├── schema.go          # TableSchema struct and CREATE TABLE logic
│       ├── insert.go          # INSERT SQL generation with random data
│       └── utils.go           # Helper functions (BuildTableName, etc.)
├── examples/                   # Example YAML files
│   ├── example_table.yaml     # Basic table example
│   └── products_table.yaml    # Another table example
├── output/                     # Generated SQL files (created automatically)
│   ├── *_create.sql           # CREATE TABLE statements
│   └── *_insert.sql           # INSERT statements with random data
├── go.mod                     # Go module definition
└── README.md                  # This file
```

## How to Run

```bash
# Build the application
go build .

# Generate SQL from YAML schema
./data-generator examples/example_table.yaml

# Generated SQL files will be saved to output/ directory:
# - output/example_table_create.sql
# - output/example_table_insert.sql
```

## Features

✅ **Always generates both CREATE TABLE and INSERT SQL**  
✅ **Smart random data generation** based on column types  
✅ **Respects nullable constraints** (10% chance for NULL values)  
✅ **Supports Databricks catalog.schema.table format**  

## YAML Input Format

```yaml
table_name: "your_table_name"
catalog: "main"          # Optional
schema: "gold"           # Optional  
rows: 5                  # Number of INSERT rows to generate
columns:
  - name: "column_name"
    type: "DATA_TYPE"
    nullable: true/false
    comment: "optional comment"
```

## Supported Databricks Data Types

- `BIGINT`, `INT`, `SMALLINT`, `TINYINT`
- `STRING`, `VARCHAR(n)`
- `BOOLEAN`
- `TIMESTAMP`, `DATE`
- `DECIMAL(precision, scale)`
- `DOUBLE`, `FLOAT`

## Package Architecture

### `main` Package
- **Purpose**: CLI interface only
- **Responsibilities**: Command-line parsing, file I/O, user interaction, output management

### `internal/sqlgen` Package  
- **Purpose**: Core SQL generation logic
- **Benefits**: 
  - Reusable across different interfaces
  - Easily testable
  - Follows single responsibility principle
  - Cannot be imported by external projects

### Key Go Concepts Demonstrated

1. **Package Organization**: Proper separation of concerns
2. **Internal Packages**: Using `internal/` to prevent external imports
3. **Struct Methods**: Object-oriented patterns in Go
4. **Error Handling**: Explicit error checking
5. **YAML Parsing**: Using struct tags for data binding
6. **String Building**: Efficient string construction
7. **Random Data Generation**: Type-aware test data creation
8. **File Organization**: Dedicated output directory structure

## Generated Output

Running the tool generates two SQL files in the `output/` directory:
- `<filename>_create.sql` - CREATE TABLE statement
- `<filename>_insert.sql` - INSERT statements with random data

Both files use the same fully-qualified table name format and are ready to execute in Databricks.

The output directory is automatically created if it doesn't exist, keeping your workspace organized and separating generated files from source code. 