package sqlgen

import (
	"fmt"
	"strings"
)

// TableSchema represents the overall table definition
type TableSchema struct {
	TableName string   `yaml:"table_name"`
	Catalog   string   `yaml:"catalog"`
	Schema    string   `yaml:"schema"`
	Columns   []Column `yaml:"columns"`
	RowCount  int      `yaml:"rows"` // Number of rows to generate for INSERT
}

// Column represents a single column in the table
type Column struct {
	Name       string `yaml:"name"`
	Type       string `yaml:"type"`
	Nullable   bool   `yaml:"nullable"`
	Comment    string `yaml:"comment,omitempty"`
	PrimaryKey bool   `yaml:"primary_key,omitempty"`
}

// GenerateCreateTableSQL generates Databricks SQL CREATE TABLE statement
func (ts *TableSchema) GenerateCreateTableSQL() string {
	var sql strings.Builder

	// Build the table name with optional catalog and schema
	tableName := BuildTableName(ts.Catalog, ts.Schema, ts.TableName)

	// Start building the CREATE TABLE statement
	sql.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	// Add each column
	var primaryKeyColumns []string
	for i, col := range ts.Columns {
		sql.WriteString("  ")
		sql.WriteString(col.Name)
		sql.WriteString(" ")
		sql.WriteString(col.Type)

		// Add NULL/NOT NULL constraint
		if !col.Nullable || col.PrimaryKey {
			sql.WriteString(" NOT NULL")
		}

		// Add comment if provided
		if col.Comment != "" {
			// Escape single quotes in comments by doubling them
			escapedComment := strings.ReplaceAll(col.Comment, "'", "''")
			sql.WriteString(fmt.Sprintf(" COMMENT '%s'", escapedComment))
		}

		// Track primary key columns
		if col.PrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, col.Name)
		}

		// Add comma if not the last column or if we have primary keys to add
		if i < len(ts.Columns)-1 || len(primaryKeyColumns) > 0 {
			sql.WriteString(",")
		}

		sql.WriteString("\n")
	}

	// Add PRIMARY KEY constraint if we have primary key columns
	if len(primaryKeyColumns) > 0 {
		sql.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)\n", strings.Join(primaryKeyColumns, ", ")))
	}

	sql.WriteString(");")
	return sql.String()
}
