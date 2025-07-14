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
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Nullable bool   `yaml:"nullable"`
	Comment  string `yaml:"comment,omitempty"`
}

// GenerateCreateTableSQL generates Databricks SQL CREATE TABLE statement
func (ts *TableSchema) GenerateCreateTableSQL() string {
	var sql strings.Builder

	// Build the table name with optional catalog and schema
	tableName := BuildTableName(ts.Catalog, ts.Schema, ts.TableName)

	// Start building the CREATE TABLE statement
	sql.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	// Add each column
	for i, col := range ts.Columns {
		sql.WriteString("  ")
		sql.WriteString(col.Name)
		sql.WriteString(" ")
		sql.WriteString(col.Type)

		// Add NULL/NOT NULL constraint
		if !col.Nullable {
			sql.WriteString(" NOT NULL")
		}

		// Add comment if provided
		if col.Comment != "" {
			// Escape single quotes in comments by doubling them
			escapedComment := strings.ReplaceAll(col.Comment, "'", "''")
			sql.WriteString(fmt.Sprintf(" COMMENT '%s'", escapedComment))
		}

		// Add comma if not the last column
		if i < len(ts.Columns)-1 {
			sql.WriteString(",")
		}

		sql.WriteString("\n")
	}

	sql.WriteString(");")
	return sql.String()
}
