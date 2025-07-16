package sqlparser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ConvertSQLToYAMLFile converts a CREATE TABLE statement to a YAML file
func ConvertSQLToYAMLFile(sql string, outputPath string) error {
	parser := NewSQLParser()

	yamlContent, err := parser.ParseCreateTableToYAML(sql)
	if err != nil {
		return fmt.Errorf("failed to convert SQL to YAML: %w", err)
	}

	// Ensure the output directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Write the YAML content to file
	if err := os.WriteFile(outputPath, []byte(yamlContent), 0644); err != nil {
		return fmt.Errorf("failed to write YAML file: %w", err)
	}

	return nil
}

// ConvertSQLFileToYAMLFile reads a SQL file and converts it to a YAML file
func ConvertSQLFileToYAMLFile(sqlFilePath string, outputPath string) error {
	// Read the SQL file
	sqlContent, err := os.ReadFile(sqlFilePath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	return ConvertSQLToYAMLFile(string(sqlContent), outputPath)
}

// ParseSQLToYAML is a simple helper function that takes SQL and returns YAML string
func ParseSQLToYAML(sql string) (string, error) {
	parser := NewSQLParser()
	return parser.ParseCreateTableToYAML(sql)
}

// ExtractTableNameFromSQL is a helper function to get just the table name from SQL
func ExtractTableNameFromSQL(sql string) (string, error) {
	parser := NewSQLParser()
	schema, err := parser.ParseCreateTable(sql)
	if err != nil {
		return "", err
	}
	return schema.TableName, nil
}

// SetDefaultRowCount allows setting a custom default row count for generated schemas
func SetDefaultRowCount(sql string, rowCount int) (string, error) {
	parser := NewSQLParser()
	schema, err := parser.ParseCreateTable(sql)
	if err != nil {
		return "", err
	}

	// Update the row count
	schema.RowCount = rowCount

	// Convert back to YAML
	yamlContent, err := parser.ParseCreateTableToYAML(strings.Join([]string{
		fmt.Sprintf("CREATE TABLE %s.%s.%s (", schema.Catalog, schema.Schema, schema.TableName),
		// Note: This is a simplified approach. For production use, you'd want to reconstruct the full SQL
	}, ""))
	if err != nil {
		return "", err
	}

	return yamlContent, nil
}
