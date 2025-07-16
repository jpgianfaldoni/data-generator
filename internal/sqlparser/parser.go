package sqlparser

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"data-generator/internal/sqlgen"
)

// SQLParser handles parsing of CREATE TABLE statements
type SQLParser struct{}

// NewSQLParser creates a new SQL parser instance
func NewSQLParser() *SQLParser {
	return &SQLParser{}
}

// ParseCreateTable parses a CREATE TABLE statement and returns a TableSchema
func (p *SQLParser) ParseCreateTable(sql string) (*sqlgen.TableSchema, error) {
	// Clean and normalize the SQL
	sql = strings.TrimSpace(sql)
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")

	// Extract table name (with optional catalog and schema)
	tableName, catalog, schema, err := p.extractTableName(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to extract table name: %w", err)
	}

	// Extract columns
	columns, err := p.extractColumns(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to extract columns: %w", err)
	}

	return &sqlgen.TableSchema{
		TableName: tableName,
		Catalog:   catalog,
		Schema:    schema,
		Columns:   columns,
		RowCount:  1000, // Default row count, can be overridden
	}, nil
}

// ParseCreateTableToYAML parses a CREATE TABLE statement and returns YAML
func (p *SQLParser) ParseCreateTableToYAML(sql string) (string, error) {
	schema, err := p.ParseCreateTable(sql)
	if err != nil {
		return "", err
	}

	yamlData, err := yaml.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("failed to marshal to YAML: %w", err)
	}

	return string(yamlData), nil
}

// extractTableName extracts table name, catalog, and schema from CREATE TABLE statement
func (p *SQLParser) extractTableName(sql string) (string, string, string, error) {
	// Pattern to match CREATE TABLE with optional catalog and schema
	patterns := []string{
		`CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` +
			`(?:` +
			`([a-zA-Z_][a-zA-Z0-9_]*)\.\s*([a-zA-Z_][a-zA-Z0-9_]*)\.\s*([a-zA-Z_][a-zA-Z0-9_]*)|` + // catalog.schema.table
			`([a-zA-Z_][a-zA-Z0-9_]*)\.\s*([a-zA-Z_][a-zA-Z0-9_]*)|` + // schema.table
			`([a-zA-Z_][a-zA-Z0-9_]*)` + // table
			`)`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(`(?i)` + pattern)
		matches := re.FindStringSubmatch(sql)

		if len(matches) > 0 {
			// catalog.schema.table format
			if matches[1] != "" && matches[2] != "" && matches[3] != "" {
				return strings.Trim(matches[3], `"`),
					strings.Trim(matches[1], `"`),
					strings.Trim(matches[2], `"`), nil
			}
			// schema.table format
			if matches[4] != "" && matches[5] != "" {
				return strings.Trim(matches[5], `"`),
					"",
					strings.Trim(matches[4], `"`), nil
			}
			// table only format
			if matches[6] != "" {
				return strings.Trim(matches[6], `"`), "", "", nil
			}
		}
	}

	return "", "", "", fmt.Errorf("could not extract table name from SQL")
}

// extractColumns extracts column definitions from CREATE TABLE statement
func (p *SQLParser) extractColumns(sql string) ([]sqlgen.Column, error) {
	// Clean the SQL to remove table properties that come after column definitions
	cleanedSQL := p.removeTableProperties(sql)

	// Find the content between parentheses
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+[^(]+\(\s*(.*)\s*\)`)
	matches := re.FindStringSubmatch(cleanedSQL)
	if len(matches) < 2 {
		return nil, fmt.Errorf("could not find column definitions")
	}

	columnDefs := matches[1]

	// Split by commas, but be careful about commas inside parentheses (for types like DECIMAL(10,2))
	columns := p.splitColumnDefinitions(columnDefs)

	var result []sqlgen.Column
	for _, colDef := range columns {
		column, err := p.parseColumnDefinition(strings.TrimSpace(colDef))
		if err != nil {
			return nil, fmt.Errorf("failed to parse column definition '%s': %w", colDef, err)
		}
		result = append(result, column)
	}

	return result, nil
}

// removeTableProperties removes table properties like USING DELTA, TBLPROPERTIES, etc.
func (p *SQLParser) removeTableProperties(sql string) string {
	// Find the closing parenthesis of the column definitions
	parenCount := 0
	columnEndIndex := -1

	// Find CREATE TABLE pattern first
	createTableRegex := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+[^(]+\(`)
	match := createTableRegex.FindStringIndex(sql)
	if match == nil {
		return sql
	}

	// Start searching after the opening parenthesis
	startIndex := match[1] - 1 // -1 to include the opening parenthesis

findClosingParen:
	for i := startIndex; i < len(sql); i++ {
		switch sql[i] {
		case '(':
			parenCount++
		case ')':
			parenCount--
			if parenCount == 0 {
				columnEndIndex = i + 1 // Include the closing parenthesis
				break findClosingParen
			}
		}
	}

	if columnEndIndex == -1 {
		return sql // No closing parenthesis found, return original
	}

	// Return the SQL up to the end of column definitions
	return sql[:columnEndIndex]
}

// splitColumnDefinitions splits column definitions by commas, respecting parentheses
func (p *SQLParser) splitColumnDefinitions(defs string) []string {
	var result []string
	var current strings.Builder
	parenDepth := 0

	for _, char := range defs {
		switch char {
		case '(':
			parenDepth++
			current.WriteRune(char)
		case ')':
			parenDepth--
			current.WriteRune(char)
		case ',':
			if parenDepth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(char)
			}
		default:
			current.WriteRune(char)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// parseColumnDefinition parses a single column definition
func (p *SQLParser) parseColumnDefinition(colDef string) (sqlgen.Column, error) {
	// Column definition pattern: name type [NOT NULL] [COMMENT 'comment']
	parts := strings.Fields(colDef)
	if len(parts) < 2 {
		return sqlgen.Column{}, fmt.Errorf("invalid column definition: %s", colDef)
	}

	column := sqlgen.Column{
		Name:     strings.Trim(parts[0], `"`),
		Type:     p.normalizeDataType(parts[1]),
		Nullable: true, // Default to nullable
	}

	// Process remaining parts for constraints and comments
	for i := 2; i < len(parts); i++ {
		upper := strings.ToUpper(parts[i])

		switch upper {
		case "NOT":
			if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "NULL" {
				column.Nullable = false
				i++ // Skip the next "NULL" part
			}
		case "NULL":
			column.Nullable = true
		case "COMMENT":
			if i+1 < len(parts) {
				// Extract comment (remove quotes)
				comment := parts[i+1]
				comment = strings.Trim(comment, `'"`)

				// Handle multi-word comments
				if strings.HasPrefix(parts[i+1], `'`) || strings.HasPrefix(parts[i+1], `"`) {
					quote := parts[i+1][0]
					comment = parts[i+1][1:] // Remove opening quote
					i++

					// Continue collecting until closing quote
					for i < len(parts)-1 && !strings.HasSuffix(parts[i], string(quote)) {
						i++
						comment += " " + parts[i]
					}

					// Remove closing quote
					if strings.HasSuffix(comment, string(quote)) {
						comment = comment[:len(comment)-1]
					}
				}

				column.Comment = comment
			}
		}
	}

	return column, nil
}

// normalizeDataType normalizes SQL data types to match the expected format
func (p *SQLParser) normalizeDataType(dataType string) string {
	upper := strings.ToUpper(dataType)

	// Handle common type mappings
	switch {
	case strings.HasPrefix(upper, "VARCHAR"):
		return "STRING"
	case strings.HasPrefix(upper, "CHAR"):
		return "STRING"
	case strings.HasPrefix(upper, "TEXT"):
		return "STRING"
	case upper == "INTEGER" || upper == "INT":
		return "INT"
	case upper == "BIGINT":
		return "BIGINT"
	case strings.HasPrefix(upper, "DECIMAL"):
		return upper // Keep original format like DECIMAL(10,2)
	case strings.HasPrefix(upper, "NUMERIC"):
		return strings.Replace(upper, "NUMERIC", "DECIMAL", 1)
	case upper == "BOOLEAN" || upper == "BOOL":
		return "BOOLEAN"
	case upper == "TIMESTAMP":
		return "TIMESTAMP"
	case upper == "DATE":
		return "DATE"
	case upper == "TIME":
		return "TIME"
	case strings.HasPrefix(upper, "DOUBLE"):
		return "DOUBLE"
	case strings.HasPrefix(upper, "FLOAT"):
		return "FLOAT"
	default:
		return upper
	}
}
