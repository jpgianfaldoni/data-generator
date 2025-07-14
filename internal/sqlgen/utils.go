package sqlgen

import "fmt"

// BuildTableName creates a properly formatted table name with optional catalog and schema
func BuildTableName(catalog, schema, tableName string) string {
	if catalog != "" && schema != "" {
		return fmt.Sprintf("%s.%s.%s", catalog, schema, tableName)
	} else if schema != "" {
		return fmt.Sprintf("%s.%s", schema, tableName)
	} else {
		return tableName
	}
}
