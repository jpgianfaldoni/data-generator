package sqlgen

import (
	"fmt"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

// GenerateInsertSQL generates Databricks SQL INSERT statements with random data
func (ts *TableSchema) GenerateInsertSQL() string {
	if ts.RowCount <= 0 {
		return ""
	}

	var sql strings.Builder

	// Build the table name with optional catalog and schema
	tableName := BuildTableName(ts.Catalog, ts.Schema, ts.TableName)

	// Build column names for INSERT statement
	columnNames := make([]string, len(ts.Columns))
	for i, col := range ts.Columns {
		columnNames[i] = col.Name
	}

	// Start INSERT statement
	sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES\n",
		tableName, strings.Join(columnNames, ", ")))

	// Initialize primary key counters
	primaryKeyCounters := make(map[string]int64)
	for _, col := range ts.Columns {
		if col.PrimaryKey {
			primaryKeyCounters[col.Name] = 1 // Start from 1 for primary keys
		}
	}

	// Generate random data for each row
	for rowIndex := 0; rowIndex < ts.RowCount; rowIndex++ {
		sql.WriteString("  (")

		// Generate value for each column
		for colIndex, col := range ts.Columns {
			var value string
			if col.PrimaryKey {
				// Generate incremental value for primary key
				value = generatePrimaryKeyValue(col.Type, primaryKeyCounters[col.Name])
				primaryKeyCounters[col.Name]++
			} else {
				// Generate random value for non-primary key columns
				value = generateRandomValue(col.Name, col.Type, col.Nullable)
			}
			sql.WriteString(value)

			// Add comma if not last column
			if colIndex < len(ts.Columns)-1 {
				sql.WriteString(", ")
			}
		}

		sql.WriteString(")")

		// Add comma if not last row
		if rowIndex < ts.RowCount-1 {
			sql.WriteString(",")
		}

		sql.WriteString("\n")
	}

	sql.WriteString(";")
	return sql.String()
}

// generatePrimaryKeyValue creates incremental values for primary key columns
func generatePrimaryKeyValue(columnType string, counter int64) string {
	switch {
	case strings.Contains(columnType, "STRING") || strings.Contains(columnType, "VARCHAR"):
		// For string primary keys, create a pattern like "ID_001", "ID_002", etc.
		return fmt.Sprintf("'ID_%03d'", counter)
	default:
		// For all integer types (BIGINT, INT, SMALLINT, TINYINT) and others, use incremental numbers
		return fmt.Sprintf("%d", counter)
	}
}

// generateRandomValue creates random data based on column type using gofakeit
func generateRandomValue(columnName, columnType string, nullable bool) string {
	// Sometimes generate NULL for nullable columns (10% chance)
	if nullable && gofakeit.Float32() < 0.1 {
		return "NULL"
	}

	// Handle different data types with realistic data
	switch {
	case strings.Contains(columnType, "BIGINT"):
		return fmt.Sprintf("%d", gofakeit.Int64())

	case strings.Contains(columnType, "INT"):
		return fmt.Sprintf("%d", gofakeit.Int32())

	case strings.Contains(columnType, "SMALLINT"):
		return fmt.Sprintf("%d", gofakeit.IntRange(0, 32767))

	case strings.Contains(columnType, "TINYINT"):
		return fmt.Sprintf("%d", gofakeit.IntRange(0, 255))

	case strings.Contains(columnType, "STRING") || strings.Contains(columnType, "VARCHAR"):
		// Generate contextual fake data based on column name
		return fmt.Sprintf("'%s'", generateContextualValue(columnName))

	case strings.Contains(columnType, "BOOLEAN"):
		return fmt.Sprintf("%t", gofakeit.Bool())

	case strings.Contains(columnType, "TIMESTAMP"):
		// Generate random timestamp within the last 2 years
		endDate := time.Now()
		startDate := endDate.AddDate(-2, 0, 0)
		randomTime := gofakeit.DateRange(startDate, endDate)
		return fmt.Sprintf("'%s'", randomTime.Format("2006-01-02 15:04:05"))

	case strings.Contains(columnType, "DATE"):
		// Generate random date within the last 2 years
		endDate := time.Now()
		startDate := endDate.AddDate(-2, 0, 0)
		randomDate := gofakeit.DateRange(startDate, endDate)
		return fmt.Sprintf("'%s'", randomDate.Format("2006-01-02"))

	case strings.Contains(columnType, "DECIMAL"):
		// Generate realistic decimal values (e.g., for prices, percentages)
		return fmt.Sprintf("%.2f", gofakeit.Float64Range(0.01, 9999.99))

	case strings.Contains(columnType, "DOUBLE") || strings.Contains(columnType, "FLOAT"):
		return fmt.Sprintf("%.4f", gofakeit.Float64Range(0.0001, 99999.9999))

	default:
		// Default to realistic string data for unknown types
		return fmt.Sprintf("'%s'", gofakeit.Word())
	}
}

// generateContextualValue creates contextual fake data based on column name patterns
func generateContextualValue(columnName string) string {
	lowerName := strings.ToLower(columnName)

	// Generate contextual data based on common column name patterns
	switch {
	case strings.Contains(lowerName, "email"):
		return gofakeit.Email()
	case strings.Contains(lowerName, "phone"):
		return gofakeit.Phone()
	case strings.Contains(lowerName, "address"):
		return gofakeit.Address().Address
	case strings.Contains(lowerName, "city"):
		return gofakeit.City()
	case strings.Contains(lowerName, "state"):
		return gofakeit.State()
	case strings.Contains(lowerName, "country"):
		return gofakeit.Country()
	case strings.Contains(lowerName, "zip") || strings.Contains(lowerName, "postal"):
		return gofakeit.Zip()
	case strings.Contains(lowerName, "first") && strings.Contains(lowerName, "name"):
		return gofakeit.FirstName()
	case strings.Contains(lowerName, "last") && strings.Contains(lowerName, "name"):
		return gofakeit.LastName()
	case strings.Contains(lowerName, "name") || strings.Contains(lowerName, "username"):
		return gofakeit.Username()
	case strings.Contains(lowerName, "company"):
		return gofakeit.Company()
	case strings.Contains(lowerName, "job") || strings.Contains(lowerName, "title"):
		return gofakeit.JobTitle()
	case strings.Contains(lowerName, "description"):
		return gofakeit.Sentence(gofakeit.IntRange(5, 15))
	case strings.Contains(lowerName, "url") || strings.Contains(lowerName, "website"):
		return gofakeit.URL()
	case strings.Contains(lowerName, "uuid") || strings.Contains(lowerName, "guid"):
		return gofakeit.UUID()
	case strings.Contains(lowerName, "price") || strings.Contains(lowerName, "cost"):
		return fmt.Sprintf("%.2f", gofakeit.Float64Range(1.00, 999.99))
	case strings.Contains(lowerName, "product"):
		return gofakeit.Product().Name
	case strings.Contains(lowerName, "category"):
		categories := []string{"Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Automotive", "Health", "Beauty", "Food", "Toys"}
		return gofakeit.RandomString(categories)
	case strings.Contains(lowerName, "color"):
		return gofakeit.Color()
	case strings.Contains(lowerName, "status"):
		statuses := []string{"active", "inactive", "pending", "completed", "cancelled"}
		return gofakeit.RandomString(statuses)
	default:
		// Fallback to varied realistic data
		options := []string{
			gofakeit.FirstName(),
			gofakeit.LastName(),
			gofakeit.Company(),
			gofakeit.JobTitle(),
			gofakeit.City(),
			gofakeit.Product().Name,
			gofakeit.Word(),
			gofakeit.Sentence(gofakeit.IntRange(3, 8)),
		}
		return gofakeit.RandomString(options)
	}
}
