package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"data-generator/internal/sqlgen"
	"data-generator/internal/sqlparser"

	"gopkg.in/yaml.v3"
)

const outputDir = "output"

func main() {
	// Check if a filename was provided as a command line argument
	if len(os.Args) < 2 {
		fmt.Println("Usage: data-generator <yaml_file|sql_file>")
		fmt.Println("Examples:")
		fmt.Println("  data-generator examples/example_table.yaml")
		fmt.Println("  data-generator schema/create_table.sql")
		fmt.Println()
		fmt.Println("For YAML files: The file should include a 'rows' field to specify how many INSERT rows to generate.")
		fmt.Println("For SQL files: CREATE TABLE statements will be parsed and converted to YAML format first.")
		fmt.Println("Generated SQL files will be saved to the 'output/' directory.")
		os.Exit(1)
	}

	filename := os.Args[1]

	// Ensure output directory exists
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		log.Fatalf("Error creating output directory: %v", err)
	}

	var tableSchema sqlgen.TableSchema
	var yamlData []byte

	// Determine file type and process accordingly
	ext := strings.ToLower(filepath.Ext(filename))

	switch ext {
	case ".sql":
		fmt.Printf("🔧 Processing SQL file: %s\n", filename)
		fmt.Println("📝 Converting CREATE TABLE to YAML format...")

		// Read the SQL file
		sqlData, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("Error reading SQL file: %v", err)
		}

		// Parse SQL to YAML using sqlparser
		parser := sqlparser.NewSQLParser()
		yamlString, err := parser.ParseCreateTableToYAML(string(sqlData))
		if err != nil {
			log.Fatalf("Error parsing SQL to YAML: %v", err)
		}

		yamlData = []byte(yamlString)
		fmt.Println("✅ SQL successfully converted to YAML format")

		// Optionally save the converted YAML for reference
		yamlOutputFile := generateYAMLOutputFilename(filename)
		err = os.WriteFile(yamlOutputFile, yamlData, 0644)
		if err != nil {
			log.Printf("Warning: Could not save converted YAML file: %v", err)
		} else {
			fmt.Printf("📄 Converted YAML saved to: %s\n", yamlOutputFile)
		}

	case ".yaml", ".yml":
		fmt.Printf("📄 Processing YAML file: %s\n", filename)

		// Read the YAML file
		yamlData, err = os.ReadFile(filename)
		if err != nil {
			log.Fatalf("Error reading YAML file: %v", err)
		}

	default:
		log.Fatalf("Unsupported file format: %s. Please provide a .yaml, .yml, or .sql file", ext)
	}

	// Parse YAML data into TableSchema
	err = yaml.Unmarshal(yamlData, &tableSchema)
	if err != nil {
		log.Fatalf("Error parsing YAML: %v", err)
	}

	// Always generate CREATE TABLE SQL
	createSQL := tableSchema.GenerateCreateTableSQL()
	createOutputFile := generateOutputFilename(filename, "_create")

	err = os.WriteFile(createOutputFile, []byte(createSQL), 0644)
	if err != nil {
		log.Fatalf("Error writing CREATE SQL file: %v", err)
	}

	fmt.Printf("\nGenerated SQL for table: %s\n", tableSchema.TableName)
	fmt.Println(strings.Repeat("=", 50))

	fmt.Println("\n1. CREATE TABLE SQL:")
	fmt.Println("-------------------")
	fmt.Println(createSQL)
	fmt.Printf("✅ CREATE TABLE SQL saved to: %s\n", createOutputFile)

	// Generate INSERT SQL if rows > 0
	if tableSchema.RowCount > 0 {
		insertSQL := tableSchema.GenerateInsertSQL()
		insertOutputFile := generateOutputFilename(filename, "_insert")

		err = os.WriteFile(insertOutputFile, []byte(insertSQL), 0644)
		if err != nil {
			log.Fatalf("Error writing INSERT SQL file: %v", err)
		}

		fmt.Printf("\n2. INSERT SQL (%d rows):\n", tableSchema.RowCount)
		fmt.Println("------------------------")
		fmt.Println(insertSQL)
		fmt.Printf("✅ INSERT SQL saved to: %s\n", insertOutputFile)
	} else {
		fmt.Println("\n2. INSERT SQL:")
		fmt.Println("-------------")
		fmt.Println("⚠️  No INSERT SQL generated (add 'rows: N' field to YAML to generate INSERT statements)")
	}
}

// generateOutputFilename creates a .sql filename in the output directory based on the input file
func generateOutputFilename(inputFile string, suffix string) string {
	// Get just the filename without the path
	filename := filepath.Base(inputFile)

	// Get the file extension
	ext := filepath.Ext(filename)

	// Remove extension and add suffix + .sql
	baseName := strings.TrimSuffix(filename, ext)
	sqlFilename := baseName + suffix + ".sql"

	// Return the full path in the output directory
	return filepath.Join(outputDir, sqlFilename)
}

// generateYAMLOutputFilename creates a .yaml filename in the output directory for converted SQL files
func generateYAMLOutputFilename(sqlFile string) string {
	// Get just the filename without the path
	filename := filepath.Base(sqlFile)

	// Get the file extension
	ext := filepath.Ext(filename)

	// Remove extension and add .yaml
	baseName := strings.TrimSuffix(filename, ext)
	yamlFilename := baseName + "_converted.yaml"

	// Return the full path in the output directory
	return filepath.Join(outputDir, yamlFilename)
}
