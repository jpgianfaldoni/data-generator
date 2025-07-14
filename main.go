package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"data-generator/internal/sqlgen"

	"gopkg.in/yaml.v3"
)

const outputDir = "output"

func main() {
	// Check if a filename was provided as a command line argument
	if len(os.Args) < 2 {
		fmt.Println("Usage: data-generator <yaml_file>")
		fmt.Println("Example: data-generator examples/example_table.yaml")
		fmt.Println()
		fmt.Println("The YAML file should include a 'rows' field to specify how many INSERT rows to generate.")
		fmt.Println("Generated SQL files will be saved to the 'output/' directory.")
		os.Exit(1)
	}

	filename := os.Args[1]

	// Ensure output directory exists
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		log.Fatalf("Error creating output directory: %v", err)
	}

	// Read the YAML file
	yamlData, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	// Parse as TableSchema
	var tableSchema sqlgen.TableSchema
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

	fmt.Printf("Generated SQL for table: %s\n", tableSchema.TableName)
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

// generateOutputFilename creates a .sql filename in the output directory based on the input YAML file
func generateOutputFilename(yamlFile string, suffix string) string {
	// Get just the filename without the path
	filename := filepath.Base(yamlFile)

	// Get the file extension
	ext := filepath.Ext(filename)

	// Remove extension and add suffix + .sql
	baseName := strings.TrimSuffix(filename, ext)
	sqlFilename := baseName + suffix + ".sql"

	// Return the full path in the output directory
	return filepath.Join(outputDir, sqlFilename)
}
