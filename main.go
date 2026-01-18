package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cogentcore/readline"
	"github.com/connor15mcc/pbql-go/parser"
	"github.com/connor15mcc/pbql-go/schema"
)

func main() {
	var (
		query       string
		importPaths stringSlice
		format      string
		lenient     bool
	)

	flag.StringVar(&query, "q", "", "SQL query to execute")
	flag.StringVar(&query, "query", "", "SQL query to execute")
	flag.Var(&importPaths, "I", "Import paths for proto files (can be specified multiple times)")
	flag.Var(&importPaths, "import", "Import paths for proto files (can be specified multiple times)")
	flag.StringVar(&format, "f", "table", "Output format: table, json, csv")
	flag.StringVar(&format, "format", "table", "Output format: table, json, csv")
	flag.BoolVar(&lenient, "lenient", false, "Continue parsing even if some files have errors")
	flag.BoolVar(&lenient, "l", false, "Continue parsing even if some files have errors")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <proto-files-or-directories...>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Query protobuf definitions using SQL.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nTables available:\n")
		fmt.Fprintf(os.Stderr, "  files\n")
		fmt.Fprintf(os.Stderr, "  messages\n")
		fmt.Fprintf(os.Stderr, "  fields\n")
		fmt.Fprintf(os.Stderr, "  enums\n")
		fmt.Fprintf(os.Stderr, "  enum_values\n")
		fmt.Fprintf(os.Stderr, "  services\n")
		fmt.Fprintf(os.Stderr, "  methods\n")
		fmt.Fprintf(os.Stderr, "  extensions\n")
		fmt.Fprintf(os.Stderr, "  oneofs\n")
		fmt.Fprintf(os.Stderr, "  oneof_fields\n")
		fmt.Fprintf(os.Stderr, "  dependencies\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Count methods per service\n")
		fmt.Fprintf(os.Stderr, "  %s -q \"SELECT s.name, COUNT(m.name) as method_count FROM services s LEFT JOIN methods m ON s.full_name = m.service GROUP BY s.name\" ./protos/\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Find all streaming RPCs\n")
		fmt.Fprintf(os.Stderr, "  %s -q \"SELECT * FROM methods WHERE client_streaming OR server_streaming\" ./protos/\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # List messages with more than 10 fields\n")
		fmt.Fprintf(os.Stderr, "  %s -q \"SELECT m.full_name, COUNT(*) as field_count FROM messages m JOIN fields f ON m.full_name = f.message GROUP BY m.full_name HAVING COUNT(*) > 10\" ./protos/\n", os.Args[0])
	}

	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "Error: at least one proto file or directory is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	// Collect proto files
	var protoFiles []string
	var protoDirs []string
	for _, arg := range flag.Args() {
		info, err := os.Stat(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if info.IsDir() {
			protoDirs = append(protoDirs, arg)
		} else {
			protoFiles = append(protoFiles, arg)
		}
	}

	// Initialize database
	db, err := schema.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := context.Background()

	parseOpts := parser.Options{ Lenient: lenient }

	// Parse directories
	for _, dir := range protoDirs {
		result, err := parser.ParseDirectory(ctx, dir, parseOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing directory %s: %v\n", dir, err)
			os.Exit(1)
		}
		if len(result.Errors) > 0 {
			fmt.Fprintf(os.Stderr, "Parsed with %d errors (lenient mode):\n", len(result.Errors))
			for _, e := range result.Errors {
				fmt.Fprintf(os.Stderr, "  - %v\n", e)
			}
		}
		if err := db.LoadFiles(result.Files); err != nil {
			fmt.Fprintf(os.Stderr, "Error loading files from %s: %v\n", dir, err)
			os.Exit(1)
		}
	}

	// Parse individual files
	if len(protoFiles) > 0 {
		// Determine import paths from file locations
		allImportPaths := make([]string, 0, len(importPaths)+len(protoFiles))
		allImportPaths = append(allImportPaths, importPaths...)

		// Add directories containing proto files as import paths
		seenDirs := make(map[string]bool)
		for _, f := range protoFiles {
			dir := filepath.Dir(f)
			absDir, _ := filepath.Abs(dir)
			if !seenDirs[absDir] {
				seenDirs[absDir] = true
				allImportPaths = append(allImportPaths, absDir)
			}
		}

		// Convert to basenames for parsing
		baseNames := make([]string, len(protoFiles))
		for i, f := range protoFiles {
			baseNames[i] = filepath.Base(f)
		}

		// Change to first file's directory
		firstDir := filepath.Dir(protoFiles[0])
		origDir, _ := os.Getwd()
		os.Chdir(firstDir)
		defer os.Chdir(origDir)

		fileParseOpts := parser.Options{
			ImportPaths:   []string{"."},
			Lenient:       lenient,
		}
		result, err := parser.ParseFiles(ctx, baseNames, fileParseOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing files: %v\n", err)
			os.Exit(1)
		}
		if err := db.LoadFiles(result.Files); err != nil {
			fmt.Fprintf(os.Stderr, "Error loading files: %v\n", err)
			os.Exit(1)
		}
	}

	// Execute query or enter interactive mode
	if query != "" {
		if err := executeQuery(db.DB, query, format); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	} else {
		interactiveMode(db.DB, format)
	}
}

func interactiveMode(db *sql.DB, format string) {
	historyPath, _ := os.UserHomeDir()
	historyPath = filepath.Join(historyPath, ".pbql_history")

	rl, err := readline.NewFromConfig(&readline.Config{
		Prompt:      "pbql> ",
		HistoryFile: historyPath,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	currentFormat := format

	fmt.Println("pbql-go interactive mode. Type '.help' for commands, '.quit' to exit.")
	fmt.Println("Enter SQL queries to explore your protobuf definitions.")
	fmt.Println()

	for {
		line, err := rl.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Goodbye!")
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		switch strings.ToLower(line) {
		case ".quit", ".exit", ".q":
			fmt.Println("Goodbye!")
			return
		case ".help", ".h", ".?":
			printHelp()
			continue
		case ".tables":
			line = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
		case ".schema":
			printSchema()
			continue
		default:
			if strings.HasPrefix(line, ".format ") {
				newFmt := strings.TrimSpace(strings.TrimPrefix(line, ".format "))
				if newFmt == "table" || newFmt == "json" || newFmt == "csv" {
					currentFormat = newFmt
					fmt.Printf("Output format set to %s\n", newFmt)
				} else {
					fmt.Printf("Invalid format: %s. Valid formats: table, json, csv\n", newFmt)
				}
				fmt.Println()
				continue
			}
		}

		if err := executeQuery(db, line, currentFormat); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		fmt.Println()
	}
}

func printHelp() {
	fmt.Print(`Commands:
   .help, .h, .?   Show this help
   .tables         List all tables
   .schema         Show detailed schema
   .format <fmt>   Set output format (table, json, csv)
   .quit, .exit    Exit interactive mode

Example queries:
  -- Count methods per service
  SELECT s.name, COUNT(m.name) as methods 
  FROM services s 
  LEFT JOIN methods m ON s.full_name = m.service 
  GROUP BY s.name;

  -- Find all streaming RPCs
  SELECT service, name, client_streaming, server_streaming 
  FROM methods 
  WHERE client_streaming OR server_streaming;

  -- Messages with most fields
  SELECT m.name, COUNT(*) as field_count 
  FROM messages m 
  JOIN fields f ON m.full_name = f.message 
  GROUP BY m.full_name 
  ORDER BY field_count DESC 
  LIMIT 10;

  -- Find repeated fields
  SELECT message, name, type 
  FROM fields 
  WHERE is_repeated = true;

Querying options (use ::JSON to cast, then -> or json_extract_string):
  -- Services with a specific option
  SELECT name, options::JSON->'my.custom.option'->>'field' as val
  FROM services WHERE options IS NOT NULL;

  -- Find deprecated methods
  SELECT name FROM methods 
  WHERE json_extract_string(options::JSON, '$.deprecated') = 'true';

  -- Query custom extension options (use quotes for dotted keys)
  SELECT name, json_extract_string(options::JSON, '$."google.api.http".get') as path
  FROM methods WHERE options IS NOT NULL;
`)
}

func printSchema() {
	fmt.Print(`Tables:
  files (name, package, syntax, options)
  messages (full_name, name, file, parent_message, is_map_entry, options)
  fields (id, name, number, message, type, type_name, label, is_repeated, is_optional, is_map, map_key_type, map_value_type, default_value, json_name, options)
  enums (full_name, name, file, parent_message, options)
  enum_values (id, name, number, enum, options)
  services (full_name, name, file, options)
  methods (full_name, name, service, input_type, output_type, client_streaming, server_streaming, options)
  extensions (full_name, name, number, file, extendee, type, type_name, options)
  oneofs (id, name, message, options)
  oneof_fields (oneof_id, field_id)
  dependencies (file, dependency, is_public, is_weak)

Options column contains JSON with proto options. Query with:
  options::JSON->'option_name'->>'field'           -- arrow syntax
  json_extract_string(options::JSON, '$.path')     -- JSONPath syntax
  json_extract_string(options::JSON, '$."dotted.key".field')  -- quoted keys
`)
}

func executeQuery(db *sql.DB, query, format string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	switch format {
	case "json":
		return outputJSON(rows, cols)
	case "csv":
		return outputCSV(rows, cols)
	default:
		return outputTable(rows, cols)
	}
}

func outputTable(rows *sql.Rows, cols []string) error {
	// Collect all data first to calculate column widths
	var data [][]string
	colWidths := make([]int, len(cols))

	for i, col := range cols {
		colWidths[i] = len(col)
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make([]string, len(cols))
		for i, val := range values {
			row[i] = formatValue(val)
			if len(row[i]) > colWidths[i] {
				colWidths[i] = len(row[i])
			}
		}
		data = append(data, row)
	}

	// Print header
	printTableRow(cols, colWidths)
	printTableSeparator(colWidths)

	// Print data
	for _, row := range data {
		printTableRow(row, colWidths)
	}

	fmt.Printf("(%d rows)\n", len(data))
	return rows.Err()
}

func printTableRow(values []string, widths []int) {
	for i, val := range values {
		fmt.Printf("%-*s", widths[i]+2, val)
	}
	fmt.Println()
}

func printTableSeparator(widths []int) {
	for _, w := range widths {
		fmt.Print(strings.Repeat("-", w+2))
	}
	fmt.Println()
}

func outputJSON(rows *sql.Rows, cols []string) error {
	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make(map[string]interface{})
		for i, col := range cols {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}

func outputCSV(rows *sql.Rows, cols []string) error {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	if err := writer.Write(cols); err != nil {
		return err
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make([]string, len(cols))
		for i, val := range values {
			row[i] = formatValue(val)
		}

		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return rows.Err()
}

func formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case []byte:
		return string(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// stringSlice implements flag.Value for collecting multiple string flags
type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}
