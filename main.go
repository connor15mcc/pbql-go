package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cogentcore/readline"
	"github.com/connor15mcc/pbql-go/parser"
	"github.com/connor15mcc/pbql-go/schema"
	"github.com/spf13/cobra"
)

func mainE(args []string) error {
	oldArgs := os.Args
	os.Args = append([]string{"pbql-go"}, args...)
	defer func() { os.Args = oldArgs }()

	rootCmd := &cobra.Command{
		Use:   "pbql-go [flags] <proto-files-or-directories...>",
		Short: "Query protobuf definitions using SQL",
		Long: `Query protobuf definitions using SQL.

This tool allows you to explore and analyze protobuf files using SQL queries,
similar to how you would query a database.

Tables available:
  files
  messages
  fields
  enums
  enum_values
  services
  methods
  extensions
  oneofs
  oneof_fields
  dependencies`,
		Example: `  # Count methods per service
  pbql-go -q "SELECT s.name, COUNT(m.name) as method_count FROM services s LEFT JOIN methods m ON s.full_name = m.service GROUP BY s.name" ./protos/

  # Find all streaming RPCs
  pbql-go -q "SELECT * FROM methods WHERE client_streaming OR server_streaming" ./protos/

  # List messages with more than 10 fields
  pbql-go -q "SELECT m.full_name, COUNT(*) as field_count FROM messages m JOIN fields f ON m.full_name = f.message GROUP BY m.full_name HAVING COUNT(*) > 10" ./protos/`,
		RunE: func(cmd *cobra.Command, cmdArgs []string) error {
			query, _ := cmd.Flags().GetString("query")
			format, _ := cmd.Flags().GetString("format")
			verbose, _ := cmd.Flags().GetCount("verbose")

			if len(cmdArgs) == 0 {
				return fmt.Errorf("at least one proto file or directory is required")
			}

			// Collect proto files
			var protoFiles []string
			var protoDirs []string
			for _, arg := range cmdArgs {
				info, err := os.Stat(arg)
				if err != nil {
					return fmt.Errorf("error: %v", err)
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
				return fmt.Errorf("error initializing database: %v", err)
			}
			defer db.Close()

			ctx := context.Background()

			// Set up structured logging based on verbosity level
			var logWriter io.Writer = os.Stderr
			var handler slog.Handler
			if verbose == 0 {
				handler = slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelError})
			} else if verbose == 1 {
				handler = slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelInfo})
			} else {
				handler = slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelDebug})
			}
			slog.SetDefault(slog.New(handler))

			// Parse directories
			for _, dir := range protoDirs {
				result, err := parser.ParseDirectory(ctx, dir, parser.Options{ Lenient: true })
				if err != nil {
					return fmt.Errorf("error parsing directory %s: %v", dir, err)
				}
				if len(result.Errors) > 0 {
					slog.Info("parsed directory with errors", "dir", dir, "error_count", len(result.Errors))
					for _, e := range result.Errors {
						slog.Debug("parse error", "error", e)
					}
				} else {
					slog.Debug("parsed directory successfully", "dir", dir)
				}
				if err := db.LoadFiles(result.Files); err != nil {
					return fmt.Errorf("error loading files from %s: %v", dir, err)
				}
			}

			// Parse individual files
			if len(protoFiles) > 0 {
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

				result, err := parser.ParseFiles(ctx, baseNames, parser.Options{ ImportPaths: []string{"."}, Lenient: true })
				if err != nil {
					return fmt.Errorf("error parsing files: %v", err)
				}
				if len(result.Errors) > 0 {
					slog.Info("parsed files with errors", "error_count", len(result.Errors))
					for _, e := range result.Errors {
						slog.Debug("parse error", "error", e)
					}
				} else {
					slog.Debug("parsed files successfully")
				}
				if err := db.LoadFiles(result.Files); err != nil {
					return fmt.Errorf("error loading files: %v", err)
				}
			}

			// Execute query or enter interactive mode
			if query != "" {
				if err := executeQuery(db.DB, query, format); err != nil {
					return fmt.Errorf("error: %v", err)
				}
			} else {
				if err := interactiveMode(db.DB, format); err != nil {
					return err
				}
			}
			return nil
		},
	}

	rootCmd.Flags().StringP("query", "q", "", "SQL query to execute")
	rootCmd.Flags().StringP("format", "f", "table", "Output format: table, json, csv")
	rootCmd.Flags().CountP("verbose", "v", "Increase verbosity (specify multiple times: -v, -vv, -vvv)")

	return rootCmd.Execute()
}

func main() {
	err := mainE(os.Args[1:])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}


func interactiveMode(db *sql.DB, format string) error {
	historyPath, _ := os.UserHomeDir()
	historyPath = filepath.Join(historyPath, ".pbql_history")

	rl, err := readline.NewFromConfig(&readline.Config{
		Prompt:      "pbql> ",
		HistoryFile: historyPath,
	})
	if err != nil {
		return fmt.Errorf("error initializing readline: %v", err)
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
			return nil
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
	return nil
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


