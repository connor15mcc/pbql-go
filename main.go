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
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/connor15mcc/pbql-go/parser"
	"github.com/connor15mcc/pbql-go/schema"
	"github.com/connor15mcc/pbql-go/tui"
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
				result, err := parser.ParseDirectory(ctx, dir, parser.Options{})
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

				result, err := parser.ParseFiles(ctx, baseNames, parser.Options{ ImportPaths: []string{"."} })
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
	return tui.Run(db, format)
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

func getHistoryPath() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	return filepath.Join(usr.HomeDir, ".pbql_history"), nil
}

func appendToHistory(query string) error {
	path, err := getHistoryPath()
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	timestamp := time.Now().Format(time.RFC3339)
	_, err = f.WriteString(fmt.Sprintf("# %s\n%s\n\n", timestamp, query))
	return err
}
