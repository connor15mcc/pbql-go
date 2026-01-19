# pbql

Query protobuf definitions using SQL.

## Installation

```bash
go install $GOPATH/bin/pbql github.com/connor15mcc/pbql-go@latest
mv ~/go/bin/pbql-go ~/go/bin/pbql # or use $GOPATH if set
```

Or build locally:

```bash
git clone https://github.com/connor15mcc/pbql-go
cd pbql-go
go build -o pbql
```

## Usage

<!-- HELP START -->
```
Query protobuf definitions using SQL.

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
  dependencies

Usage:
  pbql-go [flags] <proto-files-or-directories...>

Examples:
  # Count methods per service
  pbql-go -q "SELECT s.name, COUNT(m.name) as method_count FROM services s LEFT JOIN methods m ON s.full_name = m.service GROUP BY s.name" ./protos/

  # Find all streaming RPCs
  pbql-go -q "SELECT * FROM methods WHERE client_streaming OR server_streaming" ./protos/

  # List messages with more than 10 fields
  pbql-go -q "SELECT m.full_name, COUNT(*) as field_count FROM messages m JOIN fields f ON m.full_name = f.message GROUP BY m.full_name HAVING COUNT(*) > 10" ./protos/

Flags:
  -f, --format string   Output format: table, json, csv (default "table")
  -h, --help            help for pbql-go
  -q, --query string    SQL query to execute
  -v, --verbose count   Increase verbosity (specify multiple times: -v, -vv, -vvv)
```
<!-- HELP END -->

### Interactive Mode

If no query is provided, enter interactive mode with command history and line editing.

Commands:
- `.help`, `.h`, `.?`: Show help
- `.tables`: List all tables
- `.schema`: Show detailed schema
- `.format <fmt>`: Set output format (table, json, csv)
- `.quit`, `.exit`: Exit

### Available Tables

- `files`: Proto file information
- `messages`: Message definitions
- `fields`: Field definitions
- `enums`: Enum definitions
- `enum_values`: Enum value definitions
- `services`: Service definitions
- `methods`: RPC method definitions
- `extensions`: Extension definitions
- `oneofs`: Oneof definitions
- `oneof_fields`: Oneof field mappings
- `dependencies`: Import dependencies

## Examples

Count methods per service:
```sql
SELECT s.name, COUNT(m.name) as method_count
FROM services s
LEFT JOIN methods m ON s.full_name = m.service
GROUP BY s.name
```

Find all streaming RPCs:
```sql
SELECT * FROM methods WHERE client_streaming OR server_streaming
```

List messages with more than 10 fields:
```sql
SELECT m.full_name, COUNT(*) as field_count
FROM messages m
JOIN fields f ON m.full_name = f.message
GROUP BY m.full_name
HAVING COUNT(*) > 10
```
