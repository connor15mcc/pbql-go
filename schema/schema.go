package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/bufbuild/protocompile/linker"
	"github.com/duckdb/duckdb-go/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// DB wraps the DuckDB connection with proto-specific operations.
type DB struct {
	*sql.DB
	conn driver.Conn
}

func New() (*DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	var driverConn driver.Conn
	if err := conn.Raw(func(dc any) error {
		driverConn = dc.(driver.Conn)
		return nil
	}); err != nil {
		conn.Close()
		db.Close()
		return nil, fmt.Errorf("failed to get driver connection: %w", err)
	}

	d := &DB{DB: db, conn: driverConn}
	if err := d.createSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return d, nil
}

func (d *DB) createSchema() error {
	// Create tables without foreign key constraints for faster loading
	// DuckDB doesn't enforce FK constraints anyway, they're just metadata
	// Note: options stored as JSON - query directly with -> or json_extract_string
	schemas := []string{
		`CREATE TABLE files (
			name VARCHAR PRIMARY KEY,
			package VARCHAR,
			syntax VARCHAR,
			options JSON
		)`,
		`CREATE TABLE messages (
			full_name VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			file VARCHAR NOT NULL,
			parent_message VARCHAR,
			is_map_entry BOOLEAN DEFAULT FALSE,
			options JSON
		)`,
		`CREATE TABLE fields (
			id VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			number INTEGER NOT NULL,
			message VARCHAR NOT NULL,
			type VARCHAR NOT NULL,
			type_name VARCHAR,
			label VARCHAR,
			is_repeated BOOLEAN DEFAULT FALSE,
			is_optional BOOLEAN DEFAULT FALSE,
			is_map BOOLEAN DEFAULT FALSE,
			map_key_type VARCHAR,
			map_value_type VARCHAR,
			default_value VARCHAR,
			json_name VARCHAR,
			options JSON
		)`,
		`CREATE TABLE enums (
			full_name VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			file VARCHAR NOT NULL,
			parent_message VARCHAR,
			options JSON
		)`,
		`CREATE TABLE enum_values (
			id VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			number INTEGER NOT NULL,
			enum VARCHAR NOT NULL,
			options JSON
		)`,
		`CREATE TABLE services (
			full_name VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			file VARCHAR NOT NULL,
			options JSON
		)`,
		`CREATE TABLE methods (
			full_name VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			service VARCHAR NOT NULL,
			input_type VARCHAR NOT NULL,
			output_type VARCHAR NOT NULL,
			client_streaming BOOLEAN DEFAULT FALSE,
			server_streaming BOOLEAN DEFAULT FALSE,
			options JSON
		)`,
		`CREATE TABLE extensions (
			full_name VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			number INTEGER NOT NULL,
			file VARCHAR NOT NULL,
			extendee VARCHAR NOT NULL,
			type VARCHAR NOT NULL,
			type_name VARCHAR,
			options JSON
		)`,
		`CREATE TABLE oneofs (
			id VARCHAR PRIMARY KEY,
			name VARCHAR NOT NULL,
			message VARCHAR NOT NULL,
			options JSON
		)`,
		`CREATE TABLE oneof_fields (
			oneof_id VARCHAR NOT NULL,
			field_id VARCHAR NOT NULL,
			PRIMARY KEY (oneof_id, field_id)
		)`,
		`CREATE TABLE dependencies (
			file VARCHAR NOT NULL,
			dependency VARCHAR NOT NULL,
			is_public BOOLEAN DEFAULT FALSE,
			is_weak BOOLEAN DEFAULT FALSE,
			PRIMARY KEY (file, dependency)
		)`,
	}

	for _, schema := range schemas {
		if _, err := d.Exec(schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return nil
}

// bulkLoader holds appenders for all tables for efficient bulk loading.
type bulkLoader struct {
	files        *duckdb.Appender
	messages     *duckdb.Appender
	fields       *duckdb.Appender
	enums        *duckdb.Appender
	enumValues   *duckdb.Appender
	services     *duckdb.Appender
	methods      *duckdb.Appender
	extensions   *duckdb.Appender
	oneofs       *duckdb.Appender
	oneofFields  *duckdb.Appender
	dependencies *duckdb.Appender

	// resolver for extension type resolution
	resolver linker.Resolver
}

func newBulkLoader(conn driver.Conn, resolver linker.Resolver) (*bulkLoader, error) {
	var err error
	bl := &bulkLoader{resolver: resolver}

	bl.files, err = duckdb.NewAppenderFromConn(conn, "", "files")
	if err != nil {
		return nil, fmt.Errorf("failed to create files appender: %w", err)
	}

	bl.messages, err = duckdb.NewAppenderFromConn(conn, "", "messages")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create messages appender: %w", err)
	}

	bl.fields, err = duckdb.NewAppenderFromConn(conn, "", "fields")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create fields appender: %w", err)
	}

	bl.enums, err = duckdb.NewAppenderFromConn(conn, "", "enums")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create enums appender: %w", err)
	}

	bl.enumValues, err = duckdb.NewAppenderFromConn(conn, "", "enum_values")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create enum_values appender: %w", err)
	}

	bl.services, err = duckdb.NewAppenderFromConn(conn, "", "services")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create services appender: %w", err)
	}

	bl.methods, err = duckdb.NewAppenderFromConn(conn, "", "methods")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create methods appender: %w", err)
	}

	bl.extensions, err = duckdb.NewAppenderFromConn(conn, "", "extensions")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create extensions appender: %w", err)
	}

	bl.oneofs, err = duckdb.NewAppenderFromConn(conn, "", "oneofs")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create oneofs appender: %w", err)
	}

	bl.oneofFields, err = duckdb.NewAppenderFromConn(conn, "", "oneof_fields")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create oneof_fields appender: %w", err)
	}

	bl.dependencies, err = duckdb.NewAppenderFromConn(conn, "", "dependencies")
	if err != nil {
		bl.Close()
		return nil, fmt.Errorf("failed to create dependencies appender: %w", err)
	}

	return bl, nil
}

func (bl *bulkLoader) Close() error {
	var firstErr error
	closeAppender := func(a *duckdb.Appender) {
		if a != nil {
			if err := a.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	closeAppender(bl.files)
	closeAppender(bl.messages)
	closeAppender(bl.fields)
	closeAppender(bl.enums)
	closeAppender(bl.enumValues)
	closeAppender(bl.services)
	closeAppender(bl.methods)
	closeAppender(bl.extensions)
	closeAppender(bl.oneofs)
	closeAppender(bl.oneofFields)
	closeAppender(bl.dependencies)

	return firstErr
}

func (bl *bulkLoader) Flush() error {
	appenders := []*duckdb.Appender{
		bl.files, bl.messages, bl.fields, bl.enums, bl.enumValues,
		bl.services, bl.methods, bl.extensions, bl.oneofs, bl.oneofFields, bl.dependencies,
	}
	for _, a := range appenders {
		if a != nil {
			if err := a.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// extractOptions extracts options from a protobuf descriptor and returns JSON.
// Returns nil if no options are set.
func (bl *bulkLoader) extractOptions(opts proto.Message) any {
	if opts == nil {
		return nil
	}

	msg := opts.ProtoReflect()
	if !msg.IsValid() {
		return nil
	}

	// Check if any fields are set
	hasFields := false
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		hasFields = true
		return false // stop iteration, we just need to know if there are any
	})

	if !hasFields {
		return nil
	}

	// Build a map of option name -> value
	result := make(map[string]any)

	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		var optionName string
		if fd.IsExtension() {
			// For extensions, use the full name
			optionName = string(fd.FullName())
		} else {
			// For standard options, use the field name
			optionName = string(fd.Name())
		}

		result[optionName] = valueToInterface(fd, v, bl.resolver)
		return true
	})

	// Return JSON object for DuckDB JSON column
	return result
}

// valueToInterface converts a protoreflect.Value to a Go any for JSON serialization.
func valueToInterface(fd protoreflect.FieldDescriptor, v protoreflect.Value, resolver linker.Resolver) any {
	if fd.IsList() {
		list := v.List()
		result := make([]any, list.Len())
		for i := 0; i < list.Len(); i++ {
			result[i] = scalarToInterface(fd, list.Get(i), resolver)
		}
		return result
	}

	if fd.IsMap() {
		m := v.Map()
		result := make(map[string]any)
		m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			keyStr := fmt.Sprintf("%v", k.Interface())
			result[keyStr] = scalarToInterface(fd.MapValue(), v, resolver)
			return true
		})
		return result
	}

	return scalarToInterface(fd, v, resolver)
}

// scalarToInterface converts a scalar protoreflect.Value to a Go any.
func scalarToInterface(fd protoreflect.FieldDescriptor, v protoreflect.Value, resolver linker.Resolver) any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return v.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(v.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return v.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(v.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return v.Uint()
	case protoreflect.FloatKind:
		return float32(v.Float())
	case protoreflect.DoubleKind:
		return v.Float()
	case protoreflect.StringKind:
		return v.String()
	case protoreflect.BytesKind:
		return v.Bytes()
	case protoreflect.EnumKind:
		// Return enum value name if we can resolve it, otherwise the number
		enumVal := fd.Enum().Values().ByNumber(v.Enum())
		if enumVal != nil {
			return string(enumVal.Name())
		}
		return int32(v.Enum())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return messageToInterface(v.Message(), resolver)
	default:
		return v.Interface()
	}
}

// messageToInterface converts a protoreflect.Message to a map for JSON serialization.
func messageToInterface(msg protoreflect.Message, resolver linker.Resolver) map[string]any {
	if !msg.IsValid() {
		return nil
	}

	result := make(map[string]any)
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		var fieldName string
		if fd.IsExtension() {
			fieldName = string(fd.FullName())
		} else {
			fieldName = string(fd.Name())
		}
		result[fieldName] = valueToInterface(fd, v, resolver)
		return true
	})

	return result
}

// LoadFiles loads parsed proto files into the database using bulk loading.
func (d *DB) LoadFiles(files []linker.File) error {
	if len(files) == 0 {
		return nil
	}

	// Create a combined resolver from all files for extension resolution
	var resolver linker.Resolver
	if len(files) > 0 {
		resolver = linker.ResolverFromFile(files[0])
	}

	bl, err := newBulkLoader(d.conn, resolver)
	if err != nil {
		return err
	}
	defer bl.Close()

	for _, f := range files {
		if err := loadFile(bl, f); err != nil {
			return err
		}
	}

	return bl.Flush()
}

func loadFile(bl *bulkLoader, f linker.File) error {
	fileName := f.Path()
	pkgName := string(f.Package())

	syntax := "proto2"
	switch f.Syntax() {
	case protoreflect.Proto3:
		syntax = "proto3"
	case protoreflect.Editions:
		syntax = "editions"
	}

	fileOpts := bl.extractOptions(f.Options())

	if err := bl.files.AppendRow(fileName, pkgName, syntax, fileOpts); err != nil {
		return fmt.Errorf("failed to append file %s: %w", fileName, err)
	}

	// Dependencies
	imports := f.Imports()
	for i := 0; i < imports.Len(); i++ {
		imp := imports.Get(i)
		if err := bl.dependencies.AppendRow(fileName, imp.Path(), imp.IsPublic, imp.IsWeak); err != nil {
			return fmt.Errorf("failed to append dependency: %w", err)
		}
	}

	// Messages
	for i := 0; i < f.Messages().Len(); i++ {
		if err := loadMessage(bl, f.Messages().Get(i), fileName, nil); err != nil {
			return err
		}
	}

	// Enums
	for i := 0; i < f.Enums().Len(); i++ {
		if err := loadEnum(bl, f.Enums().Get(i), fileName, nil); err != nil {
			return err
		}
	}

	// Services
	for i := 0; i < f.Services().Len(); i++ {
		if err := loadService(bl, f.Services().Get(i), fileName); err != nil {
			return err
		}
	}

	// Extensions
	for i := 0; i < f.Extensions().Len(); i++ {
		if err := loadExtension(bl, f.Extensions().Get(i), fileName); err != nil {
			return err
		}
	}

	return nil
}

func loadMessage(bl *bulkLoader, msg protoreflect.MessageDescriptor, fileName string, parentMsg *string) error {
	fullName := string(msg.FullName())

	// For Appender, convert *string to any properly (nil or string value)
	var parent any
	if parentMsg != nil {
		parent = *parentMsg
	}

	msgOpts := bl.extractOptions(msg.Options())

	if err := bl.messages.AppendRow(fullName, string(msg.Name()), fileName, parent, msg.IsMapEntry(), msgOpts); err != nil {
		return fmt.Errorf("failed to append message %s: %w", fullName, err)
	}

	// Oneofs
	oneofIDs := make(map[int]string)
	for i := 0; i < msg.Oneofs().Len(); i++ {
		oneof := msg.Oneofs().Get(i)
		oneofID := fmt.Sprintf("%s.%s", fullName, oneof.Name())
		oneofIDs[i] = oneofID

		oneofOpts := bl.extractOptions(oneof.Options())

		if err := bl.oneofs.AppendRow(oneofID, string(oneof.Name()), fullName, oneofOpts); err != nil {
			return fmt.Errorf("failed to append oneof %s: %w", oneofID, err)
		}
	}

	// Fields
	for i := 0; i < msg.Fields().Len(); i++ {
		if err := loadField(bl, msg.Fields().Get(i), fullName, oneofIDs); err != nil {
			return err
		}
	}

	// Nested messages
	for i := 0; i < msg.Messages().Len(); i++ {
		if err := loadMessage(bl, msg.Messages().Get(i), fileName, &fullName); err != nil {
			return err
		}
	}

	// Nested enums
	for i := 0; i < msg.Enums().Len(); i++ {
		if err := loadEnum(bl, msg.Enums().Get(i), fileName, &fullName); err != nil {
			return err
		}
	}

	// Nested extensions
	for i := 0; i < msg.Extensions().Len(); i++ {
		if err := loadExtension(bl, msg.Extensions().Get(i), fileName); err != nil {
			return err
		}
	}

	return nil
}

func loadField(bl *bulkLoader, field protoreflect.FieldDescriptor, msgFullName string, oneofIDs map[int]string) error {
	fieldID := fmt.Sprintf("%s.%s", msgFullName, field.Name())

	var typeName any
	if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
		typeName = string(field.Message().FullName())
	} else if field.Kind() == protoreflect.EnumKind {
		typeName = string(field.Enum().FullName())
	}

	var label any
	switch field.Cardinality() {
	case protoreflect.Required:
		label = "required"
	case protoreflect.Repeated:
		label = "repeated"
	case protoreflect.Optional:
		label = "optional"
	}

	isMap := field.IsMap()
	var mapKeyType, mapValueType any
	if isMap {
		mapKeyType = field.MapKey().Kind().String()

		vt := field.MapValue().Kind().String()
		if field.MapValue().Kind() == protoreflect.MessageKind {
			vt = string(field.MapValue().Message().FullName())
		} else if field.MapValue().Kind() == protoreflect.EnumKind {
			vt = string(field.MapValue().Enum().FullName())
		}
		mapValueType = vt
	}

	var defaultVal any
	if field.HasDefault() {
		defaultVal = fmt.Sprintf("%v", field.Default().Interface())
	}

	fieldOpts := bl.extractOptions(field.Options())

	err := bl.fields.AppendRow(
		fieldID,
		string(field.Name()),
		int32(field.Number()),
		msgFullName,
		field.Kind().String(),
		typeName,
		label,
		field.IsList(),
		field.HasOptionalKeyword(),
		isMap,
		mapKeyType,
		mapValueType,
		defaultVal,
		field.JSONName(),
		fieldOpts,
	)
	if err != nil {
		return fmt.Errorf("failed to append field %s: %w", fieldID, err)
	}

	// Link to oneof
	if field.ContainingOneof() != nil && !field.ContainingOneof().IsSynthetic() {
		oneofIdx := field.ContainingOneof().Index()
		if oneofID, ok := oneofIDs[oneofIdx]; ok {
			if err := bl.oneofFields.AppendRow(oneofID, fieldID); err != nil {
				return fmt.Errorf("failed to link field to oneof: %w", err)
			}
		}
	}

	return nil
}

func loadEnum(bl *bulkLoader, enum protoreflect.EnumDescriptor, fileName string, parentMsg *string) error {
	fullName := string(enum.FullName())

	var parent any
	if parentMsg != nil {
		parent = *parentMsg
	}

	enumOpts := bl.extractOptions(enum.Options())

	if err := bl.enums.AppendRow(fullName, string(enum.Name()), fileName, parent, enumOpts); err != nil {
		return fmt.Errorf("failed to append enum %s: %w", fullName, err)
	}

	for i := 0; i < enum.Values().Len(); i++ {
		val := enum.Values().Get(i)
		valID := fmt.Sprintf("%s.%s", fullName, val.Name())

		valOpts := bl.extractOptions(val.Options())

		if err := bl.enumValues.AppendRow(valID, string(val.Name()), int32(val.Number()), fullName, valOpts); err != nil {
			return fmt.Errorf("failed to append enum value %s: %w", valID, err)
		}
	}

	return nil
}

func loadService(bl *bulkLoader, svc protoreflect.ServiceDescriptor, fileName string) error {
	fullName := string(svc.FullName())

	svcOpts := bl.extractOptions(svc.Options())

	if err := bl.services.AppendRow(fullName, string(svc.Name()), fileName, svcOpts); err != nil {
		return fmt.Errorf("failed to append service %s: %w", fullName, err)
	}

	for i := 0; i < svc.Methods().Len(); i++ {
		method := svc.Methods().Get(i)
		methodFullName := fmt.Sprintf("%s.%s", fullName, method.Name())

		methodOpts := bl.extractOptions(method.Options())

		err := bl.methods.AppendRow(
			methodFullName,
			string(method.Name()),
			fullName,
			string(method.Input().FullName()),
			string(method.Output().FullName()),
			method.IsStreamingClient(),
			method.IsStreamingServer(),
			methodOpts,
		)
		if err != nil {
			return fmt.Errorf("failed to append method %s: %w", methodFullName, err)
		}
	}

	return nil
}

func loadExtension(bl *bulkLoader, ext protoreflect.ExtensionDescriptor, fileName string) error {
	fullName := string(ext.FullName())

	var typeName any
	if ext.Kind() == protoreflect.MessageKind || ext.Kind() == protoreflect.GroupKind {
		typeName = string(ext.Message().FullName())
	} else if ext.Kind() == protoreflect.EnumKind {
		typeName = string(ext.Enum().FullName())
	}

	extOpts := bl.extractOptions(ext.Options())

	err := bl.extensions.AppendRow(
		fullName,
		string(ext.Name()),
		int32(ext.Number()),
		fileName,
		string(ext.ContainingMessage().FullName()),
		ext.Kind().String(),
		typeName,
		extOpts,
	)
	if err != nil {
		return fmt.Errorf("failed to append extension %s: %w", fullName, err)
	}

	return nil
}
