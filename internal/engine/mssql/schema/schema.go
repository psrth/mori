package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DumpResult holds the complete result of an MSSQL schema dump and analysis.
type DumpResult struct {
	SchemaSQL string
	Tables    map[string]TableMeta
}

// DumpSchema queries the Prod MSSQL database to extract schema DDL.
// Unlike MySQL/PostgreSQL, MSSQL has no built-in dump tool, so we reconstruct
// CREATE TABLE statements from INFORMATION_SCHEMA.
func DumpSchema(ctx context.Context, db *sql.DB, dbName string) (string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
		 WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
		 ORDER BY TABLE_NAME`)
	if err != nil {
		return "", fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	var ddl strings.Builder
	for _, tableName := range tableNames {
		createStmt, err := generateCreateTable(ctx, db, tableName)
		if err != nil {
			return "", fmt.Errorf("failed to generate DDL for table %q: %w", tableName, err)
		}
		ddl.WriteString(createStmt)
		ddl.WriteString("\n\n")
	}

	return ddl.String(), nil
}

// generateCreateTable constructs a CREATE TABLE statement from INFORMATION_SCHEMA.
func generateCreateTable(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	// Query column definitions.
	colRows, err := db.QueryContext(ctx, `
		SELECT
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE,
			c.IS_NULLABLE,
			c.COLUMN_DEFAULT,
			COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity
		FROM INFORMATION_SCHEMA.COLUMNS c
		WHERE c.TABLE_NAME = @p1 AND c.TABLE_SCHEMA = 'dbo'
		ORDER BY c.ORDINAL_POSITION`, tableName)
	if err != nil {
		return "", fmt.Errorf("failed to query columns: %w", err)
	}
	defer colRows.Close()

	var columns []string
	for colRows.Next() {
		var colName, dataType, isNullable string
		var charMaxLen, numPrecision, numScale sql.NullInt64
		var colDefault sql.NullString
		var isIdentity int

		if err := colRows.Scan(&colName, &dataType, &charMaxLen, &numPrecision, &numScale, &isNullable, &colDefault, &isIdentity); err != nil {
			return "", fmt.Errorf("failed to scan column: %w", err)
		}

		col := fmt.Sprintf("    [%s] %s", colName, formatDataType(dataType, charMaxLen, numPrecision, numScale))

		if isIdentity == 1 {
			col += " IDENTITY(1,1)"
		}

		if isNullable == "NO" {
			col += " NOT NULL"
		}

		if colDefault.Valid && colDefault.String != "" {
			col += " DEFAULT " + colDefault.String
		}

		columns = append(columns, col)
	}
	if err := colRows.Err(); err != nil {
		return "", err
	}

	// Query primary key constraint.
	pkRows, err := db.QueryContext(ctx, `
		SELECT kcu.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
			AND kcu.TABLE_NAME = tc.TABLE_NAME
		WHERE tc.TABLE_NAME = @p1
			AND tc.TABLE_SCHEMA = 'dbo'
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kcu.ORDINAL_POSITION`, tableName)
	if err != nil {
		return "", fmt.Errorf("failed to query PK: %w", err)
	}
	defer pkRows.Close()

	var pkCols []string
	for pkRows.Next() {
		var col string
		if err := pkRows.Scan(&col); err != nil {
			return "", err
		}
		pkCols = append(pkCols, fmt.Sprintf("[%s]", col))
	}
	if err := pkRows.Err(); err != nil {
		return "", err
	}

	if len(pkCols) > 0 {
		columns = append(columns, fmt.Sprintf("    CONSTRAINT [PK_%s] PRIMARY KEY (%s)", tableName, strings.Join(pkCols, ", ")))
	}

	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE [%s] (\n", tableName))
	ddl.WriteString(strings.Join(columns, ",\n"))
	ddl.WriteString("\n);")

	return ddl.String(), nil
}

// formatDataType formats an MSSQL data type with its size/precision.
func formatDataType(dataType string, charMaxLen, numPrecision, numScale sql.NullInt64) string {
	upper := strings.ToUpper(dataType)
	switch upper {
	case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY":
		if charMaxLen.Valid {
			if charMaxLen.Int64 == -1 {
				return upper + "(MAX)"
			}
			return fmt.Sprintf("%s(%d)", upper, charMaxLen.Int64)
		}
		return upper
	case "DECIMAL", "NUMERIC":
		if numPrecision.Valid && numScale.Valid {
			return fmt.Sprintf("%s(%d,%d)", upper, numPrecision.Int64, numScale.Int64)
		}
		return upper
	case "FLOAT":
		if numPrecision.Valid {
			return fmt.Sprintf("%s(%d)", upper, numPrecision.Int64)
		}
		return upper
	default:
		return upper
	}
}

// StripForeignKeys removes FK constraint definitions from schema DDL.
func StripForeignKeys(schemaSQL string) string {
	lines := strings.Split(schemaSQL, "\n")
	var result []string
	for _, line := range lines {
		upper := strings.ToUpper(strings.TrimSpace(line))
		if strings.Contains(upper, "FOREIGN KEY") && strings.Contains(upper, "REFERENCES") {
			continue
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

// DetectTableMetadata queries the MSSQL INFORMATION_SCHEMA for all user tables
// and their PK info.
func DetectTableMetadata(ctx context.Context, db *sql.DB) (map[string]TableMeta, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
		 WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	tables := make(map[string]TableMeta)
	for _, tableName := range tableNames {
		meta, err := detectTablePK(ctx, db, tableName)
		if err != nil {
			return nil, err
		}
		tables[tableName] = meta
	}
	return tables, nil
}

func detectTablePK(ctx context.Context, db *sql.DB, tableName string) (TableMeta, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			kcu.COLUMN_NAME,
			c.DATA_TYPE,
			COLUMNPROPERTY(OBJECT_ID('dbo.' + kcu.TABLE_NAME), kcu.COLUMN_NAME, 'IsIdentity') AS is_identity
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
			AND kcu.TABLE_NAME = tc.TABLE_NAME
		JOIN INFORMATION_SCHEMA.COLUMNS c
			ON c.TABLE_NAME = kcu.TABLE_NAME
			AND c.COLUMN_NAME = kcu.COLUMN_NAME
			AND c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		WHERE tc.TABLE_NAME = @p1
			AND tc.TABLE_SCHEMA = 'dbo'
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kcu.ORDINAL_POSITION`, tableName)
	if err != nil {
		return TableMeta{}, fmt.Errorf("failed to query PK for table %q: %w", tableName, err)
	}
	defer rows.Close()

	var pkColumns []string
	var pkTypes []string
	var identities []bool
	for rows.Next() {
		var col, dataType string
		var isIdentity int
		if err := rows.Scan(&col, &dataType, &isIdentity); err != nil {
			return TableMeta{}, fmt.Errorf("failed to scan PK column for table %q: %w", tableName, err)
		}
		pkColumns = append(pkColumns, col)
		pkTypes = append(pkTypes, dataType)
		identities = append(identities, isIdentity == 1)
	}
	if err := rows.Err(); err != nil {
		return TableMeta{}, err
	}

	if len(pkColumns) == 0 {
		return TableMeta{PKType: "none"}, nil
	}

	pkType := classifyPKType(pkTypes, identities)
	if len(pkColumns) > 1 {
		pkType = "composite"
	}

	return TableMeta{
		PKColumns: pkColumns,
		PKType:    pkType,
	}, nil
}

func classifyPKType(dataTypes []string, identities []bool) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToLower(dataTypes[0])
	isIdentity := len(identities) > 0 && identities[0]

	// Check for IDENTITY columns (equivalent to auto_increment).
	if isIdentity {
		if dt == "bigint" {
			return "bigserial"
		}
		return "serial"
	}

	// Check for UUID-like types.
	if dt == "uniqueidentifier" || dt == "char" || dt == "varchar" || dt == "nchar" || dt == "nvarchar" {
		return "uuid"
	}

	// Integer types without IDENTITY.
	if dt == "int" || dt == "integer" || dt == "bigint" || dt == "smallint" || dt == "tinyint" {
		return "serial"
	}

	return dt
}

// ApplySchema connects to Shadow MSSQL and executes the schema SQL.
func ApplySchema(ctx context.Context, db *sql.DB, schemaSQL string) error {
	stmts := splitStatements(schemaSQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		// Skip comments.
		if strings.HasPrefix(stmt, "--") || strings.HasPrefix(stmt, "/*") {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// Log but don't fail on individual statement errors.
			continue
		}
	}
	return nil
}

// splitStatements splits a SQL dump into individual statements.
// Handles both semicolons and GO batch separators.
func splitStatements(sqlText string) []string {
	var stmts []string
	var current strings.Builder
	inString := false
	quote := byte(0)

	lines := strings.Split(sqlText, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Handle GO batch separator.
		if !inString && strings.EqualFold(trimmed, "GO") {
			if s := strings.TrimSpace(current.String()); s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
			continue
		}

		for i := 0; i < len(line); i++ {
			c := line[i]
			if inString {
				current.WriteByte(c)
				if c == quote {
					inString = false
				}
				continue
			}
			if c == '\'' {
				inString = true
				quote = c
				current.WriteByte(c)
				continue
			}
			if c == ';' {
				if s := strings.TrimSpace(current.String()); s != "" {
					stmts = append(stmts, s)
				}
				current.Reset()
				continue
			}
			current.WriteByte(c)
		}
		current.WriteByte('\n')
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// DetectIdentityOffsets queries Prod for the current max IDENTITY values
// and computes Shadow offsets.
func DetectIdentityOffsets(ctx context.Context, db *sql.DB, tables map[string]TableMeta) (map[string]int64, error) {
	offsets := make(map[string]int64)

	for tableName, meta := range tables {
		if meta.PKType != "serial" && meta.PKType != "bigserial" {
			continue
		}
		if len(meta.PKColumns) != 1 {
			continue
		}

		pkCol := meta.PKColumns[0]
		var maxVal sql.NullInt64
		query := fmt.Sprintf("SELECT MAX([%s]) FROM [%s]", pkCol, tableName)
		if err := db.QueryRowContext(ctx, query).Scan(&maxVal); err != nil {
			return nil, fmt.Errorf("failed to get max PK for table %q: %w", tableName, err)
		}

		prodMax := int64(0)
		if maxVal.Valid {
			prodMax = maxVal.Int64
		}

		offsets[tableName] = computeOffset(prodMax)
	}
	return offsets, nil
}

func computeOffset(prodMax int64) int64 {
	a := prodMax * 10
	b := prodMax + 10_000_000
	if a > b {
		return a
	}
	return b
}

// ApplyIdentityOffsets reseeds IDENTITY columns on Shadow tables.
func ApplyIdentityOffsets(ctx context.Context, db *sql.DB, offsets map[string]int64) error {
	for tableName, offset := range offsets {
		stmt := fmt.Sprintf("DBCC CHECKIDENT ('[%s]', RESEED, %d)", tableName, offset)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// Non-critical: skip on error.
			continue
		}
	}
	return nil
}
