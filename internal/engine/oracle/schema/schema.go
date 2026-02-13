package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DumpResult holds the complete result of an Oracle schema dump and analysis.
type DumpResult struct {
	SchemaSQL string
	Tables    map[string]TableMeta
}

// DumpSchema generates DDL statements for all user tables by querying
// Oracle data dictionary views. This avoids requiring expdp/impdp tools.
func DumpSchema(ctx context.Context, db *sql.DB, schemaName string) (string, error) {
	upperSchema := strings.ToUpper(schemaName)

	// Get all base tables owned by the schema.
	rows, err := db.QueryContext(ctx,
		`SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1 ORDER BY TABLE_NAME`,
		upperSchema)
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

	var ddlParts []string

	// Generate CREATE TABLE statements.
	for _, tableName := range tableNames {
		ddl, err := generateCreateTableDDL(ctx, db, upperSchema, tableName)
		if err != nil {
			return "", fmt.Errorf("failed to generate DDL for %s: %w", tableName, err)
		}
		ddlParts = append(ddlParts, ddl)
	}

	// Generate sequences.
	seqDDL, err := generateSequenceDDL(ctx, db, upperSchema)
	if err != nil {
		return "", fmt.Errorf("failed to generate sequence DDL: %w", err)
	}
	if seqDDL != "" {
		ddlParts = append(ddlParts, seqDDL)
	}

	return strings.Join(ddlParts, "\n\n"), nil
}

func generateCreateTableDDL(ctx context.Context, db *sql.DB, owner, tableName string) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
		FROM ALL_TAB_COLUMNS
		WHERE OWNER = :1 AND TABLE_NAME = :2
		ORDER BY COLUMN_ID`, owner, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName, dataType, nullable string
		var dataLength int
		var dataPrecision, dataScale sql.NullInt64
		var dataDefault sql.NullString

		if err := rows.Scan(&colName, &dataType, &dataLength, &dataPrecision, &dataScale, &nullable, &dataDefault); err != nil {
			return "", err
		}

		colDef := fmt.Sprintf("  %s %s", colName, formatDataType(dataType, dataLength, dataPrecision, dataScale))
		if dataDefault.Valid && strings.TrimSpace(dataDefault.String) != "" {
			colDef += " DEFAULT " + strings.TrimSpace(dataDefault.String)
		}
		if nullable == "N" {
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	// Add primary key constraint.
	pkCols, err := getPrimaryKeyColumns(ctx, db, owner, tableName)
	if err != nil {
		return "", err
	}
	if len(pkCols) > 0 {
		columns = append(columns, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);", tableName, strings.Join(columns, ",\n")), nil
}

func formatDataType(dataType string, dataLength int, dataPrecision, dataScale sql.NullInt64) string {
	switch strings.ToUpper(dataType) {
	case "NUMBER":
		if dataPrecision.Valid && dataScale.Valid {
			if dataScale.Int64 == 0 {
				return fmt.Sprintf("NUMBER(%d)", dataPrecision.Int64)
			}
			return fmt.Sprintf("NUMBER(%d,%d)", dataPrecision.Int64, dataScale.Int64)
		}
		if dataPrecision.Valid {
			return fmt.Sprintf("NUMBER(%d)", dataPrecision.Int64)
		}
		return "NUMBER"
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW":
		return fmt.Sprintf("%s(%d)", dataType, dataLength)
	case "FLOAT":
		if dataPrecision.Valid {
			return fmt.Sprintf("FLOAT(%d)", dataPrecision.Int64)
		}
		return "FLOAT"
	default:
		return dataType
	}
}

func getPrimaryKeyColumns(ctx context.Context, db *sql.DB, owner, tableName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT cc.COLUMN_NAME
		FROM ALL_CONSTRAINTS c
		JOIN ALL_CONS_COLUMNS cc ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND cc.OWNER = c.OWNER
		WHERE c.OWNER = :1 AND c.TABLE_NAME = :2 AND c.CONSTRAINT_TYPE = 'P'
		ORDER BY cc.POSITION`, owner, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func generateSequenceDDL(ctx context.Context, db *sql.DB, owner string) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER
		FROM ALL_SEQUENCES
		WHERE SEQUENCE_OWNER = :1
		ORDER BY SEQUENCE_NAME`, owner)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var ddls []string
	for rows.Next() {
		var seqName string
		var minVal, maxVal, incBy, lastNum int64
		if err := rows.Scan(&seqName, &minVal, &maxVal, &incBy, &lastNum); err != nil {
			return "", err
		}
		ddl := fmt.Sprintf("CREATE SEQUENCE %s START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d;",
			seqName, lastNum, incBy, minVal, maxVal)
		ddls = append(ddls, ddl)
	}
	return strings.Join(ddls, "\n"), rows.Err()
}

// StripForeignKeys removes FK constraint definitions from DDL output.
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

// DetectTableMetadata queries Oracle data dictionary for all user tables and their PK info.
func DetectTableMetadata(ctx context.Context, db *sql.DB, schemaName string) (map[string]TableMeta, error) {
	upperSchema := strings.ToUpper(schemaName)

	rows, err := db.QueryContext(ctx,
		`SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME NOT LIKE 'BIN$%' ORDER BY TABLE_NAME`,
		upperSchema)
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
		meta, err := detectTablePK(ctx, db, upperSchema, tableName)
		if err != nil {
			return nil, err
		}
		tables[tableName] = meta
	}
	return tables, nil
}

func detectTablePK(ctx context.Context, db *sql.DB, owner, tableName string) (TableMeta, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT cc.COLUMN_NAME, tc.DATA_TYPE
		FROM ALL_CONSTRAINTS c
		JOIN ALL_CONS_COLUMNS cc ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND cc.OWNER = c.OWNER
		JOIN ALL_TAB_COLUMNS tc ON tc.OWNER = c.OWNER AND tc.TABLE_NAME = c.TABLE_NAME AND tc.COLUMN_NAME = cc.COLUMN_NAME
		WHERE c.OWNER = :1 AND c.TABLE_NAME = :2 AND c.CONSTRAINT_TYPE = 'P'
		ORDER BY cc.POSITION`, owner, tableName)
	if err != nil {
		return TableMeta{}, fmt.Errorf("failed to query PK for table %q: %w", tableName, err)
	}
	defer rows.Close()

	var pkColumns []string
	var pkTypes []string
	for rows.Next() {
		var col, dataType string
		if err := rows.Scan(&col, &dataType); err != nil {
			return TableMeta{}, fmt.Errorf("failed to scan PK column for table %q: %w", tableName, err)
		}
		pkColumns = append(pkColumns, col)
		pkTypes = append(pkTypes, dataType)
	}
	if err := rows.Err(); err != nil {
		return TableMeta{}, err
	}

	if len(pkColumns) == 0 {
		return TableMeta{PKType: "none"}, nil
	}

	pkType := classifyPKType(pkTypes)
	if len(pkColumns) > 1 {
		pkType = "composite"
	}

	return TableMeta{
		PKColumns: pkColumns,
		PKType:    pkType,
	}, nil
}

func classifyPKType(dataTypes []string) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToUpper(dataTypes[0])

	switch dt {
	case "NUMBER":
		// Could be serial (identity/sequence-backed) or manual.
		// Default to "serial" since most Oracle NUMBER PKs are sequence-backed.
		return "serial"
	case "RAW":
		return "uuid"
	case "VARCHAR2", "NVARCHAR2", "CHAR":
		return "uuid"
	default:
		return dt
	}
}

// ApplySchema connects to Shadow Oracle and executes the DDL statements.
func ApplySchema(ctx context.Context, db *sql.DB, schemaSQL string) error {
	stmts := splitStatements(schemaSQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
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
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inString := false
	escape := false
	quote := byte(0)

	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if escape {
			current.WriteByte(c)
			escape = false
			continue
		}
		if c == '\\' && inString {
			current.WriteByte(c)
			escape = true
			continue
		}
		if inString {
			current.WriteByte(c)
			if c == quote {
				// Oracle uses '' for escaping single quotes.
				if c == '\'' && i+1 < len(sql) && sql[i+1] == '\'' {
					current.WriteByte(sql[i+1])
					i++
					continue
				}
				inString = false
			}
			continue
		}
		if c == '\'' || c == '"' {
			inString = true
			quote = c
			current.WriteByte(c)
			continue
		}
		if c == ';' {
			stmts = append(stmts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// DetectSequenceOffsets queries Prod for current sequence values
// and computes Shadow offsets.
func DetectSequenceOffsets(ctx context.Context, db *sql.DB, schemaName string, tables map[string]TableMeta) (map[string]int64, error) {
	offsets := make(map[string]int64)
	upperSchema := strings.ToUpper(schemaName)

	for tableName, meta := range tables {
		if meta.PKType != "serial" && meta.PKType != "bigserial" {
			continue
		}
		if len(meta.PKColumns) != 1 {
			continue
		}

		pkCol := meta.PKColumns[0]
		var maxVal sql.NullInt64
		query := fmt.Sprintf(`SELECT MAX("%s") FROM "%s"."%s"`, pkCol, upperSchema, tableName)
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

// ApplySequenceOffsets adjusts sequences on the Shadow database to start
// above the production max values.
func ApplySequenceOffsets(ctx context.Context, db *sql.DB, schemaName string, offsets map[string]int64) error {
	upperSchema := strings.ToUpper(schemaName)

	for tableName, offset := range offsets {
		// Try to find a sequence associated with this table.
		// Common Oracle naming convention: <TABLE>_SEQ or <TABLE>_ID_SEQ.
		seqNames := []string{
			tableName + "_SEQ",
			tableName + "_ID_SEQ",
		}

		for _, seqName := range seqNames {
			// Check if the sequence exists.
			var count int
			err := db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = :1 AND SEQUENCE_NAME = :2`,
				upperSchema, seqName).Scan(&count)
			if err != nil || count == 0 {
				continue
			}

			// Drop and recreate with new start value.
			stmt := fmt.Sprintf(`ALTER SEQUENCE "%s"."%s" RESTART START WITH %d`, upperSchema, seqName, offset)
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				// Non-critical: skip on error.
				continue
			}
			break
		}
	}
	return nil
}
