package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strings"

	"github.com/mori-dev/mori/internal/engine/mysql/connstr"
)

// DumpResult holds the complete result of a MySQL schema dump and analysis.
type DumpResult struct {
	SchemaSQL string
	Tables    map[string]TableMeta
}

// DumpSchema runs mysqldump --no-data against the Prod database.
// It runs mysqldump inside a Docker container to ensure tool availability.
func DumpSchema(ctx context.Context, dsn *connstr.DSN, image string) (string, error) {
	args := []string{
		"run", "--rm",
		"-e", dsn.MysqldumpEnv(),
		"--add-host", "host.docker.internal:host-gateway",
		image,
		"mysqldump",
	}
	args = append(args, dsn.MysqldumpDockerArgs()...)

	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("mysqldump failed: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("mysqldump failed: %w", err)
	}
	return string(out), nil
}

// StripForeignKeys removes FK constraint definitions from a mysqldump output.
// MySQL dump includes FK constraints inline with CREATE TABLE, but we also
// handle ALTER TABLE ADD CONSTRAINT forms.
func StripForeignKeys(schemaSQL string) string {
	// Remove ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY lines
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

// DetectTableMetadata queries the MySQL INFORMATION_SCHEMA for all user tables
// and their PK info.
func DetectTableMetadata(ctx context.Context, db *sql.DB, dbName string) (map[string]TableMeta, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
		dbName)
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
		meta, err := detectTablePK(ctx, db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		tables[tableName] = meta
	}
	return tables, nil
}

func detectTablePK(ctx context.Context, db *sql.DB, dbName, tableName string) (TableMeta, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT kcu.COLUMN_NAME, c.DATA_TYPE, c.EXTRA
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
			AND kcu.TABLE_NAME = tc.TABLE_NAME
		JOIN INFORMATION_SCHEMA.COLUMNS c
			ON c.TABLE_NAME = kcu.TABLE_NAME
			AND c.COLUMN_NAME = kcu.COLUMN_NAME
			AND c.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		WHERE tc.TABLE_NAME = ?
			AND tc.TABLE_SCHEMA = ?
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY kcu.ORDINAL_POSITION`, tableName, dbName)
	if err != nil {
		return TableMeta{}, fmt.Errorf("failed to query PK for table %q: %w", tableName, err)
	}
	defer rows.Close()

	var pkColumns []string
	var pkTypes []string
	var extras []string
	for rows.Next() {
		var col, dataType, extra string
		if err := rows.Scan(&col, &dataType, &extra); err != nil {
			return TableMeta{}, fmt.Errorf("failed to scan PK column for table %q: %w", tableName, err)
		}
		pkColumns = append(pkColumns, col)
		pkTypes = append(pkTypes, dataType)
		extras = append(extras, extra)
	}
	if err := rows.Err(); err != nil {
		return TableMeta{}, err
	}

	if len(pkColumns) == 0 {
		return TableMeta{PKType: "none"}, nil
	}

	pkType := classifyPKType(pkTypes, extras)
	if len(pkColumns) > 1 {
		pkType = "composite"
	}

	return TableMeta{
		PKColumns: pkColumns,
		PKType:    pkType,
	}, nil
}

func classifyPKType(dataTypes []string, extras []string) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToLower(dataTypes[0])
	extra := ""
	if len(extras) > 0 {
		extra = strings.ToLower(extras[0])
	}

	// Check for auto_increment
	if strings.Contains(extra, "auto_increment") {
		if dt == "bigint" {
			return "bigserial"
		}
		return "serial"
	}

	// Check for UUID-like types
	if dt == "char" || dt == "varchar" || dt == "binary" {
		return "uuid"
	}

	// Integer types without auto_increment
	if dt == "int" || dt == "integer" || dt == "bigint" || dt == "smallint" || dt == "tinyint" || dt == "mediumint" {
		return "serial"
	}

	return dt
}

// ApplySchema connects to Shadow MySQL and executes the schema SQL.
func ApplySchema(ctx context.Context, db *sql.DB, schemaSQL string) error {
	// Split by semicolons and execute each statement.
	stmts := splitStatements(schemaSQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		// Skip comments and MySQL directives
		if strings.HasPrefix(stmt, "--") || strings.HasPrefix(stmt, "/*") {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// Log but don't fail on individual statement errors (some statements
			// might be MySQL-specific directives that don't apply to Shadow).
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
				inString = false
			}
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
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

// DetectAutoIncrementOffsets queries Prod for the current max auto_increment
// values and computes Shadow offsets.
func DetectAutoIncrementOffsets(ctx context.Context, db *sql.DB, tables map[string]TableMeta) (map[string]int64, error) {
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
		query := fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`", pkCol, tableName)
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

// ApplyAutoIncrementOffsets sets the AUTO_INCREMENT values on Shadow tables.
func ApplyAutoIncrementOffsets(ctx context.Context, db *sql.DB, offsets map[string]int64) error {
	for tableName, offset := range offsets {
		stmt := fmt.Sprintf("ALTER TABLE `%s` AUTO_INCREMENT = %d", tableName, offset)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// Non-critical: skip on error.
			continue
		}
	}
	return nil
}
