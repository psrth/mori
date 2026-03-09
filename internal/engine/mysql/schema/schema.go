package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strings"
	"time"

	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/mysql/connstr"
)

// DumpResult holds the complete result of a MySQL schema dump and analysis.
type DumpResult struct {
	SchemaSQL string
	Tables    map[string]TableMeta
}

// DumpSchema runs mysqldump/mariadb-dump --no-data against the Prod database.
// It runs the dump tool inside a Docker container to ensure tool availability.
func DumpSchema(ctx context.Context, dsn *connstr.DSN, image string) (string, error) {
	// MariaDB 11+ ships mariadb-dump instead of mysqldump.
	dumpCmd := "mysqldump"
	if strings.Contains(image, "mariadb") {
		dumpCmd = "mariadb-dump"
	}
	args := []string{
		"run", "--rm",
		"-e", dsn.MysqldumpEnv(),
		"--add-host", "host.docker.internal:host-gateway",
		image,
		dumpCmd,
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

// retryOnLockConflict retries a function on MySQL lock-related errors.
// Retries up to maxRetries times with exponential backoff starting at 200ms.
func retryOnLockConflict(fn func() error, maxRetries int) error {
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !isLockConflict(err) {
			return err
		}
		if attempt < maxRetries {
			time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
		}
	}
	return err
}

func isLockConflict(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "1205") || strings.Contains(s, "1213") ||
		strings.Contains(s, "Lock wait timeout") || strings.Contains(s, "Deadlock")
}

// DetectTableMetadata queries the MySQL INFORMATION_SCHEMA for all user tables
// and their PK info.
func DetectTableMetadata(ctx context.Context, db *sql.DB, dbName string) (map[string]TableMeta, error) {
	var tableNames []string
	err := retryOnLockConflict(func() error {
		tableNames = nil
		rows, err := db.QueryContext(ctx,
			"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
			dbName)
		if err != nil {
			return fmt.Errorf("failed to query tables: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return fmt.Errorf("failed to scan table name: %w", err)
			}
			tableNames = append(tableNames, name)
		}
		return rows.Err()
	}, 5)
	if err != nil {
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

// DetectGeneratedColumns queries INFORMATION_SCHEMA for columns with GENERATED expressions
// and populates the GeneratedCols field in the provided tables map.
func DetectGeneratedColumns(ctx context.Context, db *sql.DB, dbName string, tables map[string]TableMeta) error {
	return retryOnLockConflict(func() error {
		query := `SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = ? AND EXTRA LIKE '%GENERATED%'`
		rows, err := db.QueryContext(ctx, query, dbName)
		if err != nil {
			return fmt.Errorf("detect generated columns: %w", err)
		}
		defer rows.Close()

		// Clear any previously accumulated generated cols on retry.
		for tbl, meta := range tables {
			if len(meta.GeneratedCols) > 0 {
				meta.GeneratedCols = nil
				tables[tbl] = meta
			}
		}

		for rows.Next() {
			var tableName, colName string
			if err := rows.Scan(&tableName, &colName); err != nil {
				return fmt.Errorf("scan generated column: %w", err)
			}
			tableName = strings.ToLower(tableName)
			if meta, ok := tables[tableName]; ok {
				meta.GeneratedCols = append(meta.GeneratedCols, colName)
				tables[tableName] = meta
			}
		}
		return rows.Err()
	}, 5)
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
		var prodMax int64
		err := retryOnLockConflict(func() error {
			var maxVal sql.NullInt64
			query := fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`", pkCol, tableName)
			if err := db.QueryRowContext(ctx, query).Scan(&maxVal); err != nil {
				return fmt.Errorf("failed to get max PK for table %q: %w", tableName, err)
			}
			prodMax = 0
			if maxVal.Valid {
				prodMax = maxVal.Int64
			}
			return nil
		}, 5)
		if err != nil {
			return nil, err
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

// DetectForeignKeys queries INFORMATION_SCHEMA for foreign key constraints
// and records them in the schema registry.
func DetectForeignKeys(ctx context.Context, db *sql.DB, dbName string, registry *coreSchema.Registry) error {
	type fkData struct {
		constraintName string
		childTable     string
		childColumns   []string
		parentTable    string
		parentColumns  []string
		onDelete       string
		onUpdate       string
	}

	var fkMap map[string]*fkData
	var fkOrder []string

	err := retryOnLockConflict(func() error {
		query := `SELECT kcu.CONSTRAINT_NAME, kcu.TABLE_NAME, kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
			rc.DELETE_RULE, rc.UPDATE_RULE
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
		JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
		WHERE kcu.TABLE_SCHEMA = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`

		rows, err := db.QueryContext(ctx, query, dbName)
		if err != nil {
			return fmt.Errorf("detect foreign keys: %w", err)
		}
		defer rows.Close()

		fkMap = make(map[string]*fkData)
		fkOrder = nil

		for rows.Next() {
			var constraintName, childTable, childCol, parentTable, parentCol, deleteRule, updateRule string
			if err := rows.Scan(&constraintName, &childTable, &childCol, &parentTable, &parentCol, &deleteRule, &updateRule); err != nil {
				return fmt.Errorf("scan foreign key: %w", err)
			}
			key := childTable + "." + constraintName
			fd, ok := fkMap[key]
			if !ok {
				fd = &fkData{
					constraintName: constraintName,
					childTable:     childTable,
					parentTable:    parentTable,
					onDelete:       deleteRule,
					onUpdate:       updateRule,
				}
				fkMap[key] = fd
				fkOrder = append(fkOrder, key)
			}
			fd.childColumns = append(fd.childColumns, childCol)
			fd.parentColumns = append(fd.parentColumns, parentCol)
		}
		return rows.Err()
	}, 5)
	if err != nil {
		return err
	}

	for _, key := range fkOrder {
		fd := fkMap[key]
		fk := coreSchema.ForeignKey{
			ConstraintName: fd.constraintName,
			ChildTable:     fd.childTable,
			ChildColumns:   fd.childColumns,
			ParentTable:    fd.parentTable,
			ParentColumns:  fd.parentColumns,
			OnDelete:       fd.onDelete,
			OnUpdate:       fd.onUpdate,
		}
		registry.RecordForeignKey(fd.childTable, fk)
	}

	return nil
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
