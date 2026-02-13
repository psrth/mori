package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DumpResult holds the complete result of a SQLite schema analysis.
type DumpResult struct {
	SchemaSQL string
	Tables    map[string]TableMeta
}

// DumpSchema reads all CREATE statements from sqlite_master.
func DumpSchema(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type DESC, name")
	if err != nil {
		return "", fmt.Errorf("failed to query sqlite_master: %w", err)
	}
	defer rows.Close()

	var stmts []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return "", fmt.Errorf("failed to scan schema: %w", err)
		}
		stmts = append(stmts, s+";")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	return strings.Join(stmts, "\n"), nil
}

// DetectTableMetadata queries the SQLite database for all user tables
// and their PK info using PRAGMA table_info.
func DetectTableMetadata(ctx context.Context, db *sql.DB) (map[string]TableMeta, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
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
	// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
	// pk > 0 means the column is part of the PRIMARY KEY (pk is the 1-based index in the key).
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%q)", tableName))
	if err != nil {
		return TableMeta{}, fmt.Errorf("failed to query table_info for %q: %w", tableName, err)
	}
	defer rows.Close()

	type colInfo struct {
		name     string
		dataType string
		pkIndex  int
	}
	var pkCols []colInfo

	for rows.Next() {
		var cid int
		var name, dataType string
		var notnull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notnull, &dfltValue, &pk); err != nil {
			return TableMeta{}, fmt.Errorf("failed to scan column for %q: %w", tableName, err)
		}
		if pk > 0 {
			pkCols = append(pkCols, colInfo{name: name, dataType: dataType, pkIndex: pk})
		}
	}
	if err := rows.Err(); err != nil {
		return TableMeta{}, err
	}

	if len(pkCols) == 0 {
		return TableMeta{PKType: "none"}, nil
	}

	// Sort by pk index (they should already be in order from PRAGMA, but be safe).
	for i := 0; i < len(pkCols); i++ {
		for j := i + 1; j < len(pkCols); j++ {
			if pkCols[j].pkIndex < pkCols[i].pkIndex {
				pkCols[i], pkCols[j] = pkCols[j], pkCols[i]
			}
		}
	}

	var columns []string
	var dataTypes []string
	for _, col := range pkCols {
		columns = append(columns, col.name)
		dataTypes = append(dataTypes, col.dataType)
	}

	pkType := classifyPKType(dataTypes, tableName)
	if len(columns) > 1 {
		pkType = "composite"
	}

	return TableMeta{
		PKColumns: columns,
		PKType:    pkType,
	}, nil
}

func classifyPKType(dataTypes []string, tableName string) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToUpper(strings.TrimSpace(dataTypes[0]))

	// In SQLite, INTEGER PRIMARY KEY is the ROWID alias (auto-incrementing).
	if dt == "INTEGER" {
		return "serial"
	}

	// Text-based PKs are typically UUIDs.
	if dt == "TEXT" || dt == "VARCHAR" || strings.HasPrefix(dt, "CHAR") {
		return "uuid"
	}

	// BIGINT or INT (not exact "INTEGER") are not ROWID aliases in SQLite.
	if dt == "BIGINT" || dt == "BIGINTEGER" {
		return "bigserial"
	}
	if dt == "INT" || dt == "SMALLINT" || dt == "TINYINT" || dt == "MEDIUMINT" {
		return "serial"
	}

	return dt
}

// DetectAutoIncrementOffsets queries the prod database for the current max
// auto-increment values and computes shadow offsets.
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
		query := fmt.Sprintf("SELECT MAX(%q) FROM %q", pkCol, tableName)
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

// ApplyAutoIncrementOffsets sets the auto-increment sequence in the shadow database.
// For SQLite, we update the sqlite_sequence table if it exists (AUTOINCREMENT tables),
// or insert+delete a sentinel row to advance the ROWID.
func ApplyAutoIncrementOffsets(ctx context.Context, db *sql.DB, offsets map[string]int64) error {
	for tableName, offset := range offsets {
		// Try updating sqlite_sequence first (for AUTOINCREMENT tables).
		result, err := db.ExecContext(ctx,
			"UPDATE sqlite_sequence SET seq = ? WHERE name = ?", offset, tableName)
		if err == nil {
			rows, _ := result.RowsAffected()
			if rows > 0 {
				continue
			}
		}

		// Fallback: insert a sentinel row and delete it to advance ROWID.
		// This only works for tables with a single INTEGER PRIMARY KEY.
		_, err = db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %q (rowid) VALUES (?)", tableName), offset)
		if err != nil {
			// Non-critical: skip on error.
			continue
		}
		db.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %q WHERE rowid = ?", tableName), offset)
	}
	return nil
}

// ApplySchema executes schema SQL statements on the shadow database.
func ApplySchema(ctx context.Context, db *sql.DB, schemaSQL string) error {
	stmts := splitStatements(schemaSQL)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// Log but don't fail on individual statement errors.
			continue
		}
	}
	return nil
}

func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inString := false
	quote := byte(0)

	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if inString {
			current.WriteByte(c)
			if c == quote {
				// Check for escaped quote ('' in SQLite).
				if i+1 < len(sql) && sql[i+1] == quote {
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
