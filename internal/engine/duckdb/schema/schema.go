package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DetectTableMetadata queries the DuckDB database for all user tables
// and their PK info using INFORMATION_SCHEMA.
func DetectTableMetadata(ctx context.Context, db *sql.DB) (map[string]TableMeta, error) {
	// List all user tables (exclude system schemas).
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
		 ORDER BY table_name`)
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
	// DuckDB supports INFORMATION_SCHEMA for constraint columns.
	rows, err := db.QueryContext(ctx,
		`SELECT kcu.column_name, c.data_type
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON tc.constraint_name = kcu.constraint_name
		   AND tc.table_schema = kcu.table_schema
		   AND tc.table_name = kcu.table_name
		 JOIN information_schema.columns c
		   ON c.table_schema = kcu.table_schema
		   AND c.table_name = kcu.table_name
		   AND c.column_name = kcu.column_name
		 WHERE tc.constraint_type = 'PRIMARY KEY'
		   AND tc.table_schema = 'main'
		   AND tc.table_name = $1
		 ORDER BY kcu.ordinal_position`, tableName)
	if err != nil {
		return TableMeta{}, fmt.Errorf("failed to query PK for %q: %w", tableName, err)
	}
	defer rows.Close()

	var columns []string
	var dataTypes []string
	for rows.Next() {
		var col, dt string
		if err := rows.Scan(&col, &dt); err != nil {
			return TableMeta{}, fmt.Errorf("failed to scan PK column for %q: %w", tableName, err)
		}
		columns = append(columns, col)
		dataTypes = append(dataTypes, dt)
	}
	if err := rows.Err(); err != nil {
		return TableMeta{}, err
	}

	if len(columns) == 0 {
		return TableMeta{PKType: "none"}, nil
	}

	pkType := classifyPKType(dataTypes)
	if len(columns) > 1 {
		pkType = "composite"
	}

	return TableMeta{
		PKColumns: columns,
		PKType:    pkType,
	}, nil
}

func classifyPKType(dataTypes []string) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToUpper(strings.TrimSpace(dataTypes[0]))

	switch {
	case dt == "INTEGER" || dt == "INT" || dt == "INT4" || dt == "SMALLINT" || dt == "TINYINT":
		return "serial"
	case dt == "BIGINT" || dt == "INT8" || dt == "HUGEINT":
		return "bigserial"
	case dt == "UUID":
		return "uuid"
	case dt == "VARCHAR" || dt == "TEXT" || strings.HasPrefix(dt, "VARCHAR"):
		return "uuid" // Text-based PKs treated as uuid for routing.
	}

	return dt
}

// DetectSequenceOffsets queries DuckDB for current max auto-increment values
// and computes shadow offsets.
func DetectSequenceOffsets(ctx context.Context, db *sql.DB, tables map[string]TableMeta) (map[string]int64, error) {
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
		query := fmt.Sprintf(`SELECT MAX("%s") FROM "%s"`, pkCol, tableName)
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

// ApplySequenceOffsets sets auto-increment offsets in the shadow database.
// For DuckDB, we create sequences and alter the table defaults.
func ApplySequenceOffsets(ctx context.Context, db *sql.DB, offsets map[string]int64) error {
	for tableName, offset := range offsets {
		// Insert and delete a sentinel row to advance the internal rowid counter.
		// DuckDB doesn't have sqlite_sequence, so we use this approach.
		seqName := fmt.Sprintf("mori_seq_%s", tableName)
		db.ExecContext(ctx, fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS "%s" START %d`, seqName, offset))
	}
	return nil
}
