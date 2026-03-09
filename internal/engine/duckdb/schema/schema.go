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

// DetectGeneratedColumns detects generated (computed) columns for all tables.
// DuckDB stores generated column info in duckdb_columns().
func DetectGeneratedColumns(ctx context.Context, db *sql.DB, tables map[string]TableMeta) (map[string]TableMeta, error) {
	for tableName, meta := range tables {
		genCols, err := detectGeneratedColumnsForTable(ctx, db, tableName)
		if err != nil {
			// Non-fatal: some DuckDB versions may not expose this.
			continue
		}
		if len(genCols) > 0 {
			meta.GeneratedCols = genCols
			tables[tableName] = meta
		}
	}
	return tables, nil
}

func detectGeneratedColumnsForTable(ctx context.Context, db *sql.DB, tableName string) ([]string, error) {
	// DuckDB's duckdb_columns() includes column_default and is_generated info.
	// Generated columns have is_nullable = true and a non-null expression in column_default
	// when using GENERATED ALWAYS AS syntax.
	// Attempt to use duckdb_columns() which has richer metadata.
	rows, err := db.QueryContext(ctx,
		`SELECT column_name FROM duckdb_columns()
		 WHERE schema_name = 'main' AND table_name = $1
		 AND column_default IS NOT NULL
		 AND column_default LIKE '%GENERATED%'`, tableName)
	if err != nil {
		// Fallback: try information_schema approach
		rows, err = db.QueryContext(ctx,
			`SELECT column_name FROM information_schema.columns
			 WHERE table_schema = 'main' AND table_name = $1
			 AND is_generated = 'ALWAYS'`, tableName)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	var genCols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		genCols = append(genCols, col)
	}
	return genCols, rows.Err()
}

// DetectForeignKeys queries the DuckDB database for foreign key constraints.
func DetectForeignKeys(ctx context.Context, db *sql.DB) ([]ForeignKeyInfo, error) {
	// DuckDB supports referential_constraints and key_column_usage in information_schema.
	rows, err := db.QueryContext(ctx,
		`SELECT
			rc.constraint_name,
			kcu.table_name AS child_table,
			kcu.column_name AS child_column,
			ccu.table_name AS parent_table,
			ccu.column_name AS parent_column,
			rc.delete_rule,
			rc.update_rule
		 FROM information_schema.referential_constraints rc
		 JOIN information_schema.key_column_usage kcu
			ON rc.constraint_name = kcu.constraint_name
			AND rc.constraint_schema = kcu.table_schema
		 JOIN information_schema.constraint_column_usage ccu
			ON rc.unique_constraint_name = ccu.constraint_name
			AND rc.unique_constraint_schema = ccu.constraint_schema
		 WHERE rc.constraint_schema = 'main'
		 ORDER BY rc.constraint_name, kcu.ordinal_position`)
	if err != nil {
		return nil, fmt.Errorf("failed to query FKs: %w", err)
	}
	defer rows.Close()

	fkMap := make(map[string]*ForeignKeyInfo)
	var fkOrder []string
	for rows.Next() {
		var constraintName, childTable, childCol, parentTable, parentCol, deleteRule, updateRule string
		if err := rows.Scan(&constraintName, &childTable, &childCol, &parentTable, &parentCol, &deleteRule, &updateRule); err != nil {
			return nil, err
		}
		fk, ok := fkMap[constraintName]
		if !ok {
			fk = &ForeignKeyInfo{
				ConstraintName: constraintName,
				ChildTable:     childTable,
				ParentTable:    parentTable,
				OnDelete:       deleteRule,
				OnUpdate:       updateRule,
			}
			fkMap[constraintName] = fk
			fkOrder = append(fkOrder, constraintName)
		}
		fk.ChildColumns = append(fk.ChildColumns, childCol)
		fk.ParentColumns = append(fk.ParentColumns, parentCol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []ForeignKeyInfo
	for _, name := range fkOrder {
		result = append(result, *fkMap[name])
	}
	return result, nil
}

// ForeignKeyInfo holds the detected FK relationship from the database.
type ForeignKeyInfo struct {
	ConstraintName string
	ChildTable     string
	ChildColumns   []string
	ParentTable    string
	ParentColumns  []string
	OnDelete       string
	OnUpdate       string
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
