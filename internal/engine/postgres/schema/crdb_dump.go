package schema

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
)

// CRDBDumpSchema exports the schema from a CockroachDB production database
// using SHOW CREATE ALL TABLES (CockroachDB's native schema export).
// The output is cleaned to be compatible with a PostgreSQL shadow database.
func CRDBDumpSchema(ctx context.Context, conn *pgx.Conn) (string, error) {
	rows, err := conn.Query(ctx, "SHOW CREATE ALL TABLES")
	if err != nil {
		return "", fmt.Errorf("SHOW CREATE ALL TABLES failed: %w", err)
	}
	defer rows.Close()

	var stmts []string
	for rows.Next() {
		var stmt string
		if err := rows.Scan(&stmt); err != nil {
			return "", fmt.Errorf("scanning SHOW CREATE ALL TABLES row: %w", err)
		}
		stmts = append(stmts, stmt)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	raw := strings.Join(stmts, "\n")
	return CleanCRDBSchema(raw), nil
}

// crdb-specific syntax patterns to strip for PostgreSQL compatibility.
var (
	// FAMILY clauses inside CREATE TABLE: FAMILY "primary" (col1, col2, ...)
	crdbFamilyRe = regexp.MustCompile(`(?im),?\s*FAMILY\s+"[^"]*"\s*\([^)]*\)`)

	// Hash-sharded index hints: USING HASH WITH BUCKET_COUNT = N
	crdbHashShardedRe = regexp.MustCompile(`(?i)\s+USING\s+HASH\s+WITH\s+BUCKET_COUNT\s*=\s*\d+`)

	// INTERLEAVE IN PARENT clauses
	crdbInterleaveRe = regexp.MustCompile(`(?i)\s+INTERLEAVE\s+IN\s+PARENT\s+\S+\s*\([^)]*\)`)

	// ALTER TABLE ... CONFIGURE ZONE statements
	crdbConfigureZoneRe = regexp.MustCompile(`(?im)^ALTER\s+(?:TABLE|INDEX|DATABASE|RANGE)\s+[^\n]*CONFIGURE\s+ZONE[^\n]*;\s*\n?`)

	// SHOW RANGES and other CRDB-specific statements
	crdbShowRangesRe = regexp.MustCompile(`(?im)^SHOW\s+RANGES[^\n]*;\s*\n?`)

	// ALTER TABLE ... SPLIT AT statements
	crdbSplitAtRe = regexp.MustCompile(`(?im)^ALTER\s+TABLE\s+[^\n]*SPLIT\s+AT[^\n]*;\s*\n?`)

	// unique_rowid() default expressions → remove so PG uses SERIAL/BIGSERIAL
	crdbUniqueRowIDRe = regexp.MustCompile(`(?i)\s+DEFAULT\s+unique_rowid\(\)`)

	// rowid column definitions that CockroachDB auto-adds for PK-less tables
	// e.g., rowid INT8 NOT NULL DEFAULT unique_rowid(),
	crdbRowidColRe = regexp.MustCompile(`(?im),?\s*rowid\s+INT8\s+NOT\s+NULL\s+DEFAULT\s+unique_rowid\(\)\s*,?`)

	// FK constraints — strip same as pg_dump output (enforced at proxy layer)
	crdbFKRe = regexp.MustCompile(`(?im),?\s*CONSTRAINT\s+\S+\s+FOREIGN\s+KEY\s*\([^)]*\)\s*REFERENCES\s+\S+\s*\([^)]*\)[^,)]*`)

	// ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY (separate statements)
	crdbAlterFKRe = regexp.MustCompile(`(?im)^ALTER\s+TABLE\s+[^\n]*ADD\s+CONSTRAINT\s+[^\n]*FOREIGN\s+KEY[^\n]*;\s*\n?`)

	// VALIDATE CONSTRAINT statements
	crdbValidateRe = regexp.MustCompile(`(?im)^ALTER\s+TABLE\s+[^\n]*VALIDATE\s+CONSTRAINT[^\n]*;\s*\n?`)
)

// CleanCRDBSchema transforms CockroachDB DDL into PostgreSQL-compatible DDL.
func CleanCRDBSchema(raw string) string {
	s := raw

	// Strip CRDB-specific syntax
	s = crdbFamilyRe.ReplaceAllString(s, "")
	s = crdbHashShardedRe.ReplaceAllString(s, "")
	s = crdbInterleaveRe.ReplaceAllString(s, "")
	s = crdbConfigureZoneRe.ReplaceAllString(s, "")
	s = crdbShowRangesRe.ReplaceAllString(s, "")
	s = crdbSplitAtRe.ReplaceAllString(s, "")
	s = crdbValidateRe.ReplaceAllString(s, "")

	// Strip FK constraints (proxy-layer enforcement)
	s = crdbFKRe.ReplaceAllString(s, "")
	s = crdbAlterFKRe.ReplaceAllString(s, "")

	// Replace unique_rowid() defaults — PostgreSQL will use SERIAL sequences
	s = crdbUniqueRowIDRe.ReplaceAllString(s, "")

	// Strip auto-generated rowid columns for PK-less tables
	s = crdbRowidColRe.ReplaceAllString(s, "")

	// Normalize CockroachDB type names to PostgreSQL equivalents
	s = normalizeCRDBTypes(s)

	// Clean up any trailing commas before closing parens caused by stripping
	s = cleanTrailingCommas(s)

	return s
}

// normalizeCRDBTypes replaces CockroachDB-specific type names with PostgreSQL equivalents.
func normalizeCRDBTypes(s string) string {
	// INT8 → BIGINT (CockroachDB uses INT8 as default integer type)
	s = regexp.MustCompile(`(?i)\bINT8\b`).ReplaceAllString(s, "BIGINT")
	// INT4 → INTEGER
	s = regexp.MustCompile(`(?i)\bINT4\b`).ReplaceAllString(s, "INTEGER")
	// INT2 → SMALLINT
	s = regexp.MustCompile(`(?i)\bINT2\b`).ReplaceAllString(s, "SMALLINT")
	// FLOAT4 → REAL
	s = regexp.MustCompile(`(?i)\bFLOAT4\b`).ReplaceAllString(s, "REAL")
	// FLOAT8 → DOUBLE PRECISION
	s = regexp.MustCompile(`(?i)\bFLOAT8\b`).ReplaceAllString(s, "DOUBLE PRECISION")
	// STRING → TEXT (CockroachDB alias)
	s = regexp.MustCompile(`(?i)\bSTRING\b`).ReplaceAllString(s, "TEXT")
	return s
}

// cleanTrailingCommas removes trailing commas before closing parentheses
// that result from stripping FAMILY/FK/rowid clauses.
func cleanTrailingCommas(s string) string {
	// Match comma followed by optional whitespace and closing paren
	re := regexp.MustCompile(`,\s*\)`)
	return re.ReplaceAllString(s, "\n)")
}

// CRDBClassifyPKType classifies PK type for CockroachDB tables.
// CockroachDB SERIAL columns use unique_rowid() instead of nextval(),
// so we need different detection logic.
func CRDBClassifyPKType(dataTypes []string, defaults []string) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToLower(dataTypes[0])
	def := ""
	if len(defaults) > 0 {
		def = strings.ToLower(defaults[0])
	}

	if dt == "uuid" {
		return "uuid"
	}

	// CockroachDB SERIAL uses unique_rowid()
	if strings.Contains(def, "unique_rowid") {
		if dt == "bigint" || dt == "int8" {
			return "bigserial"
		}
		if dt == "smallint" || dt == "int2" {
			return "smallserial"
		}
		return "serial"
	}

	// Also check for nextval() (some CockroachDB versions/configs use sequences)
	if strings.Contains(def, "nextval") {
		if dt == "bigint" {
			return "bigserial"
		}
		if dt == "smallint" {
			return "smallserial"
		}
		return "serial"
	}

	// Integer types without explicit default — treat as serial for offset purposes
	if dt == "integer" || dt == "bigint" || dt == "int4" || dt == "int8" {
		if dt == "bigint" || dt == "int8" {
			return "bigserial"
		}
		return "serial"
	}

	return dt
}

// CRDBFullDump performs the complete schema dump pipeline for a CockroachDB prod.
// Uses SHOW CREATE ALL TABLES instead of pg_dump, and CockroachDB-aware PK classification.
func CRDBFullDump(ctx context.Context, conn *pgx.Conn) (*DumpResult, error) {
	const maxRetries = 5

	// CockroachDB has no extensions — skip extension detection.
	var exts []Extension

	// Dump schema using SHOW CREATE ALL TABLES.
	var schemaSQL string
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		schemaSQL, e = CRDBDumpSchema(ctx, conn)
		return e
	}); err != nil {
		return nil, fmt.Errorf("CockroachDB schema dump failed: %w", err)
	}

	// Detect table metadata using pg_catalog (works on CockroachDB).
	var tables map[string]TableMeta
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		tables, e = DetectTableMetadata(ctx, conn)
		return e
	}); err != nil {
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}

	// Re-classify PK types using CRDB-aware logic (unique_rowid detection).
	tables = reclassifyCRDBPKTypes(ctx, conn, tables)

	// Detect generated columns (graceful failure if unsupported).
	DetectGeneratedColumns(ctx, conn, tables)

	// Detect sequence offsets — MAX(pk) works on CockroachDB.
	var sequences map[string]SequenceOffset
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		sequences, e = DetectSequenceOffsets(ctx, conn, tables)
		return e
	}); err != nil {
		return nil, fmt.Errorf("sequence offset detection failed: %w", err)
	}

	// Detect foreign keys using pg_catalog (works on CockroachDB).
	var foreignKeys []coreSchema.ForeignKey
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		foreignKeys, e = DetectForeignKeys(ctx, conn)
		return e
	}); err != nil {
		return nil, fmt.Errorf("foreign key detection failed: %w", err)
	}

	return &DumpResult{
		SchemaSQL:   schemaSQL,
		Extensions:  exts,
		Sequences:   sequences,
		Tables:      tables,
		ForeignKeys: foreignKeys,
	}, nil
}

// reclassifyCRDBPKTypes re-examines PK type classification for CockroachDB tables.
// It queries column defaults directly and uses CRDBClassifyPKType instead of the
// PostgreSQL classifyPKType (which doesn't recognize unique_rowid()).
func reclassifyCRDBPKTypes(ctx context.Context, conn *pgx.Conn, tables map[string]TableMeta) map[string]TableMeta {
	for tableName, meta := range tables {
		if meta.PKType == "none" || meta.PKType == "uuid" || meta.PKType == "composite" {
			continue
		}
		if len(meta.PKColumns) != 1 {
			continue
		}

		pkCol := meta.PKColumns[0]
		var dataType, colDefault string
		err := conn.QueryRow(ctx, `
			SELECT COALESCE(format_type(a.atttypid, a.atttypmod), ''),
			       COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
			WHERE n.nspname = 'public'
			  AND c.relname = $1
			  AND a.attname = $2
			  AND a.attnum > 0
			  AND NOT a.attisdropped`,
			tableName, pkCol).Scan(&dataType, &colDefault)
		if err != nil {
			continue // Keep existing classification on error
		}

		newType := CRDBClassifyPKType([]string{dataType}, []string{colDefault})
		meta.PKType = newType
		tables[tableName] = meta
	}
	return tables
}
