package proxy

import (
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"strings"
)

// utilTableName generates a deterministic temp table name for materialization.
// The name is based on a hash of the SQL query to allow reuse within a session.
// Format: _mori_util_{first 12 hex chars of sha256}
func utilTableName(sql string) string {
	h := sha256.Sum256([]byte(sql))
	return fmt.Sprintf("_mori_util_%x", h[:6])
}

// materializeToUtilTable runs a SELECT query as a merged read (prod+shadow merge),
// then creates a temp table on shadow containing the merged results.
// Returns the temp table name. Caller is responsible for dropping it after use.
//
// maxRows caps the number of rows materialized (0 = unlimited).
func (rh *ReadHandler) materializeToUtilTable(
	querySQL string,
	tableName string,
	maxRows int,
) (string, error) {
	utilName := utilTableName(querySQL)

	// Execute the base query as a merged read to get merged rows from both backends.
	columns, values, nulls, err := rh.mergedReadCore(nil, querySQL)
	if err != nil {
		return "", fmt.Errorf("merged read for materialization: %w", err)
	}

	if len(values) == 0 {
		// No rows — create an empty temp table with the right schema.
		createSQL := buildCreateTempTableSQL(utilName, columns)
		if _, err := execQuery(rh.shadowConn, createSQL); err != nil {
			return "", fmt.Errorf("creating empty util table: %w", err)
		}
		return utilName, nil
	}

	// Apply max rows cap.
	if maxRows > 0 && len(values) > maxRows {
		values = values[:maxRows]
		nulls = nulls[:maxRows]
		if rh.verbose {
			log.Printf("[conn %d] materialization capped at %d rows (max_rows_hydrate)", rh.connID, maxRows)
		}
	}

	// Create the temp table on shadow.
	createSQL := buildCreateTempTableSQL(utilName, columns)
	result, err := execQuery(rh.shadowConn, createSQL)
	if err != nil {
		return "", fmt.Errorf("creating util table: %w", err)
	}
	if result.Error != "" {
		// Table might already exist from a previous call in the same session.
		// Try dropping and recreating.
		dropAndRecreate := fmt.Sprintf("DROP TABLE IF EXISTS %s; %s", quoteIdent(utilName), createSQL)
		if _, err := execQuery(rh.shadowConn, dropAndRecreate); err != nil {
			return "", fmt.Errorf("recreating util table: %w", err)
		}
	}

	// Insert merged rows into the temp table in batches.
	if err := bulkInsertToUtilTable(rh.shadowConn, utilName, columns, values, nulls); err != nil {
		// Clean up on failure.
		dropUtilTable(rh.shadowConn, utilName)
		return "", fmt.Errorf("populating util table: %w", err)
	}

	if rh.verbose {
		log.Printf("[conn %d] materialized %d rows into %s", rh.connID, len(values), utilName)
	}

	return utilName, nil
}

// dropUtilTable drops a utility temp table on shadow. Errors are silently ignored
// since this is best-effort cleanup.
func dropUtilTable(shadowConn net.Conn, utilName string) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(utilName))
	execQuery(shadowConn, sql) //nolint: errcheck
}

// buildCreateTempTableSQL builds a CREATE TEMP TABLE statement with column
// definitions inferred from ColumnInfo metadata.
func buildCreateTempTableSQL(tableName string, columns []ColumnInfo) string {
	var colDefs []string
	for _, col := range columns {
		pgType := oidToTypeName(col.OID)
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdent(col.Name), pgType))
	}
	return fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (%s)",
		quoteIdent(tableName), strings.Join(colDefs, ", "))
}

// bulkInsertToUtilTable inserts rows into a utility table using multi-row INSERT.
// Batches inserts to avoid exceeding PostgreSQL's parameter limits.
func bulkInsertToUtilTable(
	shadowConn net.Conn,
	tableName string,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) error {
	if len(values) == 0 {
		return nil
	}

	// Build column list.
	var colNames []string
	for _, col := range columns {
		colNames = append(colNames, quoteIdent(col.Name))
	}
	colList := strings.Join(colNames, ", ")

	// Insert in batches of 100 rows.
	const batchSize = 100
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}

		var rowStrings []string
		for ri, row := range values[i:end] {
			rowIdx := i + ri
			var vals []string
			for j, v := range row {
				if j < len(nulls[rowIdx]) && nulls[rowIdx][j] {
					vals = append(vals, "NULL")
				} else {
					vals = append(vals, quoteLiteral(v))
				}
			}
			rowStrings = append(rowStrings, "("+strings.Join(vals, ", ")+")")
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			quoteIdent(tableName), colList, strings.Join(rowStrings, ", "))

		result, err := execQuery(shadowConn, insertSQL)
		if err != nil {
			return fmt.Errorf("batch insert: %w", err)
		}
		if result.Error != "" {
			return fmt.Errorf("batch insert error: %s", result.Error)
		}
	}

	return nil
}

// oidToTypeName converts a PostgreSQL OID to an approximate SQL type name.
// Used for creating temp tables with the right column types.
func oidToTypeName(oid uint32) string {
	switch oid {
	case 16:
		return "BOOLEAN"
	case 20:
		return "BIGINT"
	case 21:
		return "SMALLINT"
	case 23:
		return "INTEGER"
	case 25:
		return "TEXT"
	case 26:
		return "OID"
	case 700:
		return "REAL"
	case 701:
		return "DOUBLE PRECISION"
	case 1042:
		return "CHAR"
	case 1043:
		return "VARCHAR"
	case 1082:
		return "DATE"
	case 1083:
		return "TIME"
	case 1114:
		return "TIMESTAMP"
	case 1184:
		return "TIMESTAMPTZ"
	case 1186:
		return "INTERVAL"
	case 1700:
		return "NUMERIC"
	case 2950:
		return "UUID"
	case 3802:
		return "JSONB"
	case 114:
		return "JSON"
	case 142:
		return "XML"
	case 17:
		return "BYTEA"
	case 1009:
		return "TEXT[]"
	case 1007:
		return "INTEGER[]"
	default:
		return "TEXT" // Safe fallback — TEXT accepts any value.
	}
}
