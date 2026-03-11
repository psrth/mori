package proxy

import (
	"crypto/sha256"
	"fmt"
	"net"
	"strings"

	"github.com/psrth/mori/internal/core"
)

// utilTableName generates a deterministic temp table name from a SQL query hash.
func utilTableName(sql string) string {
	h := sha256.Sum256([]byte(sql))
	return fmt.Sprintf("_mori_util_%x", h[:6])
}

// materializeToUtilTable runs a SELECT through merged read and materializes
// the results into a temporary table on Shadow.
func (rh *ReadHandler) materializeToUtilTable(querySQL string, tableName string, maxRows int) (string, error) {
	// Create a classification for the base query.
	cl, err := rh.classifyForMaterialization(querySQL)
	if err != nil {
		return "", fmt.Errorf("classify for materialization: %w", err)
	}

	// Cap the query if maxRows > 0.
	cappedSQL := querySQL
	if maxRows > 0 && !hasOuterLimit(cappedSQL) {
		cappedSQL = fmt.Sprintf("%s LIMIT %d", cappedSQL, maxRows)
	}

	// Execute merged read.
	columns, values, nulls, err := rh.mergedReadCore(cl, cappedSQL)
	if err != nil {
		return "", fmt.Errorf("merged read for materialization: %w", err)
	}

	// Create temp table.
	createSQL := buildCreateTempTableSQL(tableName, columns)
	createResult, err := execMySQLQuery(rh.shadowConn, createSQL)
	if err != nil {
		return "", fmt.Errorf("create temp table: %w", err)
	}
	if createResult.Error != "" {
		// Table might exist from a previous query in the same session.
		dropUtilTable(rh.shadowConn, tableName)
		createResult, err = execMySQLQuery(rh.shadowConn, createSQL)
		if err != nil {
			return "", fmt.Errorf("create temp table (retry): %w", err)
		}
		if createResult.Error != "" {
			return "", fmt.Errorf("create temp table: %s", createResult.Error)
		}
	}

	// Bulk insert rows.
	if len(values) > 0 {
		if err := bulkInsertToUtilTable(rh.shadowConn, tableName, columns, values, nulls); err != nil {
			dropUtilTable(rh.shadowConn, tableName)
			return "", fmt.Errorf("bulk insert: %w", err)
		}
	}

	return tableName, nil
}

// dropUtilTable drops a temporary table (best-effort, ignores errors).
func dropUtilTable(shadowConn net.Conn, utilName string) {
	execMySQLQuery(shadowConn, fmt.Sprintf("DROP TEMPORARY TABLE IF EXISTS `%s`", utilName))
}

// buildCreateTempTableSQL constructs a CREATE TEMPORARY TABLE statement.
// Uses TEXT for all columns as a safe default since MySQL handles type coercion.
func buildCreateTempTableSQL(tableName string, columns []ColumnInfo) string {
	var colDefs []string
	for _, col := range columns {
		colType := mysqlColumnType(col.RawDef)
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", col.Name, colType))
	}
	return fmt.Sprintf("CREATE TEMPORARY TABLE IF NOT EXISTS `%s` (%s)",
		tableName, strings.Join(colDefs, ", "))
}

// bulkInsertToUtilTable inserts rows into a temp table in batches.
func bulkInsertToUtilTable(shadowConn net.Conn, tableName string, columns []ColumnInfo, values [][]string, nulls [][]bool) error {
	const batchSize = 100

	var colNames []string
	for _, col := range columns {
		colNames = append(colNames, "`"+col.Name+"`")
	}
	colList := strings.Join(colNames, ", ")

	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		batch := values[i:end]
		batchNulls := nulls[i:end]

		var rowParts []string
		for ri, row := range batch {
			var valParts []string
			for ci, val := range row {
				if ci < len(batchNulls[ri]) && batchNulls[ri][ci] {
					valParts = append(valParts, "NULL")
				} else {
					valParts = append(valParts, "'"+strings.ReplaceAll(val, "'", "''")+"'")
				}
			}
			rowParts = append(rowParts, "("+strings.Join(valParts, ", ")+")")
		}

		insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
			tableName, colList, strings.Join(rowParts, ", "))

		result, err := execMySQLQuery(shadowConn, insertSQL)
		if err != nil {
			return fmt.Errorf("batch insert: %w", err)
		}
		if result.Error != "" {
			return fmt.Errorf("batch insert error: %s", result.Error)
		}
	}

	return nil
}

// mysqlColumnType extracts a MySQL column type name from a column definition.
// Falls back to TEXT if the type cannot be determined.
func mysqlColumnType(rawDef []byte) string {
	if len(rawDef) == 0 {
		return "TEXT"
	}

	// Parse the column definition packet to find the type byte.
	// Format: catalog + schema + table + org_table + name + org_name + filler(1) +
	//         charset(2) + column_length(4) + column_type(1) + flags(2) + decimals(1) + filler(2)
	pos := 0
	// Skip 6 lenenc strings.
	for i := 0; i < 6; i++ {
		if pos >= len(rawDef) {
			return "TEXT"
		}
		_, totalSize := readLenEncString(rawDef[pos:])
		pos += totalSize
	}
	// Skip filler (1 byte = 0x0c).
	pos++
	// Skip charset (2 bytes).
	pos += 2
	// Skip column_length (4 bytes).
	pos += 4
	// Read column_type (1 byte).
	if pos >= len(rawDef) {
		return "TEXT"
	}
	typeByte := rawDef[pos]

	switch typeByte {
	case 0x01: // TINY
		return "TINYINT"
	case 0x02: // SHORT
		return "SMALLINT"
	case 0x03: // LONG
		return "INT"
	case 0x04: // FLOAT
		return "FLOAT"
	case 0x05: // DOUBLE
		return "DOUBLE"
	case 0x08: // LONGLONG
		return "BIGINT"
	case 0x09: // INT24
		return "MEDIUMINT"
	case 0x00, 0xf6: // DECIMAL, NEWDECIMAL
		return "DECIMAL(65,30)"
	case 0x07: // TIMESTAMP
		return "TIMESTAMP"
	case 0x0a: // DATE
		return "DATE"
	case 0x0b: // TIME
		return "TIME"
	case 0x0c: // DATETIME
		return "DATETIME"
	case 0x0d: // YEAR
		return "YEAR"
	case 0x0f: // VARCHAR
		return "VARCHAR(16383)"
	case 0xfd: // VAR_STRING
		return "TEXT"
	case 0xfe: // STRING
		return "VARCHAR(255)"
	case 0xf5: // JSON
		return "JSON"
	case 0x10: // BIT
		return "BIT(64)"
	case 0xf9, 0xfa, 0xfb, 0xfc: // TINY_BLOB, MEDIUM_BLOB, LONG_BLOB, BLOB
		return "BLOB"
	default:
		return "TEXT"
	}
}

// classifyForMaterialization creates a Classification for a SELECT query
// used in materialization. It extracts table names and basic properties.
func (rh *ReadHandler) classifyForMaterialization(sql string) (*core.Classification, error) {
	// Build a minimal classification for merged read.
	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		RawSQL:  sql,
	}

	// Extract tables from the SQL for delta/tombstone filtering.
	upper := strings.ToUpper(strings.TrimSpace(sql))
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx >= 0 {
		rest := upper[fromIdx+6:]
		// Find the end of the FROM clause.
		endIdx := len(rest)
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "JOIN"} {
			if idx := strings.Index(rest, " "+kw+" "); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		tablePart := strings.TrimSpace(sql[fromIdx+6 : fromIdx+6+endIdx])
		parts := strings.Split(tablePart, ",")
		for _, part := range parts {
			tokens := strings.Fields(strings.TrimSpace(part))
			if len(tokens) > 0 {
				name := strings.Trim(tokens[0], "`")
				if name != "" && !strings.HasPrefix(strings.ToUpper(name), "(") {
					cl.Tables = append(cl.Tables, name)
				}
			}
		}
	}

	return cl, nil
}
