package proxy

import (
	"crypto/md5"
	"fmt"
	"net"
	"strings"
)

// utilTableName generates a unique temp table name for materialization.
// MSSQL temp tables use # prefix and are session-scoped.
func utilTableName(sql string) string {
	hash := md5.Sum([]byte(sql))
	return fmt.Sprintf("#_mori_util_%x", hash[:6])
}

// buildCreateTempTableSQL builds CREATE TABLE #name (col1 type1, ...) for MSSQL.
func buildCreateTempTableSQL(tableName string, columns []TDSColumnInfo) string {
	if len(columns) == 0 {
		return ""
	}
	var colDefs []string
	for _, col := range columns {
		// Use NVARCHAR(4000) as a safe default for materialized data.
		colDefs = append(colDefs, fmt.Sprintf("%s NVARCHAR(4000) NULL", quoteIdentMSSQL(col.Name)))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(colDefs, ", "))
}

// bulkInsertToUtilTable inserts rows into a temp table in batches.
func bulkInsertToUtilTable(conn net.Conn, tableName string, columns []TDSColumnInfo, values [][]string, nulls [][]bool) error {
	if len(values) == 0 {
		return nil
	}

	// Insert in batches of 1000 rows (MSSQL VALUES limit is 1000 rows per INSERT).
	const batchSize = 1000
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = quoteIdentMSSQL(col.Name)
	}
	colList := strings.Join(colNames, ", ")

	for start := 0; start < len(values); start += batchSize {
		end := min(start+batchSize, len(values))

		var rowStrs []string
		for i := start; i < end; i++ {
			row := values[i]
			var vals []string
			for j, val := range row {
				isNull := len(nulls) > i && len(nulls[i]) > j && nulls[i][j]
				if isNull {
					vals = append(vals, "NULL")
				} else {
					vals = append(vals, "N"+quoteLiteralMSSQL(val))
				}
			}
			rowStrs = append(rowStrs, "("+strings.Join(vals, ", ")+")")
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			tableName, colList, strings.Join(rowStrs, ", "))
		result, err := execTDSQuery(conn, insertSQL)
		if err != nil {
			return fmt.Errorf("bulk insert to util table: %w", err)
		}
		if result.Error != "" {
			return fmt.Errorf("bulk insert error: %s", result.Error)
		}
	}
	return nil
}

// dropUtilTable drops a temp table.
func dropUtilTable(conn net.Conn, tableName string) {
	sql := fmt.Sprintf("IF OBJECT_ID('tempdb..%s') IS NOT NULL DROP TABLE %s", tableName, tableName)
	execTDSQuery(conn, sql) //nolint:errcheck
}

// materializeToTempTable creates a temp table, inserts the given rows, and
// returns the temp table name. The caller is responsible for dropping the table.
func materializeToTempTable(conn net.Conn, sql string, columns []TDSColumnInfo, values [][]string, nulls [][]bool) (string, error) {
	tempName := utilTableName(sql)

	// Drop if exists from a previous attempt.
	dropUtilTable(conn, tempName)

	// Create the temp table.
	createSQL := buildCreateTempTableSQL(tempName, columns)
	if createSQL == "" {
		return "", fmt.Errorf("no columns for temp table")
	}
	result, err := execTDSQuery(conn, createSQL)
	if err != nil {
		return "", fmt.Errorf("create temp table: %w", err)
	}
	if result.Error != "" {
		return "", fmt.Errorf("create temp table error: %s", result.Error)
	}

	// Insert the data.
	if err := bulkInsertToUtilTable(conn, tempName, columns, values, nulls); err != nil {
		dropUtilTable(conn, tempName)
		return "", err
	}

	return tempName, nil
}
