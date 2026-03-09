package proxy

import (
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
)

// handleWindowRead handles queries with window functions.
func (rh *ReadHandler) handleWindowRead(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.windowReadCore(cl, cl.RawSQL)
	if err != nil {
		if re, ok := err.(*mysqlRelayError); ok {
			_, writeErr := clientConn.Write(re.rawMsgs)
			return writeErr
		}
		return err
	}

	response := buildMySQLSelectResponse(columns, values, nulls)
	_, err = clientConn.Write(response)
	return err
}

// windowReadCore implements the window function strategy:
// 1. Build base SELECT (same FROM/WHERE, no window functions)
// 2. Merged read of base query
// 3. Materialize to temp table on Shadow
// 4. Rewrite original query to read from temp table
// 5. Execute on Shadow (MySQL computes windows on correct dataset)
func (rh *ReadHandler) windowReadCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	// Build base SELECT.
	baseSQL := buildWindowBaseQueryMySQL(querySQL)
	if baseSQL == "" {
		return rh.windowFallback(querySQL)
	}

	// Generate temp table name.
	utilName := utilTableName(querySQL)

	// Materialize merged results into temp table.
	_, err := rh.materializeToUtilTable(baseSQL, utilName, 0)
	if err != nil {
		rh.logf("window materialization failed, falling back: %v", err)
		return rh.windowFallback(querySQL)
	}
	defer dropUtilTable(rh.shadowConn, utilName)

	// Rewrite query to use temp table.
	rewritten := rewriteWindowQueryMySQL(querySQL, cl.Tables, utilName)

	// Execute rewritten query on Shadow.
	result, err := execMySQLQuery(rh.shadowConn, rewritten)
	if err != nil {
		return rh.windowFallback(querySQL)
	}
	if result.Error != "" {
		rh.logf("window rewritten query error: %s", result.Error)
		return rh.windowFallback(querySQL)
	}

	return result.Columns, result.RowValues, result.RowNulls, nil
}

// buildWindowBaseQueryMySQL creates a SELECT * with the same FROM/WHERE
// but without window functions, ORDER BY, LIMIT.
func buildWindowBaseQueryMySQL(sql string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}

	afterFrom := sql[fromIdx:]

	// Strip ORDER BY, LIMIT, window-related clauses.
	stripped := afterFrom
	for _, kw := range []string{" ORDER BY ", " LIMIT "} {
		if idx := strings.Index(strings.ToUpper(stripped), kw); idx >= 0 {
			stripped = stripped[:idx]
		}
	}

	return "SELECT *" + stripped
}

// rewriteWindowQueryMySQL rewrites a window function query to read from
// the temp table instead of the original table(s).
func rewriteWindowQueryMySQL(sql string, tables []string, utilName string) string {
	result := sql
	for _, table := range tables {
		// Replace backtick-quoted table name.
		result = strings.Replace(result, "`"+table+"`", "`"+utilName+"`", -1)
		// Replace unquoted table name (word boundary aware).
		result = replaceColumnName(result, table, "`"+utilName+"`")
	}

	// Strip table qualifiers from column references (e.g., t.col -> col).
	// This is needed because the temp table is a flat materialized result.
	for _, table := range tables {
		result = strings.ReplaceAll(result, "`"+table+"`.", "")
		result = strings.ReplaceAll(result, table+".", "")
	}

	return result
}

// windowFallback executes the query on Shadow only when materialization fails.
func (rh *ReadHandler) windowFallback(sql string) ([]ColumnInfo, [][]string, [][]bool, error) {
	result, err := execMySQLQuery(rh.shadowConn, sql)
	if err != nil {
		return nil, nil, nil, err
	}
	if result.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}
	return result.Columns, result.RowValues, result.RowNulls, nil
}
