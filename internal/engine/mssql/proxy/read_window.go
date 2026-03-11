package proxy

import (
	"log"
	"net"
	"strings"

	"github.com/psrth/mori/internal/core"
)

// handleWindowFunctionRead handles queries with window functions by:
// 1. Building a base SELECT (stripping window functions)
// 2. Running through merged read pipeline
// 3. Materializing into a temp table
// 4. Rewriting the query to reference the temp table
// 5. Executing on Shadow
func (rh *ReadHandler) handleWindowFunctionRead(
	clientConn net.Conn,
	fullPayload []byte,
	cl *core.Classification,
) error {
	if len(cl.Tables) == 0 {
		// Can't determine base table — fall back to Prod.
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	table := cl.Tables[0]

	// Step 1: Build base SELECT (SELECT * FROM table WHERE ...).
	baseSQL := rh.buildWindowBaseQuery(cl.RawSQL, table)
	if baseSQL == "" {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}
	baseSQL = rh.capSQL(baseSQL)

	// Step 2: Merged read on the base query.
	baseCl := *cl
	baseCl.HasWindowFunc = false
	baseCl.IsComplexRead = false
	baseCl.HasAggregate = false
	baseCl.HasSetOp = false

	columns, values, nulls, err := rh.mergedReadCore(&baseCl, baseSQL)
	if err != nil {
		if re, ok := err.(*relayError); ok {
			_, writeErr := clientConn.Write(re.rawMsgs)
			return writeErr
		}
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	if len(columns) == 0 {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Step 3: Materialize into temp table.
	tempName, err := materializeToTempTable(rh.shadowConn, cl.RawSQL, columns, values, nulls)
	if err != nil {
		if rh.verbose {
			log.Printf("[conn %d] window func: failed to materialize: %v", rh.connID, err)
		}
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}
	defer dropUtilTable(rh.shadowConn, tempName)

	// Step 4: Rewrite original query to reference temp table.
	rewrittenSQL := rewriteTableReference(cl.RawSQL, table, tempName)

	// Step 5: Execute on Shadow.
	allHeaders := extractAllHeaders(fullPayload)
	var batchMsg []byte
	if allHeaders != nil {
		batchMsg = buildSQLBatchWithHeaders(allHeaders, rewrittenSQL)
	} else {
		batchMsg = buildSQLBatchMessage(rewrittenSQL)
	}
	// Execute on Shadow to let SQL Server compute window functions natively.
	return forwardAndRelay(batchMsg, rh.shadowConn, clientConn)
}

// buildWindowBaseQuery strips window function expressions and builds a base SELECT.
func (rh *ReadHandler) buildWindowBaseQuery(sql, table string) string {
	upper := strings.ToUpper(sql)
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}
	// Use SELECT * FROM table [WHERE ...] to get the base data.
	return "SELECT *" + sql[fromIdx:]
}

// rewriteTableReference replaces references to the original table with the temp table.
func rewriteTableReference(sql, originalTable, tempTable string) string {
	// Replace both bracketed and unbracketed forms of the table name.
	result := sql

	// Replace [tableName] form.
	bracketedOrig := "[" + originalTable + "]"
	result = strings.Replace(result, bracketedOrig, tempTable, -1)
	result = strings.Replace(result, strings.ToUpper(bracketedOrig), tempTable, -1)
	result = strings.Replace(result, strings.ToLower(bracketedOrig), tempTable, -1)

	// Replace unbracketed form (case-insensitive word boundary).
	// Use a simple approach: replace after FROM and JOIN keywords.
	result = replaceTableNameAfterKeywords(result, originalTable, tempTable)

	return result
}

// replaceTableNameAfterKeywords replaces table name occurrences in FROM and JOIN clauses.
func replaceTableNameAfterKeywords(sql, oldTable, newTable string) string {
	lower := strings.ToLower(oldTable)

	keywords := []string{"FROM ", "JOIN ", "INTO ", "UPDATE ", "TABLE "}
	result := []byte(sql)

	for _, kw := range keywords {
		off := 0
		for {
			idx := strings.Index(strings.ToUpper(string(result[off:])), kw)
			if idx < 0 {
				break
			}
			absIdx := off + idx + len(kw)
			rest := string(result[absIdx:])
			trimmed := strings.TrimSpace(rest)

			// Check if the next word matches the table name (case-insensitive).
			trimmedLower := strings.ToLower(trimmed)
			if strings.HasPrefix(trimmedLower, lower) {
				nextChar := byte(' ')
				if len(trimmedLower) > len(lower) {
					nextChar = trimmedLower[len(lower)]
				}
				if nextChar == ' ' || nextChar == '\t' || nextChar == '\n' || nextChar == ',' ||
					nextChar == ';' || nextChar == ')' || nextChar == 0 {
					// Found a match — replace it.
					startOfName := absIdx + (len(rest) - len(trimmed))
					endOfName := startOfName + len(oldTable)
					newResult := make([]byte, 0, len(result)+(len(newTable)-len(oldTable)))
					newResult = append(newResult, result[:startOfName]...)
					newResult = append(newResult, []byte(newTable)...)
					newResult = append(newResult, result[endOfName:]...)
					result = newResult
				}
			}
			off = absIdx + 1
		}
	}
	return string(result)
}

// fallbackToProd sends the original query to Prod and relays the response.
func (rh *ReadHandler) fallbackToProd(clientConn net.Conn, fullPayload []byte, sql string) error {
	allHeaders := extractAllHeaders(fullPayload)
	var batchMsg []byte
	if allHeaders != nil {
		batchMsg = buildSQLBatchWithHeaders(allHeaders, sql)
	} else {
		batchMsg = buildSQLBatchMessage(sql)
	}
	return forwardAndRelay(batchMsg, rh.prodConn, clientConn)
}
