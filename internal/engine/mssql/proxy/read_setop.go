package proxy

import (
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/psrth/mori/internal/core"
)

// handleSetOpRead handles queries with UNION/INTERSECT/EXCEPT by:
// 1. Decomposing into leaf SELECTs
// 2. Each leaf through merged read independently
// 3. Materializing into temp table
// 4. Rewriting query to reference temp table and executing on Shadow
func (rh *ReadHandler) handleSetOpRead(
	clientConn net.Conn,
	fullPayload []byte,
	cl *core.Classification,
) error {
	if len(cl.Tables) == 0 {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Decompose into leaf SELECTs.
	leaves, ops := splitSetOps(cl.RawSQL)
	if len(leaves) < 2 {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	// Process each leaf through merged read.
	var allColumns []TDSColumnInfo
	var allValues [][]string
	var allNulls [][]bool

	for i, leaf := range leaves {
		cappedLeaf := rh.capSQL(leaf)
		leafCl := &core.Classification{
			OpType:    core.OpRead,
			SubType:   core.SubSelect,
			RawSQL:    cappedLeaf,
			Tables:    extractTablesFromSQL(leaf),
			HasSetOp:  false,
		}

		columns, values, nulls, err := rh.mergedReadCore(leafCl, cappedLeaf)
		if err != nil {
			if rh.verbose {
				log.Printf("[conn %d] set op: leaf %d failed: %v", rh.connID, i, err)
			}
			return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
		}

		if i == 0 {
			allColumns = columns
		}

		if i == 0 || (len(ops) > i-1 && strings.ToUpper(ops[i-1]) == "UNION ALL") {
			// UNION ALL: append all rows.
			allValues = append(allValues, values...)
			allNulls = append(allNulls, nulls...)
		} else if len(ops) > i-1 {
			opUpper := strings.ToUpper(ops[i-1])
			switch {
			case strings.HasPrefix(opUpper, "UNION"):
				// UNION (distinct): append, will dedup below.
				allValues = append(allValues, values...)
				allNulls = append(allNulls, nulls...)
			case strings.HasPrefix(opUpper, "INTERSECT"):
				allValues, allNulls = intersectRows(allValues, allNulls, values, nulls)
			case strings.HasPrefix(opUpper, "EXCEPT"):
				allValues, allNulls = exceptRows(allValues, allNulls, values, nulls)
			default:
				allValues = append(allValues, values...)
				allNulls = append(allNulls, nulls...)
			}
		}
	}

	// For UNION (not UNION ALL), dedup by full row.
	if len(ops) > 0 && strings.ToUpper(ops[0]) == "UNION" {
		allValues, allNulls = dedupFullRows(allValues, allNulls)
	}

	// Materialize and execute original query pattern on Shadow.
	if len(allColumns) == 0 {
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}

	tempName, err := materializeToTempTable(rh.shadowConn, cl.RawSQL, allColumns, allValues, allNulls)
	if err != nil {
		if rh.verbose {
			log.Printf("[conn %d] set op: failed to materialize: %v", rh.connID, err)
		}
		return rh.fallbackToProd(clientConn, fullPayload, cl.RawSQL)
	}
	defer dropUtilTable(rh.shadowConn, tempName)

	// Build a simple SELECT from the temp table.
	colNames := make([]string, len(allColumns))
	for i, col := range allColumns {
		colNames[i] = quoteIdentMSSQL(col.Name)
	}
	selectSQL := "SELECT " + strings.Join(colNames, ", ") + " FROM " + tempName
	if cl.OrderBy != "" {
		selectSQL += " " + cl.OrderBy
	}
	if cl.HasLimit && cl.Limit > 0 {
		selectSQL = "SELECT TOP " + strconv.Itoa(cl.Limit) + " " + strings.Join(colNames, ", ") + " FROM " + tempName
		if cl.OrderBy != "" {
			selectSQL += " " + cl.OrderBy
		}
	}

	allHeaders := extractAllHeaders(fullPayload)
	var batchMsg []byte
	if allHeaders != nil {
		batchMsg = buildSQLBatchWithHeaders(allHeaders, selectSQL)
	} else {
		batchMsg = buildSQLBatchMessage(selectSQL)
	}
	return forwardAndRelay(batchMsg, rh.shadowConn, clientConn)
}

// reSetOpSplit splits SQL at UNION/INTERSECT/EXCEPT keywords.
var reSetOpSplit = regexp.MustCompile(`(?i)\b(UNION\s+ALL|UNION|INTERSECT|EXCEPT)\b`)

// splitSetOps decomposes a query with set operations into leaf SELECTs and operators.
func splitSetOps(sql string) (leaves []string, ops []string) {
	indices := reSetOpSplit.FindAllStringIndex(sql, -1)
	matches := reSetOpSplit.FindAllString(sql, -1)

	if len(indices) == 0 {
		return []string{sql}, nil
	}

	prev := 0
	for i, idx := range indices {
		leaf := strings.TrimSpace(sql[prev:idx[0]])
		if leaf != "" {
			leaves = append(leaves, leaf)
		}
		ops = append(ops, strings.TrimSpace(matches[i]))
		prev = idx[1]
	}
	// Last leaf.
	if prev < len(sql) {
		last := strings.TrimSpace(sql[prev:])
		if last != "" {
			leaves = append(leaves, last)
		}
	}
	return leaves, ops
}

// extractTablesFromSQL extracts table names from a SQL statement (simple regex).
func extractTablesFromSQL(sql string) []string {
	upper := strings.ToUpper(sql)
	fromIdx := strings.Index(upper, "FROM ")
	if fromIdx < 0 {
		return nil
	}
	rest := upper[fromIdx+5:]
	// Cut at keywords.
	for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "UNION", "INTERSECT", "EXCEPT", "JOIN"} {
		if idx := strings.Index(rest, " "+kw+" "); idx >= 0 {
			rest = rest[:idx]
		}
	}
	parts := strings.Split(rest, ",")
	var tables []string
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) > 0 {
			name := strings.Trim(fields[0], "[]`\"")
			if dotIdx := strings.LastIndex(name, "."); dotIdx >= 0 {
				name = name[dotIdx+1:]
			}
			if name != "" {
				tables = append(tables, strings.ToLower(name))
			}
		}
	}
	return tables
}

// intersectRows returns only rows that exist in both left and right sets.
func intersectRows(leftVals [][]string, leftNulls [][]bool, rightVals [][]string, rightNulls [][]bool) ([][]string, [][]bool) {
	rightSet := make(map[string]int)
	for i, row := range rightVals {
		key := rowKey(row, rightNulls[i])
		rightSet[key] = i
	}
	var outVals [][]string
	var outNulls [][]bool
	for i, row := range leftVals {
		key := rowKey(row, leftNulls[i])
		if _, exists := rightSet[key]; exists {
			outVals = append(outVals, row)
			outNulls = append(outNulls, leftNulls[i])
		}
	}
	return outVals, outNulls
}

// exceptRows returns rows in left that are not in right.
func exceptRows(leftVals [][]string, leftNulls [][]bool, rightVals [][]string, rightNulls [][]bool) ([][]string, [][]bool) {
	rightSet := make(map[string]bool)
	for i, row := range rightVals {
		key := rowKey(row, rightNulls[i])
		rightSet[key] = true
	}
	var outVals [][]string
	var outNulls [][]bool
	for i, row := range leftVals {
		key := rowKey(row, leftNulls[i])
		if !rightSet[key] {
			outVals = append(outVals, row)
			outNulls = append(outNulls, leftNulls[i])
		}
	}
	return outVals, outNulls
}

// dedupFullRows removes duplicate rows based on all column values.
func dedupFullRows(values [][]string, nulls [][]bool) ([][]string, [][]bool) {
	seen := make(map[string]bool)
	var outVals [][]string
	var outNulls [][]bool
	for i, row := range values {
		key := rowKey(row, nulls[i])
		if !seen[key] {
			seen[key] = true
			outVals = append(outVals, row)
			outNulls = append(outNulls, nulls[i])
		}
	}
	return outVals, outNulls
}

// rowKey builds a string key from a row for set operation dedup.
func rowKey(values []string, nulls []bool) string {
	var sb strings.Builder
	for i, v := range values {
		if i > 0 {
			sb.WriteByte('|')
		}
		if len(nulls) > i && nulls[i] {
			sb.WriteString("<NULL>")
		} else {
			sb.WriteString(v)
		}
	}
	return sb.String()
}

