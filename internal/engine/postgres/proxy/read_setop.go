package proxy

import (
	"fmt"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
)

// handleSetOperation handles UNION/INTERSECT/EXCEPT queries on dirty tables.
// Strategy: decompose into sub-SELECTs, run each through merged read,
// apply the set operation in memory.
func (rh *ReadHandler) handleSetOperation(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.setOperationCore(cl, cl.RawSQL)
	if err != nil {
		if re, ok := err.(*relayError); ok {
			_, writeErr := clientConn.Write(re.rawMsgs)
			return writeErr
		}
		return err
	}
	response := buildSelectResponse(columns, values, nulls)
	_, err = clientConn.Write(response)
	return err
}

// setOperationCore decomposes set operations and applies them in memory.
func (rh *ReadHandler) setOperationCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	result, err := pg_query.Parse(querySQL)
	if err != nil {
		return rh.setOpFallback(querySQL)
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return rh.setOpFallback(querySQL)
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return rh.setOpFallback(querySQL)
	}

	// Check if this is actually a set operation.
	op := sel.GetOp()
	if op == pg_query.SetOperation_SETOP_NONE {
		// Not a set operation — shouldn't reach here, but handle gracefully.
		return rh.setOpFallback(querySQL)
	}

	// Recursively decompose and execute.
	columns, values, nulls, err := rh.executeSetOp(sel, cl)
	if err != nil {
		return rh.setOpFallback(querySQL)
	}

	// Apply ORDER BY from the top-level query.
	if cl.OrderBy != "" {
		sortMerged(columns, values, nulls, cl.OrderBy)
	}

	// Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(values) > cl.Limit {
		values = values[:cl.Limit]
		nulls = nulls[:cl.Limit]
	}

	return columns, values, nulls, nil
}

// executeSetOp recursively processes set operation nodes.
func (rh *ReadHandler) executeSetOp(sel *pg_query.SelectStmt, cl *core.Classification) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	op := sel.GetOp()

	// Base case: this is a plain SELECT (leaf node).
	if op == pg_query.SetOperation_SETOP_NONE {
		// Deparse this SELECT and run as merged read.
		leafSQL, err := deparseSelectStmt(sel)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("deparse leaf SELECT: %w", err)
		}

		// Apply materialization row cap to the leaf query.
		cappedLeafSQL := rh.capSQL(leafSQL)

		// Extract tables from this specific leaf for the classification.
		leafTables, _ := classifyForDirtyCheck(cappedLeafSQL)

		// Build a classification for the leaf.
		leafCl := &core.Classification{
			Tables: leafTables,
			RawSQL: cappedLeafSQL,
		}

		return rh.mergedReadCore(leafCl, cappedLeafSQL)
	}

	// Recursive case: left OP right.
	leftSel := sel.GetLarg()
	rightSel := sel.GetRarg()

	if leftSel == nil || rightSel == nil {
		return nil, nil, nil, fmt.Errorf("set operation missing operand")
	}

	// Execute left side.
	leftCols, leftVals, leftNulls, err := rh.executeSetOp(leftSel, cl)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("left operand: %w", err)
	}

	// Execute right side.
	rightCols, rightVals, rightNulls, err := rh.executeSetOp(rightSel, cl)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("right operand: %w", err)
	}

	// Use left columns as canonical.
	columns := leftCols
	if len(columns) == 0 {
		columns = rightCols
	}

	// Apply the set operation.
	isAll := sel.GetAll()

	switch op {
	case pg_query.SetOperation_SETOP_UNION:
		if isAll {
			// UNION ALL: concatenate.
			values := append(leftVals, rightVals...)
			nulls := append(leftNulls, rightNulls...)
			return columns, values, nulls, nil
		}
		// UNION: concatenate then deduplicate by full row.
		values := append(leftVals, rightVals...)
		nulls := append(leftNulls, rightNulls...)
		values, nulls = deduplicateByFullRow(values, nulls)
		return columns, values, nulls, nil

	case pg_query.SetOperation_SETOP_INTERSECT:
		values, nulls := intersectRows(leftVals, leftNulls, rightVals, rightNulls, isAll)
		return columns, values, nulls, nil

	case pg_query.SetOperation_SETOP_EXCEPT:
		values, nulls := exceptRows(leftVals, leftNulls, rightVals, rightNulls, isAll)
		return columns, values, nulls, nil
	}

	return nil, nil, nil, fmt.Errorf("unsupported set operation: %v", op)
}

// deparseSelectStmt deparses a SelectStmt back to SQL.
func deparseSelectStmt(sel *pg_query.SelectStmt) (string, error) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: sel,
					},
				},
			},
		},
	}
	return pg_query.Deparse(result)
}

// setOpRowKey builds a string key from a row for deduplication/set operations.
func setOpRowKey(row []string, nulls []bool) string {
	var parts []string
	for i, v := range row {
		if i < len(nulls) && nulls[i] {
			parts = append(parts, "\x00NULL")
		} else {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "\x01")
}

// deduplicateByFullRow removes duplicate rows based on full-row equality.
func deduplicateByFullRow(values [][]string, nulls [][]bool) ([][]string, [][]bool) {
	seen := make(map[string]bool)
	var result [][]string
	var resultNulls [][]bool
	for i, row := range values {
		var rowNulls []bool
		if i < len(nulls) {
			rowNulls = nulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, row)
		resultNulls = append(resultNulls, rowNulls)
	}
	return result, resultNulls
}

// intersectRows returns rows that appear in both left and right.
// If isAll is true, preserves duplicates up to the minimum count in each side.
// If isAll is false, returns distinct rows that appear in both.
func intersectRows(
	leftVals [][]string, leftNulls [][]bool,
	rightVals [][]string, rightNulls [][]bool,
	isAll bool,
) ([][]string, [][]bool) {
	rightCounts := make(map[string]int)
	for i, row := range rightVals {
		var rowNulls []bool
		if i < len(rightNulls) {
			rowNulls = rightNulls[i]
		}
		rightCounts[setOpRowKey(row, rowNulls)]++
	}

	seen := make(map[string]bool)
	var result [][]string
	var resultNulls [][]bool
	for i, row := range leftVals {
		var rowNulls []bool
		if i < len(leftNulls) {
			rowNulls = leftNulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		if rightCounts[key] > 0 {
			if isAll {
				rightCounts[key]--
				result = append(result, row)
				resultNulls = append(resultNulls, rowNulls)
			} else if !seen[key] {
				seen[key] = true
				result = append(result, row)
				resultNulls = append(resultNulls, rowNulls)
			}
		}
	}
	return result, resultNulls
}

// exceptRows returns rows from left minus rows from right.
// If isAll is true, preserves duplicates (subtracts counts).
// If isAll is false, returns distinct rows from left not in right.
func exceptRows(
	leftVals [][]string, leftNulls [][]bool,
	rightVals [][]string, rightNulls [][]bool,
	isAll bool,
) ([][]string, [][]bool) {
	rightCounts := make(map[string]int)
	for i, row := range rightVals {
		var rowNulls []bool
		if i < len(rightNulls) {
			rowNulls = rightNulls[i]
		}
		rightCounts[setOpRowKey(row, rowNulls)]++
	}

	seen := make(map[string]bool)
	var result [][]string
	var resultNulls [][]bool
	for i, row := range leftVals {
		var rowNulls []bool
		if i < len(leftNulls) {
			rowNulls = leftNulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		if isAll {
			if rightCounts[key] > 0 {
				rightCounts[key]--
			} else {
				result = append(result, row)
				resultNulls = append(resultNulls, rowNulls)
			}
		} else {
			if rightCounts[key] == 0 && !seen[key] {
				seen[key] = true
				result = append(result, row)
				resultNulls = append(resultNulls, rowNulls)
			}
		}
	}
	return result, resultNulls
}

// setOpFallback executes on shadow only as a fallback.
func (rh *ReadHandler) setOpFallback(sql string) ([]ColumnInfo, [][]string, [][]bool, error) {
	result, err := execQuery(rh.shadowConn, sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow set op fallback: %w", err)
	}
	if result.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}
	return result.Columns, result.RowValues, result.RowNulls, nil
}
