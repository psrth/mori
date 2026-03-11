package proxy

import (
	"fmt"
	"net"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/psrth/mori/internal/core"
)

// handleSetOperation handles UNION/INTERSECT/EXCEPT queries.
func (rh *ReadHandler) handleSetOperation(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.setOperationCore(cl, cl.RawSQL)
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

// setOperationCore decomposes set operations, executes leaves through merged
// read, and applies the set operation in memory.
func (rh *ReadHandler) setOperationCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	parser, _ := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	stmt, err := parser.Parse(querySQL)
	if err != nil {
		return rh.setOpFallback(querySQL)
	}

	union, ok := stmt.(*sqlparser.Union)
	if !ok {
		return rh.setOpFallback(querySQL)
	}

	// Execute left and right sides recursively.
	leftCols, leftVals, leftNulls, err := rh.executeSetOpSide(union.Left, cl)
	if err != nil {
		return rh.setOpFallback(querySQL)
	}
	rightCols, rightVals, rightNulls, err := rh.executeSetOpSide(union.Right, cl)
	if err != nil {
		return rh.setOpFallback(querySQL)
	}

	// Use left columns as canonical.
	columns := leftCols
	if len(columns) == 0 {
		columns = rightCols
	}

	// Apply set operation.
	var resultValues [][]string
	var resultNulls [][]bool

	// Distinct=true means UNION DISTINCT; Distinct=false means UNION ALL.
	isAll := !union.Distinct

	// Determine operation type from the SQL.
	upperSQL := strings.ToUpper(querySQL)
	if strings.Contains(upperSQL, " INTERSECT ") {
		resultValues, resultNulls = intersectRows(leftVals, leftNulls, rightVals, rightNulls, isAll)
	} else if strings.Contains(upperSQL, " EXCEPT ") {
		resultValues, resultNulls = exceptRows(leftVals, leftNulls, rightVals, rightNulls, isAll)
	} else {
		// UNION / UNION ALL
		resultValues = append(leftVals, rightVals...)
		resultNulls = append(leftNulls, rightNulls...)
		if !isAll {
			resultValues, resultNulls = deduplicateByFullRow(resultValues, resultNulls)
		}
	}

	// Apply ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(columns, resultValues, resultNulls, cl.OrderBy)
	}
	// Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(resultValues) > cl.Limit {
		resultValues = resultValues[:cl.Limit]
		resultNulls = resultNulls[:cl.Limit]
	}

	return columns, resultValues, resultNulls, nil
}

// executeSetOpSide executes one side of a set operation through merged read.
func (rh *ReadHandler) executeSetOpSide(
	stmt sqlparser.TableStatement, cl *core.Classification,
) ([]ColumnInfo, [][]string, [][]bool, error) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		leafSQL := sqlparser.String(s)
		leafCl := *cl
		leafCl.HasSetOp = false
		leafCl.HasLimit = false
		leafCl.Limit = 0
		leafCl.OrderBy = ""
		leafCl.RawSQL = leafSQL
		// Extract tables from the leaf select.
		leafCl.Tables = nil
		for _, from := range s.From {
			if t, ok := from.(*sqlparser.AliasedTableExpr); ok {
				if tn, ok := t.Expr.(sqlparser.TableName); ok {
					leafCl.Tables = append(leafCl.Tables, tn.Name.String())
				}
			}
		}
		return rh.mergedReadCore(&leafCl, leafSQL)

	case *sqlparser.Union:
		// Recursive union — process both sides.
		leftCols, leftVals, leftNulls, err := rh.executeSetOpSide(s.Left, cl)
		if err != nil {
			return nil, nil, nil, err
		}
		rightCols, rightVals, rightNulls, err := rh.executeSetOpSide(s.Right, cl)
		if err != nil {
			return nil, nil, nil, err
		}
		_ = rightCols

		resultValues := append(leftVals, rightVals...)
		resultNulls := append(leftNulls, rightNulls...)

		isAll := !s.Distinct
		if !isAll {
			resultValues, resultNulls = deduplicateByFullRow(resultValues, resultNulls)
		}

		return leftCols, resultValues, resultNulls, nil

	default:
		return nil, nil, nil, fmt.Errorf("unsupported set operation side type")
	}
}

// setOpRowKey creates a string key from row values for deduplication.
func setOpRowKey(row []string, nulls []bool) string {
	parts := make([]string, len(row))
	for i, v := range row {
		if i < len(nulls) && nulls[i] {
			parts[i] = "\x00NULL"
		} else {
			parts[i] = v
		}
	}
	return strings.Join(parts, "\x01")
}

// deduplicateByFullRow removes duplicate rows (used for UNION without ALL).
func deduplicateByFullRow(values [][]string, nulls [][]bool) ([][]string, [][]bool) {
	seen := make(map[string]bool)
	var dedupValues [][]string
	var dedupNulls [][]bool

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
		dedupValues = append(dedupValues, row)
		dedupNulls = append(dedupNulls, rowNulls)
	}

	return dedupValues, dedupNulls
}

// intersectRows returns rows appearing in both left and right.
func intersectRows(
	leftVals [][]string, leftNulls [][]bool,
	rightVals [][]string, rightNulls [][]bool,
	isAll bool,
) ([][]string, [][]bool) {
	// Build count map from right side.
	rightCounts := make(map[string]int)
	for i, row := range rightVals {
		var rowNulls []bool
		if i < len(rightNulls) {
			rowNulls = rightNulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		rightCounts[key]++
	}

	var resultValues [][]string
	var resultNulls [][]bool

	for i, row := range leftVals {
		var rowNulls []bool
		if i < len(leftNulls) {
			rowNulls = leftNulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		count := rightCounts[key]
		if count > 0 {
			resultValues = append(resultValues, row)
			resultNulls = append(resultNulls, rowNulls)
			if isAll {
				rightCounts[key]--
			} else {
				rightCounts[key] = 0 // Only include once.
			}
		}
	}

	return resultValues, resultNulls
}

// exceptRows returns left rows minus right rows.
func exceptRows(
	leftVals [][]string, leftNulls [][]bool,
	rightVals [][]string, rightNulls [][]bool,
	isAll bool,
) ([][]string, [][]bool) {
	// Build count map from right side.
	rightCounts := make(map[string]int)
	for i, row := range rightVals {
		var rowNulls []bool
		if i < len(rightNulls) {
			rowNulls = rightNulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		rightCounts[key]++
	}

	var resultValues [][]string
	var resultNulls [][]bool

	seen := make(map[string]bool)
	for i, row := range leftVals {
		var rowNulls []bool
		if i < len(leftNulls) {
			rowNulls = leftNulls[i]
		}
		key := setOpRowKey(row, rowNulls)
		count := rightCounts[key]
		if count > 0 {
			if isAll {
				rightCounts[key]--
			}
			continue // Skip this row.
		}
		if !isAll {
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		resultValues = append(resultValues, row)
		resultNulls = append(resultNulls, rowNulls)
	}

	return resultValues, resultNulls
}

// setOpFallback executes the query on Shadow only.
func (rh *ReadHandler) setOpFallback(sql string) ([]ColumnInfo, [][]string, [][]bool, error) {
	result, err := execMySQLQuery(rh.shadowConn, sql)
	if err != nil {
		return nil, nil, nil, err
	}
	if result.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}
	return result.Columns, result.RowValues, result.RowNulls, nil
}
