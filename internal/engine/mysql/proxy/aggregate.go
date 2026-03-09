package proxy

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/mori-dev/mori/internal/core"
)

// aggregateSpec describes a single aggregate function in a SELECT.
type aggregateSpec struct {
	funcName   string // "count", "sum", "avg", "min", "max"
	argCol     string // column argument (empty for COUNT(*))
	alias      string // output column name
	isDistinct bool
	isStar     bool // true for COUNT(*)
}

// groupBySpec holds parsed aggregate query information.
type groupBySpec struct {
	groupCols  []string
	aggregates []aggregateSpec
	baseSQL    string
}

// fullAggregateReadCore handles aggregate queries by merging at row level
// then re-computing aggregates in Go.
func (rh *ReadHandler) fullAggregateReadCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	if len(cl.Tables) == 0 {
		return nil, nil, nil, fmt.Errorf("aggregate read with no tables")
	}
	table := cl.Tables[0]

	spec := rh.buildGroupBySpecMySQL(querySQL, table)
	if spec == nil {
		return rh.materializeAndAggregateMySQL(cl, querySQL, table)
	}

	baseCl := *cl
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""

	baseCols, baseValues, baseNulls, err := rh.mergedReadCore(&baseCl, spec.baseSQL)
	if err != nil {
		return rh.materializeAndAggregateMySQL(cl, querySQL, table)
	}

	resultCols, resultValues, resultNulls := reAggregateRows(spec, baseCols, baseValues, baseNulls)

	havingExpr := extractHavingFromSQL(querySQL)
	if havingExpr != nil {
		resultValues, resultNulls = applyHavingFilter(havingExpr, resultCols, spec, resultValues, resultNulls)
	}

	if cl.OrderBy != "" {
		sortMerged(resultCols, resultValues, resultNulls, cl.OrderBy)
	}
	if cl.HasLimit && cl.Limit > 0 && len(resultValues) > cl.Limit {
		resultValues = resultValues[:cl.Limit]
		resultNulls = resultNulls[:cl.Limit]
	}

	return resultCols, resultValues, resultNulls, nil
}

// buildGroupBySpecMySQL parses a GROUP BY aggregate query using Vitess.
// Returns nil if the query can't be decomposed for in-Go re-aggregation.
func (rh *ReadHandler) buildGroupBySpecMySQL(sql, table string) *groupBySpec {
	parser, err := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	if err != nil {
		return nil
	}
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil
	}

	// Extract GROUP BY columns.
	var groupCols []string
	if sel.GroupBy != nil {
		for _, gb := range sel.GroupBy.Exprs {
			switch e := gb.(type) {
			case *sqlparser.ColName:
				groupCols = append(groupCols, e.Name.String())
			default:
				// Complex GROUP BY expression — can't decompose.
				return nil
			}
		}
	}

	// Extract aggregates from SELECT list.
	var aggregates []aggregateSpec
	neededCols := make(map[string]bool)
	for _, col := range groupCols {
		neededCols[col] = true
	}

	if sel.SelectExprs == nil {
		return nil
	}

	for _, expr := range sel.SelectExprs.Exprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}

		// Check if the expression is an aggregate function using the AggrFunc interface.
		aggrFunc, isAggr := aliasedExpr.Expr.(sqlparser.AggrFunc)
		if !isAggr {
			// Non-aggregate column (should be in GROUP BY).
			if col, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
				neededCols[col.Name.String()] = true
			}
			continue
		}

		funcName := aggrFunc.AggrName()

		// Check for unsupported aggregates.
		switch funcName {
		case "group_concat", "json_arrayagg", "json_objectagg":
			return nil // Fall to materialization.
		case "count", "sum", "avg", "min", "max":
			// Supported.
		default:
			return nil
		}

		agg := aggregateSpec{
			funcName: funcName,
		}

		// Determine alias.
		if !aliasedExpr.As.IsEmpty() {
			agg.alias = aliasedExpr.As.String()
		} else {
			agg.alias = sqlparser.String(aliasedExpr.Expr)
		}

		// Handle CountStar separately.
		if _, isStar := aliasedExpr.Expr.(*sqlparser.CountStar); isStar {
			agg.isStar = true
			agg.funcName = "count"
			aggregates = append(aggregates, agg)
			continue
		}

		// Extract distinct and argument from aggregate types.
		switch fn := aliasedExpr.Expr.(type) {
		case *sqlparser.Count:
			agg.isDistinct = fn.Distinct
			if len(fn.Args) > 0 {
				if col, ok := fn.Args[0].(*sqlparser.ColName); ok {
					agg.argCol = col.Name.String()
					neededCols[agg.argCol] = true
				} else {
					return nil // Complex argument expression.
				}
			}
		case *sqlparser.Sum:
			agg.isDistinct = fn.Distinct
			if fn.Arg != nil {
				if col, ok := fn.Arg.(*sqlparser.ColName); ok {
					agg.argCol = col.Name.String()
					neededCols[agg.argCol] = true
				} else {
					return nil
				}
			}
		case *sqlparser.Avg:
			agg.isDistinct = fn.Distinct
			if fn.Arg != nil {
				if col, ok := fn.Arg.(*sqlparser.ColName); ok {
					agg.argCol = col.Name.String()
					neededCols[agg.argCol] = true
				} else {
					return nil
				}
			}
		case *sqlparser.Min:
			if fn.Arg != nil {
				if col, ok := fn.Arg.(*sqlparser.ColName); ok {
					agg.argCol = col.Name.String()
					neededCols[agg.argCol] = true
				} else {
					return nil
				}
			}
		case *sqlparser.Max:
			if fn.Arg != nil {
				if col, ok := fn.Arg.(*sqlparser.ColName); ok {
					agg.argCol = col.Name.String()
					neededCols[agg.argCol] = true
				} else {
					return nil
				}
			}
		default:
			return nil
		}

		// Reject DISTINCT on non-COUNT.
		if agg.isDistinct && funcName != "count" {
			return nil
		}

		aggregates = append(aggregates, agg)
	}

	if len(aggregates) == 0 {
		return nil
	}

	// Build base SELECT: all needed columns, same FROM/WHERE, no GROUP BY/HAVING/ORDER/LIMIT.
	meta, ok := rh.tables[table]
	if ok && len(meta.PKColumns) > 0 {
		neededCols[meta.PKColumns[0]] = true
	}

	var colList []string
	for col := range neededCols {
		colList = append(colList, "`"+col+"`")
	}

	// Extract FROM + WHERE portion.
	upper := strings.ToUpper(sql)
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return nil
	}

	afterFrom := sql[fromIdx:]
	// Strip GROUP BY and everything after.
	stripped := stripGroupByAndAfterMySQL(afterFrom)

	baseSQL := "SELECT " + strings.Join(colList, ", ") + stripped

	return &groupBySpec{
		groupCols:  groupCols,
		aggregates: aggregates,
		baseSQL:    baseSQL,
	}
}

// stripGroupByAndAfterMySQL removes GROUP BY, HAVING, ORDER BY, LIMIT.
func stripGroupByAndAfterMySQL(sql string) string {
	upper := strings.ToUpper(sql)
	keywords := []string{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "}
	minIdx := len(sql)
	for _, kw := range keywords {
		if idx := strings.Index(upper, kw); idx >= 0 && idx < minIdx {
			minIdx = idx
		}
	}
	return sql[:minIdx]
}

// reAggregateRows groups rows and computes aggregates.
func reAggregateRows(spec *groupBySpec, columns []ColumnInfo, values [][]string, nulls [][]bool) (
	[]ColumnInfo, [][]string, [][]bool,
) {
	colIdx := make(map[string]int)
	for i, col := range columns {
		colIdx[strings.ToLower(col.Name)] = i
	}

	// Group rows by GROUP BY key.
	type group struct {
		key        string
		keyValues  []string
		rowIndices []int
	}
	groups := make(map[string]*group)
	var groupOrder []string

	for i, row := range values {
		var keyParts []string
		var keyValues []string
		for _, gc := range spec.groupCols {
			idx, ok := colIdx[strings.ToLower(gc)]
			if ok && idx < len(row) {
				keyParts = append(keyParts, row[idx])
				keyValues = append(keyValues, row[idx])
			} else {
				keyParts = append(keyParts, "\x00NULL")
				keyValues = append(keyValues, "")
			}
		}
		key := strings.Join(keyParts, "\x01")

		g, exists := groups[key]
		if !exists {
			g = &group{key: key, keyValues: keyValues}
			groups[key] = g
			groupOrder = append(groupOrder, key)
		}
		g.rowIndices = append(g.rowIndices, i)
	}

	// If no GROUP BY, treat all rows as one group.
	if len(spec.groupCols) == 0 && len(groups) == 0 {
		allIndices := make([]int, len(values))
		for i := range values {
			allIndices[i] = i
		}
		groups[""] = &group{key: "", rowIndices: allIndices}
		groupOrder = []string{""}
	}

	// Build result columns.
	var resultCols []ColumnInfo
	for _, gc := range spec.groupCols {
		resultCols = append(resultCols, ColumnInfo{Name: gc})
	}
	for _, agg := range spec.aggregates {
		resultCols = append(resultCols, ColumnInfo{Name: agg.alias})
	}

	// Compute aggregates per group.
	var resultValues [][]string
	var resultNulls [][]bool

	for _, key := range groupOrder {
		g := groups[key]
		var row []string
		var rowNulls []bool

		// Add GROUP BY values.
		for _, kv := range g.keyValues {
			row = append(row, kv)
			rowNulls = append(rowNulls, false)
		}

		// Compute each aggregate.
		for _, agg := range spec.aggregates {
			val, isNull := computeAggregateMySQL(agg, g.rowIndices, colIdx, values, nulls)
			row = append(row, val)
			rowNulls = append(rowNulls, isNull)
		}

		resultValues = append(resultValues, row)
		resultNulls = append(resultNulls, rowNulls)
	}

	return resultCols, resultValues, resultNulls
}

// computeAggregateMySQL computes a single aggregate over a set of row indices.
func computeAggregateMySQL(
	agg aggregateSpec,
	rowIndices []int,
	colIdx map[string]int,
	values [][]string,
	nulls [][]bool,
) (string, bool) {
	argIdx := -1
	if agg.argCol != "" {
		if idx, ok := colIdx[strings.ToLower(agg.argCol)]; ok {
			argIdx = idx
		}
	}

	switch agg.funcName {
	case "count":
		if agg.isStar {
			return strconv.Itoa(len(rowIndices)), false
		}
		if argIdx < 0 {
			return "0", false
		}
		if agg.isDistinct {
			seen := make(map[string]bool)
			for _, ri := range rowIndices {
				if ri < len(nulls) && argIdx < len(nulls[ri]) && nulls[ri][argIdx] {
					continue
				}
				if ri < len(values) && argIdx < len(values[ri]) {
					seen[values[ri][argIdx]] = true
				}
			}
			return strconv.Itoa(len(seen)), false
		}
		count := 0
		for _, ri := range rowIndices {
			if ri < len(nulls) && argIdx < len(nulls[ri]) && nulls[ri][argIdx] {
				continue
			}
			count++
		}
		return strconv.Itoa(count), false

	case "sum":
		if argIdx < 0 {
			return "", true
		}
		sum := 0.0
		hasValue := false
		for _, ri := range rowIndices {
			if ri < len(nulls) && argIdx < len(nulls[ri]) && nulls[ri][argIdx] {
				continue
			}
			if ri < len(values) && argIdx < len(values[ri]) {
				if n, err := strconv.ParseFloat(values[ri][argIdx], 64); err == nil {
					sum += n
					hasValue = true
				}
			}
		}
		if !hasValue {
			return "", true
		}
		return formatNumericMySQL(sum), false

	case "avg":
		if argIdx < 0 {
			return "", true
		}
		sum := 0.0
		count := 0
		for _, ri := range rowIndices {
			if ri < len(nulls) && argIdx < len(nulls[ri]) && nulls[ri][argIdx] {
				continue
			}
			if ri < len(values) && argIdx < len(values[ri]) {
				if n, err := strconv.ParseFloat(values[ri][argIdx], 64); err == nil {
					sum += n
					count++
				}
			}
		}
		if count == 0 {
			return "", true
		}
		avg := sum / float64(count)
		return formatNumericMySQL(avg), false

	case "min":
		if argIdx < 0 {
			return "", true
		}
		minVal := ""
		hasValue := false
		for _, ri := range rowIndices {
			if ri < len(nulls) && argIdx < len(nulls[ri]) && nulls[ri][argIdx] {
				continue
			}
			if ri < len(values) && argIdx < len(values[ri]) {
				val := values[ri][argIdx]
				if !hasValue || compareValues(val, minVal) < 0 {
					minVal = val
					hasValue = true
				}
			}
		}
		if !hasValue {
			return "", true
		}
		return minVal, false

	case "max":
		if argIdx < 0 {
			return "", true
		}
		maxVal := ""
		hasValue := false
		for _, ri := range rowIndices {
			if ri < len(nulls) && argIdx < len(nulls[ri]) && nulls[ri][argIdx] {
				continue
			}
			if ri < len(values) && argIdx < len(values[ri]) {
				val := values[ri][argIdx]
				if !hasValue || compareValues(val, maxVal) > 0 {
					maxVal = val
					hasValue = true
				}
			}
		}
		if !hasValue {
			return "", true
		}
		return maxVal, false
	}

	return "", true
}

// formatNumericMySQL formats a float64 for MySQL output.
func formatNumericMySQL(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// extractHavingFromSQL parses SQL to extract the HAVING clause.
func extractHavingFromSQL(sql string) sqlparser.Expr {
	parser, err := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	if err != nil {
		return nil
	}
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Having == nil {
		return nil
	}
	return sel.Having.Expr
}

// applyHavingFilter filters aggregated rows by evaluating the HAVING clause.
func applyHavingFilter(
	havingExpr sqlparser.Expr,
	columns []ColumnInfo,
	_ *groupBySpec,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	var filteredValues [][]string
	var filteredNulls [][]bool

	for i, row := range values {
		rowMap := make(map[string]string, len(columns))
		nullMap := make(map[string]bool, len(columns))
		for j, col := range columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
			}
			if j < len(nulls[i]) {
				nullMap[col.Name] = nulls[i][j]
			}
		}

		if evaluateWhere(havingExpr, rowMap, nullMap) {
			filteredValues = append(filteredValues, row)
			filteredNulls = append(filteredNulls, nulls[i])
		}
	}

	return filteredValues, filteredNulls
}

// materializeAndAggregateMySQL handles complex aggregates by materializing
// rows to a temp table and re-executing the query on Shadow.
func (rh *ReadHandler) materializeAndAggregateMySQL(
	cl *core.Classification, querySQL, table string,
) ([]ColumnInfo, [][]string, [][]bool, error) {
	// Build base SELECT: strip aggregates, GROUP BY, HAVING.
	baseSQL := buildMaterializationBaseQueryMySQL(querySQL)
	if baseSQL == "" {
		// Can't decompose — fall back to Shadow-only.
		result, err := execMySQLQuery(rh.shadowConn, querySQL)
		if err != nil {
			return nil, nil, nil, err
		}
		if result.Error != "" {
			return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
		}
		return result.Columns, result.RowValues, result.RowNulls, nil
	}

	utilName := utilTableName(querySQL)
	_, err := rh.materializeToUtilTable(baseSQL, utilName, 0)
	if err != nil {
		rh.logf("materialization failed, falling back to shadow: %v", err)
		result, qerr := execMySQLQuery(rh.shadowConn, querySQL)
		if qerr != nil {
			return nil, nil, nil, qerr
		}
		if result.Error != "" {
			return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
		}
		return result.Columns, result.RowValues, result.RowNulls, nil
	}
	defer dropUtilTable(rh.shadowConn, utilName)

	// Rewrite query to use temp table.
	rewritten := rewriteAggregateTableMySQL(querySQL, table, utilName)

	result, err := execMySQLQuery(rh.shadowConn, rewritten)
	if err != nil {
		return nil, nil, nil, err
	}
	if result.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}

	return result.Columns, result.RowValues, result.RowNulls, nil
}

// buildMaterializationBaseQueryMySQL strips GROUP BY/HAVING/ORDER BY/LIMIT
// from an aggregate query to get a base row-level SELECT.
func buildMaterializationBaseQueryMySQL(sql string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}
	afterFrom := sql[fromIdx:]
	stripped := stripGroupByAndAfterMySQL(afterFrom)

	return "SELECT *" + stripped
}

// rewriteAggregateTableMySQL replaces table references with the temp table name.
func rewriteAggregateTableMySQL(sql, originalTable, utilName string) string {
	// Simple approach: replace the table name in the FROM clause.
	result := sql
	// Try backtick-quoted replacement first.
	result = strings.Replace(result, "`"+originalTable+"`", "`"+utilName+"`", 1)
	if result == sql {
		// Try unquoted replacement.
		result = replaceColumnName(sql, originalTable, "`"+utilName+"`")
	}
	return result
}
