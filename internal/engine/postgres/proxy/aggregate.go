package proxy

import (
	"math"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// aggregateSpec describes a single aggregate expression in a GROUP BY query.
type aggregateSpec struct {
	FuncName  string // e.g., "count", "sum", "avg", "min", "max"
	ArgCol    string // column argument (empty for COUNT(*))
	Alias     string // output column name
	IsDistinct bool
	IsStar    bool // true for COUNT(*)
}

// groupBySpec describes the full GROUP BY decomposition of a query.
type groupBySpec struct {
	GroupCols  []string        // GROUP BY column names
	Aggregates []aggregateSpec // Aggregate expressions in SELECT
	BaseSQL    string          // SELECT pk, group_cols, agg_source_cols FROM ... WHERE ...
	HavingAST  *pg_query.Node  // HAVING clause (nil if none)
}

// buildGroupBySpec parses a GROUP BY aggregate query and returns a spec
// for row-level merge + re-aggregation. Returns nil if the query can't
// be decomposed (e.g., complex expressions in GROUP BY, unsupported aggregates).
func (rh *ReadHandler) buildGroupBySpec(sql, table string) *groupBySpec {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return nil
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return nil
	}

	// Must have GROUP BY.
	if len(sel.GetGroupClause()) == 0 {
		return nil
	}

	// Find PK column.
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return nil
	}
	pkCol := meta.PKColumns[0]

	// Extract GROUP BY column names.
	var groupCols []string
	for _, gc := range sel.GetGroupClause() {
		colName := resolveNodeColumnName(gc)
		if colName == "" {
			return nil // Complex GROUP BY expression — unsupported.
		}
		groupCols = append(groupCols, colName)
	}

	// Extract aggregate expressions from SELECT target list.
	var aggregates []aggregateSpec
	needCols := make(map[string]bool) // Source columns needed for aggregation.
	for _, col := range groupCols {
		needCols[strings.ToLower(col)] = true
	}

	for _, target := range sel.GetTargetList() {
		rt := target.GetResTarget()
		if rt == nil {
			continue
		}
		val := rt.GetVal()
		if val == nil {
			continue
		}

		// Determine output alias.
		alias := rt.GetName()

		// Check if this is an aggregate function call.
		if fc := val.GetFuncCall(); fc != nil {
			spec := parseAggregateFunc(fc)
			if spec == nil {
				return nil // Unsupported aggregate.
			}
			if alias != "" {
				spec.Alias = alias
			} else if spec.Alias == "" {
				spec.Alias = spec.FuncName
			}
			aggregates = append(aggregates, *spec)
			if spec.ArgCol != "" {
				needCols[strings.ToLower(spec.ArgCol)] = true
			}
			continue
		}

		// Non-aggregate column in SELECT — should be in GROUP BY.
		colName := resolveNodeColumnName(val)
		if colName == "" {
			return nil // Complex expression in SELECT — unsupported.
		}
		needCols[strings.ToLower(colName)] = true
	}

	if len(aggregates) == 0 {
		return nil // No aggregates found — shouldn't be handled here.
	}

	// Build base SELECT: pk + all needed columns.
	var selectParts []string
	selectParts = append(selectParts, quoteIdent(pkCol))
	added := map[string]bool{strings.ToLower(pkCol): true}
	for col := range needCols {
		if added[col] {
			continue
		}
		selectParts = append(selectParts, quoteIdent(col))
		added[col] = true
	}

	// Reconstruct FROM + WHERE from the original query.
	fromIdx := findFromPosition(sql)
	if fromIdx < 0 {
		return nil
	}
	rest := sql[fromIdx:]
	// Strip GROUP BY, HAVING, ORDER BY, LIMIT from the rest.
	rest = stripGroupByAndAfter(rest)

	baseSQL := "SELECT " + strings.Join(selectParts, ", ") + " " + rest

	return &groupBySpec{
		GroupCols:  groupCols,
		Aggregates: aggregates,
		BaseSQL:    baseSQL,
		HavingAST:  sel.GetHavingClause(),
	}
}

// parseAggregateFunc extracts an aggregateSpec from a FuncCall node.
// Returns nil for unsupported aggregate functions.
func parseAggregateFunc(fc *pg_query.FuncCall) *aggregateSpec {
	var funcName string
	for _, nameNode := range fc.GetFuncname() {
		if s := nameNode.GetString_(); s != nil {
			funcName = strings.ToLower(s.GetSval())
		}
	}

	switch funcName {
	case "count", "sum", "avg", "min", "max":
		// OK — supported.
	default:
		return nil // Unsupported aggregate.
	}

	spec := &aggregateSpec{
		FuncName:   funcName,
		IsDistinct: fc.GetAggDistinct(),
	}

	args := fc.GetArgs()
	if fc.GetAggStar() {
		spec.IsStar = true
		spec.Alias = funcName
	} else if len(args) == 1 {
		colName := resolveNodeColumnName(args[0])
		if colName == "" {
			return nil // Complex expression as aggregate argument.
		}
		spec.ArgCol = colName
		spec.Alias = funcName
	} else if len(args) == 0 && funcName == "count" {
		spec.IsStar = true
		spec.Alias = "count"
	} else {
		return nil // Multi-arg aggregate — unsupported.
	}

	return spec
}

// reAggregateRows groups merged row-level results by GROUP BY columns and
// computes aggregate values per group.
func reAggregateRows(
	spec *groupBySpec,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([]ColumnInfo, [][]string, [][]bool) {
	// Build column index.
	colIdx := make(map[string]int)
	for i, c := range columns {
		colIdx[strings.ToLower(c.Name)] = i
	}

	// Group rows by GROUP BY columns.
	type group struct {
		key    string
		keyVals []string
		rows   []int // indices into values
	}
	groups := make(map[string]*group)
	var groupOrder []string

	for i, row := range values {
		var keyParts []string
		var keyVals []string
		for _, gc := range spec.GroupCols {
			idx, ok := colIdx[strings.ToLower(gc)]
			if !ok || idx >= len(row) {
				keyParts = append(keyParts, "\x00NULL")
				keyVals = append(keyVals, "")
			} else if nulls[i][idx] {
				keyParts = append(keyParts, "\x00NULL")
				keyVals = append(keyVals, "")
			} else {
				keyParts = append(keyParts, row[idx])
				keyVals = append(keyVals, row[idx])
			}
		}
		key := strings.Join(keyParts, "\x01")
		if g, ok := groups[key]; ok {
			g.rows = append(g.rows, i)
		} else {
			groups[key] = &group{key: key, keyVals: keyVals, rows: []int{i}}
			groupOrder = append(groupOrder, key)
		}
	}

	// Build result columns: group columns + aggregate columns.
	var resultCols []ColumnInfo
	for _, gc := range spec.GroupCols {
		oid := uint32(25) // text
		if idx, ok := colIdx[strings.ToLower(gc)]; ok {
			oid = columns[idx].OID
		}
		resultCols = append(resultCols, ColumnInfo{Name: gc, OID: oid})
	}
	for _, agg := range spec.Aggregates {
		oid := uint32(20) // int8 for counts
		switch agg.FuncName {
		case "sum", "avg":
			oid = 1700 // numeric
		case "min", "max":
			if agg.ArgCol != "" {
				if idx, ok := colIdx[strings.ToLower(agg.ArgCol)]; ok {
					oid = columns[idx].OID
				}
			}
		}
		resultCols = append(resultCols, ColumnInfo{Name: agg.Alias, OID: oid})
	}

	// Compute aggregates per group.
	var resultValues [][]string
	var resultNulls [][]bool

	for _, key := range groupOrder {
		g := groups[key]
		var row []string
		var rowNulls []bool

		// Add group column values.
		for j := range spec.GroupCols {
			if j < len(g.keyVals) {
				row = append(row, g.keyVals[j])
				rowNulls = append(rowNulls, g.keyVals[j] == "" && strings.Contains(g.key, "\x00NULL"))
			} else {
				row = append(row, "")
				rowNulls = append(rowNulls, true)
			}
		}

		// Compute each aggregate.
		for _, agg := range spec.Aggregates {
			val, isNull := computeAggregate(agg, g.rows, colIdx, values, nulls)
			row = append(row, val)
			rowNulls = append(rowNulls, isNull)
		}

		resultValues = append(resultValues, row)
		resultNulls = append(resultNulls, rowNulls)
	}

	return resultCols, resultValues, resultNulls
}

// computeAggregate computes a single aggregate over a set of row indices.
func computeAggregate(
	agg aggregateSpec,
	rowIndices []int,
	colIdx map[string]int,
	values [][]string,
	nulls [][]bool,
) (string, bool) {
	switch agg.FuncName {
	case "count":
		if agg.IsStar {
			return strconv.Itoa(len(rowIndices)), false
		}
		idx, ok := colIdx[strings.ToLower(agg.ArgCol)]
		if !ok {
			return "0", false
		}
		count := 0
		seen := make(map[string]bool)
		for _, ri := range rowIndices {
			if idx < len(nulls[ri]) && nulls[ri][idx] {
				continue // NULL values don't count.
			}
			if agg.IsDistinct {
				val := values[ri][idx]
				if seen[val] {
					continue
				}
				seen[val] = true
			}
			count++
		}
		return strconv.Itoa(count), false

	case "sum":
		idx, ok := colIdx[strings.ToLower(agg.ArgCol)]
		if !ok {
			return "", true
		}
		sum := 0.0
		hasValue := false
		for _, ri := range rowIndices {
			if idx < len(nulls[ri]) && nulls[ri][idx] {
				continue
			}
			v, err := strconv.ParseFloat(values[ri][idx], 64)
			if err != nil {
				continue
			}
			sum += v
			hasValue = true
		}
		if !hasValue {
			return "", true
		}
		return formatNumeric(sum), false

	case "avg":
		idx, ok := colIdx[strings.ToLower(agg.ArgCol)]
		if !ok {
			return "", true
		}
		sum := 0.0
		count := 0
		for _, ri := range rowIndices {
			if idx < len(nulls[ri]) && nulls[ri][idx] {
				continue
			}
			v, err := strconv.ParseFloat(values[ri][idx], 64)
			if err != nil {
				continue
			}
			sum += v
			count++
		}
		if count == 0 {
			return "", true
		}
		return formatNumeric(sum / float64(count)), false

	case "min":
		idx, ok := colIdx[strings.ToLower(agg.ArgCol)]
		if !ok {
			return "", true
		}
		var minVal string
		hasValue := false
		for _, ri := range rowIndices {
			if idx < len(nulls[ri]) && nulls[ri][idx] {
				continue
			}
			v := values[ri][idx]
			if !hasValue || compareValues(v, minVal) < 0 {
				minVal = v
				hasValue = true
			}
		}
		if !hasValue {
			return "", true
		}
		return minVal, false

	case "max":
		idx, ok := colIdx[strings.ToLower(agg.ArgCol)]
		if !ok {
			return "", true
		}
		var maxVal string
		hasValue := false
		for _, ri := range rowIndices {
			if idx < len(nulls[ri]) && nulls[ri][idx] {
				continue
			}
			v := values[ri][idx]
			if !hasValue || compareValues(v, maxVal) > 0 {
				maxVal = v
				hasValue = true
			}
		}
		if !hasValue {
			return "", true
		}
		return maxVal, false
	}

	return "", true
}

// formatNumeric formats a float64 as a string, using integer format when possible.
func formatNumeric(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// findFromPosition returns the byte position of " FROM " in a SQL string (case-insensitive).
func findFromPosition(sql string) int {
	upper := strings.ToUpper(sql)
	return strings.Index(upper, " FROM ")
}

// stripGroupByAndAfter removes GROUP BY, HAVING, ORDER BY, and LIMIT clauses
// from a SQL fragment (everything after FROM ... WHERE ...).
func stripGroupByAndAfter(sql string) string {
	upper := strings.ToUpper(sql)

	// Find the earliest of GROUP BY, HAVING, ORDER BY, LIMIT.
	cutoff := len(sql)
	for _, keyword := range []string{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "} {
		idx := strings.Index(upper, keyword)
		if idx >= 0 && idx < cutoff {
			cutoff = idx
		}
	}

	return strings.TrimSpace(sql[:cutoff])
}

// evaluateHaving evaluates a HAVING clause against an aggregated row.
// Returns true if the row satisfies HAVING or if evaluation fails (safe fallback).
func evaluateHaving(havingAST *pg_query.Node, row map[string]string) bool {
	if havingAST == nil {
		return true
	}
	// Use the where evaluator with null map (no nulls in aggregated results).
	nullMap := make(map[string]bool)
	return evaluateWhere(havingAST, row, nullMap)
}
