package proxy

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/psrth/mori/internal/core"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// aggregateSpec describes a single aggregate expression in a GROUP BY query.
type aggregateSpec struct {
	FuncName     string          // e.g., "count", "sum", "avg", "min", "max"
	ArgCol       string          // column argument (empty for COUNT(*))
	Alias        string          // output column name
	IsDistinct   bool
	IsStar       bool            // true for COUNT(*)
	ArgExpr      *pg_query.Node  // raw argument node if expression (non-simple column)
	ArgExprAlias string          // alias assigned to the expression argument
}

// groupBySpec describes the full GROUP BY decomposition of a query.
type groupBySpec struct {
	GroupCols  []string        // GROUP BY column names
	Aggregates []aggregateSpec // Aggregate expressions in SELECT
	BaseSQL    string          // SELECT pk, group_cols, agg_source_cols FROM ... WHERE ...
	HavingAST  *pg_query.Node  // HAVING clause (nil if none)
}

// exprAlias tracks an expression that needs to be aliased in the base SELECT.
type exprAlias struct {
	exprSQL string // the SQL text of the expression
	alias   string // the alias to use
}

// buildGroupBySpec parses a GROUP BY aggregate query and returns a spec
// for row-level merge + re-aggregation. Returns nil if the query can't
// be decomposed (e.g., unsupported aggregates).
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

	// Track expression aliases for GROUP BY keys and aggregate arguments.
	var exprAliases []exprAlias
	grpAliasCounter := 0
	aggAliasCounter := 0

	// Extract GROUP BY column names, supporting expression keys.
	var groupCols []string
	for _, gc := range sel.GetGroupClause() {
		colName := resolveNodeColumnName(gc)
		if colName != "" {
			groupCols = append(groupCols, colName)
			continue
		}

		// Expression GROUP BY key: deparse and alias it.
		exprText := deparseNode(gc)
		if exprText == "" {
			return nil // Can't deparse — unsupported.
		}
		alias := fmt.Sprintf("_mori_grp_%d", grpAliasCounter)
		grpAliasCounter++
		exprAliases = append(exprAliases, exprAlias{exprSQL: exprText, alias: alias})
		groupCols = append(groupCols, alias)
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
			// DISTINCT on non-COUNT aggregates (SUM, AVG) can't be re-aggregated
			// in Go — fall through to materialize-then-reexecute path.
			if spec.IsDistinct && spec.FuncName != "count" {
				return nil
			}
			if alias != "" {
				spec.Alias = alias
			} else if spec.Alias == "" {
				spec.Alias = spec.FuncName
			}

			// Handle expression aggregate arguments.
			if spec.ArgExpr != nil && spec.ArgCol == "" {
				exprText := deparseNode(spec.ArgExpr)
				if exprText == "" {
					return nil // Can't deparse arg expression.
				}
				argAlias := fmt.Sprintf("_mori_agg_%d", aggAliasCounter)
				aggAliasCounter++
				spec.ArgCol = argAlias
				spec.ArgExprAlias = argAlias
				exprAliases = append(exprAliases, exprAlias{exprSQL: exprText, alias: argAlias})
				needCols[strings.ToLower(argAlias)] = true
			} else if spec.ArgCol != "" {
				needCols[strings.ToLower(spec.ArgCol)] = true
			}

			aggregates = append(aggregates, *spec)
			continue
		}

		// Non-aggregate column in SELECT — should be in GROUP BY.
		colName := resolveNodeColumnName(val)
		if colName == "" {
			// Expression in SELECT that is not an aggregate — may be a GROUP BY expression.
			// Check if it matches one of our expression aliases.
			exprText := deparseNode(val)
			if exprText == "" {
				return nil
			}
			// Check if this expression text matches any GROUP BY expression alias.
			found := false
			for _, ea := range exprAliases {
				if ea.exprSQL == exprText {
					needCols[strings.ToLower(ea.alias)] = true
					found = true
					break
				}
			}
			if !found {
				return nil // Unrecognized expression in SELECT.
			}
			continue
		}
		needCols[strings.ToLower(colName)] = true
	}

	if len(aggregates) == 0 {
		return nil // No aggregates found — shouldn't be handled here.
	}

	// Pull HAVING-referenced columns.
	if sel.GetHavingClause() != nil {
		havingCols := collectHavingColumns(sel.GetHavingClause())
		for _, hc := range havingCols {
			needCols[strings.ToLower(hc)] = true
		}
	}

	// Build set of expression alias names for lookup.
	exprAliasSet := make(map[string]string) // alias -> exprSQL
	for _, ea := range exprAliases {
		exprAliasSet[strings.ToLower(ea.alias)] = ea.exprSQL
	}

	// Build base SELECT: pk + all needed columns (with expression aliases).
	var selectParts []string
	selectParts = append(selectParts, quoteIdent(pkCol))
	added := map[string]bool{strings.ToLower(pkCol): true}
	for col := range needCols {
		if added[col] {
			continue
		}
		if exprSQL, isExpr := exprAliasSet[col]; isExpr {
			// Expression with alias.
			selectParts = append(selectParts, exprSQL+" AS "+quoteIdent(col))
		} else {
			selectParts = append(selectParts, quoteIdent(col))
		}
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

// deparseNode deparses a single AST node to SQL text.
// It wraps the node in a minimal SELECT statement, deparses it, and extracts
// the expression text.
func deparseNode(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	// Wrap the node as a single target in a SELECT.
	selStmt := &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{Node: &pg_query.Node_ResTarget{ResTarget: &pg_query.ResTarget{
				Val: node,
			}}},
		},
	}
	parseResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: &pg_query.Node{
			Node: &pg_query.Node_SelectStmt{SelectStmt: selStmt},
		}}},
	}
	deparsed, err := pg_query.Deparse(parseResult)
	if err != nil {
		return ""
	}
	// deparsed is "SELECT <expr>". Extract just <expr>.
	upper := strings.ToUpper(deparsed)
	selectIdx := strings.Index(upper, "SELECT ")
	if selectIdx < 0 {
		return ""
	}
	return strings.TrimSpace(deparsed[selectIdx+7:])
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
		if colName != "" {
			spec.ArgCol = colName
		} else {
			// Expression argument — store the node for later processing.
			spec.ArgExpr = args[0]
		}
		spec.Alias = funcName
	} else if len(args) == 0 && funcName == "count" {
		spec.IsStar = true
		spec.Alias = "count"
	} else {
		return nil // Multi-arg aggregate — unsupported.
	}

	return spec
}

// collectHavingColumns recursively walks a HAVING AST and collects all
// column reference names.
func collectHavingColumns(node *pg_query.Node) []string {
	if node == nil {
		return nil
	}

	var cols []string

	// Check if this node is a ColumnRef.
	if cr := node.GetColumnRef(); cr != nil {
		name := resolveNodeColumnName(node)
		if name != "" {
			cols = append(cols, name)
		}
		return cols
	}

	// BoolExpr: walk args.
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			cols = append(cols, collectHavingColumns(arg)...)
		}
		return cols
	}

	// A_Expr: walk lexpr and rexpr.
	if ae := node.GetAExpr(); ae != nil {
		cols = append(cols, collectHavingColumns(ae.GetLexpr())...)
		cols = append(cols, collectHavingColumns(ae.GetRexpr())...)
		return cols
	}

	// FuncCall: walk args.
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			cols = append(cols, collectHavingColumns(arg)...)
		}
		return cols
	}

	// NullTest: walk arg.
	if nt := node.GetNullTest(); nt != nil {
		cols = append(cols, collectHavingColumns(nt.GetArg())...)
		return cols
	}

	// TypeCast: walk arg.
	if tc := node.GetTypeCast(); tc != nil {
		cols = append(cols, collectHavingColumns(tc.GetArg())...)
		return cols
	}

	// List: walk items.
	if list := node.GetList(); list != nil {
		for _, item := range list.GetItems() {
			cols = append(cols, collectHavingColumns(item)...)
		}
		return cols
	}

	return cols
}

// normalizeHavingForEval replaces FuncCall nodes in a HAVING AST that match
// known aggregates with ColumnRef nodes pointing to the aggregate's alias.
// Returns a deep copy of the AST with replacements applied.
func normalizeHavingForEval(node *pg_query.Node, aggregates []aggregateSpec) *pg_query.Node {
	if node == nil {
		return nil
	}

	// Deep copy the node to avoid mutating the original.
	cloned := proto.Clone(node).(*pg_query.Node)
	normalizeHavingInPlace(cloned, aggregates)
	return cloned
}

// normalizeHavingInPlace recursively walks the AST and replaces FuncCall nodes
// that match aggregates with ColumnRef nodes in-place.
func normalizeHavingInPlace(node *pg_query.Node, aggregates []aggregateSpec) {
	if node == nil {
		return
	}

	// BoolExpr: process each arg.
	if be := node.GetBoolExpr(); be != nil {
		for i, arg := range be.GetArgs() {
			replaced := tryReplaceFuncCall(arg, aggregates)
			if replaced != nil {
				be.Args[i] = replaced
			} else {
				normalizeHavingInPlace(arg, aggregates)
			}
		}
		return
	}

	// A_Expr: process lexpr and rexpr.
	if ae := node.GetAExpr(); ae != nil {
		if replaced := tryReplaceFuncCall(ae.GetLexpr(), aggregates); replaced != nil {
			ae.Lexpr = replaced
		} else {
			normalizeHavingInPlace(ae.GetLexpr(), aggregates)
		}
		if replaced := tryReplaceFuncCall(ae.GetRexpr(), aggregates); replaced != nil {
			ae.Rexpr = replaced
		} else {
			normalizeHavingInPlace(ae.GetRexpr(), aggregates)
		}
		return
	}

	// NullTest: process arg.
	if nt := node.GetNullTest(); nt != nil {
		if replaced := tryReplaceFuncCall(nt.GetArg(), aggregates); replaced != nil {
			nt.Arg = replaced
		} else {
			normalizeHavingInPlace(nt.GetArg(), aggregates)
		}
		return
	}
}

// tryReplaceFuncCall checks if the node is a FuncCall matching one of the
// aggregates and returns a ColumnRef node pointing to the aggregate's alias.
// Returns nil if no match.
func tryReplaceFuncCall(node *pg_query.Node, aggregates []aggregateSpec) *pg_query.Node {
	if node == nil {
		return nil
	}
	fc := node.GetFuncCall()
	if fc == nil {
		return nil
	}

	// Extract function name.
	var funcName string
	for _, nameNode := range fc.GetFuncname() {
		if s := nameNode.GetString_(); s != nil {
			funcName = strings.ToLower(s.GetSval())
		}
	}

	// Determine argument info.
	isStar := fc.GetAggStar() || (len(fc.GetArgs()) == 0 && funcName == "count")
	var argCol string
	if !isStar && len(fc.GetArgs()) == 1 {
		argCol = resolveNodeColumnName(fc.GetArgs()[0])
	}

	// Match against aggregates.
	for _, agg := range aggregates {
		if strings.ToLower(agg.FuncName) != funcName {
			continue
		}
		if agg.IsStar && isStar {
			return makeColumnRefNode(agg.Alias)
		}
		if !agg.IsStar && !isStar && strings.EqualFold(agg.ArgCol, argCol) {
			return makeColumnRefNode(agg.Alias)
		}
	}

	return nil
}

// makeColumnRefNode creates a ColumnRef AST node with the given column name.
func makeColumnRefNode(colName string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_ColumnRef{
			ColumnRef: &pg_query.ColumnRef{
				Fields: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: colName}}},
				},
			},
		},
	}
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
			// Determine OID for type-aware MIN/MAX.
			var argOID uint32
			if agg.ArgCol != "" {
				if idx, ok := colIdx[strings.ToLower(agg.ArgCol)]; ok && idx < len(columns) {
					argOID = columns[idx].OID
				}
			}
			val, isNull := computeAggregate(agg, g.rows, colIdx, values, nulls, argOID)
			row = append(row, val)
			rowNulls = append(rowNulls, isNull)
		}

		resultValues = append(resultValues, row)
		resultNulls = append(resultNulls, rowNulls)
	}

	return resultCols, resultValues, resultNulls
}

// computeAggregate computes a single aggregate over a set of row indices.
// The argOID parameter is used for type-aware MIN/MAX comparison.
func computeAggregate(
	agg aggregateSpec,
	rowIndices []int,
	colIdx map[string]int,
	values [][]string,
	nulls [][]bool,
	argOID uint32,
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
			if !hasValue || compareValuesTyped(v, minVal, argOID) < 0 {
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
			if !hasValue || compareValuesTyped(v, maxVal, argOID) > 0 {
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

// materializeAndAggregate handles aggregate queries that can't be decomposed
// by materializing the base data into a temp table and re-executing the
// original aggregate query against it. This lets PostgreSQL handle complex
// aggregates (array_agg, json_agg, string_agg, custom) natively.
func (rh *ReadHandler) materializeAndAggregate(
	cl *core.Classification,
	querySQL string,
	table string,
) ([]ColumnInfo, [][]string, [][]bool, error) {
	// Build a base SELECT * FROM table WHERE <same conditions> query.
	// This fetches all source rows without the aggregate.
	baseSQL := buildMaterializationBaseQuery(querySQL, table)
	if baseSQL == "" {
		return nil, nil, nil, fmt.Errorf("cannot build base query for materialization")
	}

	// Run the base query through merged read.
	cappedBaseSQL := rh.capSQL(baseSQL)
	baseCl := &core.Classification{
		Tables: cl.Tables,
		RawSQL: cappedBaseSQL,
	}

	baseCols, baseValues, baseNulls, err := rh.mergedReadCore(baseCl, cappedBaseSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merged read for aggregate materialization: %w", err)
	}

	if len(baseCols) == 0 {
		return nil, nil, nil, fmt.Errorf("no columns from base query")
	}

	// Materialize into temp table.
	utilName := utilTableName(querySQL + "_agg")
	createSQL := buildCreateTempTableSQL(utilName, baseCols)
	result, err := execQuery(rh.shadowConn, createSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating temp table: %w", err)
	}
	if result.Error != "" {
		// Table might already exist from a previous call in the same session.
		// Drop and recreate.
		dropUtilTable(rh.shadowConn, utilName)
		if _, err := execQuery(rh.shadowConn, createSQL); err != nil {
			return nil, nil, nil, fmt.Errorf("recreating temp table: %w", err)
		}
	}
	defer dropUtilTable(rh.shadowConn, utilName)

	if len(baseValues) > 0 {
		if err := bulkInsertToUtilTable(rh.shadowConn, utilName, baseCols, baseValues, baseNulls); err != nil {
			return nil, nil, nil, fmt.Errorf("populating temp table: %w", err)
		}
	}

	if rh.verbose {
		log.Printf("[conn %d] aggregate: materialized %d rows into %s", rh.connID, len(baseValues), utilName)
	}

	// Rewrite the original aggregate query to reference the temp table.
	rewrittenSQL := rewriteAggregateTable(querySQL, table, utilName)

	// Execute the rewritten aggregate query on shadow.
	aggResult, err := execQuery(rh.shadowConn, rewrittenSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow aggregate on temp table: %w", err)
	}
	if aggResult.Error != "" {
		return nil, nil, nil, fmt.Errorf("shadow aggregate error: %s", aggResult.Error)
	}

	return aggResult.Columns, aggResult.RowValues, aggResult.RowNulls, nil
}

// buildMaterializationBaseQuery builds a SELECT * FROM table WHERE <conditions>
// query from an aggregate query, stripping GROUP BY, HAVING, ORDER BY, LIMIT,
// and the aggregate functions from SELECT.
func buildMaterializationBaseQuery(sql, table string) string {
	// Parse the query.
	result, err := pg_query.Parse(sql)
	if err != nil {
		return ""
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return ""
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return ""
	}

	// Build SELECT * FROM <table> WHERE <same where clause>.
	// Strip GROUP BY, HAVING, ORDER BY, LIMIT — keep only FROM and WHERE.
	newSel := &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Val: &pg_query.Node{
							Node: &pg_query.Node_ColumnRef{
								ColumnRef: &pg_query.ColumnRef{
									Fields: []*pg_query.Node{
										{Node: &pg_query.Node_AStar{AStar: &pg_query.A_Star{}}},
									},
								},
							},
						},
					},
				},
			},
		},
		FromClause:  sel.GetFromClause(),
		WhereClause: sel.GetWhereClause(),
	}

	deparseResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: newSel,
					},
				},
			},
		},
	}

	baseSQL, err := pg_query.Deparse(deparseResult)
	if err != nil {
		return ""
	}
	return baseSQL
}

// rewriteAggregateTable replaces the table name in an aggregate query
// with the temp table name using AST manipulation.
func rewriteAggregateTable(sql, originalTable, utilName string) string {
	result, err := pg_query.Parse(sql)
	if err != nil {
		// Fallback: string replacement.
		return strings.Replace(sql, originalTable, utilName, 1)
	}

	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return sql
	}

	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return sql
	}

	// Replace table references in FROM clause.
	for _, fromNode := range sel.GetFromClause() {
		replaceTableRef(fromNode, originalTable, utilName)
	}

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return strings.Replace(sql, originalTable, utilName, 1)
	}
	return deparsed
}

// replaceTableRef replaces a specific table name in a FROM clause node.
func replaceTableRef(node *pg_query.Node, originalTable, newTable string) {
	if node == nil {
		return
	}
	if rv := node.GetRangeVar(); rv != nil {
		tableName := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			tableName = s + "." + tableName
		}
		if tableName == originalTable || rv.GetRelname() == originalTable {
			rv.Relname = newTable
			rv.Schemaname = ""
		}
	}
	if je := node.GetJoinExpr(); je != nil {
		replaceTableRef(je.GetLarg(), originalTable, newTable)
		replaceTableRef(je.GetRarg(), originalTable, newTable)
	}
}
