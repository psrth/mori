package proxy

import (
	"fmt"
	"log"
	"net"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
)

// handleWindowRead handles queries with window functions on dirty tables.
// Strategy: run base query (without windows) as merged read, materialize into
// temp table, re-execute original query against temp table.
func (rh *ReadHandler) handleWindowRead(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.windowReadCore(cl, cl.RawSQL)
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

// windowReadCore implements the window function materialization strategy.
//
// Window functions (SUM() OVER(), RANK(), ROW_NUMBER(), etc.) depend on the
// complete result set -- a missing or extra row changes the computation for
// every other row. The merged read pipeline operates at the row level, which
// is incompatible with window semantics.
//
// Strategy:
//  1. Build a base SELECT * query (no window functions) with the same
//     FROM and WHERE clauses.
//  2. Run the base query through the merged read pipeline to get the
//     complete, correctly merged dataset.
//  3. Materialize the merged dataset into a temp table on shadow.
//  4. Rewrite the original query to read from the temp table.
//  5. Execute the rewritten query on shadow -- PostgreSQL handles the
//     window computation on the complete dataset.
//  6. Clean up the temp table.
func (rh *ReadHandler) windowReadCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	result, err := pg_query.Parse(querySQL)
	if err != nil {
		return rh.windowFallback(querySQL)
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return rh.windowFallback(querySQL)
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return rh.windowFallback(querySQL)
	}

	// Build a base SELECT that fetches all source rows without window functions.
	baseSQL := buildWindowBaseQuery(sel)
	if baseSQL == "" {
		return rh.windowFallback(querySQL)
	}

	// Run the base query through merged read to get the complete dataset.
	// Copy the classification but disable flags that would trigger aggregate
	// or window paths recursively, and remove LIMIT since windows need all rows.
	cappedBaseSQL := rh.capSQL(baseSQL)
	baseCl := &core.Classification{
		Tables:  cl.Tables,
		RawSQL:  cappedBaseSQL,
		OrderBy: cl.OrderBy,
	}

	baseCols, baseValues, baseNulls, err := rh.mergedReadCore(baseCl, cappedBaseSQL)
	if err != nil {
		return rh.windowFallback(querySQL)
	}

	if len(baseCols) == 0 {
		return rh.windowFallback(querySQL)
	}

	// Materialize into temp table on shadow.
	utilName := utilTableName(querySQL)
	createSQL := buildCreateTempTableSQL(utilName, baseCols)

	createResult, err := execQuery(rh.shadowConn, createSQL)
	if err != nil {
		return rh.windowFallback(querySQL)
	}
	if createResult.Error != "" {
		// Table might already exist from a previous call -- drop and recreate.
		dropUtilTable(rh.shadowConn, utilName)
		if _, err := execQuery(rh.shadowConn, createSQL); err != nil {
			return rh.windowFallback(querySQL)
		}
	}
	defer dropUtilTable(rh.shadowConn, utilName)

	if len(baseValues) > 0 {
		if err := bulkInsertToUtilTable(rh.shadowConn, utilName, baseCols, baseValues, baseNulls); err != nil {
			return rh.windowFallback(querySQL)
		}
	}

	if rh.verbose {
		log.Printf("[conn %d] window: materialized %d rows into %s", rh.connID, len(baseValues), utilName)
	}

	// Rewrite the original query to read from the temp table instead of the
	// original table(s).
	rewrittenSQL := rewriteQueryTable(querySQL, cl.Tables, utilName)

	// Execute the rewritten query on shadow -- PG computes windows on the
	// complete materialized dataset.
	shadowResult, err := execQuery(rh.shadowConn, rewrittenSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow window query: %w", err)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
}

// buildWindowBaseQuery constructs a SELECT * query that fetches all source rows
// without window functions. This preserves the FROM and WHERE clauses so we get
// the correct row set, but strips any window computation from the SELECT list.
func buildWindowBaseQuery(sel *pg_query.SelectStmt) string {
	if sel.GetFromClause() == nil || len(sel.GetFromClause()) == 0 {
		return ""
	}

	// Build SELECT * with the same FROM and WHERE but no window functions.
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

	baseResult := &pg_query.ParseResult{
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

	sql, err := pg_query.Deparse(baseResult)
	if err != nil {
		return ""
	}
	return sql
}

// rewriteQueryTable rewrites a SQL query to read from a single temp table
// instead of the original table(s). The temp table contains the complete
// merged dataset (already joined if the original query had JOINs), so we
// replace the entire FROM clause with a single RangeVar and strip table
// qualifiers from all ColumnRef nodes throughout the query.
func rewriteQueryTable(sql string, tables []string, utilName string) string {
	if len(tables) == 0 {
		return sql
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		// Fallback: simple string replacement for the first table.
		return replaceFirstOccurrence(sql, tables[0], utilName)
	}

	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return sql
	}

	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return sql
	}

	// Replace the entire FROM clause with a single RangeVar pointing to the
	// temp table. The temp table already contains the fully-joined dataset,
	// so we don't need any JOINs.
	sel.FromClause = []*pg_query.Node{
		{
			Node: &pg_query.Node_RangeVar{
				RangeVar: &pg_query.RangeVar{
					Relname: utilName,
					Inh:     true,
				},
			},
		},
	}

	// Strip table qualifiers from all ColumnRef nodes in the query.
	// Since everything is now in one flat table, "e"."name" -> "name".
	stripColumnRefQualifiers(stmts[0].GetStmt())

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return replaceFirstOccurrence(sql, tables[0], utilName)
	}
	return deparsed
}

// stripColumnRefQualifiers walks an AST node tree and strips table/alias
// qualifiers from ColumnRef nodes. A qualified reference like [String("e"),
// String("name")] is reduced to [String("name")].
func stripColumnRefQualifiers(node *pg_query.Node) {
	if node == nil {
		return
	}

	// ColumnRef: strip qualifier if present.
	if cr := node.GetColumnRef(); cr != nil {
		fields := cr.GetFields()
		if len(fields) == 2 {
			// Two-part reference: table.column -- keep only the column.
			if fields[0].GetString_() != nil && (fields[1].GetString_() != nil || fields[1].GetAStar() != nil) {
				cr.Fields = fields[1:]
			}
		}
		return
	}

	// SelectStmt: walk target list, FROM, WHERE, ORDER BY, GROUP BY, HAVING,
	// window clauses, and LIMIT/OFFSET.
	if sel := node.GetSelectStmt(); sel != nil {
		for _, target := range sel.GetTargetList() {
			stripColumnRefQualifiers(target)
		}
		for _, from := range sel.GetFromClause() {
			stripColumnRefQualifiers(from)
		}
		stripColumnRefQualifiers(sel.GetWhereClause())
		for _, sc := range sel.GetSortClause() {
			stripColumnRefQualifiers(sc)
		}
		for _, gc := range sel.GetGroupClause() {
			stripColumnRefQualifiers(gc)
		}
		stripColumnRefQualifiers(sel.GetHavingClause())
		for _, wc := range sel.GetWindowClause() {
			stripColumnRefQualifiers(wc)
		}
		stripColumnRefQualifiers(sel.GetLimitCount())
		stripColumnRefQualifiers(sel.GetLimitOffset())
		return
	}

	// ResTarget: walk the value expression.
	if rt := node.GetResTarget(); rt != nil {
		stripColumnRefQualifiers(rt.GetVal())
		return
	}

	// FuncCall: walk args and the OVER clause (WindowDef).
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			stripColumnRefQualifiers(arg)
		}
		if over := fc.GetOver(); over != nil {
			for _, p := range over.GetPartitionClause() {
				stripColumnRefQualifiers(p)
			}
			for _, o := range over.GetOrderClause() {
				stripColumnRefQualifiers(o)
			}
		}
		// Walk aggregate filter.
		stripColumnRefQualifiers(fc.GetAggFilter())
		return
	}

	// SortBy: walk the sort expression node.
	if sb := node.GetSortBy(); sb != nil {
		stripColumnRefQualifiers(sb.GetNode())
		return
	}

	// A_Expr: walk left and right expressions.
	if ae := node.GetAExpr(); ae != nil {
		stripColumnRefQualifiers(ae.GetLexpr())
		stripColumnRefQualifiers(ae.GetRexpr())
		return
	}

	// BoolExpr: walk all args.
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			stripColumnRefQualifiers(arg)
		}
		return
	}

	// NullTest: walk arg.
	if nt := node.GetNullTest(); nt != nil {
		stripColumnRefQualifiers(nt.GetArg())
		return
	}

	// TypeCast: walk arg.
	if tc := node.GetTypeCast(); tc != nil {
		stripColumnRefQualifiers(tc.GetArg())
		return
	}

	// SubLink (subqueries in WHERE, etc.): walk the test expression.
	if sl := node.GetSubLink(); sl != nil {
		stripColumnRefQualifiers(sl.GetTestexpr())
		// Don't recurse into the subquery itself -- it has its own scope.
		return
	}

	// CaseExpr: walk args and default.
	if ce := node.GetCaseExpr(); ce != nil {
		stripColumnRefQualifiers(ce.GetArg())
		stripColumnRefQualifiers(ce.GetDefresult())
		for _, when := range ce.GetArgs() {
			stripColumnRefQualifiers(when)
		}
		return
	}

	// CaseWhen: walk expr and result.
	if cw := node.GetCaseWhen(); cw != nil {
		stripColumnRefQualifiers(cw.GetExpr())
		stripColumnRefQualifiers(cw.GetResult())
		return
	}

	// CoalesceExpr: walk args.
	if ce := node.GetCoalesceExpr(); ce != nil {
		for _, arg := range ce.GetArgs() {
			stripColumnRefQualifiers(arg)
		}
		return
	}

	// List: walk items.
	if list := node.GetList(); list != nil {
		for _, item := range list.GetItems() {
			stripColumnRefQualifiers(item)
		}
		return
	}

	// WindowDef: walk partition and order clauses.
	if wd := node.GetWindowDef(); wd != nil {
		for _, p := range wd.GetPartitionClause() {
			stripColumnRefQualifiers(p)
		}
		for _, o := range wd.GetOrderClause() {
			stripColumnRefQualifiers(o)
		}
		return
	}

	// JoinExpr: walk left, right, and quals (in case FROM is walked before replacement).
	if je := node.GetJoinExpr(); je != nil {
		stripColumnRefQualifiers(je.GetLarg())
		stripColumnRefQualifiers(je.GetRarg())
		stripColumnRefQualifiers(je.GetQuals())
		return
	}

	// RangeVar: nothing to strip (leaf node for FROM).
}

// replaceFirstOccurrence replaces the first occurrence of old with new in s.
// Used as a string-level fallback when AST rewriting fails.
func replaceFirstOccurrence(s, old, new string) string {
	idx := findWordBoundary(s, old)
	if idx < 0 {
		return s
	}
	return s[:idx] + new + s[idx+len(old):]
}

// findWordBoundary finds the index of the first occurrence of word in s
// that sits on word boundaries (not part of a larger identifier).
func findWordBoundary(s, word string) int {
	for i := 0; i <= len(s)-len(word); i++ {
		if s[i:i+len(word)] != word {
			continue
		}
		// Check left boundary.
		if i > 0 && isIdentChar(s[i-1]) {
			continue
		}
		// Check right boundary.
		end := i + len(word)
		if end < len(s) && isIdentChar(s[end]) {
			continue
		}
		return i
	}
	return -1
}

// windowFallback executes the query on shadow only as a fallback when the
// materialization strategy cannot be applied (parse failures, etc.).
func (rh *ReadHandler) windowFallback(sql string) ([]ColumnInfo, [][]string, [][]bool, error) {
	if rh.verbose {
		log.Printf("[conn %d] window: falling back to shadow-only execution", rh.connID)
	}
	result, err := execQuery(rh.shadowConn, sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow window fallback: %w", err)
	}
	if result.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}
	return result.Columns, result.RowValues, result.RowNulls, nil
}
