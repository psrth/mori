package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
)

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
//
// Point update (PKs extractable): for each (table, pk), hydrate from Prod if
// the row is not already in Shadow, then execute the UPDATE on Shadow and
// track the affected PKs in the delta map.
//
// Bulk update (no PKs): query Prod for matching rows, hydrate them into
// Shadow, then execute the UPDATE on Shadow and track all affected PKs.
func (w *WriteHandler) handleUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Enforce FK constraints on the SET values before executing the UPDATE.
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		if err := w.fkEnforcer.EnforceUpdate(cl.Tables[0], cl.RawSQL); err != nil {
			if w.verbose {
				log.Printf("[conn %d] FK violation on UPDATE: %v", w.connID, err)
			}
			return sendFKError(clientConn, err.Error())
		}
	}

	if len(cl.PKs) == 0 {
		// Attempt bulk hydration for single-table UPDATEs with PK metadata.
		if !cl.IsJoin && len(cl.Tables) == 1 {
			table := cl.Tables[0]
			if meta, ok := w.tables[table]; ok && len(meta.PKColumns) > 0 {
				return w.handleBulkUpdate(clientConn, rawMsg, cl)
			}
		}
		// Fallback: multi-table UPDATE or no PK metadata.
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE without PKs, forwarding to Shadow without hydration", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Hydrate any cross-table references (subqueries in SET/WHERE) from Prod.
	w.hydrateReferencedTables(cl.RawSQL, cl.Tables[0])

	// Point update: hydrate missing rows, then execute.
	for _, pk := range cl.PKs {
		if w.deltaMap.IsDelta(pk.Table, pk.PK) {
			continue // Already in Shadow.
		}
		if err := w.hydrateRow(pk.Table, pk.PK); err != nil {
			if w.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", w.connID, pk.Table, pk.PK, err)
			}
			// Non-fatal: the UPDATE may still succeed if the row exists
			// in Shadow or doesn't exist at all.
		}
	}

	// Execute the UPDATE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Update delta map for all PKs.
	for _, pk := range cl.PKs {
		if w.inTxn() {
			w.deltaMap.Stage(pk.Table, pk.PK)
		} else {
			w.deltaMap.Add(pk.Table, pk.PK)
		}
	}

	// Only persist immediately in autocommit mode; txn commit handles persistence.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// handleBulkUpdate handles UPDATE statements where no PKs could be extracted
// from the WHERE clause (e.g., range conditions, IS NULL, complex expressions).
//
// It queries Prod for all rows matching the WHERE clause, hydrates them into
// Shadow, executes the UPDATE on Shadow, and tracks all affected PKs.
func (w *WriteHandler) handleBulkUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	table := cl.Tables[0]
	meta := w.tables[table]
	pkCol := meta.PKColumns[0]

	// 1. Build a SELECT query to discover matching rows from Prod.
	selectSQL, err := buildBulkHydrationQuery(cl.RawSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: failed to build hydration query: %v", w.connID, err)
		}
		// Fallback: forward to Shadow without hydration, then track delta PKs.
		return w.shadowOnlyUpdateWithDeltaTracking(clientConn, rawMsg, cl.RawSQL, table, pkCol)
	}

	// Rewrite the hydration SELECT for Prod compatibility (strip shadow-only columns
	// from WHERE so Prod doesn't reject the query).
	if w.schemaRegistry != nil {
		rewritten, skipProd := rewriteSQLForProd(selectSQL, w.schemaRegistry, cl.Tables)
		if skipProd {
			// The entire WHERE is on shadow-only columns — no Prod rows can match.
			// Execute UPDATE on Shadow only and track delta PKs from Shadow.
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration query irrelevant for Prod, Shadow-only", w.connID)
			}
			return w.shadowOnlyUpdateWithDeltaTracking(clientConn, rawMsg, cl.RawSQL, table, pkCol)
		}
		selectSQL = rewritten
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrating from Prod with: %s", w.connID, selectSQL)
	}

	// 2. Execute on Prod to get all matching rows.
	result, err := execQuery(w.prodConn, selectSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod query failed: %v", w.connID, err)
		}
		return w.shadowOnlyUpdateWithDeltaTracking(clientConn, rawMsg, cl.RawSQL, table, pkCol)
	}
	if result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod query error: %s", w.connID, result.Error)
		}
		return w.shadowOnlyUpdateWithDeltaTracking(clientConn, rawMsg, cl.RawSQL, table, pkCol)
	}

	// 3. Find PK column index in results.
	pkIdx := -1
	for i, col := range result.Columns {
		if col.Name == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: PK column %q not found in Prod results", w.connID, pkCol)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// 4. Hydrate each matching row into Shadow.
	skipCols := toSkipSet(meta.GeneratedCols)
	var affectedPKs []string
	hydratedCount := 0
	for i, row := range result.RowValues {
		if result.RowNulls[i][pkIdx] {
			continue // NULL PK, skip.
		}
		pk := row[pkIdx]

		if w.deltaMap.IsDelta(table, pk) {
			affectedPKs = append(affectedPKs, pk)
			continue // Already in Shadow.
		}

		insertSQL := buildInsertSQL(table, result.Columns, row, result.RowNulls[i], skipCols)
		shadowResult, err := execQuery(w.shadowConn, insertSQL)
		if err != nil {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration INSERT failed for PK %s: %v", w.connID, pk, err)
			}
			continue
		}
		if shadowResult.Error != "" {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration INSERT for PK %s: %s", w.connID, pk, shadowResult.Error)
			}
			continue
		}
		affectedPKs = append(affectedPKs, pk)
		hydratedCount++
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrated %d rows (%d total matched)", w.connID, hydratedCount, len(affectedPKs))
	}

	// 5. Hydrate cross-table references (subqueries in SET/WHERE) from Prod.
	w.hydrateReferencedTables(cl.RawSQL, table)

	// 6. Execute UPDATE on Shadow. If we discovered concrete PKs, rewrite the WHERE
	// clause to use pk IN (...) instead of the original WHERE (which may contain
	// subqueries that reference tables not fully available in Shadow). This mirrors
	// the bulk DELETE pattern — resolve PKs on Prod, execute with concrete values on Shadow.
	if len(affectedPKs) > 0 {
		rewrittenSQL, rewriteErr := buildRewrittenUpdateWithPKs(cl.RawSQL, pkCol, affectedPKs)
		if rewriteErr != nil {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: rewrite failed, using original: %v", w.connID, rewriteErr)
			}
			if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
				return err
			}
		} else {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: rewritten with %d PKs: %s", w.connID, len(affectedPKs), truncateSQL(rewrittenSQL, 200))
			}
			rewrittenMsg := buildQueryMsg(rewrittenSQL)
			if err := forwardAndRelay(rewrittenMsg, w.shadowConn, clientConn); err != nil {
				return err
			}
		}
	} else {
		// No matching rows — forward original (will return 0 rows).
		if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
			return err
		}
	}

	// 7. Track all affected PKs in the delta map.
	for _, pk := range affectedPKs {
		if w.inTxn() {
			w.deltaMap.Stage(table, pk)
		} else {
			w.deltaMap.Add(table, pk)
		}
	}

	// 7. Persist delta map (autocommit only).
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// shadowOnlyUpdateWithDeltaTracking executes an UPDATE on Shadow without Prod
// hydration, then queries Shadow for the affected PKs and adds them to the
// delta map. This is used when the hydration query cannot run on Prod (e.g.,
// WHERE clause references shadow-only columns).
func (w *WriteHandler) shadowOnlyUpdateWithDeltaTracking(
	clientConn net.Conn,
	rawMsg []byte,
	rawSQL string,
	table string,
	pkCol string,
) error {
	// Execute the UPDATE on Shadow and relay to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Discover affected PKs by querying Shadow with a SELECT using the same
	// WHERE clause as the original UPDATE. We build this by selecting just the
	// PK column from the table with the original WHERE.
	pkSelectSQL := fmt.Sprintf("SELECT %s FROM %s", quoteIdent(pkCol), quoteIdent(table))

	// Extract the WHERE clause from the original UPDATE by parsing it.
	parseResult, parseErr := pg_query.Parse(rawSQL)
	if parseErr == nil {
		stmts := parseResult.GetStmts()
		if len(stmts) > 0 {
			if upd := stmts[0].GetStmt().GetUpdateStmt(); upd != nil && upd.GetWhereClause() != nil {
				// Rebuild as a SELECT <pk> FROM <table> WHERE <original where>
				sel := &pg_query.SelectStmt{
					TargetList: []*pg_query.Node{
						{Node: &pg_query.Node_ResTarget{ResTarget: &pg_query.ResTarget{
							Val: &pg_query.Node{Node: &pg_query.Node_ColumnRef{ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: pkCol}}}},
							}}},
						}}},
					},
					FromClause: []*pg_query.Node{
						{Node: &pg_query.Node_RangeVar{RangeVar: upd.GetRelation()}},
					},
					WhereClause: upd.GetWhereClause(),
				}
				deparsed, dErr := pg_query.Deparse(&pg_query.ParseResult{
					Stmts: []*pg_query.RawStmt{{Stmt: &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{SelectStmt: sel},
					}}},
				})
				if dErr == nil {
					pkSelectSQL = deparsed
				}
			}
		}
	}

	// Execute on Shadow to discover affected PKs.
	result, err := execQuery(w.shadowConn, pkSelectSQL)
	if err != nil || result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: shadow-only, PK discovery failed", w.connID)
		}
		return nil // Non-fatal: UPDATE already executed.
	}

	// Find PK column index.
	pkIdx := -1
	for i, col := range result.Columns {
		if col.Name == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		return nil
	}

	// Track delta PKs.
	tracked := 0
	for i, row := range result.RowValues {
		if result.RowNulls[i][pkIdx] {
			continue
		}
		pk := row[pkIdx]
		if w.inTxn() {
			w.deltaMap.Stage(table, pk)
		} else {
			w.deltaMap.Add(table, pk)
		}
		tracked++
	}

	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: shadow-only, tracked %d delta PKs", w.connID, tracked)
	}

	return nil
}

// buildBulkHydrationQuery transforms an UPDATE statement into a SELECT *
// query with the same FROM and WHERE clauses. This allows querying Prod
// for all rows that the UPDATE would affect.
//
// Example:
//
//	UPDATE conversations SET org_id = 1 WHERE org_id IS NULL
//	→ SELECT * FROM conversations WHERE org_id IS NULL
func buildBulkHydrationQuery(rawSQL string) (string, error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}

	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return "", fmt.Errorf("no statements found")
	}

	upd := stmts[0].GetStmt().GetUpdateStmt()
	if upd == nil {
		return "", fmt.Errorf("not an UPDATE statement")
	}

	rel := upd.GetRelation()
	if rel == nil {
		return "", fmt.Errorf("UPDATE has no target relation")
	}

	// Build SELECT * target list.
	starTarget := &pg_query.Node{
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
	}

	// Build FROM clause: target table + any additional FROM tables.
	fromClause := []*pg_query.Node{
		{Node: &pg_query.Node_RangeVar{RangeVar: rel}},
	}
	fromClause = append(fromClause, upd.GetFromClause()...)

	// Build SELECT statement reusing the WHERE clause.
	selectStmt := &pg_query.SelectStmt{
		TargetList:  []*pg_query.Node{starTarget},
		FromClause:  fromClause,
		WhereClause: upd.GetWhereClause(),
	}

	deparseResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: selectStmt,
					},
				},
			},
		},
	}

	sql, err := pg_query.Deparse(deparseResult)
	if err != nil {
		return "", fmt.Errorf("deparse: %w", err)
	}

	return sql, nil
}

// hydrateRow fetches a single row from Prod by PK and inserts it into Shadow.
// If the row does not exist in Prod, it is a no-op.
func (w *WriteHandler) hydrateRow(table, pk string) error {
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return fmt.Errorf("no PK metadata for table %q", table)
	}

	// Build SELECT query using the first PK column.
	// For composite PKs, a future phase will handle all columns.
	pkCol := meta.PKColumns[0]
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s",
		quoteIdent(table), quoteIdent(pkCol), quoteLiteral(pk))

	result, err := execQuery(w.prodConn, selectSQL)
	if err != nil {
		return fmt.Errorf("prod SELECT: %w", err)
	}
	if result.Error != "" {
		return fmt.Errorf("prod SELECT error: %s", result.Error)
	}
	if len(result.RowValues) == 0 {
		return nil // Row doesn't exist in Prod; nothing to hydrate.
	}

	// Build and execute INSERT into Shadow, skipping generated columns.
	skipCols := toSkipSet(meta.GeneratedCols)
	insertSQL := buildInsertSQL(table, result.Columns, result.RowValues[0], result.RowNulls[0], skipCols)

	shadowResult, err := execQuery(w.shadowConn, insertSQL)
	if err != nil {
		return fmt.Errorf("shadow INSERT: %w", err)
	}
	if shadowResult.Error != "" {
		// Could be a constraint violation from concurrent hydration.
		// ON CONFLICT DO NOTHING handles this; log and continue.
		if w.verbose {
			log.Printf("[conn %d] hydration INSERT for (%s, %s): %s", w.connID, table, pk, shadowResult.Error)
		}
	}

	return nil
}

// hydrateReferencedTables detects cross-table references in an UPDATE
// statement (subqueries in SET/WHERE clauses) and copies those tables'
// data from Prod into Shadow so the subqueries can resolve correctly.
//
// For each referenced table, it tries to extract a subquery predicate to
// hydrate only matching rows. If the subquery is correlated (references
// the outer/target table) or too complex to extract, it falls back to
// full table hydration. A maxRowsHydrate cap is applied to limit the
// number of rows hydrated.
func (w *WriteHandler) hydrateReferencedTables(rawSQL string, targetTable string) {
	refTables := extractSubqueryTables(rawSQL, targetTable)
	if len(refTables) == 0 {
		return
	}

	// Extract subquery predicates for each referenced table.
	predicates := extractSubqueryPredicates(rawSQL, targetTable)

	for _, refTable := range refTables {
		// Skip if we have no metadata for the table (might be a CTE or function).
		if _, ok := w.tables[refTable]; !ok {
			continue
		}
		// Skip if the table already has deltas (has been tainted — data exists in Shadow).
		if w.deltaMap.CountForTable(refTable) > 0 {
			continue
		}

		// Try predicate-based hydration first.
		selectSQL := ""
		if pred, ok := predicates[refTable]; ok && pred != "" {
			selectSQL = fmt.Sprintf("SELECT * FROM %s WHERE %s", quoteIdent(refTable), pred)
			if w.verbose {
				log.Printf("[conn %d] cross-table hydration: predicate-based for %s: %s", w.connID, refTable, selectSQL)
			}
		} else {
			// Fallback: full table hydration.
			selectSQL = fmt.Sprintf("SELECT * FROM %s", quoteIdent(refTable))
			if w.verbose {
				log.Printf("[conn %d] cross-table hydration: full table for %s", w.connID, refTable)
			}
		}

		// Apply max rows cap.
		if w.maxRowsHydrate > 0 {
			selectSQL += fmt.Sprintf(" LIMIT %d", w.maxRowsHydrate)
		}

		result, err := execQuery(w.prodConn, selectSQL)
		if err != nil || result.Error != "" {
			if w.verbose {
				log.Printf("[conn %d] cross-table hydration: failed to read %s from Prod", w.connID, refTable)
			}
			continue
		}

		refSkipCols := toSkipSet(w.tables[refTable].GeneratedCols)
		inserted := 0
		for i, row := range result.RowValues {
			insertSQL := buildInsertSQL(refTable, result.Columns, row, result.RowNulls[i], refSkipCols)
			shadowResult, err := execQuery(w.shadowConn, insertSQL)
			if err != nil {
				continue
			}
			if shadowResult.Error == "" {
				inserted++
			}
		}

		if w.verbose {
			log.Printf("[conn %d] cross-table hydration: inserted %d/%d rows into %s",
				w.connID, inserted, len(result.RowValues), refTable)
		}
	}
}

// extractSubqueryPredicates parses an UPDATE statement and attempts to extract
// WHERE predicates from subqueries for each referenced table. Returns a map
// of table name → predicate SQL string. If a subquery is correlated (references
// the target/outer table) or too complex, the table is omitted from the map,
// causing the caller to fall back to full table hydration.
func extractSubqueryPredicates(rawSQL string, targetTable string) map[string]string {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return nil
	}
	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return nil
	}
	upd := stmts[0].GetStmt().GetUpdateStmt()
	if upd == nil {
		return nil
	}

	predicates := make(map[string]string)

	// Walk SET clause target values to find SubLinks.
	for _, target := range upd.GetTargetList() {
		if rt := target.GetResTarget(); rt != nil {
			extractPredicatesFromNode(rt.GetVal(), targetTable, predicates)
		}
	}

	// Walk WHERE clause.
	extractPredicatesFromNode(upd.GetWhereClause(), targetTable, predicates)

	return predicates
}

// extractPredicatesFromNode recursively walks AST nodes looking for SubLink
// (subquery) nodes. For each subquery, it extracts the table and WHERE predicate
// if the subquery is simple and non-correlated.
func extractPredicatesFromNode(node *pg_query.Node, targetTable string, predicates map[string]string) {
	if node == nil {
		return
	}

	// SubLink: subquery (EXISTS, IN, scalar subquery, etc.)
	if sl := node.GetSubLink(); sl != nil {
		extractPredicatesFromNode(sl.GetTestexpr(), targetTable, predicates)
		if subSel := sl.GetSubselect(); subSel != nil {
			if sel := subSel.GetSelectStmt(); sel != nil {
				extractPredicateFromSelect(sel, targetTable, predicates)
			}
		}
		return
	}

	// BoolExpr: AND/OR/NOT
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			extractPredicatesFromNode(arg, targetTable, predicates)
		}
		return
	}

	// A_Expr: comparisons
	if ae := node.GetAExpr(); ae != nil {
		extractPredicatesFromNode(ae.GetLexpr(), targetTable, predicates)
		extractPredicatesFromNode(ae.GetRexpr(), targetTable, predicates)
		return
	}

	// ResTarget: in SET clause
	if rt := node.GetResTarget(); rt != nil {
		extractPredicatesFromNode(rt.GetVal(), targetTable, predicates)
		return
	}

	// FuncCall: function arguments
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			extractPredicatesFromNode(arg, targetTable, predicates)
		}
		return
	}
}

// extractPredicateFromSelect extracts a table name and WHERE predicate from a
// simple SELECT subquery. If the subquery is correlated (its WHERE references
// the outer/target table), or if it has no WHERE, the entry is skipped.
func extractPredicateFromSelect(sel *pg_query.SelectStmt, targetTable string, predicates map[string]string) {
	if sel == nil {
		return
	}

	// Find the table(s) in the FROM clause.
	for _, from := range sel.GetFromClause() {
		rv := from.GetRangeVar()
		if rv == nil {
			continue
		}
		refTable := rv.GetRelname()
		if refTable == "" || refTable == targetTable {
			continue
		}

		// Check if there's a WHERE clause we can extract.
		where := sel.GetWhereClause()
		if where == nil {
			// No predicate — caller will fall back to full hydration.
			continue
		}

		// Check if the WHERE clause is correlated (references the target table).
		if isCorrelatedPredicate(where, targetTable) {
			// Correlated subquery — fall back to full hydration.
			continue
		}

		// Try to deparse the WHERE clause into SQL.
		predSQL, err := deparseWhereClause(where, refTable)
		if err != nil {
			continue
		}

		// Only set if we haven't already set a predicate for this table,
		// or if this one is more specific.
		if _, exists := predicates[refTable]; !exists {
			predicates[refTable] = predSQL
		}
	}

	// Recurse into WHERE for nested subqueries.
	extractPredicatesFromNode(sel.GetWhereClause(), targetTable, predicates)
}

// isCorrelatedPredicate checks if a WHERE clause node references the target
// (outer) table by looking for qualified column references like "target_table.col".
func isCorrelatedPredicate(node *pg_query.Node, targetTable string) bool {
	if node == nil {
		return false
	}

	// Check ColumnRef for qualified references like "target_table"."col"
	if cr := node.GetColumnRef(); cr != nil {
		fields := cr.GetFields()
		if len(fields) >= 2 {
			if s := fields[0].GetString_(); s != nil {
				if s.GetSval() == targetTable {
					return true
				}
			}
		}
		return false
	}

	// BoolExpr
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			if isCorrelatedPredicate(arg, targetTable) {
				return true
			}
		}
		return false
	}

	// A_Expr
	if ae := node.GetAExpr(); ae != nil {
		return isCorrelatedPredicate(ae.GetLexpr(), targetTable) ||
			isCorrelatedPredicate(ae.GetRexpr(), targetTable)
	}

	// NullTest
	if nt := node.GetNullTest(); nt != nil {
		return isCorrelatedPredicate(nt.GetArg(), targetTable)
	}

	// SubLink
	if sl := node.GetSubLink(); sl != nil {
		return isCorrelatedPredicate(sl.GetTestexpr(), targetTable)
	}

	// FuncCall args
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			if isCorrelatedPredicate(arg, targetTable) {
				return true
			}
		}
	}

	return false
}

// deparseWhereClause converts a WHERE clause AST node back to SQL by wrapping
// it in a minimal SELECT statement and deparsing, then extracting the WHERE part.
func deparseWhereClause(where *pg_query.Node, tableName string) (string, error) {
	// Build a dummy SELECT 1 FROM table WHERE <clause> and deparse it.
	sel := &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{Node: &pg_query.Node_ResTarget{ResTarget: &pg_query.ResTarget{
				Val: &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{Ival: &pg_query.Integer{Ival: 1}},
				}}},
			}}},
		},
		FromClause: []*pg_query.Node{
			{Node: &pg_query.Node_RangeVar{RangeVar: &pg_query.RangeVar{
				Relname: tableName,
				Inh:     true,
			}}},
		},
		WhereClause: where,
	}

	deparsed, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: &pg_query.Node{
			Node: &pg_query.Node_SelectStmt{SelectStmt: sel},
		}}},
	})
	if err != nil {
		return "", fmt.Errorf("deparse: %w", err)
	}

	// Extract just the WHERE clause from "SELECT 1 FROM table WHERE ..."
	upper := strings.ToUpper(deparsed)
	whereIdx := strings.Index(upper, "WHERE ")
	if whereIdx < 0 {
		return "", fmt.Errorf("no WHERE in deparsed output")
	}

	return strings.TrimSpace(deparsed[whereIdx+len("WHERE "):]), nil
}

// extractSubqueryTables parses an UPDATE statement and returns all table names
// referenced in subqueries (SubLink nodes) within SET and WHERE clauses,
// excluding the target table.
func extractSubqueryTables(rawSQL string, targetTable string) []string {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return nil
	}
	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return nil
	}
	upd := stmts[0].GetStmt().GetUpdateStmt()
	if upd == nil {
		return nil
	}

	seen := make(map[string]bool)

	// Walk SET clause target values.
	for _, target := range upd.GetTargetList() {
		if rt := target.GetResTarget(); rt != nil {
			collectRangeVarsFromNode(rt.GetVal(), seen)
		}
	}

	// Walk WHERE clause.
	collectRangeVarsFromNode(upd.GetWhereClause(), seen)

	// Walk FROM clause (additional tables in UPDATE ... FROM ...).
	for _, from := range upd.GetFromClause() {
		collectRangeVarsFromNode(from, seen)
	}

	// Remove target table.
	delete(seen, targetTable)

	var tables []string
	for t := range seen {
		tables = append(tables, t)
	}
	return tables
}

// collectRangeVarsFromNode recursively finds RangeVar (table reference) nodes
// in the AST and adds their names to the seen map.
func collectRangeVarsFromNode(node *pg_query.Node, seen map[string]bool) {
	if node == nil {
		return
	}

	// RangeVar: direct table reference.
	if rv := node.GetRangeVar(); rv != nil {
		name := rv.GetRelname()
		if name != "" {
			seen[name] = true
		}
		return
	}

	// SubLink: subquery (EXISTS, IN, scalar subquery, etc.)
	if sl := node.GetSubLink(); sl != nil {
		collectRangeVarsFromNode(sl.GetTestexpr(), seen)
		if subSel := sl.GetSubselect(); subSel != nil {
			collectRangeVarsFromSelectStmt(subSel.GetSelectStmt(), seen)
		}
		return
	}

	// BoolExpr: AND/OR/NOT
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			collectRangeVarsFromNode(arg, seen)
		}
		return
	}

	// A_Expr: comparisons
	if ae := node.GetAExpr(); ae != nil {
		collectRangeVarsFromNode(ae.GetLexpr(), seen)
		collectRangeVarsFromNode(ae.GetRexpr(), seen)
		return
	}

	// ResTarget: in SET clause
	if rt := node.GetResTarget(); rt != nil {
		collectRangeVarsFromNode(rt.GetVal(), seen)
		return
	}

	// FuncCall: function arguments
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			collectRangeVarsFromNode(arg, seen)
		}
		return
	}
}

// collectRangeVarsFromSelectStmt extracts table references from a SELECT statement.
func collectRangeVarsFromSelectStmt(sel *pg_query.SelectStmt, seen map[string]bool) {
	if sel == nil {
		return
	}
	for _, from := range sel.GetFromClause() {
		if rv := from.GetRangeVar(); rv != nil {
			name := rv.GetRelname()
			if name != "" {
				seen[name] = true
			}
		}
		// JoinExpr
		if je := from.GetJoinExpr(); je != nil {
			collectRangeVarsFromNode(je.GetLarg(), seen)
			collectRangeVarsFromNode(je.GetRarg(), seen)
		}
	}
	// Recurse into WHERE for nested subqueries.
	collectRangeVarsFromNode(sel.GetWhereClause(), seen)
}

// buildRewrittenUpdateWithPKs takes an UPDATE statement and replaces its WHERE
// clause with `pk_col IN (v1, v2, ...)` using concrete PK values discovered
// from Prod. This avoids re-evaluating subqueries in the WHERE clause on Shadow,
// where referenced tables may not have the correct data.
//
// The SET clause, FROM clause, and RETURNING clause are preserved from the original.
func buildRewrittenUpdateWithPKs(rawSQL string, pkCol string, pkValues []string) (string, error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return "", fmt.Errorf("no statements found")
	}
	upd := stmts[0].GetStmt().GetUpdateStmt()
	if upd == nil {
		return "", fmt.Errorf("not an UPDATE statement")
	}

	// Build IN list values.
	inValues := make([]*pg_query.Node, len(pkValues))
	for i, pk := range pkValues {
		inValues[i] = &pg_query.Node{
			Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Sval{Sval: &pg_query.String{Sval: pk}},
			}},
		}
	}

	// Replace WHERE clause with pk_col IN (v1, v2, ...).
	upd.WhereClause = &pg_query.Node{
		Node: &pg_query.Node_AExpr{AExpr: &pg_query.A_Expr{
			Kind: pg_query.A_Expr_Kind_AEXPR_IN,
			Name: []*pg_query.Node{
				{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "="}}},
			},
			Lexpr: &pg_query.Node{
				Node: &pg_query.Node_ColumnRef{ColumnRef: &pg_query.ColumnRef{
					Fields: []*pg_query.Node{
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: pkCol}}},
					},
				}},
			},
			Rexpr: &pg_query.Node{
				Node: &pg_query.Node_List{List: &pg_query.List{
					Items: inValues,
				}},
			},
		}},
	}

	sql, err := pg_query.Deparse(parseResult)
	if err != nil {
		return "", fmt.Errorf("deparse: %w", err)
	}
	return sql, nil
}
