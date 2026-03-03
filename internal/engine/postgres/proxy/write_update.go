package proxy

import (
	"fmt"
	"log"
	"net"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
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
	var affectedPKs []string
	hydratedCount := 0
	for i, row := range result.RowValues {
		if result.RowNulls[i][pkIdx] {
			continue // NULL PK, skip.
		}
		pk := row[pkIdx]
		affectedPKs = append(affectedPKs, pk)

		if w.deltaMap.IsDelta(table, pk) {
			continue // Already in Shadow.
		}

		insertSQL := buildInsertSQL(table, result.Columns, row, result.RowNulls[i])
		shadowResult, err := execQuery(w.shadowConn, insertSQL)
		if err != nil {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration INSERT failed for PK %s: %v", w.connID, pk, err)
			}
			continue
		}
		if shadowResult.Error != "" && w.verbose {
			log.Printf("[conn %d] bulk UPDATE: hydration INSERT for PK %s: %s", w.connID, pk, shadowResult.Error)
		}
		hydratedCount++
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrated %d rows (%d total matched)", w.connID, hydratedCount, len(affectedPKs))
	}

	// 5. Hydrate cross-table references (subqueries in SET/WHERE) from Prod.
	w.hydrateReferencedTables(cl.RawSQL, table)

	// 6. Execute the original UPDATE on Shadow.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// 6. Track all affected PKs in the delta map.
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

	// Build and execute INSERT into Shadow.
	insertSQL := buildInsertSQL(table, result.Columns, result.RowValues[0], result.RowNulls[0])

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
func (w *WriteHandler) hydrateReferencedTables(rawSQL string, targetTable string) {
	refTables := extractSubqueryTables(rawSQL, targetTable)
	if len(refTables) == 0 {
		return
	}

	for _, refTable := range refTables {
		// Skip if we have no metadata for the table (might be a CTE or function).
		if _, ok := w.tables[refTable]; !ok {
			continue
		}
		// Skip if the table already has deltas (has been tainted — data exists in Shadow).
		if w.deltaMap.CountForTable(refTable) > 0 {
			continue
		}

		if w.verbose {
			log.Printf("[conn %d] cross-table hydration: copying %s from Prod to Shadow", w.connID, refTable)
		}

		selectSQL := fmt.Sprintf("SELECT * FROM %s", quoteIdent(refTable))
		result, err := execQuery(w.prodConn, selectSQL)
		if err != nil || result.Error != "" {
			if w.verbose {
				log.Printf("[conn %d] cross-table hydration: failed to read %s from Prod", w.connID, refTable)
			}
			continue
		}

		inserted := 0
		for i, row := range result.RowValues {
			insertSQL := buildInsertSQL(refTable, result.Columns, row, result.RowNulls[i])
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
