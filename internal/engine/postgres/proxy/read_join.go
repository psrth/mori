package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/psrth/mori/internal/core"
)

// handleJoinPatch implements the JOIN merged read algorithm.
// Strategy: Execute on Prod, patch delta columns from Shadow, merge with Shadow results.
func (rh *ReadHandler) handleJoinPatch(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.joinPatchCore(cl, cl.RawSQL)
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

// joinPatchCore performs the JOIN patch algorithm and returns the result
// without writing to the client. The querySQL parameter is the SQL to execute.
func (rh *ReadHandler) joinPatchCore(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	// Identify which tables have deltas/tombstones.
	deltaTables := rh.identifyDeltaTables(cl.Tables)
	if len(deltaTables) == 0 {
		// No delta tables — execute directly on Prod and return result.
		// Still need to check for schema diffs (e.g., added columns but no row changes).
		if rh.anyTableSchemaModified(cl.Tables) {
			return rh.joinPatchWithSchemaDiffs(cl, querySQL, nil)
		}
		prodResult, err := execQuery(rh.prodConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prod JOIN query: %w", err)
		}
		if prodResult.Error != "" {
			return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
		}
		return prodResult.Columns, prodResult.RowValues, prodResult.RowNulls, nil
	}

	// Step 0: Inject PK columns for delta tables if not in SELECT list.
	effectiveSQL := querySQL
	var injectedPKs map[string]string
	effectiveSQL, injectedPKs = rh.injectJoinPKs(querySQL, deltaTables)

	// Check if any table is fully shadowed — must use Shadow only.
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.IsFullyShadowed(t) {
				// Shadow-only execution.
				shadowResult, err := execQuery(rh.shadowConn, querySQL)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("shadow JOIN query (fully shadowed): %w", err)
				}
				if shadowResult.Error != "" {
					return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
				}
				return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
			}
		}
	}

	// Handle schema diffs via materialization instead of rewriteSQLForProd.
	// rewriteSQLForProd uses parse→modify AST→deparse which can corrupt CROSS JOINs
	// by dropping the right-hand table from the FROM clause. Materialization avoids
	// this by only rewriting table names (RangeVar nodes), preserving JOIN structure.
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.HasDiff(t) {
				return rh.handleJoinWithMaterialization(cl, querySQL)
			}
		}
	}

	// Step 1: Execute JOIN on Prod (no schema diffs at this point).
	prodResult, err := execQuery(rh.prodConn, effectiveSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prod JOIN query: %w", err)
	}
	if prodResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
	}

	// Adapt Prod columns and rows for schema diffs.
	adaptedProdColumns := prodResult.Columns
	adaptedProdValues := prodResult.RowValues
	adaptedProdNulls := prodResult.RowNulls
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.HasDiff(t) {
				adaptedProdColumns = rh.adaptColumns(t, adaptedProdColumns)
				adaptedProdValues, adaptedProdNulls = rh.adaptRows(t, prodResult.Columns, adaptedProdValues, adaptedProdNulls)
			}
		}
	}

	// Step 2: Find PK column indices (including injected PKs).
	pkIndices := rh.findPKIndicesForJoin(deltaTables, adaptedProdColumns, injectedPKs)

	// Steps 3-4: Classify each Prod row (clean / delta / dead) and patch delta rows.
	var patchedValues [][]string
	var patchedNulls [][]bool
	var deltaRowIndices []int // Indices of patched (delta) rows in patchedValues.

	for i, row := range adaptedProdValues {
		action := rh.classifyJoinRow(deltaTables, pkIndices, row)
		switch action {
		case joinRowClean:
			patchedValues = append(patchedValues, row)
			patchedNulls = append(patchedNulls, adaptedProdNulls[i])

		case joinRowDelta:
			patched, patchedN, err := rh.patchDeltaRow(
				deltaTables, pkIndices, adaptedProdColumns, row, adaptedProdNulls[i],
			)
			if err != nil {
				rh.logf("JOIN patch error, keeping Prod row: %v", err)
				patchedValues = append(patchedValues, row)
				patchedNulls = append(patchedNulls, adaptedProdNulls[i])
				continue
			}
			if patched != nil {
				deltaRowIndices = append(deltaRowIndices, len(patchedValues))
				patchedValues = append(patchedValues, patched)
				patchedNulls = append(patchedNulls, patchedN)
			}

		case joinRowDead:
			// Tombstoned — discard.
			continue
		}
	}

	// Step 4.5: Re-evaluate WHERE on patched delta rows.
	// After column replacement, a patched row may no longer satisfy the WHERE clause.
	if len(deltaRowIndices) > 0 {
		whereAST := extractWhereAST(querySQL)
		if whereAST != nil {
			patchedValues, patchedNulls = filterByWhere(
				whereAST, adaptedProdColumns, patchedValues, patchedNulls, deltaRowIndices)
		}
	}

	// Step 5: Execute same JOIN on Shadow (catches locally-inserted rows).
	shadowResult, err := execQuery(rh.shadowConn, effectiveSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow JOIN query: %w", err)
	}

	// Step 6: Merge Shadow + patched Prod. Shadow rows first (priority).
	resultColumns := adaptedProdColumns
	if len(shadowResult.Columns) > 0 && shadowResult.Error == "" {
		resultColumns = shadowResult.Columns
	}

	var mergedValues [][]string
	var mergedNulls [][]bool

	if shadowResult.Error == "" {
		mergedValues = append(mergedValues, shadowResult.RowValues...)
		mergedNulls = append(mergedNulls, shadowResult.RowNulls...)
	}
	mergedValues = append(mergedValues, patchedValues...)
	mergedNulls = append(mergedNulls, patchedNulls...)

	// Step 7: Deduplicate by composite key (PK columns from ALL joined tables).
	allPKIndices := rh.findPKIndicesForJoin(cl.Tables, resultColumns, injectedPKs)
	mergedValues, mergedNulls = rh.dedupJoin(cl.Tables, allPKIndices, resultColumns, mergedValues, mergedNulls)

	// Step 8: Re-sort by ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(resultColumns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Step 9: Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	// Step 10: Strip injected PK columns.
	for _, alias := range injectedPKs {
		resultColumns, mergedValues, mergedNulls = stripInjectedColumn(
			resultColumns, mergedValues, mergedNulls, alias)
	}

	return resultColumns, mergedValues, mergedNulls, nil
}

// joinPatchWithSchemaDiffs handles JOINs where tables have schema diffs but
// no delta rows. It delegates to handleJoinWithMaterialization to avoid the
// parse→modify AST→deparse roundtrip that can corrupt CROSS JOIN structure.
func (rh *ReadHandler) joinPatchWithSchemaDiffs(cl *core.Classification, querySQL string, injectedPKs map[string]string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	return rh.handleJoinWithMaterialization(cl, querySQL)
}

// handleJoinWithMaterialization handles JOINs where tables have schema diffs
// by materializing dirty tables into temp tables and executing on shadow.
// This avoids the parse→modify AST→deparse roundtrip of rewriteSQLForProd,
// which can corrupt CROSS JOIN structure (dropping the right-hand table from
// the FROM clause). Instead, we only rewrite RangeVar table names — which is
// safe because it doesn't change the JOIN structure.
//
// Scoped materialization: instead of materializing entire tables, single-table
// WHERE predicates from the original query are pushed down into the materialization
// query. This reduces data volume dramatically for filtered JOINs.
func (rh *ReadHandler) handleJoinWithMaterialization(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	// Check if any table is fully shadowed — must use Shadow only.
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.IsFullyShadowed(t) {
				return rh.shadowOnlyQuery(querySQL)
			}
		}
	}

	var utilTables []string
	defer func() {
		for _, ut := range utilTables {
			dropUtilTable(rh.shadowConn, ut)
		}
	}()

	// Extract single-table predicates from the query's WHERE clause for pushdown.
	tablePredicates, tableToAlias := extractSingleTablePredicates(querySQL)

	// Collect ALL tables referenced in the query, including SubLink tables
	// (correlated subqueries in the SELECT list). These need materialization too
	// because the rewritten query runs on shadow, where clean tables are empty.
	allTables := cl.Tables
	subLinkTables := collectSubLinkTablesFromSQL(querySQL)
	for _, t := range subLinkTables {
		found := false
		for _, existing := range allTables {
			if existing == t {
				found = true
				break
			}
		}
		if !found {
			allTables = append(allTables, t)
		}
	}

	// Materialize ALL tables into temp tables. Even clean tables must be
	// materialized because the rewritten query runs on shadow, where clean
	// tables have no data. The merged read for clean tables efficiently
	// copies prod data (shadow is empty, no filtering needed).
	tableMap := make(map[string]string)
	for _, table := range allTables {
		// Build materialization query with pushed-down predicates if available.
		selectSQL := rh.buildScopedMaterializationSQL(table, tablePredicates, tableToAlias)

		utilName, mErr := rh.materializeSubquery(selectSQL, table)
		if mErr != nil {
			if rh.verbose {
				log.Printf("[conn %d] JOIN materialization failed for %s: %v", rh.connID, table, mErr)
			}
			// Materialization failed — fall back to shadow-only.
			return rh.shadowOnlyQuery(querySQL)
		}
		tableMap[table] = utilName
		utilTables = append(utilTables, utilName)
	}

	if len(tableMap) == 0 {
		// No tables to materialize — execute on shadow as-is.
		return rh.shadowOnlyQuery(querySQL)
	}

	// Parse the original SQL and rewrite table references to temp tables.
	result, parseErr := pg_query.Parse(querySQL)
	if parseErr != nil {
		return rh.shadowOnlyQuery(querySQL)
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return rh.shadowOnlyQuery(querySQL)
	}
	rewriteTableRefsInNode(stmts[0].GetStmt(), tableMap)

	rewrittenSQL, deparseErr := pg_query.Deparse(result)
	if deparseErr != nil {
		return rh.shadowOnlyQuery(querySQL)
	}

	if rh.verbose {
		log.Printf("[conn %d] JOIN materialized rewrite: %s", rh.connID, truncateSQL(rewrittenSQL, 200))
	}

	// Execute the rewritten query on shadow.
	shadowResult, execErr := execQuery(rh.shadowConn, rewrittenSQL)
	if execErr != nil {
		return nil, nil, nil, fmt.Errorf("shadow JOIN query (materialized): %w", execErr)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
}

// buildScopedMaterializationSQL builds a SELECT query for materializing a table,
// scoped by any pushed-down WHERE predicates from the original JOIN query.
// If predicates are available, the query is: SELECT * FROM <table> AS <alias> WHERE <predicates>
// Otherwise, falls back to: SELECT * FROM <table>
func (rh *ReadHandler) buildScopedMaterializationSQL(
	table string,
	predicates map[string]string,
	tableToAlias map[string]string,
) string {
	predSQL, hasPred := predicates[table]
	if !hasPred || predSQL == "" {
		return fmt.Sprintf("SELECT * FROM %s", quoteIdent(table))
	}

	// Use the alias from the original query so qualified column refs in the
	// predicate resolve correctly (e.g., m.created_at when alias is "m").
	alias := tableToAlias[table]
	if alias != "" && alias != table {
		return fmt.Sprintf("SELECT * FROM %s AS %s WHERE %s",
			quoteIdent(table), quoteIdent(alias), predSQL)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", quoteIdent(table), predSQL)
}

// extractSingleTablePredicates parses a SELECT query's WHERE clause and
// identifies AND-terms that reference only a single table. Returns:
//   - predicates: map from table name to deparsed WHERE SQL for that table
//   - tableToAlias: map from table name to its alias in the query
func extractSingleTablePredicates(querySQL string) (predicates map[string]string, tableToAlias map[string]string) {
	result, err := pg_query.Parse(querySQL)
	if err != nil {
		return nil, nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return nil, nil
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return nil, nil
	}
	where := sel.GetWhereClause()
	if where == nil {
		return nil, nil
	}

	// Build bidirectional alias maps.
	aliasToTable := make(map[string]string) // alias → table name
	tableToAlias = make(map[string]string)  // table name → alias
	for _, node := range sel.GetFromClause() {
		collectBidirectionalAliases(node, aliasToTable, tableToAlias)
	}

	// Decompose WHERE into AND terms.
	terms := decomposeAND(where)

	// Group terms by the single table they reference.
	tableTerms := make(map[string][]*pg_query.Node)
	for _, term := range terms {
		refs := collectQualifiedColumnRefs(term)
		if len(refs) == 0 {
			continue // no qualified column refs — can't determine table, skip
		}
		tableName := ""
		singleTable := true
		for ref := range refs {
			tbl := ref
			if mapped, ok := aliasToTable[ref]; ok {
				tbl = mapped
			}
			if tableName == "" {
				tableName = tbl
			} else if tableName != tbl {
				singleTable = false
				break
			}
		}
		if singleTable && tableName != "" {
			tableTerms[tableName] = append(tableTerms[tableName], term)
		}
	}

	// Deparse each table's combined predicates.
	predicates = make(map[string]string)
	for table, terms := range tableTerms {
		var combined *pg_query.Node
		if len(terms) == 1 {
			combined = terms[0]
		} else {
			combined = &pg_query.Node{
				Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
					Boolop: pg_query.BoolExprType_AND_EXPR,
					Args:   terms,
				}},
			}
		}

		// Deparse using the alias so qualified refs resolve correctly.
		alias := tableToAlias[table]
		if alias == "" {
			alias = table
		}
		predSQL, dErr := deparsePredicateWithAlias(combined, table, alias)
		if dErr != nil {
			continue
		}
		predicates[table] = predSQL
	}

	return predicates, tableToAlias
}

// collectBidirectionalAliases walks FROM clause nodes and builds both
// alias→table and table→alias mappings.
func collectBidirectionalAliases(node *pg_query.Node, aliasToTable, tableToAlias map[string]string) {
	if node == nil {
		return
	}
	if rv := node.GetRangeVar(); rv != nil {
		tableName := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			tableName = s + "." + tableName
		}
		alias := tableName
		if a := rv.GetAlias(); a != nil && a.GetAliasname() != "" {
			alias = a.GetAliasname()
		}
		aliasToTable[alias] = tableName
		tableToAlias[tableName] = alias
		return
	}
	if je := node.GetJoinExpr(); je != nil {
		collectBidirectionalAliases(je.GetLarg(), aliasToTable, tableToAlias)
		collectBidirectionalAliases(je.GetRarg(), aliasToTable, tableToAlias)
	}
}

// decomposeAND splits a WHERE clause into AND-terms. If the top-level node is
// a BoolExpr AND, returns its arguments. Otherwise returns a single-element slice.
func decomposeAND(node *pg_query.Node) []*pg_query.Node {
	if node == nil {
		return nil
	}
	if be := node.GetBoolExpr(); be != nil && be.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
		return be.GetArgs()
	}
	return []*pg_query.Node{node}
}

// collectQualifiedColumnRefs walks an AST node and returns the set of table
// qualifiers from qualified column references (e.g., "m" from "m.created_at").
// Unqualified column refs are ignored since they can't be mapped to a table.
func collectQualifiedColumnRefs(node *pg_query.Node) map[string]bool {
	refs := make(map[string]bool)
	walkQualifiedColumnRefs(node, refs)
	return refs
}

func walkQualifiedColumnRefs(node *pg_query.Node, refs map[string]bool) {
	if node == nil {
		return
	}
	if cr := node.GetColumnRef(); cr != nil {
		fields := cr.GetFields()
		if len(fields) >= 2 {
			if s := fields[0].GetString_(); s != nil {
				refs[s.GetSval()] = true
			}
		}
		return
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			walkQualifiedColumnRefs(arg, refs)
		}
		return
	}
	if ae := node.GetAExpr(); ae != nil {
		walkQualifiedColumnRefs(ae.GetLexpr(), refs)
		walkQualifiedColumnRefs(ae.GetRexpr(), refs)
		return
	}
	if nt := node.GetNullTest(); nt != nil {
		walkQualifiedColumnRefs(nt.GetArg(), refs)
		return
	}
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.GetArgs() {
			walkQualifiedColumnRefs(arg, refs)
		}
		return
	}
	if tc := node.GetTypeCast(); tc != nil {
		walkQualifiedColumnRefs(tc.GetArg(), refs)
		return
	}
}

// deparsePredicateWithAlias deparses a WHERE clause node back to SQL text.
// Uses a dummy SELECT to deparse, with the FROM clause including the alias
// so qualified column refs (e.g., m.col) resolve correctly.
func deparsePredicateWithAlias(where *pg_query.Node, tableName, alias string) (string, error) {
	rv := &pg_query.RangeVar{Relname: tableName, Inh: true}
	if alias != "" && alias != tableName {
		rv.Alias = &pg_query.Alias{Aliasname: alias}
	}

	sel := &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{Node: &pg_query.Node_ResTarget{ResTarget: &pg_query.ResTarget{
				Val: &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{Ival: &pg_query.Integer{Ival: 1}},
				}}},
			}}},
		},
		FromClause: []*pg_query.Node{
			{Node: &pg_query.Node_RangeVar{RangeVar: rv}},
		},
		WhereClause: where,
	}

	deparsed, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: &pg_query.Node{
			Node: &pg_query.Node_SelectStmt{SelectStmt: sel},
		}}},
	})
	if err != nil {
		return "", fmt.Errorf("deparse predicate: %w", err)
	}

	upper := strings.ToUpper(deparsed)
	whereIdx := strings.Index(upper, "WHERE ")
	if whereIdx < 0 {
		return "", fmt.Errorf("no WHERE in deparsed output")
	}
	return strings.TrimSpace(deparsed[whereIdx+len("WHERE "):]), nil
}

// collectSubLinkTablesFromSQL parses a SQL query and extracts table names
// from SubLink nodes (correlated subqueries in SELECT list, WHERE, etc.).
// These tables are not in the FROM clause and would be missed by extractTablesFromNodes.
func collectSubLinkTablesFromSQL(querySQL string) []string {
	result, err := pg_query.Parse(querySQL)
	if err != nil {
		return nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return nil
	}

	var tables []string
	seen := make(map[string]bool)
	collectSubLinkTablesFromNode(stmts[0].GetStmt(), &tables, seen)
	return tables
}

// collectSubLinkTablesFromNode recursively walks an AST node and extracts
// table names from SubLink nodes (correlated subqueries).
func collectSubLinkTablesFromNode(node *pg_query.Node, tables *[]string, seen map[string]bool) {
	if node == nil {
		return
	}
	if sl := node.GetSubLink(); sl != nil {
		if subSel := sl.GetSubselect(); subSel != nil {
			// Extract tables from the subquery's FROM clause.
			if sel := subSel.GetSelectStmt(); sel != nil {
				for _, from := range sel.GetFromClause() {
					collectTablesFromNode(from, tables)
				}
				// Recurse into subquery's target list for nested SubLinks.
				for _, target := range sel.GetTargetList() {
					collectSubLinkTablesFromNode(target, tables, seen)
				}
			}
		}
		return
	}
	if sel := node.GetSelectStmt(); sel != nil {
		// Walk target list (SELECT list) for SubLinks.
		for _, target := range sel.GetTargetList() {
			collectSubLinkTablesFromNode(target, tables, seen)
		}
		// Walk WHERE clause for SubLinks.
		collectSubLinkTablesFromNode(sel.GetWhereClause(), tables, seen)
		return
	}
	if rt := node.GetResTarget(); rt != nil {
		collectSubLinkTablesFromNode(rt.GetVal(), tables, seen)
		return
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			collectSubLinkTablesFromNode(arg, tables, seen)
		}
		return
	}
	if ae := node.GetAExpr(); ae != nil {
		collectSubLinkTablesFromNode(ae.GetLexpr(), tables, seen)
		collectSubLinkTablesFromNode(ae.GetRexpr(), tables, seen)
		return
	}
}

// anyTableSchemaModified reports whether any of the given tables have schema diffs.
func (rh *ReadHandler) anyTableSchemaModified(tables []string) bool {
	if rh.schemaRegistry != nil {
		for _, t := range tables {
			if rh.schemaRegistry.HasDiff(t) {
				return true
			}
		}
	}
	return false
}

// injectJoinPKs injects PK columns for delta tables into the SELECT list of a JOIN query.
// Returns the modified SQL and a map from table name to injected alias column name.
func (rh *ReadHandler) injectJoinPKs(sql string, deltaTables []string) (string, map[string]string) {
	aliases := extractJoinTableAliases(sql)
	if aliases == nil {
		return sql, nil
	}

	upper := strings.ToUpper(strings.TrimSpace(sql))
	selectIdx := strings.Index(upper, "SELECT")
	fromIdx := findOuterFromIndex(upper)
	if selectIdx < 0 || fromIdx <= selectIdx {
		return sql, nil
	}

	// SELECT * includes everything, no injection needed.
	selectPart := strings.TrimSpace(upper[selectIdx+6 : fromIdx])
	if selectPart == "*" || strings.HasPrefix(selectPart, "DISTINCT *") {
		return sql, nil
	}

	selectList := strings.ToLower(sql[selectIdx+6 : fromIdx])

	injected := make(map[string]string) // table name → injected alias column name
	result := sql
	for _, table := range deltaTables {
		meta, ok := rh.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]

		// Use table-alias-aware matching: c.id should NOT match users PK "id".
		tableAlias := table
		if a, ok := aliases[table]; ok && a != "" {
			tableAlias = a
		}
		if containsColumnForTable(selectList, pkCol, tableAlias) {
			continue
		}

		alias, ok := aliases[table]
		if !ok || alias == "" {
			alias = table
		}

		// Use a unique alias to avoid collision with same-named PKs.
		injectedAlias := "_mori_pk_" + strings.ReplaceAll(table, ".", "_")
		expr := quoteIdent(alias) + "." + quoteIdent(pkCol) + " AS " + quoteIdent(injectedAlias)
		result = injectSelectExpr(result, expr)
		injected[table] = injectedAlias

		// Update selectList for subsequent column checks.
		selectList = injectedAlias + "," + selectList
	}

	if len(injected) == 0 {
		return sql, nil
	}
	return result, injected
}

// findOuterFromIndex finds the position of the first " FROM " at parenthesis
// depth 0 (not inside a subquery). Returns -1 if not found.
func findOuterFromIndex(upper string) int {
	depth := 0
	target := " FROM "
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+len(target) <= len(upper) && upper[i:i+len(target)] == target {
				return i
			}
		}
	}
	return -1
}

// containsColumnForTable checks if a SELECT list contains a specific column
// for a given table alias. A qualified column (e.g., "u.id") only matches if
// the qualifier matches the expected alias. An unqualified column matches any table.
func containsColumnForTable(selectList, col, tableAlias string) bool {
	lowerCol := strings.ToLower(col)
	lowerAlias := strings.ToLower(tableAlias)

	parts := strings.Split(selectList, ",")
	for _, part := range parts {
		name := strings.TrimSpace(part)
		// Handle aliases: "col AS alias"
		if idx := strings.Index(strings.ToLower(name), " as "); idx >= 0 {
			name = strings.TrimSpace(name[:idx])
		}
		name = strings.Trim(name, `"`)
		lowerName := strings.ToLower(name)

		if dotIdx := strings.LastIndex(lowerName, "."); dotIdx >= 0 {
			// Qualified: only match if qualifier matches expected alias
			qualifier := strings.Trim(lowerName[:dotIdx], `"`)
			colPart := strings.Trim(lowerName[dotIdx+1:], `"`)
			if colPart == lowerCol && qualifier == lowerAlias {
				return true
			}
		} else {
			// Unqualified: matches any table
			if lowerName == lowerCol {
				return true
			}
		}
	}
	return false
}

// findPKIndicesForJoin extends findPKIndicesForTables with injected PK columns
// and handles renamed PK columns via schema diffs.
func (rh *ReadHandler) findPKIndicesForJoin(
	tables []string, columns []ColumnInfo, injectedPKs map[string]string,
) map[string]int {
	result := rh.findPKIndicesForTables(tables, columns)

	for table, alias := range injectedPKs {
		if _, found := result[table]; found {
			continue
		}
		for i, col := range columns {
			if col.Name == alias {
				result[table] = i
				break
			}
		}
	}

	// Also check renamed columns when looking for PK.
	if rh.schemaRegistry != nil {
		for _, table := range tables {
			if _, found := result[table]; found {
				continue // Already resolved.
			}
			meta, ok := rh.tables[table]
			if !ok || len(meta.PKColumns) == 0 {
				continue
			}
			pkCol := meta.PKColumns[0]
			diff := rh.schemaRegistry.GetDiff(table)
			if diff == nil {
				continue
			}
			for oldName, newName := range diff.Renamed {
				if newName == pkCol {
					// PK was renamed — look for old name in Prod results.
					for i, col := range columns {
						if col.Name == oldName {
							result[table] = i
							break
						}
					}
				}
			}
		}
	}

	return result
}

// injectSelectExpr injects a raw SQL expression at the beginning of the SELECT list.
func injectSelectExpr(sql, expr string) string {
	upper := strings.ToUpper(sql)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return sql
	}
	insertPos := selectIdx + 6
	afterSelect := strings.TrimSpace(sql[insertPos:])
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT") {
		insertPos += (len(sql[insertPos:]) - len(afterSelect)) + 8
	}
	return sql[:insertPos] + " " + expr + "," + sql[insertPos:]
}

type joinRowAction int

const (
	joinRowClean joinRowAction = iota
	joinRowDelta
	joinRowDead
)

// identifyDeltaTables returns the subset of tables that have deltas or tombstones.
func (rh *ReadHandler) identifyDeltaTables(tables []string) []string {
	var result []string
	for _, t := range tables {
		if rh.deltaMap.CountForTable(t) > 0 || rh.tombstones.CountForTable(t) > 0 {
			result = append(result, t)
		}
	}
	return result
}

// findPKIndicesForTables returns a map from table name to the column index
// of that table's PK in the result set.
func (rh *ReadHandler) findPKIndicesForTables(deltaTables []string, columns []ColumnInfo) map[string]int {
	result := make(map[string]int)
	for _, table := range deltaTables {
		meta, ok := rh.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]
		for i, col := range columns {
			if col.Name == pkCol || col.Name == table+"."+pkCol {
				result[table] = i
				break
			}
		}
	}
	return result
}

// classifyJoinRow determines whether a Prod JOIN row is clean, delta, or dead.
func (rh *ReadHandler) classifyJoinRow(
	deltaTables []string,
	pkIndices map[string]int,
	row []string,
) joinRowAction {
	for _, table := range deltaTables {
		idx, ok := pkIndices[table]
		if !ok || idx >= len(row) {
			continue
		}
		pk := row[idx]
		if rh.tombstones.IsTombstoned(table, pk) {
			return joinRowDead
		}
		if rh.deltaMap.IsDelta(table, pk) {
			return joinRowDelta
		}
	}
	return joinRowClean
}

// patchDeltaRow replaces delta columns in a Prod row with Shadow values.
// For each delta table, fetches the row from Shadow and replaces relevant columns.
func (rh *ReadHandler) patchDeltaRow(
	deltaTables []string,
	pkIndices map[string]int,
	columns []ColumnInfo,
	row []string,
	rowNulls []bool,
) ([]string, []bool, error) {
	patched := make([]string, len(row))
	copy(patched, row)
	pNulls := make([]bool, len(rowNulls))
	copy(pNulls, rowNulls)

	for _, table := range deltaTables {
		idx, ok := pkIndices[table]
		if !ok || idx >= len(row) {
			continue
		}
		pk := row[idx]
		if !rh.deltaMap.IsDelta(table, pk) {
			continue
		}

		// Fetch the Shadow version of this row.
		meta, ok := rh.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]
		selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s",
			quoteIdent(table), quoteIdent(pkCol), quoteLiteral(pk))

		shadowRow, err := execQuery(rh.shadowConn, selectSQL)
		if err != nil {
			return nil, nil, fmt.Errorf("shadow fetch for patch: %w", err)
		}
		if len(shadowRow.RowValues) == 0 {
			continue // Row not in Shadow — keep Prod version.
		}

		// Replace columns from this table in the joined result.
		shadowColIdx := buildColumnIndex(shadowRow.Columns)
		for j, col := range columns {
			if sIdx, ok := shadowColIdx[col.Name]; ok {
				patched[j] = shadowRow.RowValues[0][sIdx]
				pNulls[j] = shadowRow.RowNulls[0][sIdx]
			}
		}
	}

	return patched, pNulls, nil
}

// dedupJoin deduplicates merged JOIN rows using a composite key.
// It tries to use PK columns from all joined tables. If PK columns
// can't be unambiguously identified (e.g., multiple tables share the
// same column name "id"), falls back to full-row dedup.
func (rh *ReadHandler) dedupJoin(
	tables []string,
	pkIndices map[string]int,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	// Build list of unique key column indices.
	// Detect ambiguous mapping: if two tables map to the same column index,
	// PK-based dedup is unreliable for one-to-many JOINs.
	seenIdx := make(map[int]bool)
	var keyIndices []int
	ambiguous := false

	for _, table := range tables {
		if idx, ok := pkIndices[table]; ok {
			if seenIdx[idx] {
				ambiguous = true
				break
			}
			seenIdx[idx] = true
			keyIndices = append(keyIndices, idx)
		}
	}

	if ambiguous || len(keyIndices) == 0 {
		// Fall back to full-row dedup: use all column values as key.
		keyIndices = make([]int, len(columns))
		for i := range keyIndices {
			keyIndices[i] = i
		}
	}

	seen := make(map[string]bool)
	var dedupValues [][]string
	var dedupNulls [][]bool

	for i, row := range values {
		var keyParts []string
		for _, idx := range keyIndices {
			if idx < len(row) {
				keyParts = append(keyParts, row[idx])
			}
		}
		key := strings.Join(keyParts, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		dedupValues = append(dedupValues, row)
		dedupNulls = append(dedupNulls, nulls[i])
	}

	return dedupValues, dedupNulls
}
