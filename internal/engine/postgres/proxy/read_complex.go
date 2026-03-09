package proxy

import (
	"fmt"
	"log"
	"net"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
)

// handleComplexRead handles CTE and derived table queries on dirty tables
// by decomposing, materializing dirty sub-queries, and rewriting.
func (rh *ReadHandler) handleComplexRead(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.complexReadCore(cl, cl.RawSQL)
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

// complexReadCore decomposes a complex query (CTEs, derived tables), materializes
// dirty sub-queries into temp tables on shadow, rewrites the AST, and executes.
func (rh *ReadHandler) complexReadCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	result, err := pg_query.Parse(querySQL)
	if err != nil {
		// Can't parse -- fall back to shadow-only execution.
		return rh.shadowOnlyQuery(querySQL)
	}

	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return rh.shadowOnlyQuery(querySQL)
	}

	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return rh.shadowOnlyQuery(querySQL)
	}

	var utilTables []string // Track temp tables for cleanup.
	defer func() {
		for _, ut := range utilTables {
			dropUtilTable(rh.shadowConn, ut)
		}
	}()

	modified := false

	// Handle CTEs (WITH clauses).
	if ctes := sel.GetWithClause(); ctes != nil {
		if ctes.GetRecursive() {
			// Recursive CTE: materialize dirty base tables, not the CTE body.
			// The CTE body self-references the CTE name, so it can't be executed
			// in isolation. Instead, we materialize each dirty base table into a
			// temp table via merged read, rewrite all table references in the
			// entire query to point to the temp tables, then execute the complete
			// rewritten query (with WITH RECURSIVE intact) on shadow.
			tableMap, newUtils, ok := rh.materializeDirtyBaseTables(stmts[0].GetStmt(), ctes)
			utilTables = append(utilTables, newUtils...)
			if ok {
				rewriteTableRefsInNode(stmts[0].GetStmt(), tableMap)
				modified = true
			}
		} else {
			// Non-recursive CTEs: materialize the CTE body as a standalone query.
			for _, cteNode := range ctes.GetCtes() {
				cte := cteNode.GetCommonTableExpr()
				if cte == nil {
					continue
				}

				cteName := cte.GetCtename()
				cteQuery := cte.GetCtequery()
				if cteQuery == nil {
					continue
				}

				// Deparse the CTE query to SQL.
				cteSQL, err := deparseStmtNode(cteQuery)
				if err != nil {
					continue
				}

				// Check if this CTE references dirty tables.
				if !rh.isDirtyQuery(cteSQL) {
					continue // Clean CTE -- leave in place.
				}

				// Materialize dirty CTE into a temp table.
				utilName, err := rh.materializeSubquery(cteSQL, cteName)
				if err != nil {
					if rh.verbose {
						log.Printf("[conn %d] CTE materialization failed for %s: %v", rh.connID, cteName, err)
					}
					continue // Skip -- leave CTE in place.
				}
				utilTables = append(utilTables, utilName)

				// Replace CTE query with a simple SELECT * FROM temp_table.
				replacementSQL := fmt.Sprintf("SELECT * FROM %s", quoteIdent(utilName))
				replacementResult, err := pg_query.Parse(replacementSQL)
				if err != nil {
					continue
				}
				replStmts := replacementResult.GetStmts()
				if len(replStmts) > 0 {
					cte.Ctequery = replStmts[0].GetStmt()
					modified = true
				}
			}
		}
	}

	// Handle derived tables (subqueries in FROM clause).
	if fromClauses := sel.GetFromClause(); len(fromClauses) > 0 {
		for i, fromNode := range fromClauses {
			newNode, replaced, newUtils := rh.rewriteDerivedTables(fromNode)
			if replaced {
				sel.FromClause[i] = newNode
				utilTables = append(utilTables, newUtils...)
				modified = true
			}
		}
	}

	// Materialize dirty plain RangeVar tables in FROM clause.
	// This handles cases like LATERAL joins or CROSS JOINs where the outer
	// table is a plain RangeVar (not a derived table or CTE) but is dirty.
	// Uses scoped materialization with pushed-down WHERE predicates to reduce data volume.
	{
		tablePredicates, tableToAlias := extractSingleTablePredicates(querySQL)
		tableMap := make(map[string]string)
		rh.collectDirtyRangeVarsFromNodes(sel.GetFromClause(), tableMap, &utilTables, tablePredicates, tableToAlias)
		if len(tableMap) > 0 {
			rewriteTableRefsInNode(stmts[0].GetStmt(), tableMap)
			modified = true
		}
	}

	if !modified {
		// Nothing was dirty -- execute on shadow as-is.
		return rh.shadowOnlyQuery(querySQL)
	}

	// Deparse the modified AST back to SQL.
	rewrittenSQL, err := pg_query.Deparse(result)
	if err != nil {
		return rh.shadowOnlyQuery(querySQL)
	}

	if rh.verbose {
		log.Printf("[conn %d] complex read rewritten: %s", rh.connID, truncateSQL(rewrittenSQL, 200))
	}

	// Execute the rewritten query on shadow.
	shadowResult, err := execQuery(rh.shadowConn, rewrittenSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow complex query: %w", err)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
}

// rewriteDerivedTables recursively walks FROM clause nodes and replaces
// dirty subqueries with temp table references.
func (rh *ReadHandler) rewriteDerivedTables(node *pg_query.Node) (*pg_query.Node, bool, []string) {
	if node == nil {
		return node, false, nil
	}

	var utilTables []string
	modified := false

	// Check for RangeSubselect (derived table / subquery in FROM).
	if rs := node.GetRangeSubselect(); rs != nil {
		subquery := rs.GetSubquery()
		if subquery != nil {
			subSQL, err := deparseStmtNode(subquery)
			if err == nil && rh.isDirtyQuery(subSQL) {
				alias := ""
				if a := rs.GetAlias(); a != nil {
					alias = a.GetAliasname()
				}
				if alias == "" {
					alias = "_mori_derived"
				}

				if rs.GetLateral() {
					// LATERAL subqueries reference columns from outer tables, so they
					// cannot be executed as standalone queries (causes "missing
					// FROM-clause entry" errors). Instead, materialize the dirty base
					// tables that the LATERAL references into temp tables, rewrite
					// table references inside the subquery to point at the temp
					// tables, and let PostgreSQL handle the LATERAL join natively.
					tables, tErr := classifyForDirtyCheck(subSQL)
					if tErr == nil {
						seen := make(map[string]bool)
						tableMap := make(map[string]string)
						for _, table := range tables {
							if seen[table] || !rh.isTableDirty(table) {
								continue
							}
							seen[table] = true
							selectSQL := fmt.Sprintf("SELECT * FROM %s", quoteIdent(table))
							utilName, mErr := rh.materializeSubquery(selectSQL, table)
							if mErr != nil {
								if rh.verbose {
									log.Printf("[conn %d] LATERAL base table materialization failed for %s: %v", rh.connID, table, mErr)
								}
								continue
							}
							tableMap[table] = utilName
							utilTables = append(utilTables, utilName)
						}
						if len(tableMap) > 0 {
							rewriteTableRefsInNode(subquery, tableMap)
							modified = true
						}
					}
				} else {
					utilName, err := rh.materializeSubquery(subSQL, alias)
					if err == nil {
						utilTables = append(utilTables, utilName)
						// Replace with a RangeVar pointing to the temp table.
						newNode := &pg_query.Node{
							Node: &pg_query.Node_RangeVar{
								RangeVar: &pg_query.RangeVar{
									Relname: utilName,
									Inh:     true,
								},
							},
						}
						// Preserve the alias so outer query column references still resolve.
						if a := rs.GetAlias(); a != nil {
							newNode.GetRangeVar().Alias = a
						}
						return newNode, true, utilTables
					} else if rh.verbose {
						log.Printf("[conn %d] derived table materialization failed for %s: %v", rh.connID, alias, err)
					}
				}
			}
		}
	}

	// Recurse into JoinExpr.
	if je := node.GetJoinExpr(); je != nil {
		lNode, lMod, lUtils := rh.rewriteDerivedTables(je.GetLarg())
		rNode, rMod, rUtils := rh.rewriteDerivedTables(je.GetRarg())
		if lMod {
			je.Larg = lNode
			modified = true
			utilTables = append(utilTables, lUtils...)
		}
		if rMod {
			je.Rarg = rNode
			modified = true
			utilTables = append(utilTables, rUtils...)
		}
	}

	return node, modified, utilTables
}

// isDirtyQuery checks if a SQL query references any dirty tables
// (tables with deltas, tombstones, or schema diffs).
func (rh *ReadHandler) isDirtyQuery(sql string) bool {
	tables, err := classifyForDirtyCheck(sql)
	if err != nil {
		return false
	}
	for _, table := range tables {
		if rh.deltaMap != nil && rh.deltaMap.CountForTable(table) > 0 {
			return true
		}
		if rh.tombstones != nil && rh.tombstones.CountForTable(table) > 0 {
			return true
		}
		if rh.schemaRegistry != nil && rh.schemaRegistry.HasDiff(table) {
			return true
		}
	}
	return false
}

// classifyForDirtyCheck extracts table names from a SQL query for dirty checking.
// Returns a slice of table names.
func classifyForDirtyCheck(sql string) ([]string, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, stmt := range result.GetStmts() {
		collectTablesFromNode(stmt.GetStmt(), &tables)
	}
	return tables, nil
}

// collectTablesFromNode recursively extracts table names from an AST node.
func collectTablesFromNode(node *pg_query.Node, tables *[]string) {
	if node == nil {
		return
	}
	if rv := node.GetRangeVar(); rv != nil {
		name := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			name = s + "." + name
		}
		if name != "" {
			*tables = append(*tables, name)
		}
	}
	if sel := node.GetSelectStmt(); sel != nil {
		for _, from := range sel.GetFromClause() {
			collectTablesFromNode(from, tables)
		}
		if where := sel.GetWhereClause(); where != nil {
			collectTablesFromNode(where, tables)
		}
		if wc := sel.GetWithClause(); wc != nil {
			for _, cte := range wc.GetCtes() {
				if c := cte.GetCommonTableExpr(); c != nil {
					collectTablesFromNode(c.GetCtequery(), tables)
				}
			}
		}
	}
	if je := node.GetJoinExpr(); je != nil {
		collectTablesFromNode(je.GetLarg(), tables)
		collectTablesFromNode(je.GetRarg(), tables)
	}
	if rs := node.GetRangeSubselect(); rs != nil {
		collectTablesFromNode(rs.GetSubquery(), tables)
	}
}

// materializeDirtyBaseTables collects all dirty base tables referenced in the
// query AST (excluding CTE names), materializes each into a temp table via
// merged read, and returns a mapping from original table name to temp table name.
// Returns (tableMap, utilTables, modified). modified is true if at least one
// table was materialized.
func (rh *ReadHandler) materializeDirtyBaseTables(
	stmtNode *pg_query.Node,
	withClause *pg_query.WithClause,
) (map[string]string, []string, bool) {
	// Collect CTE names so we can exclude them from the base table list.
	cteNames := make(map[string]bool)
	for _, cteNode := range withClause.GetCtes() {
		if c := cteNode.GetCommonTableExpr(); c != nil {
			cteNames[c.GetCtename()] = true
		}
	}

	// Collect all table names referenced in the entire query AST.
	var allTables []string
	collectTablesFromNode(stmtNode, &allTables)

	// Deduplicate and filter out CTE names to get only real base tables.
	seen := make(map[string]bool)
	var baseTables []string
	for _, t := range allTables {
		if cteNames[t] || seen[t] {
			continue
		}
		seen[t] = true
		baseTables = append(baseTables, t)
	}

	// Materialize each dirty base table into a temp table.
	tableMap := make(map[string]string)
	var utilTables []string
	for _, table := range baseTables {
		if !rh.isTableDirty(table) {
			continue
		}

		selectSQL := fmt.Sprintf("SELECT * FROM %s", quoteIdent(table))
		utilName, err := rh.materializeSubquery(selectSQL, table)
		if err != nil {
			if rh.verbose {
				log.Printf("[conn %d] base table materialization failed for %s: %v", rh.connID, table, err)
			}
			continue
		}
		tableMap[table] = utilName
		utilTables = append(utilTables, utilName)
	}

	return tableMap, utilTables, len(tableMap) > 0
}

// collectDirtyRangeVarsFromNodes walks FROM clause nodes and materializes any
// dirty plain RangeVar tables into temp tables. It recurses into JoinExpr
// children to find nested RangeVar nodes. The tableMap is populated with
// original→temp table mappings and utilTables is appended with new temp names.
// Uses scoped materialization with pushed-down WHERE predicates when available.
func (rh *ReadHandler) collectDirtyRangeVarsFromNodes(nodes []*pg_query.Node, tableMap map[string]string, utilTables *[]string, tablePredicates map[string]string, tableToAlias map[string]string) {
	for _, node := range nodes {
		rh.collectDirtyRangeVarsFromNode(node, tableMap, utilTables, tablePredicates, tableToAlias)
	}
}

func (rh *ReadHandler) collectDirtyRangeVarsFromNode(node *pg_query.Node, tableMap map[string]string, utilTables *[]string, tablePredicates map[string]string, tableToAlias map[string]string) {
	if node == nil {
		return
	}
	if rv := node.GetRangeVar(); rv != nil {
		tableName := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			tableName = s + "." + tableName
		}
		if _, already := tableMap[tableName]; already {
			return
		}
		if rh.isTableDirty(tableName) {
			selectSQL := rh.buildScopedMaterializationSQL(tableName, tablePredicates, tableToAlias)
			utilName, mErr := rh.materializeSubquery(selectSQL, tableName)
			if mErr != nil {
				if rh.verbose {
					log.Printf("[conn %d] plain RangeVar materialization failed for %s: %v", rh.connID, tableName, mErr)
				}
				return
			}
			tableMap[tableName] = utilName
			*utilTables = append(*utilTables, utilName)
		}
	}
	if je := node.GetJoinExpr(); je != nil {
		rh.collectDirtyRangeVarsFromNode(je.GetLarg(), tableMap, utilTables, tablePredicates, tableToAlias)
		rh.collectDirtyRangeVarsFromNode(je.GetRarg(), tableMap, utilTables, tablePredicates, tableToAlias)
	}
}

// isTableDirty checks if a single table has deltas, tombstones, or schema diffs.
func (rh *ReadHandler) isTableDirty(table string) bool {
	if rh.deltaMap != nil && rh.deltaMap.CountForTable(table) > 0 {
		return true
	}
	if rh.tombstones != nil && rh.tombstones.CountForTable(table) > 0 {
		return true
	}
	if rh.schemaRegistry != nil && rh.schemaRegistry.HasDiff(table) {
		return true
	}
	return false
}

// rewriteTableRefsInNode recursively walks an AST node and rewrites all
// RangeVar references for tables in tableMap to point to their temp table names.
func rewriteTableRefsInNode(node *pg_query.Node, tableMap map[string]string) {
	if node == nil || len(tableMap) == 0 {
		return
	}

	if rv := node.GetRangeVar(); rv != nil {
		name := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			name = s + "." + name
		}
		if utilName, ok := tableMap[name]; ok {
			// Preserve the original table name as an alias so that qualified
			// column references (e.g. tags.color) still resolve after the
			// RangeVar is rewritten to the temp table name.
			if rv.GetAlias() == nil || rv.GetAlias().GetAliasname() == "" {
				rv.Alias = &pg_query.Alias{Aliasname: rv.GetRelname()}
			}
			rv.Relname = utilName
			rv.Schemaname = "" // Temp tables have no schema prefix.
		}
	}

	if sel := node.GetSelectStmt(); sel != nil {
		for _, from := range sel.GetFromClause() {
			rewriteTableRefsInNode(from, tableMap)
		}
		if where := sel.GetWhereClause(); where != nil {
			rewriteTableRefsInNode(where, tableMap)
		}
		if wc := sel.GetWithClause(); wc != nil {
			for _, cte := range wc.GetCtes() {
				if c := cte.GetCommonTableExpr(); c != nil {
					rewriteTableRefsInNode(c.GetCtequery(), tableMap)
				}
			}
		}
		// Recurse into set operations (UNION ALL in recursive CTEs).
		if sel.GetLarg() != nil {
			rewriteTableRefsInNode(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: sel.GetLarg()}}, tableMap)
		}
		if sel.GetRarg() != nil {
			rewriteTableRefsInNode(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: sel.GetRarg()}}, tableMap)
		}
	}

	if je := node.GetJoinExpr(); je != nil {
		rewriteTableRefsInNode(je.GetLarg(), tableMap)
		rewriteTableRefsInNode(je.GetRarg(), tableMap)
		if je.GetQuals() != nil {
			rewriteTableRefsInNode(je.GetQuals(), tableMap)
		}
	}

	if rs := node.GetRangeSubselect(); rs != nil {
		rewriteTableRefsInNode(rs.GetSubquery(), tableMap)
	}
}

// materializeSubquery runs a merged read on a subquery and materializes
// the results into a temp table on shadow.
func (rh *ReadHandler) materializeSubquery(sql string, hint string) (string, error) {
	utilName := utilTableName(sql)

	// Build a minimal classification for the merged read.
	// mergedReadCore checks cl.HasAggregate which defaults to false,
	// and cl.Tables which we populate from the subquery.
	cappedSQL := rh.capSQL(sql)
	tables, _ := classifyForDirtyCheck(cappedSQL)
	cl := &core.Classification{
		Tables: tables,
		RawSQL: cappedSQL,
	}

	// Run merged read to get merged results from prod + shadow.
	columns, values, nulls, err := rh.mergedReadCore(cl, cappedSQL)
	if err != nil {
		return "", fmt.Errorf("merged read for %s: %w", hint, err)
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("no columns returned for %s", hint)
	}

	// Create temp table on shadow and insert rows.
	createSQL := buildCreateTempTableSQL(utilName, columns)
	createResult, err := execQuery(rh.shadowConn, createSQL)
	if err != nil {
		return "", fmt.Errorf("creating temp table for %s: %w", hint, err)
	}
	if createResult.Error != "" {
		// Table might already exist from a previous call -- drop and recreate.
		dropUtilTable(rh.shadowConn, utilName)
		if _, err := execQuery(rh.shadowConn, createSQL); err != nil {
			return "", fmt.Errorf("recreating temp table for %s: %w", hint, err)
		}
	}

	if len(values) > 0 {
		if err := bulkInsertToUtilTable(rh.shadowConn, utilName, columns, values, nulls); err != nil {
			dropUtilTable(rh.shadowConn, utilName)
			return "", fmt.Errorf("inserting into temp table for %s: %w", hint, err)
		}
	}

	if rh.verbose {
		log.Printf("[conn %d] materialized %d rows into %s for %s", rh.connID, len(values), utilName, hint)
	}

	return utilName, nil
}

// deparseStmtNode deparses a single statement node back to SQL.
func deparseStmtNode(node *pg_query.Node) (string, error) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: node},
		},
	}
	return pg_query.Deparse(result)
}

// shadowOnlyQuery executes a query on shadow only and returns the results.
func (rh *ReadHandler) shadowOnlyQuery(sql string) ([]ColumnInfo, [][]string, [][]bool, error) {
	result, err := execQuery(rh.shadowConn, sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow query: %w", err)
	}
	if result.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}
	return result.Columns, result.RowValues, result.RowNulls, nil
}
