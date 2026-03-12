package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/psrth/mori/internal/core"
)

// handleComplexRead handles CTE and derived table queries on dirty tables
// by decomposing, materializing dirty sub-queries, and rewriting.
func (rh *ReadHandler) handleComplexRead(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.complexReadCore(cl, cl.RawSQL)
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

// complexReadCore decomposes a complex query (CTEs, derived tables), materializes
// dirty sub-queries into temp tables on shadow, rewrites the AST, and executes.
func (rh *ReadHandler) complexReadCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	parser, err := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	if err != nil {
		return rh.complexFallback(querySQL)
	}

	stmt, err := parser.Parse(querySQL)
	if err != nil {
		// Can't parse -- fall back to shadow-only execution.
		return rh.complexFallback(querySQL)
	}

	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return rh.complexFallback(querySQL)
	}

	var utilTables []string // Track temp tables for cleanup.
	defer func() {
		for _, ut := range utilTables {
			dropUtilTable(rh.shadowConn, ut)
		}
	}()

	modified := false

	// Handle CTEs (WITH clauses).
	if sel.With != nil {
		if sel.With.Recursive {
			// Recursive CTE: materialize dirty base tables, not the CTE body.
			// The CTE body self-references the CTE name, so it can't be executed
			// in isolation. Instead, we materialize each dirty base table into a
			// temp table via merged read, rewrite all table references in the
			// entire query to point to the temp tables, then execute the complete
			// rewritten query (with WITH RECURSIVE intact) on shadow.
			tableMap, newUtils, didMaterialize := rh.materializeDirtyBaseTablesMySQL(sel)
			utilTables = append(utilTables, newUtils...)
			if didMaterialize {
				rewriteTableRefsMySQL(sel, tableMap)
				modified = true
			}
		} else {
			// Non-recursive CTEs: materialize the CTE body as a standalone query.
			for _, cte := range sel.With.CTEs {
				if cte == nil || cte.Subquery == nil {
					continue
				}

				cteName := cte.ID.String()
				cteSQL := sqlparser.String(cte.Subquery)

				// Check if this CTE references dirty tables.
				if !rh.isDirtyQueryMySQL(cteSQL) {
					continue // Clean CTE -- leave in place.
				}

				// Materialize dirty CTE into a temp table.
				utilName, mErr := rh.materializeSubqueryMySQL(cteSQL, cteName)
				if mErr != nil {
					if rh.verbose {
						log.Printf("[conn %d] CTE materialization failed for %s: %v", rh.connID, cteName, mErr)
					}
					continue // Skip -- leave CTE in place.
				}
				utilTables = append(utilTables, utilName)

				// Replace CTE body with a simple SELECT * FROM temp_table.
				replacementSQL := fmt.Sprintf("SELECT * FROM `%s`", utilName)
				replacementStmt, pErr := parser.Parse(replacementSQL)
				if pErr != nil {
					continue
				}
				if replSel, ok := replacementStmt.(*sqlparser.Select); ok {
					cte.Subquery = replSel
					modified = true
				}
			}
		}
	}

	// Handle derived tables (subqueries in FROM clause).
	if len(sel.From) > 0 {
		for i, fromExpr := range sel.From {
			newExpr, replaced, newUtils := rh.rewriteDerivedTablesMySQL(fromExpr, parser)
			if replaced {
				sel.From[i] = newExpr
				utilTables = append(utilTables, newUtils...)
				modified = true
			}
		}
	}

	if !modified {
		// Nothing was dirty -- execute on shadow as-is.
		return rh.complexFallback(querySQL)
	}

	// Deparse the modified AST back to SQL.
	rewrittenSQL := sqlparser.String(sel)

	if rh.verbose {
		log.Printf("[conn %d] complex read rewritten: %s", rh.connID, truncateSQL(rewrittenSQL, 200))
	}

	// Execute the rewritten query on shadow.
	shadowResult, err := execMySQLQuery(rh.shadowConn, rewrittenSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow complex query: %w", err)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
}

// rewriteDerivedTablesMySQL recursively walks FROM clause nodes and replaces
// dirty subqueries with temp table references.
func (rh *ReadHandler) rewriteDerivedTablesMySQL(
	tableExpr sqlparser.TableExpr,
	parser *sqlparser.Parser,
) (sqlparser.TableExpr, bool, []string) {
	if tableExpr == nil {
		return tableExpr, false, nil
	}

	var utilTables []string
	modified := false

	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		// Check for derived table (subquery in FROM).
		dt, ok := expr.Expr.(*sqlparser.DerivedTable)
		if !ok || dt.Select == nil {
			return tableExpr, false, nil
		}

		subSQL := sqlparser.String(dt.Select)
		if !rh.isDirtyQueryMySQL(subSQL) {
			return tableExpr, false, nil
		}

		alias := expr.As.String()
		if alias == "" {
			alias = "_mori_derived"
		}

		if dt.Lateral {
			// LATERAL subqueries reference columns from outer tables, so they
			// cannot be executed as standalone queries. Instead, materialize
			// ALL base tables that the LATERAL references into temp tables,
			// rewrite table references inside the subquery to point at the temp
			// tables, and let MySQL handle the LATERAL join natively.
			// All tables must be materialized (not just dirty) because the
			// rewritten query runs on shadow, where clean tables have no data.
			tables := collectTableNamesFromStmt(dt.Select)
			seen := make(map[string]bool)
			tableMap := make(map[string]string)

			for _, table := range tables {
				if seen[table] {
					continue
				}
				seen[table] = true

				selectSQL := fmt.Sprintf("SELECT * FROM `%s`", table)
				utilName, mErr := rh.materializeSubqueryMySQL(selectSQL, table)
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
				rewriteTableRefsInStmt(dt.Select, tableMap)
				modified = true
			}
		} else {
			// Non-LATERAL derived table: materialize the entire subquery.
			utilName, err := rh.materializeSubqueryMySQL(subSQL, alias)
			if err != nil {
				if rh.verbose {
					log.Printf("[conn %d] derived table materialization failed for %s: %v", rh.connID, alias, err)
				}
				return tableExpr, false, nil
			}
			utilTables = append(utilTables, utilName)

			// Replace with a simple table reference pointing to the temp table.
			newExpr := &sqlparser.AliasedTableExpr{
				Expr: sqlparser.TableName{
					Name: sqlparser.NewIdentifierCS(utilName),
				},
				As: expr.As, // Preserve the alias so outer column references still resolve.
			}
			return newExpr, true, utilTables
		}

	case *sqlparser.JoinTableExpr:
		// Recurse into JOIN children.
		lExpr, lMod, lUtils := rh.rewriteDerivedTablesMySQL(expr.LeftExpr, parser)
		rExpr, rMod, rUtils := rh.rewriteDerivedTablesMySQL(expr.RightExpr, parser)
		if lMod {
			expr.LeftExpr = lExpr
			modified = true
			utilTables = append(utilTables, lUtils...)
		}
		if rMod {
			expr.RightExpr = rExpr
			modified = true
			utilTables = append(utilTables, rUtils...)
		}

	case *sqlparser.ParenTableExpr:
		// Recurse into parenthesized table expressions.
		for i, inner := range expr.Exprs {
			newInner, innerMod, innerUtils := rh.rewriteDerivedTablesMySQL(inner, parser)
			if innerMod {
				expr.Exprs[i] = newInner
				modified = true
				utilTables = append(utilTables, innerUtils...)
			}
		}
	}

	return tableExpr, modified, utilTables
}

// isDirtyQueryMySQL checks if a SQL query references any dirty tables
// (tables with deltas, tombstones, or schema diffs).
func (rh *ReadHandler) isDirtyQueryMySQL(sql string) bool {
	parser, err := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	if err != nil {
		return false
	}
	stmt, err := parser.Parse(sql)
	if err != nil {
		return false
	}
	tables := collectTableNamesFromStmt(stmt)
	for _, table := range tables {
		if rh.isTableDirtyMySQL(table) {
			return true
		}
	}
	return false
}

// isTableDirtyMySQL checks if a single table has deltas, tombstones, or schema diffs.
func (rh *ReadHandler) isTableDirtyMySQL(table string) bool {
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

// collectTableNamesFromStmt extracts all table names from any Vitess AST statement.
// It walks the entire AST tree and collects unique TableName nodes.
func collectTableNamesFromStmt(stmt sqlparser.SQLNode) []string {
	var tables []string
	seen := make(map[string]bool)

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		tn, ok := node.(sqlparser.TableName)
		if !ok {
			return true, nil
		}
		name := tn.Name.String()
		if name != "" && !seen[name] {
			seen[name] = true
			tables = append(tables, name)
		}
		return true, nil
	}, stmt)

	return tables
}

// rewriteTableRefsMySQL rewrites all table name references in a *sqlparser.Select
// AST to point to temp table names according to the provided mapping.
func rewriteTableRefsMySQL(sel *sqlparser.Select, tableMap map[string]string) {
	if len(tableMap) == 0 {
		return
	}
	rewriteTableRefsInStmt(sel, tableMap)
}

// rewriteTableRefsInStmt walks an entire Vitess AST and rewrites table
// references for tables in tableMap to point to their temp table names.
// Preserves the original table name as an alias so that qualified column
// references (e.g. orders.id) still resolve after the rewrite.
func rewriteTableRefsInStmt(stmt sqlparser.SQLNode, tableMap map[string]string) {
	if stmt == nil || len(tableMap) == 0 {
		return
	}

	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		ate, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok {
			return true, nil
		}
		tn, ok := ate.Expr.(sqlparser.TableName)
		if !ok {
			return true, nil
		}
		name := tn.Name.String()
		utilName, found := tableMap[name]
		if !found {
			return true, nil
		}
		// Preserve original table name as alias if none exists, so that
		// qualified column references (e.g. t.col) still resolve.
		if ate.As.IsEmpty() {
			ate.As = sqlparser.NewIdentifierCS(name)
		}
		ate.Expr = sqlparser.TableName{
			Name: sqlparser.NewIdentifierCS(utilName),
		}
		return true, nil
	}, stmt)
}

// materializeDirtyBaseTablesMySQL collects all base tables referenced in
// the query AST (excluding CTE names), materializes each into a temp table via
// merged read, and returns a mapping from original table name to temp table name.
// ALL tables are materialized (not just dirty ones) because the rewritten query
// runs on shadow, where clean tables have no data.
// Returns (tableMap, utilTables, modified). modified is true if at least one
// table was materialized.
func (rh *ReadHandler) materializeDirtyBaseTablesMySQL(
	sel *sqlparser.Select,
) (map[string]string, []string, bool) {
	// Collect CTE names so we can exclude them from the base table list.
	cteNames := make(map[string]bool)
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			if cte != nil {
				cteNames[cte.ID.String()] = true
			}
		}
	}

	// Collect all table names referenced in the entire query AST.
	allTables := collectTableNamesFromStmt(sel)

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

	// Materialize ALL base tables into temp tables. Even clean tables must
	// be materialized because the rewritten query runs on shadow, where
	// clean tables have no data.
	tableMap := make(map[string]string)
	var utilTables []string
	for _, table := range baseTables {
		selectSQL := fmt.Sprintf("SELECT * FROM `%s`", table)
		utilName, err := rh.materializeSubqueryMySQL(selectSQL, table)
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

// materializeSubqueryMySQL runs a merged read on a subquery and materializes
// the results into a temp table on shadow.
func (rh *ReadHandler) materializeSubqueryMySQL(sql string, hint string) (string, error) {
	utilName := utilTableName(sql)

	// Build a minimal classification for the merged read.
	cl, err := rh.classifyForMaterialization(sql)
	if err != nil {
		return "", fmt.Errorf("classify for materialization of %s: %w", hint, err)
	}

	// Cap the query with a reasonable limit to prevent excessive data.
	cappedSQL := sql
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.Contains(upper, "LIMIT") {
		cappedSQL = fmt.Sprintf("%s LIMIT 100000", sql)
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
	createResult, err := execMySQLQuery(rh.shadowConn, createSQL)
	if err != nil {
		return "", fmt.Errorf("creating temp table for %s: %w", hint, err)
	}
	if createResult.Error != "" {
		// Table might already exist from a previous call -- drop and recreate.
		dropUtilTable(rh.shadowConn, utilName)
		createResult, err = execMySQLQuery(rh.shadowConn, createSQL)
		if err != nil {
			return "", fmt.Errorf("recreating temp table for %s: %w", hint, err)
		}
		if createResult.Error != "" {
			return "", fmt.Errorf("create temp table error for %s: %s", hint, createResult.Error)
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

// complexFallback executes the query on Shadow only when decomposition
// is not possible or not needed.
func (rh *ReadHandler) complexFallback(sql string) ([]ColumnInfo, [][]string, [][]bool, error) {
	result, err := execMySQLQuery(rh.shadowConn, sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow complex fallback: %w", err)
	}
	if result.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
	}
	return result.Columns, result.RowValues, result.RowNulls, nil
}
