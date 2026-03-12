package proxy

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/psrth/mori/internal/core"
)

// executeWindowRead handles queries with window functions by materializing
// base data into a temp table and re-executing the window query on Shadow.
func (p *Proxy) executeWindowRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}
	table := cl.Tables[0]

	// Build base query by stripping window functions.
	baseSQL := buildWindowBaseQuery(sqlStr, table)
	if baseSQL == "" {
		// Can't decompose — fall back to Shadow if affected, Prod otherwise.
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}

	// Create classification for the base query.
	baseCl := *cl
	baseCl.HasWindowFunc = false
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""
	baseCl.RawSQL = baseSQL

	// Materialize base data.
	utilName, err := p.materializeToUtilTable(baseSQL, &baseCl, connID)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] window read materialization failed: %v", connID, err)
		}
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	defer p.dropUtilTable(utilName)

	// Rewrite original query to use temp table.
	rewrittenSQL := rewriteTableRef(sqlStr, table, utilName)

	// Execute rewritten query on Shadow.
	return p.executeQuery(p.shadowDB, rewrittenSQL, connID)
}

// executeSetOpRead handles UNION/INTERSECT/EXCEPT queries by executing each
// sub-SELECT through merged read and combining results.
func (p *Proxy) executeSetOpRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	// Split by set operators.
	parts := splitSetOperations(sqlStr)
	if len(parts) <= 1 {
		// No set operations found — fall back to regular merged read.
		return p.executeMergedRead(sqlStr, cl, connID)
	}

	// Execute each part as a merged read, then combine.
	var allCols []string
	var allRows [][]sql.NullString
	var allNulls [][]bool

	for i, part := range parts {
		partCl, err := p.classifier.Classify(part.sql)
		if err != nil {
			return p.executeQuery(p.prodDB, sqlStr, connID)
		}
		partCl.RawSQL = part.sql

		cols, rows, nulls, err := p.mergedReadRows(part.sql, partCl, connID)
		if err != nil {
			return p.executeQuery(p.prodDB, sqlStr, connID)
		}

		if i == 0 {
			allCols = cols
			allRows = rows
			allNulls = nulls
		} else {
			switch part.op {
			case "UNION ALL":
				allRows = append(allRows, rows...)
				allNulls = append(allNulls, nulls...)
			case "UNION":
				allRows = append(allRows, rows...)
				allNulls = append(allNulls, nulls...)
				allRows, allNulls = deduplicateByFullRow(allRows, allNulls)
			case "INTERSECT":
				allRows, allNulls = intersectRows(allRows, allNulls, rows, nulls)
			case "EXCEPT":
				allRows, allNulls = exceptRows(allRows, allNulls, rows, nulls)
			}
		}
	}

	// Apply ORDER BY and LIMIT from the original query.
	if cl.OrderBy != "" {
		sortMergedRows(allCols, allCols, allRows, allNulls, cl.OrderBy)
	}
	if cl.HasLimit && cl.Limit > 0 && len(allRows) > cl.Limit {
		allRows = allRows[:cl.Limit]
		allNulls = allNulls[:cl.Limit]
	}

	return buildMergedResponse(allCols, allRows, allNulls)
}

// executeJoinPatch handles JOIN queries by materializing each table's merged
// data into a temp table on shadow, rewriting table references, and executing
// the full JOIN on shadow. This lets DuckDB handle the JOIN natively with
// correct merged data. ALL tables are materialized (not just dirty ones)
// because the rewritten query runs on shadow, where clean tables have no data.
func (p *Proxy) executeJoinPatch(sqlStr string, cl *core.Classification, connID int64) []byte {
	if cl == nil || len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Check if any joined table is dirty.
	anyDirty := false
	for _, table := range cl.Tables {
		if p.isTableAffected(table) {
			anyDirty = true
			break
		}
	}
	if !anyDirty {
		// No dirty tables — execute directly on prod.
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Collect ALL tables referenced in the query, including SubLink tables
	// (correlated subqueries in the SELECT list). These need materialization too
	// because the rewritten query runs on shadow, where clean tables are empty.
	allTables := make([]string, len(cl.Tables))
	copy(allTables, cl.Tables)
	subLinkTables := collectSubLinkTablesFromSQL(sqlStr)
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

	// Materialize ALL tables into temp tables on shadow. Even clean tables
	// must be materialized because the rewritten query runs on shadow, where
	// clean tables have no data. The merged read for clean tables efficiently
	// copies prod data (shadow is empty, no filtering needed).
	tableMap := make(map[string]string) // original table -> temp table
	for _, table := range allTables {
		baseSQL := fmt.Sprintf(`SELECT * FROM "%s"`, table)
		baseCl := &core.Classification{
			OpType:  core.OpRead,
			SubType: core.SubSelect,
			Tables:  []string{table},
			RawSQL:  baseSQL,
		}

		utilName, err := p.materializeToUtilTable(baseSQL, baseCl, connID)
		if err != nil {
			if p.verbose {
				log.Printf("[conn %d] join patch materialization failed for %s: %v", connID, table, err)
			}
			continue
		}
		tableMap[table] = utilName
	}

	// Clean up temp tables on return.
	defer func() {
		for _, utilName := range tableMap {
			p.dropUtilTable(utilName)
		}
	}()

	if len(tableMap) == 0 {
		// Materialization failed for all tables — fall back to prod.
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Rewrite query to use temp tables.
	rewrittenSQL := sqlStr
	for origTable, utilName := range tableMap {
		rewrittenSQL = rewriteTableRef(rewrittenSQL, origTable, utilName)
	}

	if p.verbose {
		log.Printf("[conn %d] join patch: materialized %d tables, executing on shadow", connID, len(tableMap))
	}

	// Execute rewritten JOIN on shadow.
	return p.executeQuery(p.shadowDB, rewrittenSQL, connID)
}

// executeComplexRead handles CTEs, derived tables, and other complex queries
// by materializing base data into temp tables and re-executing on Shadow.
// ALL referenced tables are materialized (not just dirty ones) because the
// rewritten query runs on shadow, where clean tables have no data.
func (p *Proxy) executeComplexRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Collect ALL tables, including those from subqueries in the SELECT list.
	allTables := make([]string, len(cl.Tables))
	copy(allTables, cl.Tables)
	subLinkTables := collectSubLinkTablesFromSQL(sqlStr)
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

	// Check if any table is affected — if none, run on prod directly.
	anyAffected := false
	for _, table := range allTables {
		if p.isTableAffected(table) {
			anyAffected = true
			break
		}
	}
	if !anyAffected {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Materialize ALL tables into temp tables. Even clean tables must be
	// materialized because the rewritten query runs on shadow.
	tableMap := make(map[string]string) // original table -> temp table
	for _, table := range allTables {
		baseSQL := fmt.Sprintf(`SELECT * FROM "%s"`, table)
		baseCl := &core.Classification{
			OpType:  core.OpRead,
			SubType: core.SubSelect,
			Tables:  []string{table},
			RawSQL:  baseSQL,
		}

		utilName, err := p.materializeToUtilTable(baseSQL, baseCl, connID)
		if err != nil {
			if p.verbose {
				log.Printf("[conn %d] complex read materialization failed for %s: %v", connID, table, err)
			}
			continue
		}
		tableMap[table] = utilName
	}

	// Clean up temp tables on return.
	defer func() {
		for _, utilName := range tableMap {
			p.dropUtilTable(utilName)
		}
	}()

	// Rewrite query to use temp tables.
	rewrittenSQL := sqlStr
	for origTable, utilName := range tableMap {
		rewrittenSQL = rewriteTableRef(rewrittenSQL, origTable, utilName)
	}

	// Execute on Shadow.
	return p.executeQuery(p.shadowDB, rewrittenSQL, connID)
}

// isTableAffected checks if a table has deltas, tombstones, or schema diffs.
func (p *Proxy) isTableAffected(table string) bool {
	if p.deltaMap != nil && (p.deltaMap.CountForTable(table) > 0 || p.deltaMap.HasInserts(table)) {
		return true
	}
	if p.tombstones != nil && p.tombstones.CountForTable(table) > 0 {
		return true
	}
	if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
		return true
	}
	return false
}

// buildWindowBaseQuery builds a SELECT * FROM table WHERE ... query
// stripping window functions, to get base rows for materialization.
func buildWindowBaseQuery(sqlStr, _ string) string {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	fromIdx := findOuterFromIndex(upper)
	if fromIdx < 0 {
		return ""
	}
	// Use SELECT * FROM ... with the original WHERE clause.
	rest := sqlStr[fromIdx:]
	// Strip ORDER BY and LIMIT.
	for _, kw := range []string{"ORDER BY", "LIMIT", "OFFSET", "FETCH"} {
		if idx := strings.Index(strings.ToUpper(rest), kw); idx >= 0 {
			rest = rest[:idx]
		}
	}
	return fmt.Sprintf(`SELECT * %s`, strings.TrimSpace(rest))
}

// rewriteTableRef replaces table references in SQL from one name to another.
func rewriteTableRef(sqlStr, fromTable, toTable string) string {
	// Replace quoted references.
	sqlStr = strings.ReplaceAll(sqlStr, `"`+fromTable+`"`, `"`+toTable+`"`)

	// Replace unquoted references with word boundaries.
	pattern := `(?i)\b` + regexp.QuoteMeta(fromTable) + `\b`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(sqlStr, `"`+toTable+`"`)
}

// setOpPart represents a part of a set operation query.
type setOpPart struct {
	sql string
	op  string // "", "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
}

// splitSetOperations splits a query by UNION/INTERSECT/EXCEPT operators.
func splitSetOperations(sqlStr string) []setOpPart {
	upper := strings.ToUpper(sqlStr)

	// Find set operation positions.
	type opPos struct {
		idx int
		op  string
		len int
	}

	var ops []opPos
	for _, op := range []string{"UNION ALL", "UNION", "INTERSECT", "EXCEPT"} {
		searchFrom := 0
		for {
			idx := strings.Index(upper[searchFrom:], op)
			if idx < 0 {
				break
			}
			pos := searchFrom + idx
			// Verify it's not inside parentheses.
			depth := 0
			for i := range pos {
				if sqlStr[i] == '(' {
					depth++
				} else if sqlStr[i] == ')' {
					depth--
				}
			}
			if depth == 0 {
				ops = append(ops, opPos{idx: pos, op: op, len: len(op)})
			}
			searchFrom = pos + len(op)
		}
	}

	if len(ops) == 0 {
		return []setOpPart{{sql: sqlStr}}
	}

	// Sort by position and deduplicate (UNION ALL matches before UNION).
	// Simple sort by position.
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && ops[j].idx < ops[j-1].idx; j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}

	// Deduplicate overlapping matches (UNION ALL vs UNION at same position).
	var deduped []opPos
	for i, op := range ops {
		if i > 0 && op.idx < deduped[len(deduped)-1].idx+deduped[len(deduped)-1].len {
			continue
		}
		deduped = append(deduped, op)
	}

	var parts []setOpPart
	prev := 0
	for _, op := range deduped {
		part := strings.TrimSpace(sqlStr[prev:op.idx])
		if prev == 0 {
			parts = append(parts, setOpPart{sql: part, op: ""})
		} else {
			parts = append(parts, setOpPart{sql: part, op: deduped[len(parts)-1].op})
		}
		prev = op.idx + op.len
	}
	// Last part.
	if prev < len(sqlStr) {
		lastPart := strings.TrimSpace(sqlStr[prev:])
		op := ""
		if len(deduped) > 0 {
			op = deduped[len(deduped)-1].op
		}
		parts = append(parts, setOpPart{sql: lastPart, op: op})
	}

	return parts
}

// deduplicateByFullRow removes duplicate rows by comparing all column values.
func deduplicateByFullRow(rows [][]sql.NullString, nulls [][]bool) ([][]sql.NullString, [][]bool) {
	seen := make(map[string]bool)
	var deduped [][]sql.NullString
	var dedupedNulls [][]bool

	for i, row := range rows {
		key := rowKey(row)
		if seen[key] {
			continue
		}
		seen[key] = true
		deduped = append(deduped, row)
		dedupedNulls = append(dedupedNulls, nulls[i])
	}
	return deduped, dedupedNulls
}

func rowKey(row []sql.NullString) string {
	var parts []string
	for _, v := range row {
		if v.Valid {
			parts = append(parts, v.String)
		} else {
			parts = append(parts, "\x00NULL\x00")
		}
	}
	return strings.Join(parts, "\x01")
}

// intersectRows returns rows that appear in both sets.
func intersectRows(a [][]sql.NullString, aNulls [][]bool, b [][]sql.NullString, _ [][]bool) ([][]sql.NullString, [][]bool) {
	bKeys := make(map[string]bool)
	for _, row := range b {
		bKeys[rowKey(row)] = true
	}

	var result [][]sql.NullString
	var resultNulls [][]bool
	for i, row := range a {
		if bKeys[rowKey(row)] {
			result = append(result, row)
			resultNulls = append(resultNulls, aNulls[i])
		}
	}
	return result, resultNulls
}

// exceptRows returns rows in A that don't appear in B.
func exceptRows(a [][]sql.NullString, aNulls [][]bool, b [][]sql.NullString, _ [][]bool) ([][]sql.NullString, [][]bool) {
	bKeys := make(map[string]bool)
	for _, row := range b {
		bKeys[rowKey(row)] = true
	}

	var result [][]sql.NullString
	var resultNulls [][]bool
	for i, row := range a {
		if !bKeys[rowKey(row)] {
			result = append(result, row)
			resultNulls = append(resultNulls, aNulls[i])
		}
	}
	return result, resultNulls
}

// collectSubLinkTablesFromSQL parses a SQL query and extracts table names
// from SubLink nodes (correlated subqueries in SELECT list, WHERE, etc.).
// These tables are not in the FROM clause and would be missed by the classifier.
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
					collectTablesFromNode(from, tables, seen)
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

// collectTablesFromNode extracts table names from AST nodes (RangeVar, JoinExpr).
func collectTablesFromNode(node *pg_query.Node, tables *[]string, seen map[string]bool) {
	if node == nil {
		return
	}
	if rv := node.GetRangeVar(); rv != nil {
		tableName := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			tableName = s + "." + tableName
		}
		if tableName != "" && !seen[tableName] {
			seen[tableName] = true
			*tables = append(*tables, tableName)
		}
		return
	}
	if je := node.GetJoinExpr(); je != nil {
		collectTablesFromNode(je.GetLarg(), tables, seen)
		collectTablesFromNode(je.GetRarg(), tables, seen)
	}
}
