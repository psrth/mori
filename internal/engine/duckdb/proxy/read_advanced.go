package proxy

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/mori-dev/mori/internal/core"
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

// executeComplexRead handles CTEs, derived tables, and other complex queries
// by materializing base data into temp tables and re-executing on Shadow.
func (p *Proxy) executeComplexRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// For each affected table, materialize into a temp table.
	tableMap := make(map[string]string) // original table -> temp table
	for _, table := range cl.Tables {
		if !p.isTableAffected(table) {
			continue
		}

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
	fromIdx := strings.Index(upper, " FROM ")
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
