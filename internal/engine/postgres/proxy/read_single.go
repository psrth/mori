package proxy

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/psrth/mori/internal/core"
)

// handleMergedRead implements the single-table merged read algorithm.
// Steps: Shadow query -> Prod query (over-fetched) -> filter -> adapt -> merge -> dedup -> sort -> limit -> respond.
func (rh *ReadHandler) handleMergedRead(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.mergedReadCore(cl, cl.RawSQL)
	if err != nil {
		// Check if this is a relay error (raw response already sent).
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

// relayError wraps raw backend response bytes for errors that should be
// relayed directly to the client (e.g., Shadow/Prod query errors).
type relayError struct {
	rawMsgs []byte
	msg     string
}

func (e *relayError) Error() string { return e.msg }

// mergedReadCore performs the merged read algorithm and returns the result
// without writing to the client. The querySQL parameter is the SQL to execute
// (may differ from cl.RawSQL when parameters have been substituted).
func (rh *ReadHandler) mergedReadCore(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	// Aggregate queries need special handling: row-level merge then re-aggregate.
	if cl.HasAggregate {
		return rh.aggregateReadCore(cl, querySQL)
	}

	if len(cl.Tables) == 0 {
		return nil, nil, nil, fmt.Errorf("merged read with no tables")
	}
	table := cl.Tables[0]

	// Step 0: Inject PK column into query if needed for dedup.
	// Skip PK injection for JOINs — unqualified column names cause ambiguity.
	injectedPK := ""
	effectiveSQL := querySQL
	if !cl.IsJoin {
		if meta, ok := rh.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(querySQL, pkCol) {
				effectiveSQL = injectPKColumn(querySQL, pkCol)
				injectedPK = pkCol
			}
		} else if meta, ok := rh.tables[table]; ok && meta.PKType == "none" {
			// No PK — inject a virtual PK for dedup and tombstone filtering.
			// CockroachDB uses "rowid" (implicit hidden column); PostgreSQL uses "ctid".
			virtualPK := "ctid"
			if rh.isCockroachDB {
				virtualPK = "rowid"
			}
			if needsVirtualPKInjection(querySQL, virtualPK) {
				effectiveSQL = injectVirtualPKColumn(querySQL, virtualPK)
				injectedPK = virtualPK
			}
		}
	}

	// Step 1: Execute query on Shadow verbatim.
	shadowResult, err := execQuery(rh.shadowConn, effectiveSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow query: %w", err)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	// Step 1.5: Rewrite Prod query for schema diffs (DDL changes).
	// If the table only exists in Shadow (CREATE TABLE), skip Prod entirely.
	prodSQL := effectiveSQL
	skipProd := false
	if rh.schemaRegistry != nil {
		rewritten, shadowOnly := rh.rewriteForProd(effectiveSQL, table)
		if shadowOnly {
			skipProd = true
		} else {
			prodSQL = rewritten
		}
	}

	// Check if table is fully shadowed (e.g., after TRUNCATE) — skip Prod entirely.
	if !skipProd && rh.schemaRegistry != nil && rh.schemaRegistry.IsFullyShadowed(table) {
		skipProd = true
	}

	// Step 2: Execute query on Prod (with over-fetching for LIMIT queries).
	var prodResult *QueryResult
	if skipProd {
		// Table only exists in Shadow — no Prod results.
		prodResult = &QueryResult{}
	} else {
		if cl.HasLimit && cl.Limit > 0 {
			deltaCount := rh.deltaMap.CountForTable(table)
			tombstoneCount := rh.tombstones.CountForTable(table)
			overfetch := deltaCount + tombstoneCount
			if overfetch > 0 {
				prodSQL = rewriteLimit(prodSQL, cl.Limit+overfetch)
			}
		}

		prodResult, err = execQuery(rh.prodConn, prodSQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prod query: %w", err)
		}
		if prodResult.Error != "" {
			// If Prod query fails due to schema mismatch, fall back to Shadow-only.
			hasSchemaChange := false
			if rh.schemaRegistry != nil {
				for _, t := range cl.Tables {
					if rh.schemaRegistry.HasDiff(t) {
						hasSchemaChange = true
						break
					}
				}
			}
			if hasSchemaChange {
				prodResult = &QueryResult{}
			} else {
				return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
			}
		}
	}

	// Step 3: Filter Prod results — remove delta and tombstoned rows.
	filteredValues, filteredNulls := rh.filterProdRows(table, prodResult)

	// Step 4: Schema adaptation (if SchemaRegistry has diffs for this table).
	prodColumns := rh.adaptColumns(table, prodResult.Columns)
	adaptedValues, adaptedNulls := rh.adaptRows(table, prodResult.Columns, filteredValues, filteredNulls)

	// Step 5: Merge Shadow + filtered/adapted Prod.
	mergedColumns, mergedValues, mergedNulls := mergeResults(
		shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls,
		prodColumns, adaptedValues, adaptedNulls,
	)

	// Step 6: Deduplicate by PK (Shadow rows come first, so Shadow version wins).
	mergedValues, mergedNulls = rh.dedup(table, mergedColumns, mergedValues, mergedNulls)

	// Step 7: Re-sort by original ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(mergedColumns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Step 8: Apply LIMIT to merged set.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	// Step 9: Strip injected PK column if it was added for dedup.
	if injectedPK != "" {
		mergedColumns, mergedValues, mergedNulls = stripInjectedColumn(
			mergedColumns, mergedValues, mergedNulls, injectedPK)
	}

	// Step 10: Apply DISTINCT if requested.
	if cl != nil && cl.HasDistinct {
		mergedValues, mergedNulls = deduplicateByFullRow(mergedValues, mergedNulls)
	}

	return mergedColumns, mergedValues, mergedNulls, nil
}

// aggregateReadCore handles aggregate queries on affected tables by converting
// to a row-level merged read and re-aggregating.
//
// Strategy order:
//  1. Simple COUNT(*) without GROUP BY → base SELECT pk, merge, count rows.
//  2. GROUP BY with supported aggregates → base SELECT cols, merge, re-aggregate in Go.
//  2.5. Complex aggregates (array_agg, json_agg, string_agg, etc.) → materialize
//     base data into temp table, re-execute original aggregate on shadow.
//  3. Fall back to Prod-only execution (when no schema diffs) or Shadow-only (with schema diffs).
func (rh *ReadHandler) aggregateReadCore(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	if len(cl.Tables) == 0 {
		return nil, nil, nil, fmt.Errorf("aggregate read with no tables")
	}
	table := cl.Tables[0]

	// Strategy 1: Simple COUNT(*) without GROUP BY.
	baseSQL := rh.buildAggregateBaseQuery(querySQL, table)
	if baseSQL != "" {
		return rh.executeSimpleCountAggregate(cl, baseSQL, querySQL)
	}

	// Strategy 2: GROUP BY with supported aggregates (COUNT, SUM, AVG, MIN, MAX).
	spec := rh.buildGroupBySpec(querySQL, table)
	if spec != nil {
		return rh.executeGroupByAggregate(cl, spec)
	}

	// Strategy 2.5: Materialize and re-execute for complex/unsupported aggregates.
	// For array_agg, json_agg, string_agg, and other aggregates that can't be
	// decomposed into row-level merge + re-aggregation, materialize the base data
	// into a temp table and let PostgreSQL handle the aggregate natively.
	cols, vals, nuls, matErr := rh.materializeAndAggregate(cl, querySQL, table)
	if matErr == nil {
		return cols, vals, nuls, nil
	}
	if rh.verbose {
		log.Printf("[conn %d] aggregate materialization failed, using fallback: %v", rh.connID, matErr)
	}

	// Strategy 3: Fall back to Prod or Shadow.
	return rh.aggregateFallback(cl, querySQL)
}

// executeSimpleCountAggregate handles COUNT(*) without GROUP BY through the merge pipeline.
func (rh *ReadHandler) executeSimpleCountAggregate(cl *core.Classification, baseSQL, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	baseCl := *cl
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""

	_, baseValues, _, mergeErr := rh.mergedReadCore(&baseCl, baseSQL)
	if mergeErr != nil {
		if _, ok := mergeErr.(*relayError); ok {
			prodResult, pErr := execQuery(rh.prodConn, querySQL)
			if pErr != nil {
				return nil, nil, nil, mergeErr
			}
			if prodResult.Error != "" {
				return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
			}
			return prodResult.Columns, prodResult.RowValues, prodResult.RowNulls, nil
		}
		return nil, nil, nil, mergeErr
	}

	count := len(baseValues)
	countStr := fmt.Sprintf("%d", count)
	return []ColumnInfo{{Name: "count", OID: 20}},
		[][]string{{countStr}},
		[][]bool{{false}},
		nil
}

// executeGroupByAggregate handles GROUP BY queries by fetching row-level data
// through the merge pipeline and re-aggregating in Go.
func (rh *ReadHandler) executeGroupByAggregate(cl *core.Classification, spec *groupBySpec) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	baseCl := *cl
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""

	baseCols, baseValues, baseNulls, err := rh.mergedReadCore(&baseCl, spec.BaseSQL)
	if err != nil {
		// If merge fails, try Shadow-only as last resort.
		shadowResult, sErr := execQuery(rh.shadowConn, cl.RawSQL)
		if sErr != nil || shadowResult.Error != "" {
			return nil, nil, nil, fmt.Errorf("group by merge failed: %w", err)
		}
		return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
	}

	// Re-aggregate the merged rows.
	resultCols, resultValues, resultNulls := reAggregateRows(spec, baseCols, baseValues, baseNulls)

	// Apply HAVING filter.
	if spec.HavingAST != nil {
		// Normalize HAVING AST: replace FuncCall nodes that match aggregates
		// with ColumnRef nodes so evaluateWhere can resolve their values.
		normalizedHaving := normalizeHavingForEval(spec.HavingAST, spec.Aggregates)

		var filtered [][]string
		var filteredNulls [][]bool
		for i, row := range resultValues {
			rowMap := make(map[string]string)
			for j, col := range resultCols {
				if j < len(row) {
					rowMap[col.Name] = row[j]
				}
			}
			if evaluateHaving(normalizedHaving, rowMap) {
				filtered = append(filtered, row)
				filteredNulls = append(filteredNulls, resultNulls[i])
			}
		}
		resultValues = filtered
		resultNulls = filteredNulls
	}

	// Apply ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(resultCols, resultValues, resultNulls, cl.OrderBy)
	}

	// Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(resultValues) > cl.Limit {
		resultValues = resultValues[:cl.Limit]
		resultNulls = resultNulls[:cl.Limit]
	}

	return resultCols, resultValues, resultNulls, nil
}

// aggregateFallback handles aggregate queries that can't be decomposed into
// row-level merge + re-aggregation. If the table has schema diffs, it tries
// Prod with rewritten SQL; otherwise falls back to Prod-only execution.
func (rh *ReadHandler) aggregateFallback(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	hasSchemaChange := false
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.HasDiff(t) {
				hasSchemaChange = true
				break
			}
		}
	}

	if hasSchemaChange {
		// Try rewriting the query for Prod compatibility.
		rewritten, skipProd := rewriteSQLForProd(querySQL, rh.schemaRegistry, cl.Tables)
		if !skipProd {
			prodResult, err := execQuery(rh.prodConn, rewritten)
			if err == nil && prodResult.Error == "" {
				return prodResult.Columns, prodResult.RowValues, prodResult.RowNulls, nil
			}
		}

		// Prod failed or was skipped — try Shadow-only as last resort.
		shadowResult, err := execQuery(rh.shadowConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("shadow aggregate fallback: %w", err)
		}
		if shadowResult.Error != "" {
			return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
		}
		return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
	}

	// No schema diffs — safe to run on Prod directly.
	prodResult, err := execQuery(rh.prodConn, querySQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prod aggregate query: %w", err)
	}
	if prodResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
	}
	return prodResult.Columns, prodResult.RowValues, prodResult.RowNulls, nil
}

// buildAggregateBaseQuery converts a COUNT(*) query to SELECT pk FROM table [WHERE ...].
// Returns "" if the query has GROUP BY or can't be converted.
func (rh *ReadHandler) buildAggregateBaseQuery(sql, table string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Skip GROUP BY queries — need full aggregation engine.
	if strings.Contains(upper, "GROUP BY") {
		return ""
	}

	// Find PK column for the table.
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return ""
	}
	pkCol := meta.PKColumns[0]

	// Find SELECT and FROM positions.
	selectIdx := strings.Index(upper, "SELECT")
	fromIdx := strings.Index(upper, " FROM ")
	if selectIdx < 0 || fromIdx < 0 {
		return ""
	}

	// Replace SELECT ... FROM with SELECT pk FROM, keep the rest (WHERE, etc.).
	return "SELECT " + pkCol + sql[fromIdx:]
}

// needsPKInjection returns true if the query is a simple single-table SELECT
// that doesn't include the PK column in its SELECT list.
// Returns false for queries where PK injection would break things:
// set operations (UNION/INTERSECT/EXCEPT), subqueries in FROM, CTEs.
func needsPKInjection(sql, pkCol string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Skip PK injection for set operations — adding a column breaks column count rules.
	if strings.Contains(upper, " UNION ") || strings.Contains(upper, " INTERSECT ") || strings.Contains(upper, " EXCEPT ") {
		return false
	}

	// Skip PK injection for CTEs (WITH ... AS) — column references may not resolve in outer query.
	if strings.HasPrefix(upper, "WITH ") {
		return false
	}

	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return false
	}
	afterSelect := strings.TrimSpace(upper[selectIdx+6:])
	// SELECT * already includes all columns.
	if strings.HasPrefix(afterSelect, "*") || strings.HasPrefix(afterSelect, "DISTINCT *") {
		return false
	}
	// Check if the PK column appears in the SELECT list (before FROM).
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return false
	}

	// Skip PK injection if FROM contains a subquery (derived table).
	afterFrom := strings.TrimSpace(upper[fromIdx+6:])
	if strings.HasPrefix(afterFrom, "(") {
		return false
	}

	selectList := strings.ToLower(sql[selectIdx+6 : fromIdx])
	return !containsColumn(selectList, strings.ToLower(pkCol))
}

// containsColumn checks if a comma-separated select list contains the given column name.
func containsColumn(selectList, col string) bool {
	parts := strings.Split(selectList, ",")
	for _, part := range parts {
		name := strings.TrimSpace(part)
		// Handle aliases: "col AS alias"
		if idx := strings.Index(strings.ToLower(name), " as "); idx >= 0 {
			name = strings.TrimSpace(name[:idx])
		}
		// Strip table prefix: "t.col" -> "col"
		if dotIdx := strings.LastIndex(name, "."); dotIdx >= 0 {
			name = name[dotIdx+1:]
		}
		name = strings.Trim(name, `"`)
		if name == col {
			return true
		}
	}
	return false
}

// needsVirtualPKInjection returns true if the query is a simple single-table SELECT
// where a virtual PK column (ctid for PostgreSQL, rowid for CockroachDB) should be
// injected for dedup on PK-less tables.
// Unlike needsPKInjection, virtual PK injection is needed for SELECT * too
// (system/hidden columns are not included in *).
// Returns false for set operations, subqueries in FROM, and CTEs.
func needsVirtualPKInjection(sql string, colName string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Skip for set operations.
	if strings.Contains(upper, " UNION ") || strings.Contains(upper, " INTERSECT ") || strings.Contains(upper, " EXCEPT ") {
		return false
	}

	// Skip for CTEs.
	if strings.HasPrefix(upper, "WITH ") {
		return false
	}

	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return false
	}

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return false
	}

	// Skip if FROM contains a subquery (derived table).
	afterFrom := strings.TrimSpace(upper[fromIdx+6:])
	if strings.HasPrefix(afterFrom, "(") {
		return false
	}

	// Check if the virtual PK column is already in the SELECT list.
	selectList := strings.ToLower(sql[selectIdx+6 : fromIdx])
	return !containsColumn(selectList, strings.ToLower(colName))
}

// injectVirtualPKColumn adds a virtual PK column to the SELECT list for dedup
// on PK-less tables. Handles both explicit column lists and SELECT *.
// Rewrites "SELECT col1, col2 FROM ..." to "SELECT <col>, col1, col2 FROM ..."
// and "SELECT * FROM ..." to "SELECT <col>, * FROM ...".
func injectVirtualPKColumn(sql string, colName string) string {
	upper := strings.ToUpper(sql)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return sql
	}
	insertPos := selectIdx + 6
	// Handle DISTINCT.
	afterSelect := strings.TrimSpace(sql[insertPos:])
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT") {
		insertPos += (len(sql[insertPos:]) - len(afterSelect)) + 8 // len("DISTINCT")
	}
	return sql[:insertPos] + " " + colName + "," + sql[insertPos:]
}

// findColumnIndex returns the index of a named column in the result columns.
// Returns -1 if the column is not found.
func findColumnIndex(columns []ColumnInfo, name string) int {
	for i, col := range columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// injectPKColumn adds the PK column to the SELECT list for dedup purposes.
// Rewrites "SELECT col1, col2 FROM ..." to "SELECT pk, col1, col2 FROM ...".
func injectPKColumn(sql, pkCol string) string {
	upper := strings.ToUpper(sql)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return sql
	}
	insertPos := selectIdx + 6
	// Handle DISTINCT.
	afterSelect := strings.TrimSpace(sql[insertPos:])
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT") {
		insertPos += (len(sql[insertPos:]) - len(afterSelect)) + 8 // len("DISTINCT")
	}
	return sql[:insertPos] + " " + quoteIdent(pkCol) + "," + sql[insertPos:]
}

// stripInjectedColumn removes the injected PK column from the result set.
func stripInjectedColumn(
	columns []ColumnInfo, values [][]string, nulls [][]bool, pkCol string,
) ([]ColumnInfo, [][]string, [][]bool) {
	// Find the PK column index (should be first since we injected it at position 0).
	pkIdx := -1
	for i, col := range columns {
		if col.Name == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return columns, values, nulls
	}

	// Remove column at pkIdx.
	newCols := make([]ColumnInfo, 0, len(columns)-1)
	newCols = append(newCols, columns[:pkIdx]...)
	newCols = append(newCols, columns[pkIdx+1:]...)

	newValues := make([][]string, len(values))
	newNulls := make([][]bool, len(nulls))
	for i, row := range values {
		newRow := make([]string, 0, len(row)-1)
		newRow = append(newRow, row[:pkIdx]...)
		newRow = append(newRow, row[pkIdx+1:]...)
		newValues[i] = newRow

		newNull := make([]bool, 0, len(nulls[i])-1)
		newNull = append(newNull, nulls[i][:pkIdx]...)
		newNull = append(newNull, nulls[i][pkIdx+1:]...)
		newNulls[i] = newNull
	}

	return newCols, newValues, newNulls
}

// rewriteForProd adapts a SQL query to be compatible with the Prod schema.
// Uses the schema registry to handle columns added/renamed in Shadow.
// Returns the rewritten SQL and a bool indicating if the query should
// be sent to Shadow only (e.g., table only exists in Shadow).
func (rh *ReadHandler) rewriteForProd(sql, table string) (string, bool) {
	diff := rh.schemaRegistry.GetDiff(table)
	if diff == nil {
		return sql, false
	}

	// Check if this is a new table (no columns in Prod means it was CREATE'd in Shadow).
	// If the table doesn't exist in our table metadata, it's likely a new Shadow table.
	if _, exists := rh.tables[table]; !exists {
		return sql, true // Shadow-only table.
	}

	// Use AST-based rewriting for WHERE, ORDER BY, GROUP BY, and SELECT list.
	// This handles shadow-only column references in all clauses correctly
	// (e.g., WHERE added_col IS NULL becomes a no-op for Prod).
	tables := []string{table}
	result, skipProd := rewriteSQLForProd(sql, rh.schemaRegistry, tables)
	if skipProd {
		return sql, true
	}

	// Handle renamed columns: replace new name with old name.
	for oldName, newName := range diff.Renamed {
		result = replaceColumnName(result, newName, oldName)
	}

	return result, false
}

// replaceColumnName replaces occurrences of a column name in SQL,
// handling word boundaries to avoid replacing substrings.
func replaceColumnName(sql, oldCol, newCol string) string {
	lower := strings.ToLower(sql)
	lowerOld := strings.ToLower(oldCol)
	result := sql
	offset := 0
	for {
		idx := strings.Index(strings.ToLower(result[offset:]), lowerOld)
		if idx < 0 {
			break
		}
		pos := offset + idx
		// Check word boundaries.
		before := pos == 0 || !isIdentChar(result[pos-1])
		after := pos+len(oldCol) >= len(result) || !isIdentChar(result[pos+len(oldCol)])
		if before && after {
			result = result[:pos] + newCol + result[pos+len(oldCol):]
			offset = pos + len(newCol)
		} else {
			offset = pos + len(oldCol)
		}
		_ = lower // suppress unused warning
	}
	return result
}

// isIdentChar returns true if the byte is a valid SQL identifier character.
func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

// rewriteLimit replaces the LIMIT value in a SQL string.
// Looks for "LIMIT <number>" and replaces <number> with the new limit.
func rewriteLimit(sql string, newLimit int) string {
	upper := strings.ToUpper(sql)
	idx := strings.LastIndex(upper, "LIMIT")
	if idx < 0 {
		return sql
	}
	// Find the start of the number after "LIMIT".
	afterLimit := idx + 5
	rest := sql[afterLimit:]
	trimmed := strings.TrimLeft(rest, " \t")
	whitespace := len(rest) - len(trimmed)

	// Find the end of the number.
	numEnd := 0
	for numEnd < len(trimmed) && trimmed[numEnd] >= '0' && trimmed[numEnd] <= '9' {
		numEnd++
	}
	if numEnd == 0 {
		return sql // No numeric LIMIT found.
	}

	return sql[:afterLimit] + rest[:whitespace] + strconv.Itoa(newLimit) + trimmed[numEnd:]
}

// findPKColumnIndex returns the index of the PK column in the result columns.
// Returns -1 if the table or PK column is not found.
func (rh *ReadHandler) findPKColumnIndex(table string, columns []ColumnInfo) int {
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return -1
	}
	pkCol := meta.PKColumns[0]
	for i, col := range columns {
		if col.Name == pkCol {
			return i
		}
	}
	return -1
}

// filterProdRows removes rows from Prod result where the PK is in the delta map or tombstone set.
func (rh *ReadHandler) filterProdRows(table string, result *QueryResult) ([][]string, [][]bool) {
	pkIdx := rh.findPKColumnIndex(table, result.Columns)
	if pkIdx < 0 {
		// No PK column found — try virtual PK as fallback for PK-less tables.
		// CockroachDB uses "rowid"; PostgreSQL uses "ctid".
		pkIdx = findColumnIndex(result.Columns, "ctid")
		if pkIdx < 0 {
			pkIdx = findColumnIndex(result.Columns, "rowid")
		}
		if pkIdx < 0 {
			// Neither PK nor virtual PK found — cannot filter, return all rows.
			return result.RowValues, result.RowNulls
		}
	}

	var filteredValues [][]string
	var filteredNulls [][]bool

	for i, row := range result.RowValues {
		if pkIdx >= len(row) {
			continue
		}
		pk := row[pkIdx]
		if rh.deltaMap.IsDelta(table, pk) {
			continue // Shadow has authoritative version.
		}
		if rh.tombstones.IsTombstoned(table, pk) {
			continue // Row is locally deleted.
		}
		filteredValues = append(filteredValues, row)
		filteredNulls = append(filteredNulls, result.RowNulls[i])
	}

	return filteredValues, filteredNulls
}

// adaptColumns returns the column list after schema adaptation.
// Removes columns dropped in Shadow, applies renames, adds columns added in Shadow.
func (rh *ReadHandler) adaptColumns(table string, prodColumns []ColumnInfo) []ColumnInfo {
	if rh.schemaRegistry == nil {
		return prodColumns
	}
	diff := rh.schemaRegistry.GetDiff(table)
	if diff == nil {
		return prodColumns
	}

	droppedSet := make(map[string]bool, len(diff.Dropped))
	for _, d := range diff.Dropped {
		droppedSet[d] = true
	}

	var result []ColumnInfo
	for _, col := range prodColumns {
		if droppedSet[col.Name] {
			continue
		}
		name := col.Name
		if newName, ok := diff.Renamed[name]; ok {
			name = newName
		}
		result = append(result, ColumnInfo{Name: name, OID: col.OID})
	}

	// Append added columns.
	for _, added := range diff.Added {
		result = append(result, ColumnInfo{Name: added.Name, OID: 25}) // OID 25 = text
	}

	return result
}

// adaptRows adapts Prod rows for schema diffs: strip dropped columns,
// inject NULLs for added columns.
func (rh *ReadHandler) adaptRows(
	table string,
	origColumns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	if rh.schemaRegistry == nil {
		return values, nulls
	}
	diff := rh.schemaRegistry.GetDiff(table)
	if diff == nil {
		return values, nulls
	}

	droppedSet := make(map[string]bool, len(diff.Dropped))
	for _, d := range diff.Dropped {
		droppedSet[d] = true
	}

	// Build indices of columns to keep.
	var keepIndices []int
	for i, col := range origColumns {
		if !droppedSet[col.Name] {
			keepIndices = append(keepIndices, i)
		}
	}

	numAdded := len(diff.Added)
	var adaptedValues [][]string
	var adaptedNulls [][]bool

	for i := range values {
		row := make([]string, 0, len(keepIndices)+numAdded)
		rowNulls := make([]bool, 0, len(keepIndices)+numAdded)

		for _, idx := range keepIndices {
			if idx < len(values[i]) {
				row = append(row, values[i][idx])
				rowNulls = append(rowNulls, nulls[i][idx])
			}
		}

		// Append NULLs for added columns.
		for range numAdded {
			row = append(row, "")
			rowNulls = append(rowNulls, true)
		}

		adaptedValues = append(adaptedValues, row)
		adaptedNulls = append(adaptedNulls, rowNulls)
	}

	return adaptedValues, adaptedNulls
}

// mergeResults combines Shadow and filtered/adapted Prod results.
// Shadow columns are used as canonical if available.
func mergeResults(
	shadowCols []ColumnInfo, shadowValues [][]string, shadowNulls [][]bool,
	prodCols []ColumnInfo, prodValues [][]string, prodNulls [][]bool,
) ([]ColumnInfo, [][]string, [][]bool) {
	// Use Shadow's column set as canonical if available.
	var columns []ColumnInfo
	if len(shadowCols) > 0 {
		columns = shadowCols
	} else {
		columns = prodCols
	}

	var mergedValues [][]string
	var mergedNulls [][]bool

	// Shadow rows first (they take priority in dedup).
	mergedValues = append(mergedValues, shadowValues...)
	mergedNulls = append(mergedNulls, shadowNulls...)

	// Prod rows — align columns if needed.
	if columnsMatch(columns, prodCols) {
		mergedValues = append(mergedValues, prodValues...)
		mergedNulls = append(mergedNulls, prodNulls...)
	} else {
		// Reorder Prod columns to match canonical column order.
		prodColIdx := buildColumnIndex(prodCols)
		for i, row := range prodValues {
			alignedRow := make([]string, len(columns))
			alignedNulls := make([]bool, len(columns))
			for j, col := range columns {
				if idx, ok := prodColIdx[col.Name]; ok && idx < len(row) {
					alignedRow[j] = row[idx]
					alignedNulls[j] = prodNulls[i][idx]
				} else {
					alignedRow[j] = ""
					alignedNulls[j] = true // NULL for missing column
				}
			}
			mergedValues = append(mergedValues, alignedRow)
			mergedNulls = append(mergedNulls, alignedNulls)
		}
	}

	return columns, mergedValues, mergedNulls
}

// dedup removes duplicate rows by PK, keeping the first occurrence
// (Shadow rows come first, so Shadow's version wins).
func (rh *ReadHandler) dedup(
	table string,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	meta, ok := rh.tables[table]
	if !ok {
		return values, nulls
	}

	pkIdx := -1
	if len(meta.PKColumns) > 0 {
		for i, col := range columns {
			if col.Name == meta.PKColumns[0] {
				pkIdx = i
				break
			}
		}
	}
	// No PK column found — try virtual PK as fallback for PK-less tables.
	// CockroachDB uses "rowid"; PostgreSQL uses "ctid".
	if pkIdx < 0 {
		pkIdx = findColumnIndex(columns, "ctid")
	}
	if pkIdx < 0 {
		pkIdx = findColumnIndex(columns, "rowid")
	}
	if pkIdx < 0 {
		return values, nulls
	}

	seen := make(map[string]bool)
	var dedupValues [][]string
	var dedupNulls [][]bool

	for i, row := range values {
		if pkIdx >= len(row) {
			continue
		}
		pk := row[pkIdx]
		if seen[pk] {
			continue
		}
		seen[pk] = true
		dedupValues = append(dedupValues, row)
		dedupNulls = append(dedupNulls, nulls[i])
	}

	return dedupValues, dedupNulls
}

// orderByCol represents a single column in an ORDER BY clause.
type orderByCol struct {
	name string
	desc bool
	idx  int // resolved column index, -1 if unresolved
}

// sortMerged sorts the merged result set by the ORDER BY clause.
// Supports single-column and multi-column ORDER BY.
func sortMerged(
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
	orderBy string,
) {
	orderCols := parseOrderBy(orderBy)
	if len(orderCols) == 0 {
		return
	}

	// Resolve column indices.
	colIndex := buildColumnIndex(columns)
	var resolved []orderByCol
	for _, oc := range orderCols {
		if idx, ok := colIndex[oc.name]; ok {
			oc.idx = idx
			resolved = append(resolved, oc)
		}
	}
	if len(resolved) == 0 {
		return
	}

	// Build index array and sort it.
	indices := make([]int, len(values))
	for i := range indices {
		indices[i] = i
	}

	// Build a map from order-by column index to its OID for type-aware comparison.
	colOIDs := make(map[int]uint32)
	for _, oc := range resolved {
		if oc.idx >= 0 && oc.idx < len(columns) {
			colOIDs[oc.idx] = columns[oc.idx].OID
		}
	}

	sort.SliceStable(indices, func(a, b int) bool {
		for _, oc := range resolved {
			va := values[indices[a]][oc.idx]
			vb := values[indices[b]][oc.idx]
			cmp := compareValuesTyped(va, vb, colOIDs[oc.idx])
			if cmp == 0 {
				continue // Tie — break with next column.
			}
			if oc.desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false // All columns equal.
	})

	// Apply the sorted order.
	sortedValues := make([][]string, len(values))
	sortedNulls := make([][]bool, len(nulls))
	for i, idx := range indices {
		sortedValues[i] = values[idx]
		sortedNulls[i] = nulls[idx]
	}
	copy(values, sortedValues)
	copy(nulls, sortedNulls)
}

// compareValues compares two string values, using numeric comparison
// when both are parseable as numbers, otherwise string comparison.
// Returns -1, 0, or 1.
func compareValues(a, b string) int {
	na, errA := strconv.ParseFloat(a, 64)
	nb, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		if na < nb {
			return -1
		}
		if na > nb {
			return 1
		}
		return 0
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// compareValuesTyped compares two string values using type-aware comparison
// based on the PostgreSQL OID. Falls back to compareValues for OID 0 (unknown).
func compareValuesTyped(a, b string, oid uint32) int {
	if oid == 0 {
		return compareValues(a, b)
	}

	switch oid {
	case 20, 21, 23, 26, 700, 701, 1700:
		// Numeric types: int8, int2, int4, oid, float4, float8, numeric.
		na, errA := strconv.ParseFloat(a, 64)
		nb, errB := strconv.ParseFloat(b, 64)
		if errA == nil && errB == nil {
			if na < nb {
				return -1
			}
			if na > nb {
				return 1
			}
			return 0
		}
		// Fall through to string comparison if parsing fails.
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0

	case 1082, 1114, 1184:
		// Date/timestamp types: date, timestamp, timestamptz.
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05.999999-07",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		var ta, tb time.Time
		var parsedA, parsedB bool
		for _, fmt := range formats {
			if t, err := time.Parse(fmt, a); err == nil {
				ta = t
				parsedA = true
				break
			}
		}
		for _, fmt := range formats {
			if t, err := time.Parse(fmt, b); err == nil {
				tb = t
				parsedB = true
				break
			}
		}
		if parsedA && parsedB {
			if ta.Before(tb) {
				return -1
			}
			if ta.After(tb) {
				return 1
			}
			return 0
		}
		// Fall back to string comparison.
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0

	case 16:
		// Boolean type.
		na := normBoolForCompare(a)
		nb := normBoolForCompare(b)
		if na < nb {
			return -1
		}
		if na > nb {
			return 1
		}
		return 0

	default:
		// All other types: string comparison.
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}
}

// normBoolForCompare normalizes a boolean value to "f" or "t" for comparison.
func normBoolForCompare(s string) string {
	switch strings.ToLower(s) {
	case "t", "true", "1", "yes", "on":
		return "t"
	case "f", "false", "0", "no", "off":
		return "f"
	}
	return s
}

// parseOrderBy parses an ORDER BY clause into individual columns.
// Handles both single-column ("col ASC") and multi-column ("col1 DESC, col2 ASC").
func parseOrderBy(orderBy string) []orderByCol {
	orderBy = strings.TrimRight(orderBy, "; \t\n")
	if orderBy == "" {
		return nil
	}

	parts := strings.Split(orderBy, ",")
	var result []orderByCol
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) == 0 {
			continue
		}
		col := strings.Trim(fields[0], `"`)
		// Strip table alias prefix (e.g., "u.name" -> "name").
		if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
			col = col[dotIdx+1:]
		}
		desc := false
		if len(fields) >= 2 && strings.EqualFold(fields[1], "DESC") {
			desc = true
		}
		result = append(result, orderByCol{name: col, desc: desc, idx: -1})
	}
	return result
}

// parseSimpleOrderBy is a backwards-compatible wrapper that returns a single
// column and direction. For multi-column, returns the first column.
func parseSimpleOrderBy(orderBy string) (string, bool) {
	cols := parseOrderBy(orderBy)
	if len(cols) == 0 {
		return "", false
	}
	return cols[0].name, cols[0].desc
}

// columnsMatch checks if two column slices have the same names in the same order.
func columnsMatch(a, b []ColumnInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			return false
		}
	}
	return true
}

// buildColumnIndex builds a map from column name to index.
func buildColumnIndex(cols []ColumnInfo) map[string]int {
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		idx[c.Name] = i
	}
	return idx
}
