package proxy

import (
	"fmt"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
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

	// Rewrite Prod SQL for schema diffs (added/renamed/dropped columns).
	prodSQL := effectiveSQL
	skipProd := false
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.HasDiff(t) {
				// Use rewriteSQLForProd to strip shadow-only columns from the entire query.
				rewritten, shouldSkip := rewriteSQLForProd(prodSQL, rh.schemaRegistry, cl.Tables)
				if shouldSkip {
					skipProd = true
				} else {
					prodSQL = rewritten
				}
				break // Only need to rewrite once for all tables
			}
		}
	}

	// If skipProd is true, the query cannot be meaningfully executed on Prod
	// (e.g., JOIN condition references a shadow-only column). Use Shadow only.
	if skipProd {
		shadowResult, err := execQuery(rh.shadowConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("shadow JOIN query (skip prod): %w", err)
		}
		if shadowResult.Error != "" {
			return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
		}
		return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
	}

	// Step 1: Execute JOIN on Prod (use rewritten SQL if schema diffs exist).
	prodResult, err := execQuery(rh.prodConn, prodSQL)
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
// no delta rows. It rewrites the Prod SQL, adapts columns, and merges with Shadow.
func (rh *ReadHandler) joinPatchWithSchemaDiffs(cl *core.Classification, querySQL string, injectedPKs map[string]string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	// Check if any table is fully shadowed — must use Shadow only.
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.IsFullyShadowed(t) {
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

	// Rewrite Prod SQL for schema diffs.
	prodSQL := querySQL
	if rh.schemaRegistry != nil {
		rewritten, shouldSkip := rewriteSQLForProd(querySQL, rh.schemaRegistry, cl.Tables)
		if shouldSkip {
			// Shadow-only execution.
			shadowResult, err := execQuery(rh.shadowConn, querySQL)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("shadow JOIN query (skip prod): %w", err)
			}
			if shadowResult.Error != "" {
				return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
			}
			return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
		}
		prodSQL = rewritten
	}

	// Execute on Prod with rewritten SQL.
	prodResult, err := execQuery(rh.prodConn, prodSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prod JOIN query: %w", err)
	}
	if prodResult.Error != "" {
		// If Prod query fails due to schema mismatch, fall back to Shadow-only.
		shadowResult, err := execQuery(rh.shadowConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("shadow JOIN query (prod error fallback): %w", err)
		}
		if shadowResult.Error != "" {
			return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
		}
		return shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls, nil
	}

	// Adapt Prod columns and rows for schema diffs.
	adaptedColumns := prodResult.Columns
	adaptedValues := prodResult.RowValues
	adaptedNulls := prodResult.RowNulls
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.HasDiff(t) {
				adaptedColumns = rh.adaptColumns(t, adaptedColumns)
				adaptedValues, adaptedNulls = rh.adaptRows(t, prodResult.Columns, adaptedValues, adaptedNulls)
			}
		}
	}

	// Execute on Shadow to get the canonical column set and any Shadow-only rows.
	shadowResult, err := execQuery(rh.shadowConn, querySQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow JOIN query: %w", err)
	}

	// Merge Shadow + adapted Prod. Shadow rows first (priority).
	resultColumns := adaptedColumns
	if len(shadowResult.Columns) > 0 && shadowResult.Error == "" {
		resultColumns = shadowResult.Columns
	}

	var mergedValues [][]string
	var mergedNulls [][]bool

	if shadowResult.Error == "" {
		mergedValues = append(mergedValues, shadowResult.RowValues...)
		mergedNulls = append(mergedNulls, shadowResult.RowNulls...)
	}
	mergedValues = append(mergedValues, adaptedValues...)
	mergedNulls = append(mergedNulls, adaptedNulls...)

	// Deduplicate by composite key.
	allPKIndices := rh.findPKIndicesForJoin(cl.Tables, resultColumns, injectedPKs)
	mergedValues, mergedNulls = rh.dedupJoin(cl.Tables, allPKIndices, resultColumns, mergedValues, mergedNulls)

	// Re-sort by ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(resultColumns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	return resultColumns, mergedValues, mergedNulls, nil
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
	fromIdx := strings.Index(upper, " FROM ")
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
		if containsColumn(selectList, pkCol) {
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
