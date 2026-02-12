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
	// Identify which tables have deltas/tombstones.
	deltaTables := rh.identifyDeltaTables(cl.Tables)
	if len(deltaTables) == 0 {
		// No delta tables — should not have been routed here. Fall back to Prod.
		return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.prodConn, clientConn)
	}

	// Step 1: Execute JOIN on Prod.
	prodResult, err := execQuery(rh.prodConn, cl.RawSQL)
	if err != nil {
		return fmt.Errorf("prod JOIN query: %w", err)
	}
	if prodResult.Error != "" {
		_, err := clientConn.Write(prodResult.RawMsgs)
		return err
	}

	// Step 2: Find PK column indices for each delta table in the result set.
	pkIndices := rh.findPKIndicesForTables(deltaTables, prodResult.Columns)

	// Steps 3-4: Classify each Prod row (clean / delta / dead) and patch delta rows.
	var patchedValues [][]string
	var patchedNulls [][]bool

	for i, row := range prodResult.RowValues {
		action := rh.classifyJoinRow(deltaTables, pkIndices, row)
		switch action {
		case joinRowClean:
			patchedValues = append(patchedValues, row)
			patchedNulls = append(patchedNulls, prodResult.RowNulls[i])

		case joinRowDelta:
			patched, patchedN, err := rh.patchDeltaRow(
				deltaTables, pkIndices, prodResult.Columns, row, prodResult.RowNulls[i],
			)
			if err != nil {
				rh.logf("JOIN patch error, keeping Prod row: %v", err)
				patchedValues = append(patchedValues, row)
				patchedNulls = append(patchedNulls, prodResult.RowNulls[i])
				continue
			}
			if patched != nil {
				patchedValues = append(patchedValues, patched)
				patchedNulls = append(patchedNulls, patchedN)
			}

		case joinRowDead:
			// Tombstoned — discard.
			continue
		}
	}

	// Step 5: Execute same JOIN on Shadow (catches locally-inserted rows).
	shadowResult, err := execQuery(rh.shadowConn, cl.RawSQL)
	if err != nil {
		return fmt.Errorf("shadow JOIN query: %w", err)
	}

	// Step 6: Merge Shadow + patched Prod. Shadow rows first (priority).
	columns := prodResult.Columns
	if len(shadowResult.Columns) > 0 && shadowResult.Error == "" {
		columns = shadowResult.Columns
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
	// Using all tables' PKs prevents false dedup on one-to-many JOINs where
	// multiple rows share the same delta-table PK but differ in other table PKs.
	allPKIndices := rh.findPKIndicesForTables(cl.Tables, columns)
	mergedValues, mergedNulls = rh.dedupJoin(cl.Tables, allPKIndices, columns, mergedValues, mergedNulls)

	// Step 8: Re-sort by ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(columns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Step 9: Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	// Build and send response.
	response := buildSelectResponse(columns, mergedValues, mergedNulls)
	_, err = clientConn.Write(response)
	return err
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
