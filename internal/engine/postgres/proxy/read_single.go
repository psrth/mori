package proxy

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/core"
)

// handleMergedRead implements the single-table merged read algorithm.
// Steps: Shadow query -> Prod query (over-fetched) -> filter -> adapt -> merge -> dedup -> sort -> limit -> respond.
func (rh *ReadHandler) handleMergedRead(clientConn net.Conn, cl *core.Classification) error {
	if len(cl.Tables) == 0 {
		return fmt.Errorf("merged read with no tables")
	}
	table := cl.Tables[0]

	// Step 1: Execute query on Shadow verbatim.
	shadowResult, err := execQuery(rh.shadowConn, cl.RawSQL)
	if err != nil {
		return fmt.Errorf("shadow query: %w", err)
	}
	if shadowResult.Error != "" {
		// Relay Shadow's error response to client.
		_, err := clientConn.Write(shadowResult.RawMsgs)
		return err
	}

	// Step 2: Execute query on Prod (with over-fetching for LIMIT queries).
	prodSQL := cl.RawSQL
	if cl.HasLimit && cl.Limit > 0 {
		deltaCount := rh.deltaMap.CountForTable(table)
		tombstoneCount := rh.tombstones.CountForTable(table)
		overfetch := deltaCount + tombstoneCount
		if overfetch > 0 {
			prodSQL = rewriteLimit(cl.RawSQL, cl.Limit+overfetch)
		}
	}

	prodResult, err := execQuery(rh.prodConn, prodSQL)
	if err != nil {
		return fmt.Errorf("prod query: %w", err)
	}
	if prodResult.Error != "" {
		// Relay Prod's error response to client.
		_, err := clientConn.Write(prodResult.RawMsgs)
		return err
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

	// Step 9: Build and send PG wire response.
	response := buildSelectResponse(mergedColumns, mergedValues, mergedNulls)
	_, err = clientConn.Write(response)
	return err
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
		// No PK column found — cannot filter, return all rows.
		return result.RowValues, result.RowNulls
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
	if !ok || len(meta.PKColumns) == 0 {
		return values, nulls
	}

	pkIdx := -1
	for i, col := range columns {
		if col.Name == meta.PKColumns[0] {
			pkIdx = i
			break
		}
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

// sortMerged sorts the merged result set by a simple ORDER BY clause.
// Handles single-column "column [ASC|DESC]". Skips multi-column ORDER BY.
func sortMerged(
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
	orderBy string,
) {
	col, desc := parseSimpleOrderBy(orderBy)
	if col == "" {
		return
	}

	colIdx := -1
	for i, c := range columns {
		if c.Name == col {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return
	}

	// Build index array and sort it.
	indices := make([]int, len(values))
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, b int) bool {
		va := values[indices[a]][colIdx]
		vb := values[indices[b]][colIdx]
		// Try numeric comparison first.
		na, errA := strconv.ParseFloat(va, 64)
		nb, errB := strconv.ParseFloat(vb, 64)
		if errA == nil && errB == nil {
			if desc {
				return na > nb
			}
			return na < nb
		}
		// Fall back to string comparison.
		if desc {
			return va > vb
		}
		return va < vb
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

// parseSimpleOrderBy parses a simple "column [ASC|DESC]" ORDER BY clause.
// Returns the column name and whether it's descending.
// Returns ("", false) for multi-column or complex expressions.
func parseSimpleOrderBy(orderBy string) (string, bool) {
	// Strip trailing semicolons/whitespace.
	orderBy = strings.TrimRight(orderBy, "; \t\n")

	// Skip multi-column ORDER BY.
	if strings.Contains(orderBy, ",") {
		return "", false
	}

	parts := strings.Fields(strings.TrimSpace(orderBy))
	if len(parts) == 0 {
		return "", false
	}

	col := strings.Trim(parts[0], `"`)
	// Strip table alias prefix (e.g., "u.name" -> "name").
	if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
		col = col[dotIdx+1:]
	}

	desc := false
	if len(parts) >= 2 && strings.EqualFold(parts[1], "DESC") {
		desc = true
	}

	return col, desc
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
