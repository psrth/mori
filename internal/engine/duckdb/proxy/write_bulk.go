package proxy

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
)

// handleBulkUpdate handles UPDATE statements where no PKs could be extracted
// from the WHERE clause (range conditions, IS NULL, complex expressions).
// It queries Prod for all matching rows, hydrates them into Shadow,
// executes the UPDATE on Shadow, and tracks all affected PKs.
func (p *Proxy) handleBulkUpdate(sqlStr string, cl *core.Classification, connID int64, txh *TxnHandler) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	table := cl.Tables[0]
	meta, ok := p.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	pkCol := meta.PKColumns[0]

	// 1. Build a SELECT query to discover matching rows from Prod.
	selectSQL := buildBulkUpdateSelect(sqlStr, table)
	if selectSQL == "" {
		if p.verbose {
			log.Printf("[conn %d] bulk UPDATE: failed to build hydration query, executing on shadow only", connID)
		}
		return p.shadowOnlyUpdateWithTracking(sqlStr, table, pkCol, connID, txh)
	}

	// Rewrite for Prod compatibility.
	if p.schemaRegistry != nil {
		rewritten, skipProd := rewriteSQLForProd(selectSQL, p.schemaRegistry, cl.Tables)
		if skipProd {
			return p.shadowOnlyUpdateWithTracking(sqlStr, table, pkCol, connID, txh)
		}
		selectSQL = rewritten
	}

	if p.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrating from Prod with: %s", connID, selectSQL)
	}

	// 2. Query Prod for matching rows.
	// Note: do NOT cap this query — write discovery must find ALL matching rows
	// so the delta map is complete. The hydration loop below caps how many rows
	// are actually materialized into Shadow.
	cols, rows, nulls, err := queryToRows(p.prodDB, selectSQL)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod query failed: %v", connID, err)
		}
		return p.shadowOnlyUpdateWithTracking(sqlStr, table, pkCol, connID, txh)
	}

	// 3. Find PK column index.
	pkIdx := findColIdx(cols, pkCol)
	if pkIdx < 0 {
		if p.verbose {
			log.Printf("[conn %d] bulk UPDATE: PK column %q not in results", connID, pkCol)
		}
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}

	// 4. Hydrate each matching row into Shadow.
	var affectedPKs []string
	var genCols []string
	if meta, ok := p.tables[table]; ok {
		genCols = meta.GeneratedCols
	}

	hydratedCount := 0
	maxRows := p.maxRowsHydrate
	for i, row := range rows {
		if maxRows > 0 && hydratedCount >= maxRows {
			if p.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration cap reached (%d rows)", connID, maxRows)
			}
			break
		}
		if pkIdx >= len(row) || !row[pkIdx].Valid {
			continue
		}
		pk := row[pkIdx].String

		if p.deltaMap.IsDelta(table, pk) {
			affectedPKs = append(affectedPKs, pk)
			continue
		}

		insertSQL := buildHydrateInsert(table, cols, rows[i], genCols)
		if _, err := p.shadowDB.Exec(insertSQL); err != nil {
			if p.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration failed for PK %s: %v", connID, pk, err)
			}
			continue
		}
		affectedPKs = append(affectedPKs, pk)
		hydratedCount++
		_ = nulls // used implicitly via rows
	}

	if p.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrated %d rows from Prod", connID, hydratedCount)
	}

	// 5. Execute the UPDATE on Shadow.
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)

	// 6. Track all affected PKs in delta map.
	inTxn := txh != nil && txh.InTxn()
	for _, pk := range affectedPKs {
		if inTxn {
			p.deltaMap.Stage(table, pk)
		} else {
			p.deltaMap.Add(table, pk)
		}
	}
	if !inTxn && len(affectedPKs) > 0 {
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}

	return resp
}

// handleBulkDelete handles DELETE statements where no PKs could be extracted.
// It queries Prod for matching rows' PKs, tombstones them, executes the DELETE
// on Shadow, and corrects the row count.
func (p *Proxy) handleBulkDelete(sqlStr string, cl *core.Classification, connID int64, txh *TxnHandler) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	table := cl.Tables[0]
	meta, ok := p.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	pkCol := meta.PKColumns[0]

	// 1. Build SELECT to discover matching PKs from Prod.
	selectSQL := buildBulkDeleteSelect(sqlStr, table, pkCol)
	if selectSQL == "" {
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}

	// Rewrite for Prod compatibility.
	if p.schemaRegistry != nil {
		rewritten, skipProd := rewriteSQLForProd(selectSQL, p.schemaRegistry, cl.Tables)
		if skipProd {
			return p.executeQuery(p.shadowDB, sqlStr, connID)
		}
		selectSQL = rewritten
	}

	if p.verbose {
		log.Printf("[conn %d] bulk DELETE: discovering PKs from Prod with: %s", connID, selectSQL)
	}

	// 2. Query Prod for matching PKs.
	// Note: do NOT cap this query — write discovery must find ALL matching PKs
	// so the tombstone set is complete. A partial tombstone set would cause
	// merged reads to re-surface supposedly-deleted rows.
	cols, rows, _, err := queryToRows(p.prodDB, selectSQL)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] bulk DELETE: Prod query failed: %v", connID, err)
		}
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}

	pkIdx := findColIdx(cols, pkCol)
	if pkIdx < 0 {
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}

	// 3. Collect PKs to tombstone.
	var prodPKs []string
	for _, row := range rows {
		if pkIdx < len(row) && row[pkIdx].Valid {
			prodPKs = append(prodPKs, row[pkIdx].String)
		}
	}

	// 4. Execute DELETE on Shadow.
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)

	// 5. Query Shadow for PKs that still exist (were deleted from Shadow).
	shadowPKSQL := fmt.Sprintf(`SELECT "%s" FROM "%s"`, pkCol, table)
	_, shadowRows, _, _ := queryToRows(p.shadowDB, shadowPKSQL)
	shadowPKSet := make(map[string]bool)
	for _, row := range shadowRows {
		if len(row) > 0 && row[0].Valid {
			shadowPKSet[row[0].String] = true
		}
	}

	// 6. Tombstone Prod PKs that are no longer in Shadow (or were never there).
	inTxn := txh != nil && txh.InTxn()
	tombstoneCount := 0
	for _, pk := range prodPKs {
		if p.tombstones.IsTombstoned(table, pk) {
			continue
		}
		if inTxn {
			p.tombstones.Stage(table, pk)
			p.deltaMap.Remove(table, pk)
		} else {
			p.tombstones.Add(table, pk)
			p.deltaMap.Remove(table, pk)
		}
		tombstoneCount++
	}

	if !inTxn && tombstoneCount > 0 {
		delta.WriteTombstoneSet(p.moriDir, p.tombstones)
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}

	if p.verbose {
		log.Printf("[conn %d] bulk DELETE: tombstoned %d rows from Prod", connID, tombstoneCount)
	}

	return resp
}

// shadowOnlyUpdateWithTracking executes an UPDATE on shadow only and tracks
// affected PKs from the shadow result by querying shadow post-execution.
func (p *Proxy) shadowOnlyUpdateWithTracking(sqlStr, table, pkCol string, connID int64, txh *TxnHandler) []byte {
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)

	// Discover affected PKs from shadow by querying the table.
	// This is a rough heuristic — we mark the entire table as having inserts.
	p.deltaMap.MarkInserted(table)

	return resp
}

// buildBulkUpdateSelect builds a SELECT * FROM table WHERE ... query from an UPDATE statement.
// Uses regex to extract the table and WHERE clause from the UPDATE.
func buildBulkUpdateSelect(updateSQL, table string) string {
	upper := strings.ToUpper(strings.TrimSpace(updateSQL))

	// Extract WHERE clause from UPDATE.
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		// No WHERE clause — this is UPDATE all rows. Select all.
		return fmt.Sprintf(`SELECT * FROM "%s"`, table)
	}

	// Get the WHERE clause from the original SQL (preserve case).
	whereClause := strings.TrimSpace(updateSQL[whereIdx+7:])
	// Strip trailing semicolon.
	whereClause = strings.TrimRight(whereClause, "; \t\n")

	return fmt.Sprintf(`SELECT * FROM "%s" WHERE %s`, table, whereClause)
}

// buildBulkDeleteSelect builds a SELECT pk FROM table WHERE ... from a DELETE statement.
func buildBulkDeleteSelect(deleteSQL, table, pkCol string) string {
	upper := strings.ToUpper(strings.TrimSpace(deleteSQL))

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		// No WHERE — select all PKs.
		return fmt.Sprintf(`SELECT "%s" FROM "%s"`, pkCol, table)
	}

	whereClause := strings.TrimSpace(deleteSQL[whereIdx+7:])
	whereClause = strings.TrimRight(whereClause, "; \t\n")

	// Strip RETURNING clause if present.
	returningRe := regexp.MustCompile(`(?i)\s+RETURNING\s+.*$`)
	whereClause = returningRe.ReplaceAllString(whereClause, "")

	return fmt.Sprintf(`SELECT "%s" FROM "%s" WHERE %s`, pkCol, table, whereClause)
}

// findColIdx finds the index of a column by name (case-insensitive).
func findColIdx(cols []string, name string) int {
	lower := strings.ToLower(name)
	for i, c := range cols {
		if strings.ToLower(c) == lower {
			return i
		}
	}
	return -1
}
