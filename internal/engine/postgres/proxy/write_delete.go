package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// handleDelete handles DELETE statements by executing on Shadow and adding tombstones.
//
// Point delete (PKs extractable): forward DELETE to Shadow, then add tombstones
// for affected PKs so they are filtered out of future merged reads.
// The response is corrected to reflect the actual number of tombstoned rows
// (Shadow may report 0 if the row only existed in Prod).
//
// Bulk delete (no PKs): forward to Shadow without tombstoning.
func (w *WriteHandler) handleDelete(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Enforce FK RESTRICT/NO ACTION before executing the DELETE.
	if w.fkEnforcer != nil && len(cl.PKs) > 0 && len(cl.Tables) == 1 {
		table := cl.Tables[0]
		refFKs := w.fkEnforcer.schemaRegistry.GetReferencingFKs(table)
		for _, fk := range refFKs {
			if fk.OnDelete == "RESTRICT" || fk.OnDelete == "NO ACTION" {
				var deletedPKs []string
				for _, pk := range cl.PKs {
					if pk.Table == table {
						deletedPKs = append(deletedPKs, pk.PK)
					}
				}
				if len(deletedPKs) > 0 && w.fkEnforcer.childRowsExist(fk.ChildTable, fk.ChildColumns[0], deletedPKs) {
					errMsg := fmt.Sprintf("update or delete on table %q violates foreign key constraint on table %q: "+
						"key is still referenced from table %q",
						table, fk.ChildTable, fk.ChildTable)
					if w.verbose {
						log.Printf("[conn %d] FK RESTRICT on DELETE: %s", w.connID, errMsg)
					}
					return sendFKError(clientConn, errMsg)
				}
			}
		}
	}

	if len(cl.PKs) == 0 {
		// Bulk delete: attempt PK discovery from Prod before tombstoning.
		if !cl.IsJoin && len(cl.Tables) == 1 {
			table := cl.Tables[0]
			if meta, ok := w.tables[table]; ok && len(meta.PKColumns) > 0 {
				return w.handleBulkDelete(clientConn, rawMsg, cl)
			}
		}
		// Fallback for multi-table or no PK metadata: forward to Shadow without tombstoning.
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE without PKs, no tombstones added", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Check for RETURNING clause — needs special handling to hydrate from Prod.
	if hasReturning(cl.RawSQL) && len(cl.Tables) > 0 {
		return w.handleDeleteReturning(clientConn, rawMsg, cl)
	}

	// Point delete: capture Shadow response so we can correct the row count.
	msgs, err := forwardAndCapture(rawMsg, w.shadowConn)
	if err != nil {
		return err
	}

	// Count how many PKs will be tombstoned (rows affected from the user's perspective).
	tombstoneCount := len(cl.PKs)

	// Relay response to client, replacing CommandComplete with corrected row count.
	for _, msg := range msgs {
		if msg.Type == 'C' {
			// Replace CommandComplete tag with corrected DELETE count.
			tag := fmt.Sprintf("DELETE %d", tombstoneCount)
			corrected := buildCommandCompleteMsg(tag)
			if _, err := clientConn.Write(corrected); err != nil {
				return fmt.Errorf("relaying corrected CommandComplete: %w", err)
			}
		} else {
			if _, err := clientConn.Write(msg.Raw); err != nil {
				return fmt.Errorf("relaying to client: %w", err)
			}
		}
	}

	w.addTombstones(cl)

	// Enforce FK CASCADE after tombstoning (for ON DELETE CASCADE / SET NULL).
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		var deletedPKs []string
		for _, pk := range cl.PKs {
			if pk.Table == cl.Tables[0] {
				deletedPKs = append(deletedPKs, pk.PK)
			}
		}
		if len(deletedPKs) > 0 {
			if err := w.fkEnforcer.EnforceDeleteCascade(cl.Tables[0], deletedPKs); err != nil {
				// CASCADE/restrict error after deletion — log but don't fail
				// since the delete already happened on shadow.
				if w.verbose {
					log.Printf("[conn %d] FK CASCADE warning after DELETE: %v", w.connID, err)
				}
			}
		}
	}

	return nil
}

// handleDeleteReturning handles DELETE ... RETURNING by hydrating row data from
// Prod before tombstoning. Shadow may not have the row (Prod-only rows), so we
// query Prod for the RETURNING columns and synthesize the response.
func (w *WriteHandler) handleDeleteReturning(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	table := cl.Tables[0]

	// Build a SELECT query for the RETURNING columns from Prod.
	selectSQL := buildReturningSelect(cl.RawSQL, table)

	var prodResult *QueryResult
	if selectSQL != "" {
		var err error
		prodResult, err = execQuery(w.prodConn, selectSQL)
		if err != nil {
			if w.verbose {
				log.Printf("[conn %d] RETURNING hydration from Prod failed: %v", w.connID, err)
			}
			prodResult = nil
		} else if prodResult.Error != "" {
			if w.verbose {
				log.Printf("[conn %d] RETURNING hydration error: %s", w.connID, prodResult.Error)
			}
			prodResult = nil
		}
	}

	// Forward DELETE to Shadow (capture, don't relay).
	shadowMsgs, err := forwardAndCapture(rawMsg, w.shadowConn)
	if err != nil {
		return err
	}

	// If we have Prod data for RETURNING, synthesize the response.
	if prodResult != nil && len(prodResult.Columns) > 0 && len(prodResult.RowValues) > 0 {
		tombstoneCount := len(cl.PKs)
		var buf []byte
		buf = append(buf, buildRowDescMsg(prodResult.Columns)...)
		for i := range prodResult.RowValues {
			buf = append(buf, buildDataRowMsg(prodResult.RowValues[i], prodResult.RowNulls[i])...)
		}
		tag := fmt.Sprintf("DELETE %d", tombstoneCount)
		buf = append(buf, buildCommandCompleteMsg(tag)...)
		buf = append(buf, buildReadyForQueryMsg()...)
		if _, err := clientConn.Write(buf); err != nil {
			return fmt.Errorf("relaying RETURNING response: %w", err)
		}
	} else {
		// Fallback: relay Shadow's response with corrected count.
		tombstoneCount := len(cl.PKs)
		for _, msg := range shadowMsgs {
			if msg.Type == 'C' {
				tag := fmt.Sprintf("DELETE %d", tombstoneCount)
				corrected := buildCommandCompleteMsg(tag)
				if _, err := clientConn.Write(corrected); err != nil {
					return fmt.Errorf("relaying corrected CommandComplete: %w", err)
				}
			} else {
				if _, err := clientConn.Write(msg.Raw); err != nil {
					return fmt.Errorf("relaying to client: %w", err)
				}
			}
		}
	}

	w.addTombstones(cl)
	return nil
}

// handleBulkDelete handles DELETE statements where no PKs could be extracted
// from the WHERE clause. It pre-queries Prod to discover which rows match,
// tombstones those PKs, then executes the DELETE on Shadow.
//
// This prevents deleted Prod rows from continuing to appear in merged reads.
func (w *WriteHandler) handleBulkDelete(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	table := cl.Tables[0]
	meta := w.tables[table]
	pkCol := meta.PKColumns[0]

	// 1. Build SELECT pk FROM table WHERE <same conditions> from the DELETE's WHERE.
	selectSQL, err := buildBulkDeletePKQuery(cl.RawSQL, pkCol)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: failed to build PK discovery query: %v", w.connID, err)
		}
		// Fallback: forward to Shadow without tombstoning.
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Rewrite the PK discovery SELECT for Prod compatibility (strip shadow-only columns).
	if w.schemaRegistry != nil {
		rewritten, skipProd := rewriteSQLForProd(selectSQL, w.schemaRegistry, cl.Tables)
		if skipProd {
			// The entire WHERE is on shadow-only columns — no Prod rows can match.
			// Execute DELETE on Shadow only, no tombstones needed for Prod rows.
			if w.verbose {
				log.Printf("[conn %d] bulk DELETE: PK query irrelevant for Prod, Shadow-only", w.connID)
			}
			return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
		}
		selectSQL = rewritten
	}

	if w.verbose {
		log.Printf("[conn %d] bulk DELETE: discovering PKs from Prod with: %s", w.connID, selectSQL)
	}

	// 2. Execute on Prod to discover matching PKs.
	result, err := execQuery(w.prodConn, selectSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: Prod PK discovery failed: %v", w.connID, err)
		}
		// Fallback: forward to Shadow without tombstoning.
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}
	if result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: Prod PK discovery error: %s", w.connID, result.Error)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// 3. Find PK column index in results.
	pkIdx := -1
	for i, col := range result.Columns {
		if col.Name == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx == -1 {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: PK column %q not found in Prod results", w.connID, pkCol)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Collect discovered PKs.
	var discoveredPKs []string
	for i, row := range result.RowValues {
		if result.RowNulls[i][pkIdx] {
			continue // NULL PK, skip.
		}
		discoveredPKs = append(discoveredPKs, row[pkIdx])
	}

	if w.verbose {
		log.Printf("[conn %d] bulk DELETE: discovered %d PKs from Prod", w.connID, len(discoveredPKs))
	}

	// 4. Execute the DELETE on Shadow, capture response so we can correct the row count.
	msgs, err := forwardAndCapture(rawMsg, w.shadowConn)
	if err != nil {
		return err
	}

	// Compute the correct total count:
	//   Shadow's DELETE count reflects rows it actually deleted (shadow-only inserts
	//   + previously hydrated Prod rows). Prod-only rows (those in discoveredPKs but
	//   NOT already tracked as deltas) aren't in Shadow so Shadow's count excludes them.
	//   Total = Shadow's count + count of truly Prod-only discovered PKs.
	shadowCount := 0
	for _, m := range msgs {
		if m.Type == 'C' {
			tag := parseCommandTag(m.Payload)
			fmt.Sscanf(tag, "DELETE %d", &shadowCount)
			break
		}
	}

	prodOnlyCount := 0
	for _, pk := range discoveredPKs {
		if !w.deltaMap.IsDelta(table, pk) {
			prodOnlyCount++
		}
	}

	tombstoneCount := shadowCount + prodOnlyCount

	// Relay response to client, replacing CommandComplete with corrected row count.
	for _, msg := range msgs {
		if msg.Type == 'C' {
			tag := fmt.Sprintf("DELETE %d", tombstoneCount)
			corrected := buildCommandCompleteMsg(tag)
			if _, err := clientConn.Write(corrected); err != nil {
				return fmt.Errorf("relaying corrected CommandComplete: %w", err)
			}
		} else {
			if _, err := clientConn.Write(msg.Raw); err != nil {
				return fmt.Errorf("relaying to client: %w", err)
			}
		}
	}

	// 5. Tombstone all discovered PKs.
	for _, pk := range discoveredPKs {
		if w.inTxn() {
			w.tombstones.Stage(table, pk)
		} else {
			w.tombstones.Add(table, pk)
			w.deltaMap.Remove(table, pk)
		}
	}

	if !w.inTxn() {
		if err := delta.WriteTombstoneSet(w.moriDir, w.tombstones); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist tombstone set: %v", w.connID, err)
			}
		}
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// buildBulkDeletePKQuery parses a DELETE statement and builds a SELECT query
// that retrieves the PK column using the same WHERE clause. This allows
// discovering which Prod rows would be affected by the DELETE.
//
// Example:
//
//	DELETE FROM users WHERE active = false
//	→ SELECT id FROM users WHERE active = false
func buildBulkDeletePKQuery(rawSQL string, pkCol string) (string, error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}

	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return "", fmt.Errorf("no statements found")
	}

	del := stmts[0].GetStmt().GetDeleteStmt()
	if del == nil {
		return "", fmt.Errorf("not a DELETE statement")
	}

	rel := del.GetRelation()
	if rel == nil {
		return "", fmt.Errorf("DELETE has no target relation")
	}

	// Build SELECT <pkCol> target list.
	pkTarget := &pg_query.Node{
		Node: &pg_query.Node_ResTarget{
			ResTarget: &pg_query.ResTarget{
				Val: &pg_query.Node{
					Node: &pg_query.Node_ColumnRef{
						ColumnRef: &pg_query.ColumnRef{
							Fields: []*pg_query.Node{
								{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: pkCol}}},
							},
						},
					},
				},
			},
		},
	}

	// Build FROM clause from the DELETE's relation.
	fromClause := []*pg_query.Node{
		{Node: &pg_query.Node_RangeVar{RangeVar: rel}},
	}

	// Include any USING clause tables (DELETE ... USING other_table).
	fromClause = append(fromClause, del.GetUsingClause()...)

	// Build SELECT statement reusing the WHERE clause.
	selectStmt := &pg_query.SelectStmt{
		TargetList:  []*pg_query.Node{pkTarget},
		FromClause:  fromClause,
		WhereClause: del.GetWhereClause(),
	}

	deparseResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: selectStmt,
					},
				},
			},
		},
	}

	sql, err := pg_query.Deparse(deparseResult)
	if err != nil {
		return "", fmt.Errorf("deparse: %w", err)
	}

	return sql, nil
}

// addTombstones records tombstones for deleted PKs and persists state.
func (w *WriteHandler) addTombstones(cl *core.Classification) {
	for _, pk := range cl.PKs {
		if w.inTxn() {
			w.tombstones.Stage(pk.Table, pk.PK)
		} else {
			w.tombstones.Add(pk.Table, pk.PK)
			w.deltaMap.Remove(pk.Table, pk.PK)
		}
	}

	if !w.inTxn() {
		if err := delta.WriteTombstoneSet(w.moriDir, w.tombstones); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist tombstone set: %v", w.connID, err)
			}
		}
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}
}

// hasReturning checks if a SQL statement contains a RETURNING clause.
func hasReturning(sql string) bool {
	return strings.Contains(strings.ToUpper(sql), "RETURNING")
}

// buildReturningSelect converts a DELETE ... RETURNING into a SELECT query
// that fetches the RETURNING columns from Prod. For example:
//
//	DELETE FROM users WHERE id = 21 RETURNING id, username
//	→ SELECT id, username FROM users WHERE id = 21
func buildReturningSelect(sql, table string) string {
	upper := strings.ToUpper(sql)
	retIdx := strings.LastIndex(upper, "RETURNING")
	if retIdx < 0 {
		return ""
	}

	// Extract RETURNING columns.
	retCols := strings.TrimSpace(sql[retIdx+len("RETURNING"):])
	if retCols == "" {
		return ""
	}

	// Extract WHERE clause (between "WHERE" and "RETURNING").
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return ""
	}
	whereClause := strings.TrimSpace(sql[whereIdx:retIdx])

	return fmt.Sprintf("SELECT %s FROM %s %s", retCols, table, whereClause)
}
