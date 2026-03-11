package proxy

import (
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/engine/mssql/schema"
	"github.com/psrth/mori/internal/logging"
)

// WriteHandler encapsulates write path logic for a single MSSQL connection.
type WriteHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger
	txnHandler     *TxnHandler // nil when no TxnHandler; used to check inTxn state
	maxRowsHydrate int         // cap for hydration row count; 0 = unlimited
	fkEnforcer     *FKEnforcer // nil if FK enforcement is disabled
}

// inTxn reports whether this connection is inside an explicit transaction.
func (w *WriteHandler) inTxn() bool {
	return w.txnHandler != nil && w.txnHandler.InTxn()
}

// HandleWrite dispatches a write operation based on the routing strategy.
func (w *WriteHandler) HandleWrite(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyShadowWrite:
		if cl.HasOnConflict {
			return w.handleMergeUpsert(clientConn, rawMsg, cl)
		}
		return w.handleInsert(clientConn, rawMsg, cl)
	case core.StrategyHydrateAndWrite:
		if cl.HasOnConflict {
			return w.handleMergeUpsert(clientConn, rawMsg, cl)
		}
		return w.handleUpdate(clientConn, rawMsg, cl)
	case core.StrategyShadowDelete:
		return w.handleDelete(clientConn, rawMsg, cl)
	default:
		return fmt.Errorf("unsupported write strategy: %s", strategy)
	}
}

// ---------------------------------------------------------------------------
// P0 §1.4 — INSERT PK Tracking (row-level)
// ---------------------------------------------------------------------------

// handleInsert executes an INSERT on Shadow and tracks inserted PKs.
// For IDENTITY tables, queries SCOPE_IDENTITY() after INSERT.
// For non-IDENTITY tables, extracts PKs from the INSERT VALUES clause.
// P3 §4.11: When the INSERT has an OUTPUT clause, captures returned data
// from Shadow response for PK extraction.
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	// Enforce FK constraints before executing.
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		if err := w.fkEnforcer.EnforceInsert(cl.Tables[0], cl.RawSQL); err != nil {
			if w.verbose {
				log.Printf("[conn %d] FK violation on INSERT: %v", w.connID, err)
			}
			return forwardAndRelay(buildErrorResponse(err.Error()), nil, clientConn)
		}
	}

	// P3 §4.11: If INSERT has an OUTPUT clause, capture the Shadow response
	// for PK extraction instead of simple relay.
	if cl.HasReturning && len(cl.Tables) == 1 {
		return w.handleInsertWithOutput(clientConn, rawMsg, cl)
	}

	// Execute INSERT on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Track inserted PKs at row level when possible.
	w.trackInsertPKs(cl)

	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}
	return nil
}

// trackInsertPKs extracts and tracks PKs for a single INSERT statement.
func (w *WriteHandler) trackInsertPKs(cl *core.Classification) {
	if len(cl.Tables) == 1 {
		table := cl.Tables[0]
		meta, hasMeta := w.tables[table]
		if hasMeta && len(meta.PKColumns) > 0 {
			// Try to extract PK from the INSERT VALUES clause.
			pk := w.extractInsertPK(cl.RawSQL, table, meta)
			if pk != "" {
				if w.inTxn() {
					w.deltaMap.Stage(table, pk)
				} else {
					w.deltaMap.Add(table, pk)
				}
			} else if meta.PKType == "serial" || meta.PKType == "bigserial" {
				// For IDENTITY tables, query SCOPE_IDENTITY().
				identPK := w.queryScopeIdentity()
				if identPK != "" {
					if w.inTxn() {
						w.deltaMap.Stage(table, identPK)
					} else {
						w.deltaMap.Add(table, identPK)
					}
				} else {
					// Fallback: table-level tracking.
					if w.inTxn() {
						w.deltaMap.StageInsertCount(table, 1)
					} else {
						w.deltaMap.AddInsertCount(table, 1)
					}
				}
			} else {
				// Fallback: table-level tracking.
				if w.inTxn() {
					w.deltaMap.StageInsertCount(table, 1)
				} else {
					w.deltaMap.AddInsertCount(table, 1)
				}
			}
		} else {
			// No PK metadata, use table-level tracking.
			if w.inTxn() {
				w.deltaMap.StageInsertCount(table, 1)
			} else {
				w.deltaMap.AddInsertCount(table, 1)
			}
		}
	} else {
		for _, table := range cl.Tables {
			if w.inTxn() {
				w.deltaMap.StageInsertCount(table, 1)
			} else {
				w.deltaMap.AddInsertCount(table, 1)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// P3 §4.11 — OUTPUT Clause Capture
// ---------------------------------------------------------------------------

// handleInsertWithOutput executes an INSERT with OUTPUT clause on Shadow,
// captures the returned data for PK extraction, and relays to client.
func (w *WriteHandler) handleInsertWithOutput(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	table := cl.Tables[0]
	meta, hasMeta := w.tables[table]

	// Execute on Shadow and capture the full response (don't relay yet).
	result, err := execTDSQueryRaw(w.shadowConn, rawMsg)
	if err != nil {
		return fmt.Errorf("shadow INSERT with OUTPUT: %w", err)
	}

	// Relay the raw response to the client.
	if _, err := clientConn.Write(result.RawMsgs); err != nil {
		return fmt.Errorf("relaying INSERT OUTPUT response: %w", err)
	}

	// Extract PKs from the OUTPUT result if possible.
	if hasMeta && len(meta.PKColumns) > 0 && result.Error == "" {
		// Find indices for ALL PK columns in the OUTPUT result.
		pkIndices := make([]int, len(meta.PKColumns))
		allFound := true
		for p, pkCol := range meta.PKColumns {
			pkIndices[p] = findColumnIndex(result.Columns, pkCol)
			if pkIndices[p] < 0 {
				allFound = false
				break
			}
		}
		if allFound {
			for i, row := range result.RowValues {
				// Check that all PK columns are present and non-null.
				skip := false
				pkValues := make([]string, len(meta.PKColumns))
				for p, idx := range pkIndices {
					if idx >= len(row) || result.RowNulls[i][idx] {
						skip = true
						break
					}
					pkValues[p] = row[idx]
				}
				if skip {
					continue
				}
				pk := core.SerializeCompositePK(meta.PKColumns, pkValues)
				if w.inTxn() {
					w.deltaMap.Stage(table, pk)
				} else {
					w.deltaMap.Add(table, pk)
				}
			}
		} else {
			// PK column not in OUTPUT — fall back to standard tracking.
			w.trackInsertPKs(cl)
		}
	} else {
		// No PK metadata or error — fall back to standard tracking.
		w.trackInsertPKs(cl)
	}

	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}
	return nil
}

// execTDSQueryRaw sends raw bytes to a TDS backend and reads the complete response
// into a TDSQueryResult. Unlike execTDSQuery, this sends the raw message directly
// instead of building a SQL_BATCH. Used for forwarding client messages and capturing results.
func execTDSQueryRaw(conn net.Conn, rawMsg []byte) (*TDSQueryResult, error) {
	if _, err := conn.Write(rawMsg); err != nil {
		return nil, fmt.Errorf("sending query: %w", err)
	}

	result := &TDSQueryResult{}
	var allPayload []byte
	for {
		pkt, err := readTDSPacket(conn)
		if err != nil {
			return nil, fmt.Errorf("reading response: %w", err)
		}
		result.RawMsgs = append(result.RawMsgs, pkt.Raw...)
		allPayload = append(allPayload, pkt.Payload...)
		if pkt.Status&statusEOM != 0 {
			break
		}
	}

	parseTDSTokens(allPayload, result)
	return result, nil
}

// extractInsertPK attempts to extract the PK value(s) from an INSERT VALUES clause.
// For composite PKs, extracts all PK columns and serializes them.
func (w *WriteHandler) extractInsertPK(rawSQL, table string, meta schema.TableMeta) string {
	if len(meta.PKColumns) == 0 {
		return ""
	}

	upper := strings.ToUpper(rawSQL)
	// Find column list between first ( and )
	colStart := strings.Index(upper, "(")
	valuesIdx := strings.Index(upper, "VALUES")
	if colStart < 0 || valuesIdx < 0 || colStart > valuesIdx {
		return ""
	}
	colEnd := strings.Index(upper[colStart:], ")")
	if colEnd < 0 {
		return ""
	}
	colEnd += colStart

	colList := rawSQL[colStart+1 : colEnd]
	cols := strings.Split(colList, ",")

	// Find index for each PK column.
	pkIndices := make([]int, len(meta.PKColumns))
	for p, pkCol := range meta.PKColumns {
		pkIndices[p] = -1
		for i, col := range cols {
			name := strings.TrimSpace(col)
			name = strings.Trim(name, "[]`\"")
			if strings.EqualFold(name, pkCol) {
				pkIndices[p] = i
				break
			}
		}
		if pkIndices[p] < 0 {
			return "" // PK column not found in INSERT column list.
		}
	}

	// Find VALUES ( ... )
	valStart := strings.Index(rawSQL[valuesIdx:], "(")
	if valStart < 0 {
		return ""
	}
	valStart += valuesIdx
	valEnd := strings.Index(rawSQL[valStart:], ")")
	if valEnd < 0 {
		return ""
	}
	valEnd += valStart

	valList := rawSQL[valStart+1 : valEnd]
	vals := splitValuesRespectingQuotes(valList)

	// Extract all PK values.
	pkValues := make([]string, len(meta.PKColumns))
	for p, idx := range pkIndices {
		if idx >= len(vals) {
			return ""
		}
		val := strings.TrimSpace(vals[idx])
		// Strip quotes.
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			val = val[1 : len(val)-1]
		}
		// Skip parameter references.
		if strings.HasPrefix(val, "@") {
			return ""
		}
		pkValues[p] = val
	}

	return core.SerializeCompositePK(meta.PKColumns, pkValues)
}

// splitValuesRespectingQuotes splits a comma-separated VALUES list, respecting
// quoted strings and nested parentheses.
func splitValuesRespectingQuotes(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inString := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			current.WriteByte(c)
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					current.WriteByte(s[i+1])
					i++
				} else {
					inString = false
				}
			}
			continue
		}
		switch c {
		case '\'':
			inString = true
			current.WriteByte(c)
		case '(':
			depth++
			current.WriteByte(c)
		case ')':
			depth--
			current.WriteByte(c)
		case ',':
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteByte(c)
			}
		default:
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// queryScopeIdentity queries the Shadow connection for the last inserted IDENTITY value.
func (w *WriteHandler) queryScopeIdentity() string {
	result, err := execTDSQuery(w.shadowConn, "SELECT SCOPE_IDENTITY()")
	if err != nil || result.Error != "" || len(result.RowValues) == 0 {
		return ""
	}
	if len(result.RowValues[0]) > 0 && !result.RowNulls[0][0] {
		return result.RowValues[0][0]
	}
	return ""
}

// ---------------------------------------------------------------------------
// P0 §1.2 — Bulk UPDATE Hydration
// ---------------------------------------------------------------------------

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
func (w *WriteHandler) handleUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Enforce FK constraints on the SET values before executing.
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		if err := w.fkEnforcer.EnforceUpdate(cl.Tables[0], cl.RawSQL); err != nil {
			if w.verbose {
				log.Printf("[conn %d] FK violation on UPDATE: %v", w.connID, err)
			}
			errPkt := buildErrorResponse(err.Error())
			clientConn.Write(errPkt)
			return nil
		}
	}

	// Hydrate cross-table FROM references before executing.
	if len(cl.Tables) >= 1 {
		fromTables := extractUpdateFromTables(cl.RawSQL, cl.Tables[0])
		if len(fromTables) > 0 {
			w.hydrateReferencedTables(fromTables)
		}
	}

	if len(cl.PKs) == 0 {
		// Bulk update: attempt hydration for single-table UPDATEs with PK metadata.
		if !cl.IsJoin && len(cl.Tables) == 1 {
			table := cl.Tables[0]
			if meta, ok := w.tables[table]; ok && len(meta.PKColumns) > 0 {
				return w.handleBulkUpdate(clientConn, rawMsg, cl, table, meta)
			}
		}
		// Fallback: forward to Shadow without hydration.
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE without PKs, forwarding to Shadow without hydration", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Point update: hydrate missing rows, then execute.
	for _, pk := range cl.PKs {
		if w.deltaMap.IsDelta(pk.Table, pk.PK) {
			continue // Already in Shadow.
		}
		if err := w.hydrateRow(pk.Table, pk.PK); err != nil {
			if w.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", w.connID, pk.Table, pk.PK, err)
			}
		}
	}

	// Execute the UPDATE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Update delta map for all PKs.
	for _, pk := range cl.PKs {
		if w.inTxn() {
			w.deltaMap.Stage(pk.Table, pk.PK)
		} else {
			w.deltaMap.Add(pk.Table, pk.PK)
		}
	}

	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// handleBulkUpdate handles UPDATE statements where no PKs could be extracted
// from the WHERE clause. Queries Prod for matching rows, hydrates them, then
// executes the UPDATE on Shadow.
func (w *WriteHandler) handleBulkUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
	table string,
	meta schema.TableMeta,
) error {
	// Build SELECT * FROM table WHERE <same conditions> via regex extraction.
	selectSQL := buildBulkHydrationQueryMSSQL(cl.RawSQL, table)
	if selectSQL == "" {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: failed to build hydration query", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Apply max rows cap.
	selectSQL = w.capSQL(selectSQL)

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrating from Prod with: %s", w.connID, truncateSQL(selectSQL, 200))
	}

	// Execute on Prod to get matching rows.
	result, err := execTDSQuery(w.prodConn, selectSQL)
	if err != nil || result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod query failed, forwarding to Shadow", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Find indices for ALL PK columns.
	pkIndices := make([]int, len(meta.PKColumns))
	allFound := true
	for p, pkCol := range meta.PKColumns {
		pkIndices[p] = findColumnIndex(result.Columns, pkCol)
		if pkIndices[p] < 0 {
			allFound = false
			break
		}
	}
	if !allFound {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: PK columns not in Prod results", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Hydrate each matching row into Shadow.
	skipCols := toSkipSet(meta.GeneratedCols)
	var affectedPKs []string
	hydratedCount := 0
	for i, row := range result.RowValues {
		// Extract all PK values for this row.
		skip := false
		pkValues := make([]string, len(meta.PKColumns))
		for p, idx := range pkIndices {
			if result.RowNulls[i][idx] {
				skip = true
				break
			}
			pkValues[p] = row[idx]
		}
		if skip {
			continue
		}
		pk := core.SerializeCompositePK(meta.PKColumns, pkValues)
		if w.deltaMap.IsDelta(table, pk) {
			affectedPKs = append(affectedPKs, pk)
			continue
		}
		insertSQL := buildHydrationInsertMSSQL(table, result.Columns, row, result.RowNulls[i], skipCols, meta)
		shadowResult, err := execTDSQuery(w.shadowConn, insertSQL)
		if err == nil && shadowResult.Error == "" {
			hydratedCount++
		}
		affectedPKs = append(affectedPKs, pk)
	}

	if w.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrated %d rows (%d total matched)", w.connID, hydratedCount, len(affectedPKs))
	}

	// Execute UPDATE on Shadow.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Track all affected PKs.
	for _, pk := range affectedPKs {
		if w.inTxn() {
			w.deltaMap.Stage(table, pk)
		} else {
			w.deltaMap.Add(table, pk)
		}
	}

	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// P0 §1.3 — Bulk DELETE Tombstoning
// ---------------------------------------------------------------------------

// handleDelete handles DELETE statements by executing on Shadow and adding tombstones.
// P3 §4.11: When DELETE has an OUTPUT clause (MSSQL's RETURNING equivalent),
// captures the returned data for PK extraction and tombstoning.
func (w *WriteHandler) handleDelete(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// P3 §4.11: DELETE with OUTPUT — capture returned rows for PK extraction.
	if cl.HasReturning && len(cl.Tables) == 1 {
		return w.handleDeleteWithOutput(clientConn, rawMsg, cl)
	}

	if len(cl.PKs) == 0 {
		// Bulk delete: attempt PK discovery from Prod before tombstoning.
		if !cl.IsJoin && len(cl.Tables) == 1 {
			table := cl.Tables[0]
			if meta, ok := w.tables[table]; ok && len(meta.PKColumns) > 0 {
				return w.handleBulkDelete(clientConn, rawMsg, cl, table, meta)
			}
		}
		// Fallback: forward to Shadow without tombstoning.
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE without PKs, no tombstones added", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// FK RESTRICT pre-check: block if child rows exist for RESTRICT/NO ACTION.
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		var deletedPKs []string
		for _, pk := range cl.PKs {
			if pk.Table == cl.Tables[0] {
				deletedPKs = append(deletedPKs, pk.PK)
			}
		}
		if len(deletedPKs) > 0 {
			if err := w.fkEnforcer.EnforceDeleteRestrict(cl.Tables[0], deletedPKs); err != nil {
				if w.verbose {
					log.Printf("[conn %d] FK RESTRICT violation on DELETE: %v", w.connID, err)
				}
				errPkt := buildErrorResponse(err.Error())
				clientConn.Write(errPkt) //nolint:errcheck
				return nil
			}
		}
	}

	// Point delete: execute on Shadow and tombstone.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	w.addTombstones(cl)

	// FK CASCADE post-action: cascade deletes to child tables.
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		var deletedPKs []string
		for _, pk := range cl.PKs {
			if pk.Table == cl.Tables[0] {
				deletedPKs = append(deletedPKs, pk.PK)
			}
		}
		if len(deletedPKs) > 0 {
			if err := w.fkEnforcer.EnforceDeleteCascade(cl.Tables[0], deletedPKs); err != nil {
				if w.verbose {
					log.Printf("[conn %d] FK CASCADE warning on DELETE: %v", w.connID, err)
				}
			}
		}
	}

	return nil
}

// handleDeleteWithOutput executes a DELETE with OUTPUT clause on Shadow,
// captures the returned data for PK extraction and tombstoning.
func (w *WriteHandler) handleDeleteWithOutput(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	table := cl.Tables[0]
	meta, hasMeta := w.tables[table]

	// Execute on Shadow and capture the response.
	result, err := execTDSQueryRaw(w.shadowConn, rawMsg)
	if err != nil {
		return fmt.Errorf("shadow DELETE with OUTPUT: %w", err)
	}

	// Relay the raw response to the client.
	if _, err := clientConn.Write(result.RawMsgs); err != nil {
		return fmt.Errorf("relaying DELETE OUTPUT response: %w", err)
	}

	// Extract PKs from the OUTPUT result for tombstoning.
	if hasMeta && len(meta.PKColumns) > 0 && result.Error == "" {
		// Find indices for ALL PK columns.
		pkIndices := make([]int, len(meta.PKColumns))
		allFound := true
		for p, pkCol := range meta.PKColumns {
			pkIndices[p] = findColumnIndex(result.Columns, pkCol)
			if pkIndices[p] < 0 {
				allFound = false
				break
			}
		}
		if allFound {
			for i, row := range result.RowValues {
				skip := false
				pkValues := make([]string, len(meta.PKColumns))
				for p, idx := range pkIndices {
					if idx >= len(row) || result.RowNulls[i][idx] {
						skip = true
						break
					}
					pkValues[p] = row[idx]
				}
				if skip {
					continue
				}
				pk := core.SerializeCompositePK(meta.PKColumns, pkValues)
				if w.inTxn() {
					w.tombstones.Stage(table, pk)
				} else {
					w.tombstones.Add(table, pk)
					w.deltaMap.Remove(table, pk)
				}
			}
		} else {
			// PK column not in OUTPUT — fall back to standard tombstoning.
			w.addTombstones(cl)
		}
	} else {
		w.addTombstones(cl)
	}

	// FK CASCADE post-action for OUTPUT-captured PKs.
	if w.fkEnforcer != nil {
		var outputPKs []string
		if hasMeta && len(meta.PKColumns) > 0 && result.Error == "" {
			pkIndices := make([]int, len(meta.PKColumns))
			allFound := true
			for p, pkCol := range meta.PKColumns {
				pkIndices[p] = findColumnIndex(result.Columns, pkCol)
				if pkIndices[p] < 0 {
					allFound = false
					break
				}
			}
			if allFound {
				for i, row := range result.RowValues {
					skip := false
					pkValues := make([]string, len(meta.PKColumns))
					for p, idx := range pkIndices {
						if idx < len(row) && !result.RowNulls[i][idx] {
							pkValues[p] = row[idx]
						} else {
							skip = true
							break
						}
					}
					if !skip {
						outputPKs = append(outputPKs, core.SerializeCompositePK(meta.PKColumns, pkValues))
					}
				}
			}
		}
		if len(outputPKs) > 0 {
			if err := w.fkEnforcer.EnforceDeleteCascade(table, outputPKs); err != nil {
				if w.verbose {
					log.Printf("[conn %d] FK CASCADE warning on DELETE OUTPUT: %v", w.connID, err)
				}
			}
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

// handleBulkDelete handles DELETE without extractable PKs by pre-querying Prod.
func (w *WriteHandler) handleBulkDelete(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
	table string,
	meta schema.TableMeta,
) error {
	// Build SELECT for ALL PK columns from the DELETE's WHERE clause.
	var pkColExprs []string
	for _, pkCol := range meta.PKColumns {
		pkColExprs = append(pkColExprs, quoteIdentMSSQL(pkCol))
	}
	selectCols := strings.Join(pkColExprs, ", ")

	selectSQL := buildBulkDeletePKQueryCompositeMSSQL(cl.RawSQL, selectCols, table)
	if selectSQL == "" {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: failed to build PK discovery query", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	selectSQL = w.capSQL(selectSQL)

	if w.verbose {
		log.Printf("[conn %d] bulk DELETE: discovering PKs from Prod with: %s", w.connID, truncateSQL(selectSQL, 200))
	}

	// Execute on Prod.
	result, err := execTDSQuery(w.prodConn, selectSQL)
	if err != nil || result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: Prod PK discovery failed", w.connID)
		}
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	// Find indices for ALL PK columns.
	pkIndices := make([]int, len(meta.PKColumns))
	allFound := true
	for p, pkCol := range meta.PKColumns {
		pkIndices[p] = findColumnIndex(result.Columns, pkCol)
		if pkIndices[p] < 0 {
			allFound = false
			break
		}
	}
	if !allFound {
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	var discoveredPKs []string
	for i, row := range result.RowValues {
		skip := false
		pkValues := make([]string, len(meta.PKColumns))
		for p, idx := range pkIndices {
			if result.RowNulls[i][idx] {
				skip = true
				break
			}
			pkValues[p] = row[idx]
		}
		if skip {
			continue
		}
		discoveredPKs = append(discoveredPKs, core.SerializeCompositePK(meta.PKColumns, pkValues))
	}

	if w.verbose {
		log.Printf("[conn %d] bulk DELETE: discovered %d PKs from Prod", w.connID, len(discoveredPKs))
	}

	// FK RESTRICT pre-check before executing.
	if w.fkEnforcer != nil && len(discoveredPKs) > 0 {
		if err := w.fkEnforcer.EnforceDeleteRestrict(table, discoveredPKs); err != nil {
			if w.verbose {
				log.Printf("[conn %d] FK RESTRICT violation on bulk DELETE: %v", w.connID, err)
			}
			errPkt := buildErrorResponse(err.Error())
			clientConn.Write(errPkt) //nolint:errcheck
			return nil
		}
	}

	// Execute DELETE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Tombstone all discovered PKs.
	for _, pk := range discoveredPKs {
		if w.inTxn() {
			w.tombstones.Stage(table, pk)
		} else {
			w.tombstones.Add(table, pk)
			w.deltaMap.Remove(table, pk)
		}
	}

	// FK CASCADE post-action.
	if w.fkEnforcer != nil && len(discoveredPKs) > 0 {
		if err := w.fkEnforcer.EnforceDeleteCascade(table, discoveredPKs); err != nil {
			if w.verbose {
				log.Printf("[conn %d] FK CASCADE warning on bulk DELETE: %v", w.connID, err)
			}
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

// ---------------------------------------------------------------------------
// P0 §1.5 — MERGE-as-Upsert Handling
// ---------------------------------------------------------------------------

// handleMergeUpsert handles MERGE INTO ... WHEN MATCHED/NOT MATCHED (upsert pattern).
// Hydrates matching rows from Prod into Shadow before executing the MERGE.
func (w *WriteHandler) handleMergeUpsert(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	if len(cl.Tables) == 0 {
		return forwardAndRelay(rawMsg, w.shadowConn, clientConn)
	}

	table := cl.Tables[0]
	meta, hasMeta := w.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		// No PK metadata — execute on Shadow directly.
		if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
			return err
		}
		if w.inTxn() {
			w.deltaMap.StageInsertCount(table, 1)
		} else {
			w.deltaMap.AddInsertCount(table, 1)
		}
		return nil
	}

	// Extract merge key columns and values from USING clause.
	mergeKeys, mergeValues := parseMergeDetails(cl.RawSQL)
	if len(mergeKeys) > 0 && len(mergeValues) > 0 {
		pkCol := meta.PKColumns[0]
		skipCols := toSkipSet(meta.GeneratedCols)

		for _, vals := range mergeValues {
			whereClause := buildMergeWhere(mergeKeys, vals)
			if whereClause == "" {
				continue
			}

			// Check if row exists in Shadow.
			checkSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
				quoteIdentMSSQL(pkCol), quoteIdentMSSQL(table), whereClause)
			shadowResult, err := execTDSQuery(w.shadowConn, checkSQL)
			if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
				continue // Already in Shadow.
			}

			// Query Prod for the row.
			selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s", quoteIdentMSSQL(table), whereClause)
			prodResult, err := execTDSQuery(w.prodConn, selectSQL)
			if err != nil || prodResult.Error != "" || len(prodResult.RowValues) == 0 {
				continue
			}

			// Hydrate into Shadow.
			insertSQL := buildHydrationInsertMSSQL(table, prodResult.Columns, prodResult.RowValues[0], prodResult.RowNulls[0], skipCols, meta)
			execTDSQuery(w.shadowConn, insertSQL) //nolint:errcheck
		}
	}

	// Execute the MERGE on Shadow.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Track delta: use table-level since MERGE can insert or update.
	if w.inTxn() {
		w.deltaMap.StageInsertCount(table, 1)
	} else {
		w.deltaMap.AddInsertCount(table, 1)
	}

	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Hydration helpers
// ---------------------------------------------------------------------------

// hydrateRow fetches a single row from Prod by PK and inserts it into Shadow.
// P0 §1.1: Filters out computed columns from the INSERT.
func (w *WriteHandler) hydrateRow(table, pk string) error {
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return fmt.Errorf("no PK metadata for table %q", table)
	}

	pkValues := core.DeserializeCompositePK(pk, meta.PKColumns)
	whereClause := core.BuildCompositeWHERE(meta.PKColumns, pkValues, quoteIdentMSSQL, quoteLiteralMSSQL)
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s", quoteIdentMSSQL(table), whereClause)

	result, err := execTDSQuery(w.prodConn, selectSQL)
	if err != nil {
		return fmt.Errorf("prod SELECT: %w", err)
	}
	if result.Error != "" {
		return fmt.Errorf("prod SELECT error: %s", result.Error)
	}
	if len(result.RowValues) == 0 {
		return nil // Row doesn't exist in Prod.
	}

	// Build INSERT, filtering computed columns (P0 §1.1).
	skipCols := toSkipSet(meta.GeneratedCols)
	insertSQL := buildHydrationInsertMSSQL(table, result.Columns, result.RowValues[0], result.RowNulls[0], skipCols, meta)

	shadowResult, err := execTDSQuery(w.shadowConn, insertSQL)
	if err != nil {
		return fmt.Errorf("shadow INSERT: %w", err)
	}
	if shadowResult.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] hydration INSERT for (%s, %s): %s", w.connID, table, pk, shadowResult.Error)
		}
	}

	return nil
}

// buildHydrationInsertMSSQL builds an INSERT statement for hydrating a row into Shadow.
// Filters out computed columns (P0 §1.1) and handles IDENTITY_INSERT (P0 §1.2).
// P3 §4.10: Always wraps IDENTITY_INSERT ON/OFF around each INSERT to handle
// concurrency — SQL Server only allows one table with IDENTITY_INSERT ON per session.
// The ON/OFF toggle ensures proper cleanup even when hydrating rows from multiple tables.
func buildHydrationInsertMSSQL(
	table string,
	columns []TDSColumnInfo,
	values []string,
	nulls []bool,
	skipCols map[string]bool,
	meta schema.TableMeta,
) string {
	var colNames []string
	var valParts []string

	for i, col := range columns {
		if skipCols[strings.ToLower(col.Name)] {
			continue // Skip computed columns.
		}
		colNames = append(colNames, quoteIdentMSSQL(col.Name))
		if nulls[i] {
			valParts = append(valParts, "NULL")
		} else {
			valParts = append(valParts, quoteLiteralMSSQL(values[i]))
		}
	}

	if len(colNames) == 0 {
		return ""
	}

	// Build IF NOT EXISTS guard using PK.
	var guard string
	if len(meta.PKColumns) > 0 {
		// Find PK value for the guard.
		for i, col := range columns {
			if strings.EqualFold(col.Name, meta.PKColumns[0]) {
				pkVal := "NULL"
				if !nulls[i] {
					pkVal = quoteLiteralMSSQL(values[i])
				}
				guard = fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM %s WHERE %s = %s) ",
					quoteIdentMSSQL(table), quoteIdentMSSQL(meta.PKColumns[0]), pkVal)
				break
			}
		}
	}

	hasIdentity := meta.PKType == "serial" || meta.PKType == "bigserial"
	var sb strings.Builder

	// P3 §4.10: IDENTITY_INSERT concurrency handling.
	// Always wrap ON/OFF around each INSERT to prevent "IDENTITY_INSERT is already ON
	// for table X" errors when hydrating multiple tables. The BEGIN TRY/END TRY ensures
	// cleanup even if the INSERT fails.
	if hasIdentity {
		sb.WriteString(fmt.Sprintf("SET IDENTITY_INSERT %s ON; ", quoteIdentMSSQL(table)))
		sb.WriteString("BEGIN TRY ")
	}
	sb.WriteString(guard)
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentMSSQL(table),
		strings.Join(colNames, ", "),
		strings.Join(valParts, ", ")))
	if hasIdentity {
		sb.WriteString(fmt.Sprintf("; END TRY BEGIN CATCH END CATCH; SET IDENTITY_INSERT %s OFF", quoteIdentMSSQL(table)))
	}

	return sb.String()
}

// toSkipSet converts a slice of column names to a lowercase set for filtering.
func toSkipSet(cols []string) map[string]bool {
	if len(cols) == 0 {
		return nil
	}
	s := make(map[string]bool, len(cols))
	for _, c := range cols {
		s[strings.ToLower(c)] = true
	}
	return s
}

// findColumnIndex returns the index of a column by name (case-insensitive).
func findColumnIndex(columns []TDSColumnInfo, name string) int {
	lower := strings.ToLower(name)
	for i, col := range columns {
		if strings.ToLower(col.Name) == lower {
			return i
		}
	}
	return -1
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

// ---------------------------------------------------------------------------
// P0 §1.6 — Max Rows Hydration Cap
// ---------------------------------------------------------------------------

// capSQL appends TOP N to a SELECT for hydration capping.
func (w *WriteHandler) capSQL(sql string) string {
	if w.maxRowsHydrate <= 0 {
		return sql
	}
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") {
		return sql
	}
	// If it already has TOP, don't add another.
	afterSelect := strings.TrimSpace(upper[6:])
	if strings.HasPrefix(afterSelect, "TOP") {
		return sql
	}
	// Insert TOP N after SELECT.
	return trimmed[:6] + fmt.Sprintf(" TOP %d", w.maxRowsHydrate) + trimmed[6:]
}

// ---------------------------------------------------------------------------
// SQL builders for bulk operations (regex-based, no AST parser)
// ---------------------------------------------------------------------------

// buildBulkHydrationQueryMSSQL converts an UPDATE into a SELECT * using regex.
// UPDATE table SET ... WHERE ... → SELECT * FROM table WHERE ...
func buildBulkHydrationQueryMSSQL(rawSQL, table string) string {
	upper := strings.ToUpper(rawSQL)
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		// No WHERE clause — hydrate entire table (capped).
		return fmt.Sprintf("SELECT * FROM %s", quoteIdentMSSQL(table))
	}
	whereClause := rawSQL[whereIdx:]
	return fmt.Sprintf("SELECT * FROM %s%s", quoteIdentMSSQL(table), whereClause)
}

// buildBulkDeletePKQueryMSSQL converts a DELETE into a SELECT pk_col.
// DELETE FROM table WHERE ... → SELECT pk FROM table WHERE ...
func buildBulkDeletePKQueryMSSQL(rawSQL, pkCol, table string) string {
	upper := strings.ToUpper(rawSQL)
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		return fmt.Sprintf("SELECT %s FROM %s", quoteIdentMSSQL(pkCol), quoteIdentMSSQL(table))
	}
	whereClause := rawSQL[whereIdx:]
	return fmt.Sprintf("SELECT %s FROM %s%s", quoteIdentMSSQL(pkCol), quoteIdentMSSQL(table), whereClause)
}

// buildBulkDeletePKQueryCompositeMSSQL converts a DELETE into a SELECT for multiple PK columns.
// DELETE FROM table WHERE ... → SELECT col1, col2 FROM table WHERE ...
func buildBulkDeletePKQueryCompositeMSSQL(rawSQL, selectCols, table string) string {
	upper := strings.ToUpper(rawSQL)
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		return fmt.Sprintf("SELECT %s FROM %s", selectCols, quoteIdentMSSQL(table))
	}
	whereClause := rawSQL[whereIdx:]
	return fmt.Sprintf("SELECT %s FROM %s%s", selectCols, quoteIdentMSSQL(table), whereClause)
}

// ---------------------------------------------------------------------------
// MERGE parsing helpers (regex-based)
// ---------------------------------------------------------------------------

var reMergeOn = regexp.MustCompile(`(?i)\bON\s+(.+?)\s+WHEN\b`)
var reMergeOnCol = regexp.MustCompile(`(?i)\btarget\.\[?(\w+)\]?\s*=\s*source\.\[?(\w+)\]?`)
var reMergeValues = regexp.MustCompile(`(?i)\bVALUES\s*\(([^)]+)\)`)

// parseMergeDetails extracts the ON columns and VALUES from a MERGE statement.
func parseMergeDetails(rawSQL string) (columns []string, values [][]string) {
	// Extract ON clause columns.
	if m := reMergeOn.FindStringSubmatch(rawSQL); len(m) > 1 {
		onClause := m[1]
		for _, cm := range reMergeOnCol.FindAllStringSubmatch(onClause, -1) {
			if len(cm) > 1 {
				columns = append(columns, strings.ToLower(cm[1]))
			}
		}
	}

	// Extract VALUES.
	if m := reMergeValues.FindStringSubmatch(rawSQL); len(m) > 1 {
		vals := splitValuesRespectingQuotes(m[1])
		var row []string
		for _, v := range vals {
			v = strings.TrimSpace(v)
			if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
				v = v[1 : len(v)-1]
			}
			row = append(row, v)
		}
		if len(row) > 0 {
			values = append(values, row)
		}
	}

	return columns, values
}

// ---------------------------------------------------------------------------
// Cross-table UPDATE...FROM hydration
// ---------------------------------------------------------------------------

// reUpdateFrom matches UPDATE target SET ... FROM table_refs.
// T-SQL: UPDATE t1 SET ... FROM t2 [alias] [JOIN t3 [alias] ON ...]
var reUpdateFrom = regexp.MustCompile(`(?i)\bFROM\s+(.+?)(?:\s+WHERE\b|\s+ORDER\s+BY\b|\s+OPTION\b|\s*;|\s*$)`)
var reTableRef = regexp.MustCompile(`(?i)(?:^|JOIN)\s+\[?(\w+)\]?(?:\s+(?:AS\s+)?\[?(\w+)\]?)?`)

// extractUpdateFromTables extracts table names from the FROM clause of an
// UPDATE...FROM statement, excluding the main target table.
func extractUpdateFromTables(rawSQL, targetTable string) []string {
	upper := strings.ToUpper(rawSQL)

	// Only applies to UPDATE statements with FROM.
	setIdx := strings.Index(upper, " SET ")
	if setIdx < 0 {
		return nil
	}

	// Look for FROM after SET...WHERE region.
	afterSet := upper[setIdx:]
	fromIdx := strings.Index(afterSet, " FROM ")
	if fromIdx < 0 {
		return nil
	}

	// Extract the FROM clause content.
	fromStart := setIdx + fromIdx
	m := reUpdateFrom.FindStringSubmatch(rawSQL[fromStart:])
	if len(m) < 2 {
		return nil
	}
	fromContent := m[1]

	// Extract table names from FROM clause.
	seen := make(map[string]bool)
	var tables []string
	for _, tm := range reTableRef.FindAllStringSubmatch(fromContent, -1) {
		if len(tm) < 2 {
			continue
		}
		tableName := strings.Trim(tm[1], "[]")
		lower := strings.ToLower(tableName)
		// Skip the target table and SQL keywords that might be captured.
		if strings.EqualFold(tableName, targetTable) || seen[lower] {
			continue
		}
		// Skip common keywords that the regex might capture.
		if lower == "on" || lower == "set" || lower == "where" || lower == "inner" ||
			lower == "left" || lower == "right" || lower == "outer" || lower == "cross" {
			continue
		}
		seen[lower] = true
		tables = append(tables, tableName)
	}
	return tables
}

// hydrateReferencedTables hydrates rows from FROM-clause referenced tables into Shadow.
func (w *WriteHandler) hydrateReferencedTables(tables []string) {
	for _, table := range tables {
		meta, ok := w.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]
		selectSQL := fmt.Sprintf("SELECT * FROM %s", quoteIdentMSSQL(table))
		selectSQL = w.capSQL(selectSQL)

		result, err := execTDSQuery(w.prodConn, selectSQL)
		if err != nil || result.Error != "" {
			if w.verbose {
				log.Printf("[conn %d] UPDATE FROM: failed to query Prod for %s", w.connID, table)
			}
			continue
		}

		pkIdx := findColumnIndex(result.Columns, pkCol)
		if pkIdx < 0 {
			continue
		}

		skipCols := toSkipSet(meta.GeneratedCols)
		hydrated := 0
		for i, row := range result.RowValues {
			if result.RowNulls[i][pkIdx] {
				continue
			}
			pk := row[pkIdx]
			if w.deltaMap.IsDelta(table, pk) {
				continue // Already in Shadow.
			}
			insertSQL := buildHydrationInsertMSSQL(table, result.Columns, row, result.RowNulls[i], skipCols, meta)
			shadowResult, err := execTDSQuery(w.shadowConn, insertSQL)
			if err == nil && shadowResult.Error == "" {
				hydrated++
			}
		}
		if w.verbose && hydrated > 0 {
			log.Printf("[conn %d] UPDATE FROM: hydrated %d rows from %s", w.connID, hydrated, table)
		}
	}
}

// buildMergeWhere builds a WHERE clause from merge key columns and values.
func buildMergeWhere(columns []string, values []string) string {
	if len(columns) == 0 {
		return ""
	}
	// We may have more values than columns; use min.
	n := len(columns)
	if len(values) < n {
		n = len(values)
	}
	var parts []string
	for i := 0; i < n; i++ {
		parts = append(parts, fmt.Sprintf("%s = %s", quoteIdentMSSQL(columns[i]), quoteLiteralMSSQL(values[i])))
	}
	return strings.Join(parts, " AND ")
}
