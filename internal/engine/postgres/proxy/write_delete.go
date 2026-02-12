package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

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
	if len(cl.PKs) == 0 {
		// Bulk delete: no PKs extractable. Forward to Shadow, relay directly.
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
