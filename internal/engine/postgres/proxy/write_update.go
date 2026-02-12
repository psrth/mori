package proxy

import (
	"fmt"
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
//
// Point update (PKs extractable): for each (table, pk), hydrate from Prod if
// the row is not already in Shadow, then execute the UPDATE on Shadow and
// track the affected PKs in the delta map.
//
// Bulk update (no PKs): forward to Shadow without hydration. Only rows already
// in Shadow will be affected.
func (w *WriteHandler) handleUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	if len(cl.PKs) == 0 {
		// Bulk update: no PKs extractable. Forward to Shadow only.
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
			// Non-fatal: the UPDATE may still succeed if the row exists
			// in Shadow or doesn't exist at all.
		}
	}

	// Execute the UPDATE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Update delta map for all PKs and persist.
	for _, pk := range cl.PKs {
		w.deltaMap.Add(pk.Table, pk.PK)
	}
	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}

	return nil
}

// hydrateRow fetches a single row from Prod by PK and inserts it into Shadow.
// If the row does not exist in Prod, it is a no-op.
func (w *WriteHandler) hydrateRow(table, pk string) error {
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return fmt.Errorf("no PK metadata for table %q", table)
	}

	// Build SELECT query using the first PK column.
	// For composite PKs, a future phase will handle all columns.
	pkCol := meta.PKColumns[0]
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s",
		quoteIdent(table), quoteIdent(pkCol), quoteLiteral(pk))

	result, err := execQuery(w.prodConn, selectSQL)
	if err != nil {
		return fmt.Errorf("prod SELECT: %w", err)
	}
	if result.Error != "" {
		return fmt.Errorf("prod SELECT error: %s", result.Error)
	}
	if len(result.RowValues) == 0 {
		return nil // Row doesn't exist in Prod; nothing to hydrate.
	}

	// Build and execute INSERT into Shadow.
	insertSQL := buildInsertSQL(table, result.Columns, result.RowValues[0], result.RowNulls[0])

	shadowResult, err := execQuery(w.shadowConn, insertSQL)
	if err != nil {
		return fmt.Errorf("shadow INSERT: %w", err)
	}
	if shadowResult.Error != "" {
		// Could be a constraint violation from concurrent hydration.
		// ON CONFLICT DO NOTHING handles this; log and continue.
		if w.verbose {
			log.Printf("[conn %d] hydration INSERT for (%s, %s): %s", w.connID, table, pk, shadowResult.Error)
		}
	}

	return nil
}
