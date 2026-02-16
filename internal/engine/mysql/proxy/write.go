package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// WriteHandler encapsulates write path logic for a single MySQL connection.
type WriteHandler struct {
	prodConn   net.Conn
	shadowConn net.Conn
	deltaMap   *delta.Map
	tombstones *delta.TombstoneSet
	tables     map[string]schema.TableMeta
	moriDir    string
	connID     int64
	verbose    bool
	logger     *logging.Logger
}

// HandleWrite dispatches a write operation based on the routing strategy.
func (w *WriteHandler) HandleWrite(
	clientConn net.Conn,
	rawPkt []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyShadowWrite:
		return w.handleInsert(clientConn, rawPkt, cl)
	case core.StrategyHydrateAndWrite:
		return w.handleUpdate(clientConn, rawPkt, cl)
	case core.StrategyShadowDelete:
		return w.handleDelete(clientConn, rawPkt, cl)
	default:
		return fmt.Errorf("unsupported write strategy: %s", strategy)
	}
}

// handleInsert executes an INSERT on Shadow and marks the table as having inserts.
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}
	for _, table := range cl.Tables {
		w.deltaMap.MarkInserted(table)
	}
	return nil
}

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
func (w *WriteHandler) handleUpdate(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if len(cl.PKs) == 0 {
		// Bulk update: no PKs extractable. Forward to Shadow only.
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE without PKs, forwarding to Shadow without hydration", w.connID)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
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
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Update delta map for all PKs.
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

// handleDelete handles DELETE statements by executing on Shadow and adding tombstones.
func (w *WriteHandler) handleDelete(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if len(cl.PKs) == 0 {
		// Bulk delete: forward to Shadow without tombstoning.
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE without PKs, no tombstones added", w.connID)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Forward DELETE to Shadow, relay response.
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Add tombstones for deleted PKs.
	for _, pk := range cl.PKs {
		w.tombstones.Add(pk.Table, pk.PK)
		w.deltaMap.Remove(pk.Table, pk.PK)
	}

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

	return nil
}

// hydrateRow fetches a single row from Prod by PK and inserts it into Shadow.
func (w *WriteHandler) hydrateRow(table, pk string) error {
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return fmt.Errorf("no PK metadata for table %q", table)
	}

	pkCol := meta.PKColumns[0]
	selectSQL := fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` = '%s'",
		table, pkCol, strings.ReplaceAll(pk, "'", "''"))

	result, err := execMySQLQuery(w.prodConn, selectSQL)
	if err != nil {
		return fmt.Errorf("prod SELECT: %w", err)
	}
	if result.Error != "" {
		return fmt.Errorf("prod SELECT error: %s", result.Error)
	}
	if len(result.RowValues) == 0 {
		return nil // Row doesn't exist in Prod.
	}

	// Build and execute INSERT into Shadow.
	insertSQL := buildMySQLInsertSQL(table, result.Columns, result.RowValues[0], result.RowNulls[0])

	shadowResult, err := execMySQLQuery(w.shadowConn, insertSQL)
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

// buildMySQLInsertSQL constructs an INSERT ... ON DUPLICATE KEY UPDATE statement.
func buildMySQLInsertSQL(table string, columns []ColumnInfo, values []string, nulls []bool) string {
	colNames := make([]string, len(columns))
	valParts := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = "`" + col.Name + "`"
		if nulls[i] {
			valParts[i] = "NULL"
		} else {
			valParts[i] = "'" + strings.ReplaceAll(values[i], "'", "''") + "'"
		}
	}
	// Use INSERT IGNORE to handle concurrent/duplicate hydration.
	return fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES (%s)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(valParts, ", "))
}
