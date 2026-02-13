package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// WriteHandler encapsulates write path logic for a single MSSQL connection.
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
	rawMsg []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyShadowWrite:
		return w.handleInsert(clientConn, rawMsg, cl)
	case core.StrategyHydrateAndWrite:
		return w.handleUpdate(clientConn, rawMsg, cl)
	case core.StrategyShadowDelete:
		return w.handleDelete(clientConn, rawMsg, cl)
	default:
		return fmt.Errorf("unsupported write strategy: %s", strategy)
	}
}

// handleInsert executes an INSERT on Shadow and relays the response to the client.
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
		return err
	}
	for _, table := range cl.Tables {
		w.deltaMap.MarkInserted(table)
	}
	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}
	return nil
}

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
func (w *WriteHandler) handleUpdate(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Hydrate rows from Prod to Shadow before updating.
	if len(cl.PKs) > 0 {
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
	}

	// Execute the UPDATE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
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
func (w *WriteHandler) handleDelete(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Execute DELETE on Shadow, relay response to client.
	if err := forwardAndRelay(rawMsg, w.shadowConn, clientConn); err != nil {
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
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s",
		quoteIdentMSSQL(table), quoteIdentMSSQL(pkCol), quoteLiteralMSSQL(pk))

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

	// Build INSERT into Shadow.
	colNames := make([]string, len(result.Columns))
	valParts := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		colNames[i] = quoteIdentMSSQL(col.Name)
		if result.RowNulls[0][i] {
			valParts[i] = "NULL"
		} else {
			valParts[i] = quoteLiteralMSSQL(result.RowValues[0][i])
		}
	}

	// Use a MERGE or try-insert approach for MSSQL (no ON CONFLICT).
	// Use SET IDENTITY_INSERT to allow explicit ID values.
	hasIdentity := meta.PKType == "serial" || meta.PKType == "bigserial"
	var insertSQL string
	if hasIdentity {
		insertSQL = fmt.Sprintf("SET IDENTITY_INSERT %s ON; ", quoteIdentMSSQL(table))
	}
	insertSQL += fmt.Sprintf("IF NOT EXISTS (SELECT 1 FROM %s WHERE %s = %s) INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentMSSQL(table), quoteIdentMSSQL(meta.PKColumns[0]), quoteLiteralMSSQL(pk),
		quoteIdentMSSQL(table),
		strings.Join(colNames, ", "),
		strings.Join(valParts, ", "))
	if hasIdentity {
		insertSQL += fmt.Sprintf("; SET IDENTITY_INSERT %s OFF", quoteIdentMSSQL(table))
	}

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
