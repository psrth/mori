package proxy

import (
	"fmt"
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// DDLHandler handles DDL operations for a single connection.
// It executes DDL on Shadow, parses the changes, and updates the schema registry.
type DDLHandler struct {
	shadowConn     net.Conn
	schemaRegistry *coreSchema.Registry
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger
}

// HandleDDL executes a DDL statement on Shadow and updates the schema registry.
// The response is relayed to the client. If the DDL fails on Shadow, the error
// is relayed to the client and the registry is not updated.
func (dh *DDLHandler) HandleDDL(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Execute DDL on Shadow, relay response to client, and detect errors.
	hadError, err := forwardAndRelayDDL(rawMsg, dh.shadowConn, clientConn)
	if err != nil {
		return fmt.Errorf("DDL forward: %w", err)
	}

	// If DDL failed on Shadow, skip registry update.
	if hadError {
		if dh.verbose {
			log.Printf("[conn %d] DDL failed on Shadow, skipping registry update", dh.connID)
		}
		return nil
	}

	if dh.schemaRegistry == nil {
		return nil
	}

	// Parse DDL to extract schema changes.
	changes, err := parseDDLChanges(cl.RawSQL)
	if err != nil {
		if dh.verbose {
			log.Printf("[conn %d] DDL parse warning (registry not updated): %v", dh.connID, err)
		}
		return nil // Non-fatal: DDL succeeded, we just can't track the change.
	}

	if len(changes) == 0 {
		return nil
	}

	// Apply changes to the schema registry.
	for _, ch := range changes {
		dh.applyChange(ch)
	}

	// Persist the registry.
	if err := coreSchema.WriteRegistry(dh.moriDir, dh.schemaRegistry); err != nil {
		if dh.verbose {
			log.Printf("[conn %d] failed to persist schema registry: %v", dh.connID, err)
		}
	}

	return nil
}

// applyChange records a single schema change in the registry.
func (dh *DDLHandler) applyChange(ch ddlChange) {
	switch ch.Kind {
	case ddlAddColumn:
		col := coreSchema.Column{
			Name:    ch.Column,
			Type:    ch.ColType,
			Default: ch.Default,
		}
		dh.schemaRegistry.RecordAddColumn(ch.Table, col)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: ADD COLUMN %s.%s (%s)", dh.connID, ch.Table, ch.Column, ch.ColType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("ADD COLUMN %s.%s (%s)", ch.Table, ch.Column, ch.ColType))

	case ddlDropColumn:
		dh.schemaRegistry.RecordDropColumn(ch.Table, ch.Column)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: DROP COLUMN %s.%s", dh.connID, ch.Table, ch.Column)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("DROP COLUMN %s.%s", ch.Table, ch.Column))

	case ddlRenameColumn:
		dh.schemaRegistry.RecordRenameColumn(ch.Table, ch.OldName, ch.NewName)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: RENAME COLUMN %s.%s → %s", dh.connID, ch.Table, ch.OldName, ch.NewName)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("RENAME COLUMN %s.%s -> %s", ch.Table, ch.OldName, ch.NewName))

	case ddlAlterType:
		dh.schemaRegistry.RecordTypeChange(ch.Table, ch.Column, ch.OldType, ch.NewType)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: ALTER TYPE %s.%s → %s", dh.connID, ch.Table, ch.Column, ch.NewType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("ALTER TYPE %s.%s -> %s", ch.Table, ch.Column, ch.NewType))

	case ddlDropTable:
		dh.schemaRegistry.RemoveTable(ch.Table)
		if dh.deltaMap != nil {
			dh.deltaMap.ClearTable(ch.Table)
		}
		if dh.tombstones != nil {
			dh.tombstones.ClearTable(ch.Table)
		}
		if dh.verbose {
			log.Printf("[conn %d] schema registry: DROP TABLE %s", dh.connID, ch.Table)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("DROP TABLE %s", ch.Table))

	case ddlCreateTable:
		dh.schemaRegistry.RecordNewTable(ch.Table)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: CREATE TABLE %s", dh.connID, ch.Table)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("CREATE TABLE %s", ch.Table))
	}
}

// forwardAndRelayDDL sends a message to the backend, relays the complete response
// to the client, and reports whether the backend returned an error.
func forwardAndRelayDDL(raw []byte, backend, client net.Conn) (hadError bool, err error) {
	if _, err := backend.Write(raw); err != nil {
		return false, fmt.Errorf("sending to backend: %w", err)
	}

	for {
		msg, err := readMsg(backend)
		if err != nil {
			return hadError, fmt.Errorf("reading backend response: %w", err)
		}

		if msg.Type == 'E' {
			hadError = true
		}

		if _, err := client.Write(msg.Raw); err != nil {
			return hadError, fmt.Errorf("relaying to client: %w", err)
		}

		if msg.Type == 'Z' {
			return hadError, nil
		}
	}
}
