package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/logging"
)

// savepointSnapshot captures the delta/tombstone/insert/schema state at the time
// a SAVE TRAN is created, so it can be restored on ROLLBACK TRAN <name>.
type savepointSnapshot struct {
	name       string
	deltaSnap  map[string][]string
	tombSnap   map[string][]string
	insertSnap map[string]int
	schemaSnap map[string]*coreSchema.TableDiff
}

// TxnHandler manages transaction state for a single MSSQL connection.
// It coordinates BEGIN TRAN/COMMIT/ROLLBACK across both Prod and Shadow
// backends and controls delta/tombstone staging.
type TxnHandler struct {
	prodConn        net.Conn
	shadowConn      net.Conn
	deltaMap        *delta.Map
	tombstones      *delta.TombstoneSet
	schemaRegistry  *coreSchema.Registry
	moriDir         string
	connID          int64
	verbose         bool
	logger          *logging.Logger
	inTxn           bool
	savepointStack  []savepointSnapshot
	beginSchemaSnap map[string]*coreSchema.TableDiff // schema state at BEGIN
}

// InTxn reports whether this connection is inside an explicit transaction.
func (th *TxnHandler) InTxn() bool {
	return th.inTxn
}

// HandleTxn dispatches a transaction control statement based on its SubType.
func (th *TxnHandler) HandleTxn(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	switch cl.SubType {
	case core.SubBegin:
		return th.handleBegin(clientConn, rawMsg)
	case core.SubCommit:
		return th.handleCommit(clientConn, rawMsg)
	case core.SubRollback:
		return th.handleRollback(clientConn, rawMsg)
	case core.SubSavepoint:
		// Distinguish SAVE TRAN from ROLLBACK TRAN <name>.
		upper := strings.ToUpper(strings.TrimSpace(cl.RawSQL))
		if strings.HasPrefix(upper, "ROLLBACK") {
			return th.handleRollbackToSavepoint(clientConn, rawMsg, cl.RawSQL)
		}
		return th.handleSaveTran(clientConn, rawMsg, cl.RawSQL)
	default:
		return th.forwardToBoth(clientConn, rawMsg)
	}
}

// handleBegin opens a transaction on both backends.
// P1 §2.3: Snapshots schema registry state at BEGIN.
// P1 §2.4: Sends REPEATABLE READ isolation to Prod.
func (th *TxnHandler) handleBegin(clientConn net.Conn, rawMsg []byte) error {
	// Send BEGIN TRAN to Shadow, drain response.
	hadError, err := th.drainWithErrorCheck(th.shadowConn, rawMsg)
	if err != nil {
		return fmt.Errorf("shadow BEGIN TRAN: %w", err)
	}

	if hadError {
		if err := forwardAndRelay(rawMsg, th.prodConn, clientConn); err != nil {
			return fmt.Errorf("prod BEGIN TRAN (shadow failed): %w", err)
		}
		return nil
	}

	// P1 §2.4: Send isolation level + BEGIN to Prod.
	// Use REPEATABLE READ for consistent reads during merged read.
	// (SNAPSHOT isolation requires database-level enablement; REPEATABLE READ is always available.)
	isolationSQL := "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	isolationMsg := buildSQLBatchMessage(isolationSQL)
	th.prodConn.Write(isolationMsg) //nolint:errcheck
	drainTDSResponse(th.prodConn)   //nolint:errcheck

	// Send the original BEGIN TRAN to Prod and relay to client.
	hadProdError, err := th.forwardAndRelayTxn(rawMsg, th.prodConn, clientConn)
	if err != nil {
		return fmt.Errorf("prod BEGIN TRAN: %w", err)
	}

	if !hadProdError {
		th.inTxn = true
		// P1 §2.3: Snapshot schema registry state for restore on ROLLBACK.
		if th.schemaRegistry != nil {
			th.beginSchemaSnap = th.schemaRegistry.SnapshotAll()
		}
		if th.verbose {
			log.Printf("[conn %d] BEGIN TRAN: Shadow=ok Prod=REPEATABLE READ", th.connID)
		}
		th.logger.Event(th.connID, "txn", "BEGIN TRAN")
	}

	return nil
}

// handleCommit commits on both backends and promotes staged deltas.
func (th *TxnHandler) handleCommit(clientConn net.Conn, rawMsg []byte) error {
	shadowHadError, err := th.drainWithErrorCheck(th.shadowConn, rawMsg)
	if err != nil {
		return fmt.Errorf("shadow COMMIT: %w", err)
	}

	prodHadError, err := th.forwardAndRelayTxn(rawMsg, th.prodConn, clientConn)
	if err != nil {
		return fmt.Errorf("prod COMMIT: %w", err)
	}

	th.inTxn = false
	th.savepointStack = nil
	th.beginSchemaSnap = nil

	if !shadowHadError && !prodHadError {
		th.deltaMap.Commit()
		th.deltaMap.CommitInsertCounts()
		th.tombstones.Commit()

		if err := delta.WriteDeltaMap(th.moriDir, th.deltaMap); err != nil {
			if th.verbose {
				log.Printf("[conn %d] failed to persist delta map after COMMIT: %v", th.connID, err)
			}
		}
		if err := delta.WriteTombstoneSet(th.moriDir, th.tombstones); err != nil {
			if th.verbose {
				log.Printf("[conn %d] failed to persist tombstones after COMMIT: %v", th.connID, err)
			}
		}

		if th.verbose {
			log.Printf("[conn %d] COMMIT: staged deltas promoted", th.connID)
		}
		th.logger.Event(th.connID, "txn", "COMMIT: staged deltas promoted")
	} else {
		th.deltaMap.Rollback()
		th.deltaMap.RollbackInsertCounts()
		th.tombstones.Rollback()
		if th.verbose {
			log.Printf("[conn %d] COMMIT failed (shadow=%v prod=%v): staged deltas discarded",
				th.connID, shadowHadError, prodHadError)
		}
		th.logger.Event(th.connID, "txn", "COMMIT failed: staged deltas discarded")
	}

	return nil
}

// handleRollback rolls back on both backends and discards staged deltas.
// P1 §2.3: Restores schema registry to pre-BEGIN state.
func (th *TxnHandler) handleRollback(clientConn net.Conn, rawMsg []byte) error {
	if _, err := th.drainWithErrorCheck(th.shadowConn, rawMsg); err != nil {
		return fmt.Errorf("shadow ROLLBACK: %w", err)
	}

	if _, err := th.forwardAndRelayTxn(rawMsg, th.prodConn, clientConn); err != nil {
		return fmt.Errorf("prod ROLLBACK: %w", err)
	}

	th.inTxn = false
	th.savepointStack = nil

	th.deltaMap.Rollback()
	th.deltaMap.RollbackInsertCounts()
	th.tombstones.Rollback()

	// P1 §2.3: Restore schema registry to pre-BEGIN state.
	if th.schemaRegistry != nil && th.beginSchemaSnap != nil {
		th.schemaRegistry.RestoreAll(th.beginSchemaSnap)
		th.beginSchemaSnap = nil
		if err := coreSchema.WriteRegistry(th.moriDir, th.schemaRegistry); err != nil {
			if th.verbose {
				log.Printf("[conn %d] failed to persist schema registry after ROLLBACK: %v", th.connID, err)
			}
		}
	}

	if th.verbose {
		log.Printf("[conn %d] ROLLBACK: staged deltas and schema changes discarded", th.connID)
	}
	th.logger.Event(th.connID, "txn", "ROLLBACK")

	return nil
}

// ---------------------------------------------------------------------------
// P1 §2.2 — Savepoint State Management
// ---------------------------------------------------------------------------

// handleSaveTran pushes a snapshot and forwards SAVE TRAN to both backends.
func (th *TxnHandler) handleSaveTran(clientConn net.Conn, rawMsg []byte, rawSQL string) error {
	name := parseSaveTranName(rawSQL)
	snap := savepointSnapshot{
		name:       name,
		deltaSnap:  th.deltaMap.SnapshotAll(),
		tombSnap:   th.tombstones.SnapshotAll(),
		insertSnap: th.deltaMap.SnapshotInsertedTables(),
	}
	if th.schemaRegistry != nil {
		snap.schemaSnap = th.schemaRegistry.SnapshotAll()
	}
	th.savepointStack = append(th.savepointStack, snap)

	if th.verbose {
		log.Printf("[conn %d] SAVE TRAN %s: snapshot pushed (stack depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	th.logger.Event(th.connID, "txn", fmt.Sprintf("SAVE TRAN %s", name))

	return th.forwardToBoth(clientConn, rawMsg)
}

// handleRollbackToSavepoint restores state from the matching savepoint snapshot
// and forwards ROLLBACK TRAN <name> to both backends.
// Note: MSSQL keeps savepoints after partial rollback (no implicit release).
func (th *TxnHandler) handleRollbackToSavepoint(clientConn net.Conn, rawMsg []byte, rawSQL string) error {
	name := parseRollbackTranName(rawSQL)
	idx := th.findSavepoint(name)

	if idx >= 0 {
		snap := th.savepointStack[idx]
		th.deltaMap.RestoreAll(snap.deltaSnap)
		th.tombstones.RestoreAll(snap.tombSnap)
		th.deltaMap.RestoreInsertedTables(snap.insertSnap)
		if th.schemaRegistry != nil && snap.schemaSnap != nil {
			th.schemaRegistry.RestoreAll(snap.schemaSnap)
			if err := coreSchema.WriteRegistry(th.moriDir, th.schemaRegistry); err != nil {
				if th.verbose {
					log.Printf("[conn %d] failed to persist schema registry after ROLLBACK TRAN %s: %v",
						th.connID, name, err)
				}
			}
		}
		// Trim the stack: keep up to and including the matched savepoint.
		th.savepointStack = th.savepointStack[:idx+1]

		if th.verbose {
			log.Printf("[conn %d] ROLLBACK TRAN %s: state restored (stack depth=%d)",
				th.connID, name, len(th.savepointStack))
		}
		th.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TRAN %s: restored", name))
	} else {
		if th.verbose {
			log.Printf("[conn %d] ROLLBACK TRAN %s: not found in stack, forwarding only",
				th.connID, name)
		}
		th.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TRAN %s: not in stack", name))
	}

	return th.forwardToBoth(clientConn, rawMsg)
}

// findSavepoint returns the index of the named savepoint in the stack, or -1.
func (th *TxnHandler) findSavepoint(name string) int {
	for i := len(th.savepointStack) - 1; i >= 0; i-- {
		if strings.EqualFold(th.savepointStack[i].name, name) {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Savepoint name parsing helpers (T-SQL syntax)
// ---------------------------------------------------------------------------

// parseSaveTranName extracts the savepoint name from "SAVE TRAN[SACTION] <name>".
func parseSaveTranName(rawSQL string) string {
	fields := strings.Fields(rawSQL)
	// "SAVE TRAN sp1" → fields[2]
	// "SAVE TRANSACTION sp1" → fields[2]
	if len(fields) >= 3 {
		return fields[2]
	}
	return ""
}

// parseRollbackTranName extracts the savepoint name from "ROLLBACK TRAN[SACTION] <name>".
func parseRollbackTranName(rawSQL string) string {
	fields := strings.Fields(rawSQL)
	// "ROLLBACK TRAN sp1" → fields[2]
	// "ROLLBACK TRANSACTION sp1" → fields[2]
	if len(fields) >= 3 {
		return fields[2]
	}
	return ""
}

// ---------------------------------------------------------------------------
// Backend communication helpers
// ---------------------------------------------------------------------------

// forwardToBoth sends a message to Shadow (drain response) then to Prod (relay to client).
func (th *TxnHandler) forwardToBoth(clientConn net.Conn, rawMsg []byte) error {
	th.shadowConn.Write(rawMsg) //nolint: errcheck
	if err := drainTDSResponse(th.shadowConn); err != nil {
		if th.verbose {
			log.Printf("[conn %d] shadow drain error (txn forward): %v", th.connID, err)
		}
	}
	return forwardAndRelay(rawMsg, th.prodConn, clientConn)
}

// drainWithErrorCheck sends a message to a backend and reads the complete TDS
// response. Returns whether an error token was encountered.
func (th *TxnHandler) drainWithErrorCheck(backend net.Conn, rawMsg []byte) (hadError bool, err error) {
	if _, err := backend.Write(rawMsg); err != nil {
		return false, fmt.Errorf("sending to backend: %w", err)
	}

	for {
		pkt, err := readTDSPacket(backend)
		if err != nil {
			return hadError, fmt.Errorf("reading backend response: %w", err)
		}
		if len(pkt.Payload) > 0 && pkt.Payload[0] == tokenError {
			hadError = true
		}
		if pkt.Status&statusEOM != 0 {
			return hadError, nil
		}
	}
}

// forwardAndRelayTxn sends a message to the backend, relays the complete TDS
// response to the client, and reports whether the backend returned an error token.
func (th *TxnHandler) forwardAndRelayTxn(raw []byte, backend, client net.Conn) (hadError bool, err error) {
	if _, err := backend.Write(raw); err != nil {
		return false, fmt.Errorf("sending to backend: %w", err)
	}

	for {
		pkt, err := readTDSPacket(backend)
		if err != nil {
			return hadError, fmt.Errorf("reading backend response: %w", err)
		}

		if len(pkt.Payload) > 0 && pkt.Payload[0] == tokenError {
			hadError = true
		}

		if _, err := client.Write(pkt.Raw); err != nil {
			return hadError, fmt.Errorf("relaying to client: %w", err)
		}

		if pkt.Status&statusEOM != 0 {
			return hadError, nil
		}
	}
}
