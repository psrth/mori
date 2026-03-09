package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// savepointSnapshot captures the delta/tombstone/insert/schema state at the time
// a SAVEPOINT is created, so it can be restored on ROLLBACK TO.
type savepointSnapshot struct {
	name       string
	deltaSnap  map[string][]string
	tombSnap   map[string][]string
	insertSnap map[string]int
	schemaSnap map[string]*coreSchema.TableDiff
}

// TxnHandler manages transaction state for a single connection.
// It coordinates BEGIN/COMMIT/ROLLBACK across both Prod and Shadow backends
// and controls delta/tombstone staging.
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
		// Distinguish full ROLLBACK from ROLLBACK TO SAVEPOINT.
		if isRollbackToSavepoint(cl.RawSQL) {
			return th.handleRollbackTo(clientConn, rawMsg, cl.RawSQL)
		}
		return th.handleRollback(clientConn, rawMsg)
	case core.SubSavepoint:
		return th.handleSavepoint(clientConn, rawMsg, cl.RawSQL)
	case core.SubRelease:
		return th.handleRelease(clientConn, rawMsg, cl.RawSQL)
	default:
		// Forward to both backends without state change.
		return th.forwardToBoth(clientConn, rawMsg)
	}
}

// handleBegin opens a transaction on both backends.
// Shadow gets the original BEGIN; Prod gets BEGIN ISOLATION LEVEL REPEATABLE READ
// to ensure consistent reads for merged read correctness.
func (th *TxnHandler) handleBegin(clientConn net.Conn, rawMsg []byte) error {
	// Send original BEGIN to Shadow, drain response.
	hadError, err := th.drainWithErrorCheck(th.shadowConn, rawMsg)
	if err != nil {
		return fmt.Errorf("shadow BEGIN: %w", err)
	}

	if hadError {
		// Shadow BEGIN failed — relay the error to client via Prod path won't help.
		// Send the original BEGIN to Prod as-is and relay (client sees Prod's response).
		if err := forwardAndRelay(rawMsg, th.prodConn, clientConn); err != nil {
			return fmt.Errorf("prod BEGIN (shadow failed): %w", err)
		}
		return nil
	}

	// Shadow succeeded — send REPEATABLE READ to Prod and relay to client.
	prodBeginMsg := buildQueryMsg("BEGIN ISOLATION LEVEL REPEATABLE READ")
	hadProdError, err := forwardAndRelayTxn(prodBeginMsg, th.prodConn, clientConn)
	if err != nil {
		return fmt.Errorf("prod BEGIN: %w", err)
	}

	if !hadProdError {
		th.inTxn = true
		// Snapshot schema registry state so we can restore on ROLLBACK.
		if th.schemaRegistry != nil {
			th.beginSchemaSnap = th.schemaRegistry.SnapshotAll()
		}
		if th.verbose {
			log.Printf("[conn %d] BEGIN: Shadow=ok Prod=REPEATABLE READ", th.connID)
		}
		th.logger.Event(th.connID, "txn", "BEGIN")
	}

	return nil
}

// handleCommit commits on both backends and promotes staged deltas.
func (th *TxnHandler) handleCommit(clientConn net.Conn, rawMsg []byte) error {
	// Send COMMIT to Shadow, drain response.
	shadowHadError, err := th.drainWithErrorCheck(th.shadowConn, rawMsg)
	if err != nil {
		return fmt.Errorf("shadow COMMIT: %w", err)
	}

	// Send COMMIT to Prod, relay response to client.
	prodHadError, err := forwardAndRelayTxn(rawMsg, th.prodConn, clientConn)
	if err != nil {
		return fmt.Errorf("prod COMMIT: %w", err)
	}

	th.inTxn = false
	th.savepointStack = nil // Clear savepoint stack on commit.
	th.beginSchemaSnap = nil // Schema changes are now permanent.

	// Only promote staged deltas if both backends committed successfully.
	if !shadowHadError && !prodHadError {
		th.deltaMap.Commit()
		th.deltaMap.CommitInsertCounts()
		th.tombstones.Commit()

		// Persist state.
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
		// An error occurred — discard staged entries.
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
func (th *TxnHandler) handleRollback(clientConn net.Conn, rawMsg []byte) error {
	// Send ROLLBACK to Shadow, drain response.
	if _, err := th.drainWithErrorCheck(th.shadowConn, rawMsg); err != nil {
		return fmt.Errorf("shadow ROLLBACK: %w", err)
	}

	// Send ROLLBACK to Prod, relay response to client.
	if _, err := forwardAndRelayTxn(rawMsg, th.prodConn, clientConn); err != nil {
		return fmt.Errorf("prod ROLLBACK: %w", err)
	}

	th.inTxn = false
	th.savepointStack = nil // Clear savepoint stack on full rollback.

	// Discard staged entries.
	th.deltaMap.Rollback()
	th.deltaMap.RollbackInsertCounts()
	th.tombstones.Rollback()

	// Restore schema registry to pre-BEGIN state so rolled-back DDL
	// doesn't leave stale schema diffs.
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

// forwardToBoth sends a message to Shadow (drain response) then to Prod (relay to client).
// Used for SAVEPOINT, RELEASE, and other transaction-adjacent commands.
func (th *TxnHandler) forwardToBoth(clientConn net.Conn, rawMsg []byte) error {
	th.shadowConn.Write(rawMsg) //nolint: errcheck
	if err := drainUntilReady(th.shadowConn); err != nil {
		if th.verbose {
			log.Printf("[conn %d] shadow drain error (txn forward): %v", th.connID, err)
		}
	}
	return forwardAndRelay(rawMsg, th.prodConn, clientConn)
}

// handleSavepoint pushes a snapshot of the current delta/tombstone/insert state
// onto the savepoint stack, then forwards SAVEPOINT to both backends.
func (th *TxnHandler) handleSavepoint(clientConn net.Conn, rawMsg []byte, rawSQL string) error {
	name := parseSavepointName(rawSQL)
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
		log.Printf("[conn %d] SAVEPOINT %s: snapshot pushed (stack depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	th.logger.Event(th.connID, "txn", fmt.Sprintf("SAVEPOINT %s", name))

	return th.forwardToBoth(clientConn, rawMsg)
}

// handleRelease pops the matching savepoint snapshot from the stack (discards it),
// then forwards RELEASE to both backends.
func (th *TxnHandler) handleRelease(clientConn net.Conn, rawMsg []byte, rawSQL string) error {
	name := parseReleaseName(rawSQL)

	// Pop the matching snapshot and everything above it.
	if idx := th.findSavepoint(name); idx >= 0 {
		th.savepointStack = th.savepointStack[:idx]
	}

	if th.verbose {
		log.Printf("[conn %d] RELEASE SAVEPOINT %s: snapshot popped (stack depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	th.logger.Event(th.connID, "txn", fmt.Sprintf("RELEASE SAVEPOINT %s", name))

	return th.forwardToBoth(clientConn, rawMsg)
}

// handleRollbackTo restores delta/tombstone/insert state from the matching savepoint
// snapshot, then forwards ROLLBACK TO to both backends.
// The snapshot is kept on the stack (PostgreSQL allows repeated ROLLBACK TO).
func (th *TxnHandler) handleRollbackTo(clientConn net.Conn, rawMsg []byte, rawSQL string) error {
	name := parseRollbackToName(rawSQL)
	idx := th.findSavepoint(name)

	if idx >= 0 {
		snap := th.savepointStack[idx]
		// Restore state to the savepoint.
		th.deltaMap.RestoreAll(snap.deltaSnap)
		th.tombstones.RestoreAll(snap.tombSnap)
		th.deltaMap.RestoreInsertedTables(snap.insertSnap)
		// Restore schema registry to savepoint state.
		if th.schemaRegistry != nil && snap.schemaSnap != nil {
			th.schemaRegistry.RestoreAll(snap.schemaSnap)
			if err := coreSchema.WriteRegistry(th.moriDir, th.schemaRegistry); err != nil {
				if th.verbose {
					log.Printf("[conn %d] failed to persist schema registry after ROLLBACK TO: %v", th.connID, err)
				}
			}
		}
		// Trim the stack: keep up to and including the matched savepoint,
		// discard anything pushed after it.
		th.savepointStack = th.savepointStack[:idx+1]

		if th.verbose {
			log.Printf("[conn %d] ROLLBACK TO SAVEPOINT %s: state restored (stack depth=%d)",
				th.connID, name, len(th.savepointStack))
		}
		th.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TO SAVEPOINT %s: restored", name))
	} else {
		// Savepoint not found in our stack — just forward to both backends.
		if th.verbose {
			log.Printf("[conn %d] ROLLBACK TO SAVEPOINT %s: not found in stack, forwarding only",
				th.connID, name)
		}
		th.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TO SAVEPOINT %s: not in stack", name))
	}

	return th.forwardToBoth(clientConn, rawMsg)
}

// findSavepoint returns the index of the named savepoint in the stack, or -1 if not found.
// Searches from the top of the stack (most recent) to the bottom.
func (th *TxnHandler) findSavepoint(name string) int {
	for i := len(th.savepointStack) - 1; i >= 0; i-- {
		if strings.EqualFold(th.savepointStack[i].name, name) {
			return i
		}
	}
	return -1
}

// isRollbackToSavepoint checks if a ROLLBACK statement is actually ROLLBACK TO SAVEPOINT.
func isRollbackToSavepoint(rawSQL string) bool {
	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	return strings.Contains(upper, " TO ")
}

// parseSavepointName extracts the savepoint name from "SAVEPOINT <name>".
func parseSavepointName(rawSQL string) string {
	fields := strings.Fields(rawSQL)
	if len(fields) >= 2 {
		return fields[1]
	}
	return ""
}

// parseReleaseName extracts the savepoint name from "RELEASE [SAVEPOINT] <name>".
func parseReleaseName(rawSQL string) string {
	fields := strings.Fields(rawSQL)
	// "RELEASE SAVEPOINT sp1" → fields[2]
	// "RELEASE sp1" → fields[1]
	if len(fields) >= 3 && strings.EqualFold(fields[1], "SAVEPOINT") {
		return fields[2]
	}
	if len(fields) >= 2 {
		return fields[1]
	}
	return ""
}

// parseRollbackToName extracts the savepoint name from "ROLLBACK TO [SAVEPOINT] <name>".
func parseRollbackToName(rawSQL string) string {
	fields := strings.Fields(rawSQL)
	// "ROLLBACK TO SAVEPOINT sp1" → fields[3]
	// "ROLLBACK TO sp1" → fields[2]
	if len(fields) >= 4 && strings.EqualFold(fields[2], "SAVEPOINT") {
		return fields[3]
	}
	if len(fields) >= 3 {
		return fields[2]
	}
	return ""
}

// drainWithErrorCheck sends a message to a backend and reads until ReadyForQuery.
// Returns whether an error ('E') message was encountered.
func (th *TxnHandler) drainWithErrorCheck(backend net.Conn, rawMsg []byte) (hadError bool, err error) {
	if _, err := backend.Write(rawMsg); err != nil {
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
		if msg.Type == 'Z' {
			return hadError, nil
		}
	}
}

// forwardAndRelayTxn sends a message to the backend, relays the complete response
// to the client, and reports whether the backend returned an error.
// This is identical to forwardAndRelayDDL in ddl.go.
func forwardAndRelayTxn(raw []byte, backend, client net.Conn) (hadError bool, err error) {
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
