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

// TxnHandler manages transaction state for a single MySQL connection.
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
	beginSchemaSnap map[string]*coreSchema.TableDiff // schema state at BEGIN
	savepointStack  []savepointSnapshot
}

// InTxn reports whether this connection is inside an explicit transaction.
func (th *TxnHandler) InTxn() bool {
	return th.inTxn
}

// HandleTxn dispatches a transaction control statement based on its SubType.
func (th *TxnHandler) HandleTxn(
	clientConn net.Conn,
	rawPkt []byte,
	cl *core.Classification,
) error {
	switch cl.SubType {
	case core.SubBegin:
		return th.handleBegin(clientConn, rawPkt)
	case core.SubCommit:
		return th.handleCommit(clientConn, rawPkt)
	case core.SubRollback:
		// Distinguish full ROLLBACK from ROLLBACK TO SAVEPOINT.
		if isRollbackTo(cl.RawSQL) {
			return th.handleRollbackTo(clientConn, rawPkt, cl.RawSQL)
		}
		return th.handleRollback(clientConn, rawPkt)
	case core.SubSavepoint:
		return th.handleSavepoint(clientConn, rawPkt, cl.RawSQL)
	case core.SubRelease:
		return th.handleRelease(clientConn, rawPkt, cl.RawSQL)
	default:
		// Forward to both backends without state change.
		return th.forwardToBoth(clientConn, rawPkt)
	}
}

// handleBegin opens a transaction on both backends.
func (th *TxnHandler) handleBegin(clientConn net.Conn, rawPkt []byte) error {
	// Send BEGIN to Shadow, drain response.
	hadError, err := th.drainWithErrorCheck(th.shadowConn, rawPkt)
	if err != nil {
		return fmt.Errorf("shadow BEGIN: %w", err)
	}

	if hadError {
		// Shadow BEGIN failed — send the original BEGIN to Prod and relay.
		if err := forwardAndRelay(rawPkt, th.prodConn, clientConn); err != nil {
			return fmt.Errorf("prod BEGIN (shadow failed): %w", err)
		}
		return nil
	}

	// Shadow succeeded — send BEGIN with REPEATABLE READ to Prod for consistent reads.
	// MySQL uses: START TRANSACTION WITH CONSISTENT SNAPSHOT
	// (or simply BEGIN for compatibility — the router opens REPEATABLE READ isolation
	// on shadow already via the container's default, so standard BEGIN is sufficient).
	hadProdError, err := th.forwardAndRelayTxn(rawPkt, th.prodConn, clientConn)
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
			log.Printf("[conn %d] BEGIN: Shadow=ok Prod=ok", th.connID)
		}
		th.logger.Event(th.connID, "txn", "BEGIN")
	}

	return nil
}

// handleCommit commits on both backends and promotes staged deltas.
func (th *TxnHandler) handleCommit(clientConn net.Conn, rawPkt []byte) error {
	// Send COMMIT to Shadow, drain response.
	shadowHadError, err := th.drainWithErrorCheck(th.shadowConn, rawPkt)
	if err != nil {
		return fmt.Errorf("shadow COMMIT: %w", err)
	}

	// Send COMMIT to Prod, relay response to client.
	prodHadError, err := th.forwardAndRelayTxn(rawPkt, th.prodConn, clientConn)
	if err != nil {
		return fmt.Errorf("prod COMMIT: %w", err)
	}

	th.inTxn = false
	th.savepointStack = nil  // Clear savepoint stack on commit.
	th.beginSchemaSnap = nil // Schema changes are now permanent.

	// Only promote staged deltas if both backends committed successfully.
	if !shadowHadError && !prodHadError {
		th.deltaMap.Commit()
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
func (th *TxnHandler) handleRollback(clientConn net.Conn, rawPkt []byte) error {
	// Send ROLLBACK to Shadow, drain response.
	if _, err := th.drainWithErrorCheck(th.shadowConn, rawPkt); err != nil {
		return fmt.Errorf("shadow ROLLBACK: %w", err)
	}

	// Send ROLLBACK to Prod, relay response to client.
	if _, err := th.forwardAndRelayTxn(rawPkt, th.prodConn, clientConn); err != nil {
		return fmt.Errorf("prod ROLLBACK: %w", err)
	}

	th.inTxn = false
	th.savepointStack = nil // Clear savepoint stack on full rollback.

	// Discard staged entries.
	th.deltaMap.Rollback()
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

// handleSavepoint pushes a snapshot of the current delta/tombstone/insert/schema
// state onto the savepoint stack, then forwards SAVEPOINT to both backends.
func (th *TxnHandler) handleSavepoint(clientConn net.Conn, rawPkt []byte, rawSQL string) error {
	// Forward to both backends first.
	if err := th.forwardToBoth(clientConn, rawPkt); err != nil {
		return err
	}

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
		log.Printf("[conn %d] SAVEPOINT %s: snapshot pushed (depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	th.logger.Event(th.connID, "txn", fmt.Sprintf("SAVEPOINT %s", name))
	return nil
}

// handleRelease pops the matching savepoint snapshot from the stack (discards it),
// then forwards RELEASE to both backends.
func (th *TxnHandler) handleRelease(clientConn net.Conn, rawPkt []byte, rawSQL string) error {
	if err := th.forwardToBoth(clientConn, rawPkt); err != nil {
		return err
	}

	name := parseReleaseName(rawSQL)
	// Pop the matching snapshot and everything above it.
	if idx := th.findSavepoint(name); idx >= 0 {
		th.savepointStack = th.savepointStack[:idx]
	}

	if th.verbose {
		log.Printf("[conn %d] RELEASE SAVEPOINT %s: snapshot popped (depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	th.logger.Event(th.connID, "txn", fmt.Sprintf("RELEASE SAVEPOINT %s", name))
	return nil
}

// handleRollbackTo restores delta/tombstone/insert/schema state from the matching
// savepoint snapshot, then forwards ROLLBACK TO to both backends.
// The snapshot is kept on the stack (MySQL allows repeated ROLLBACK TO).
func (th *TxnHandler) handleRollbackTo(clientConn net.Conn, rawPkt []byte, rawSQL string) error {
	if err := th.forwardToBoth(clientConn, rawPkt); err != nil {
		return err
	}

	name := parseRollbackToName(rawSQL)
	idx := th.findSavepoint(name)
	if idx < 0 {
		if th.verbose {
			log.Printf("[conn %d] ROLLBACK TO %s: no matching savepoint in stack", th.connID, name)
		}
		return nil
	}

	snap := th.savepointStack[idx]

	// Restore delta/tombstone/insert state to the savepoint.
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
		log.Printf("[conn %d] ROLLBACK TO SAVEPOINT %s: state restored (depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	th.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TO SAVEPOINT %s: restored", name))
	return nil
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

// isRollbackTo checks if a ROLLBACK statement is actually ROLLBACK TO [SAVEPOINT].
func isRollbackTo(rawSQL string) bool {
	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	return strings.Contains(upper, " TO ")
}

// parseSavepointName extracts the savepoint name from "SAVEPOINT <name>".
func parseSavepointName(rawSQL string) string {
	parts := strings.Fields(strings.TrimSpace(rawSQL))
	if len(parts) >= 2 {
		return strings.Trim(parts[1], "`'\"")
	}
	return ""
}

// parseReleaseName extracts the savepoint name from "RELEASE [SAVEPOINT] <name>".
func parseReleaseName(rawSQL string) string {
	parts := strings.Fields(strings.TrimSpace(rawSQL))
	// "RELEASE SAVEPOINT sp1" -> parts[2]
	// "RELEASE sp1" -> parts[1]
	if len(parts) >= 3 && strings.EqualFold(parts[1], "SAVEPOINT") {
		return strings.Trim(parts[2], "`'\"")
	}
	if len(parts) >= 2 {
		return strings.Trim(parts[1], "`'\"")
	}
	return ""
}

// parseRollbackToName extracts the savepoint name from "ROLLBACK TO [SAVEPOINT] <name>".
func parseRollbackToName(rawSQL string) string {
	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	idx := strings.Index(upper, " TO ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(rawSQL[idx+4:])
	parts := strings.Fields(rest)
	// "ROLLBACK TO SAVEPOINT sp1" -> parts = ["SAVEPOINT", "sp1"]
	// "ROLLBACK TO sp1" -> parts = ["sp1"]
	if len(parts) >= 2 && strings.EqualFold(parts[0], "SAVEPOINT") {
		return strings.Trim(parts[1], "`'\"")
	}
	if len(parts) >= 1 {
		return strings.Trim(parts[0], "`'\"")
	}
	return ""
}

// forwardToBoth sends a packet to Shadow (drain response) then to Prod (relay to client).
// Used for SAVEPOINT, RELEASE, and other transaction-adjacent commands.
func (th *TxnHandler) forwardToBoth(clientConn net.Conn, rawPkt []byte) error {
	th.shadowConn.Write(rawPkt) //nolint: errcheck
	if err := drainResponse(th.shadowConn); err != nil {
		if th.verbose {
			log.Printf("[conn %d] shadow drain error (txn forward): %v", th.connID, err)
		}
	}
	return forwardAndRelay(rawPkt, th.prodConn, clientConn)
}

// drainWithErrorCheck sends a packet to a backend and reads the complete response.
// Returns whether an ERR packet was encountered.
func (th *TxnHandler) drainWithErrorCheck(backend net.Conn, rawPkt []byte) (hadError bool, err error) {
	if _, err := backend.Write(rawPkt); err != nil {
		return false, fmt.Errorf("sending to backend: %w", err)
	}

	// Read complete MySQL response (OK, ERR, or result set).
	pkt, err := readMySQLPacket(backend)
	if err != nil {
		return false, fmt.Errorf("reading backend response: %w", err)
	}

	if isERRPacket(pkt.Payload) {
		return true, nil
	}

	// OK packet — single response, we're done.
	if isOKPacket(pkt.Payload) {
		return false, nil
	}

	// Result set — drain it fully.
	// Column defs until EOF.
	for {
		pkt, err = readMySQLPacket(backend)
		if err != nil {
			return hadError, fmt.Errorf("draining column defs: %w", err)
		}
		if isEOFPacket(pkt.Payload) {
			break
		}
	}
	// Rows until EOF.
	for {
		pkt, err = readMySQLPacket(backend)
		if err != nil {
			return hadError, fmt.Errorf("draining rows: %w", err)
		}
		if isEOFPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
			if isERRPacket(pkt.Payload) {
				hadError = true
			}
			return hadError, nil
		}
	}
}

// forwardAndRelayTxn sends a packet to the backend, relays the complete response
// to the client, and reports whether the backend returned an error.
func (th *TxnHandler) forwardAndRelayTxn(rawPkt []byte, backend, client net.Conn) (hadError bool, err error) {
	if _, err := backend.Write(rawPkt); err != nil {
		return false, fmt.Errorf("sending to backend: %w", err)
	}

	// Read first response packet.
	pkt, err := readMySQLPacket(backend)
	if err != nil {
		return false, fmt.Errorf("reading backend response: %w", err)
	}

	if isERRPacket(pkt.Payload) {
		hadError = true
	}

	if _, err := client.Write(pkt.Raw); err != nil {
		return hadError, fmt.Errorf("relaying to client: %w", err)
	}

	// OK or ERR: response is complete.
	if isOKPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
		return hadError, nil
	}

	// Result set: relay column defs + EOF + rows + EOF.
	for {
		pkt, err = readMySQLPacket(backend)
		if err != nil {
			return hadError, fmt.Errorf("relaying column def: %w", err)
		}
		if _, err := client.Write(pkt.Raw); err != nil {
			return hadError, fmt.Errorf("relaying to client: %w", err)
		}
		if isEOFPacket(pkt.Payload) {
			break
		}
	}
	for {
		pkt, err = readMySQLPacket(backend)
		if err != nil {
			return hadError, fmt.Errorf("relaying row: %w", err)
		}
		if isERRPacket(pkt.Payload) {
			hadError = true
		}
		if _, err := client.Write(pkt.Raw); err != nil {
			return hadError, fmt.Errorf("relaying to client: %w", err)
		}
		if isEOFPacket(pkt.Payload) || isERRPacket(pkt.Payload) {
			return hadError, nil
		}
	}
}
