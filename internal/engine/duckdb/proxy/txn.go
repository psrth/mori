package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
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

// TxnHandler manages transaction state for a single DuckDB proxy connection.
// It coordinates BEGIN/COMMIT/ROLLBACK across both Prod and Shadow databases
// and controls delta/tombstone staging.
type TxnHandler struct {
	proxy           *Proxy
	connID          int64
	inTxn           bool
	txnState        byte // 'I' (idle), 'T' (in-transaction), 'E' (error-in-transaction)
	savepointStack  []savepointSnapshot
	beginSchemaSnap map[string]*coreSchema.TableDiff // schema state at BEGIN
}

// InTxn reports whether this connection is inside an explicit transaction.
func (th *TxnHandler) InTxn() bool {
	return th.inTxn
}

// TxnState returns the current transaction state byte for ReadyForQuery messages:
// 'I' (idle), 'T' (in-transaction), 'E' (error-in-transaction).
func (th *TxnHandler) TxnState() byte {
	if th == nil || th.txnState == 0 {
		return 'I'
	}
	return th.txnState
}

// HandleTxn dispatches a transaction control statement based on its SubType.
func (th *TxnHandler) HandleTxn(clientConn net.Conn, cl *core.Classification) {
	switch cl.SubType {
	case core.SubBegin:
		th.handleBegin(clientConn)
	case core.SubCommit:
		th.handleCommit(clientConn)
	case core.SubRollback:
		// Distinguish full ROLLBACK from ROLLBACK TO SAVEPOINT.
		if isRollbackToSavepoint(cl.RawSQL) {
			th.handleRollbackTo(clientConn, cl.RawSQL)
		} else {
			th.handleRollback(clientConn)
		}
	case core.SubSavepoint:
		th.handleSavepoint(clientConn, cl.RawSQL)
	case core.SubRelease:
		th.handleRelease(clientConn, cl.RawSQL)
	default:
		// Unknown transaction-adjacent command — execute on both.
		th.handleOther(clientConn, cl.RawSQL)
	}
}

func (th *TxnHandler) handleBegin(clientConn net.Conn) {
	p := th.proxy

	// Execute BEGIN on shadow.
	_, shadowErr := p.shadowDB.Exec("BEGIN")

	// Execute BEGIN on prod (read-only, but needed for consistent reads).
	// Use REPEATABLE READ for consistent snapshot reads within the transaction.
	_, prodErr := p.prodDB.Exec("BEGIN")

	if shadowErr == nil && prodErr == nil {
		th.inTxn = true
		th.txnState = 'T'

		// Snapshot schema registry state so we can restore on ROLLBACK.
		if p.schemaRegistry != nil {
			th.beginSchemaSnap = p.schemaRegistry.SnapshotAll()
		}

		if p.verbose {
			log.Printf("[conn %d] BEGIN: Shadow=ok Prod=ok", th.connID)
		}
		p.logger.Event(th.connID, "txn", "BEGIN")
	} else {
		th.txnState = 'E'
		if p.verbose {
			log.Printf("[conn %d] BEGIN failed: shadow=%v prod=%v", th.connID, shadowErr, prodErr)
		}
	}

	clientConn.Write(buildCommandCompleteMsg("BEGIN"))
	clientConn.Write(buildReadyForQueryMsgState(th.TxnState()))
}

func (th *TxnHandler) handleCommit(clientConn net.Conn) {
	p := th.proxy

	// Execute COMMIT on shadow.
	shadowErr := p.shadowDB.QueryRow("SELECT 1").Err()
	if shadowErr == nil {
		_, shadowErr = p.shadowDB.Exec("COMMIT")
	}

	// Execute COMMIT on prod.
	p.prodDB.Exec("COMMIT")

	th.inTxn = false
	th.txnState = 'I'
	th.savepointStack = nil
	th.beginSchemaSnap = nil

	if shadowErr == nil {
		// Promote staged deltas.
		p.deltaMap.Commit()
		p.deltaMap.CommitInsertCounts()
		p.tombstones.Commit()

		if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist delta map after COMMIT: %v", th.connID, err)
			}
		}
		if err := delta.WriteTombstoneSet(p.moriDir, p.tombstones); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist tombstones after COMMIT: %v", th.connID, err)
			}
		}

		if p.verbose {
			log.Printf("[conn %d] COMMIT: staged deltas promoted", th.connID)
		}
		p.logger.Event(th.connID, "txn", "COMMIT: staged deltas promoted")
	} else {
		// Discard staged entries.
		p.deltaMap.Rollback()
		p.deltaMap.RollbackInsertCounts()
		p.tombstones.Rollback()
		if p.verbose {
			log.Printf("[conn %d] COMMIT failed: staged deltas discarded", th.connID)
		}
		p.logger.Event(th.connID, "txn", "COMMIT failed: staged deltas discarded")
	}

	clientConn.Write(buildCommandCompleteMsg("COMMIT"))
	clientConn.Write(buildReadyForQueryMsgState(th.TxnState()))
}

func (th *TxnHandler) handleRollback(clientConn net.Conn) {
	p := th.proxy

	// Execute ROLLBACK on shadow.
	p.shadowDB.Exec("ROLLBACK")

	// Execute ROLLBACK on prod.
	p.prodDB.Exec("ROLLBACK")

	th.inTxn = false
	th.txnState = 'I'
	th.savepointStack = nil

	// Discard staged entries.
	p.deltaMap.Rollback()
	p.deltaMap.RollbackInsertCounts()
	p.tombstones.Rollback()

	// Restore schema registry to pre-BEGIN state so rolled-back DDL
	// doesn't leave stale schema diffs.
	if p.schemaRegistry != nil && th.beginSchemaSnap != nil {
		p.schemaRegistry.RestoreAll(th.beginSchemaSnap)
		th.beginSchemaSnap = nil
		if err := coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist schema registry after ROLLBACK: %v", th.connID, err)
			}
		}
	}

	if p.verbose {
		log.Printf("[conn %d] ROLLBACK: staged deltas and schema changes discarded", th.connID)
	}
	p.logger.Event(th.connID, "txn", "ROLLBACK")

	clientConn.Write(buildCommandCompleteMsg("ROLLBACK"))
	clientConn.Write(buildReadyForQueryMsgState(th.TxnState()))
}

// handleSavepoint pushes a snapshot of the current delta/tombstone/insert state
// onto the savepoint stack, then executes SAVEPOINT on both backends.
func (th *TxnHandler) handleSavepoint(clientConn net.Conn, rawSQL string) {
	p := th.proxy
	name := parseSavepointName(rawSQL)

	snap := savepointSnapshot{
		name:       name,
		deltaSnap:  p.deltaMap.SnapshotAll(),
		tombSnap:   p.tombstones.SnapshotAll(),
		insertSnap: p.deltaMap.SnapshotInsertedTables(),
	}
	if p.schemaRegistry != nil {
		snap.schemaSnap = p.schemaRegistry.SnapshotAll()
	}
	th.savepointStack = append(th.savepointStack, snap)

	// Execute SAVEPOINT on both databases.
	p.shadowDB.Exec(rawSQL)
	p.prodDB.Exec(rawSQL)

	if p.verbose {
		log.Printf("[conn %d] SAVEPOINT %s: snapshot pushed (stack depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	p.logger.Event(th.connID, "txn", fmt.Sprintf("SAVEPOINT %s", name))

	clientConn.Write(buildCommandCompleteMsg("SAVEPOINT"))
	clientConn.Write(buildReadyForQueryMsgState(th.TxnState()))
}

// handleRelease pops the matching savepoint snapshot from the stack (discards it),
// then executes RELEASE on both backends.
func (th *TxnHandler) handleRelease(clientConn net.Conn, rawSQL string) {
	p := th.proxy
	name := parseReleaseName(rawSQL)

	// Pop the matching snapshot and everything above it.
	if idx := th.findSavepoint(name); idx >= 0 {
		th.savepointStack = th.savepointStack[:idx]
	}

	// Execute RELEASE on both databases.
	p.shadowDB.Exec(rawSQL)
	p.prodDB.Exec(rawSQL)

	if p.verbose {
		log.Printf("[conn %d] RELEASE SAVEPOINT %s: snapshot popped (stack depth=%d)",
			th.connID, name, len(th.savepointStack))
	}
	p.logger.Event(th.connID, "txn", fmt.Sprintf("RELEASE SAVEPOINT %s", name))

	clientConn.Write(buildCommandCompleteMsg("RELEASE"))
	clientConn.Write(buildReadyForQueryMsgState(th.TxnState()))
}

// handleRollbackTo restores delta/tombstone/insert state from the matching savepoint
// snapshot, then executes ROLLBACK TO on both backends.
// The snapshot is kept on the stack (PostgreSQL allows repeated ROLLBACK TO).
func (th *TxnHandler) handleRollbackTo(clientConn net.Conn, rawSQL string) {
	p := th.proxy
	name := parseRollbackToName(rawSQL)
	idx := th.findSavepoint(name)

	if idx >= 0 {
		snap := th.savepointStack[idx]
		// Restore state to the savepoint.
		p.deltaMap.RestoreAll(snap.deltaSnap)
		p.tombstones.RestoreAll(snap.tombSnap)
		p.deltaMap.RestoreInsertedTables(snap.insertSnap)
		// Restore schema registry to savepoint state.
		if p.schemaRegistry != nil && snap.schemaSnap != nil {
			p.schemaRegistry.RestoreAll(snap.schemaSnap)
			if err := coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry); err != nil {
				if p.verbose {
					log.Printf("[conn %d] failed to persist schema registry after ROLLBACK TO: %v", th.connID, err)
				}
			}
		}
		// Trim the stack: keep up to and including the matched savepoint,
		// discard anything pushed after it.
		th.savepointStack = th.savepointStack[:idx+1]

		// ROLLBACK TO clears the error state back to 'T'.
		th.txnState = 'T'

		if p.verbose {
			log.Printf("[conn %d] ROLLBACK TO SAVEPOINT %s: state restored (stack depth=%d)",
				th.connID, name, len(th.savepointStack))
		}
		p.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TO SAVEPOINT %s: restored", name))
	} else {
		if p.verbose {
			log.Printf("[conn %d] ROLLBACK TO SAVEPOINT %s: not found in stack, forwarding only",
				th.connID, name)
		}
		p.logger.Event(th.connID, "txn", fmt.Sprintf("ROLLBACK TO SAVEPOINT %s: not in stack", name))
	}

	// Execute on both databases.
	p.shadowDB.Exec(rawSQL)
	p.prodDB.Exec(rawSQL)

	clientConn.Write(buildCommandCompleteMsg("ROLLBACK"))
	clientConn.Write(buildReadyForQueryMsgState(th.TxnState()))
}

func (th *TxnHandler) handleOther(clientConn net.Conn, sqlStr string) {
	p := th.proxy

	// Execute on both databases.
	p.shadowDB.Exec(sqlStr)
	resp := p.executeQuery(p.prodDB, sqlStr, th.connID)
	clientConn.Write(resp)
}

// MarkError marks the transaction state as error-in-transaction ('E').
// Called when a write/DDL statement fails inside a transaction.
func (th *TxnHandler) MarkError() {
	if th != nil && th.inTxn {
		th.txnState = 'E'
	}
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
	// "RELEASE SAVEPOINT sp1" -> fields[2]
	// "RELEASE sp1" -> fields[1]
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
	// "ROLLBACK TO SAVEPOINT sp1" -> fields[3]
	// "ROLLBACK TO sp1" -> fields[2]
	if len(fields) >= 4 && strings.EqualFold(fields[2], "SAVEPOINT") {
		return fields[3]
	}
	if len(fields) >= 3 {
		return fields[2]
	}
	return ""
}
