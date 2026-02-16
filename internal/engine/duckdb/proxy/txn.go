package proxy

import (
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// TxnHandler manages transaction state for a single DuckDB proxy connection.
// It coordinates BEGIN/COMMIT/ROLLBACK across both Prod and Shadow databases
// and controls delta/tombstone staging.
type TxnHandler struct {
	proxy  *Proxy
	connID int64
	inTxn  bool
}

// InTxn reports whether this connection is inside an explicit transaction.
func (th *TxnHandler) InTxn() bool {
	return th.inTxn
}

// HandleTxn dispatches a transaction control statement based on its SubType.
func (th *TxnHandler) HandleTxn(clientConn net.Conn, cl *core.Classification) {
	switch cl.SubType {
	case core.SubBegin:
		th.handleBegin(clientConn)
	case core.SubCommit:
		th.handleCommit(clientConn)
	case core.SubRollback:
		th.handleRollback(clientConn)
	default:
		// SAVEPOINT, RELEASE, etc. — execute on both.
		th.handleOther(clientConn, cl.RawSQL)
	}
}

func (th *TxnHandler) handleBegin(clientConn net.Conn) {
	p := th.proxy

	// Execute BEGIN on shadow.
	p.shadowDB.Exec("BEGIN")

	// Execute BEGIN on prod (read-only, but needed for consistent reads).
	p.prodDB.Exec("BEGIN")

	th.inTxn = true

	if p.verbose {
		log.Printf("[conn %d] BEGIN: Shadow=ok Prod=ok", th.connID)
	}
	p.logger.Event(th.connID, "txn", "BEGIN")

	clientConn.Write(buildCommandCompleteMsg("BEGIN"))
	clientConn.Write(buildReadyForQueryMsg())
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

	if shadowErr == nil {
		// Promote staged deltas.
		p.deltaMap.Commit()
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
		p.tombstones.Rollback()
		if p.verbose {
			log.Printf("[conn %d] COMMIT failed: staged deltas discarded", th.connID)
		}
		p.logger.Event(th.connID, "txn", "COMMIT failed: staged deltas discarded")
	}

	clientConn.Write(buildCommandCompleteMsg("COMMIT"))
	clientConn.Write(buildReadyForQueryMsg())
}

func (th *TxnHandler) handleRollback(clientConn net.Conn) {
	p := th.proxy

	// Execute ROLLBACK on shadow.
	p.shadowDB.Exec("ROLLBACK")

	// Execute ROLLBACK on prod.
	p.prodDB.Exec("ROLLBACK")

	th.inTxn = false

	// Discard staged entries.
	p.deltaMap.Rollback()
	p.tombstones.Rollback()

	if p.verbose {
		log.Printf("[conn %d] ROLLBACK: staged deltas discarded", th.connID)
	}
	p.logger.Event(th.connID, "txn", "ROLLBACK")

	clientConn.Write(buildCommandCompleteMsg("ROLLBACK"))
	clientConn.Write(buildReadyForQueryMsg())
}

func (th *TxnHandler) handleOther(clientConn net.Conn, sqlStr string) {
	p := th.proxy

	// Execute on both databases.
	p.shadowDB.Exec(sqlStr)
	resp := p.executeQuery(p.prodDB, sqlStr, th.connID)
	clientConn.Write(resp)
}
