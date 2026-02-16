package proxy

import (
	"fmt"
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/logging"
)

// TxnHandler manages transaction state for a single MySQL connection.
// It coordinates BEGIN/COMMIT/ROLLBACK across both Prod and Shadow backends
// and controls delta/tombstone staging.
type TxnHandler struct {
	prodConn   net.Conn
	shadowConn net.Conn
	deltaMap   *delta.Map
	tombstones *delta.TombstoneSet
	moriDir    string
	connID     int64
	verbose    bool
	logger     *logging.Logger
	inTxn      bool
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
		return th.handleRollback(clientConn, rawPkt)
	default:
		// SAVEPOINT, RELEASE, etc. — forward to both backends without state change.
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

	// Discard staged entries.
	th.deltaMap.Rollback()
	th.tombstones.Rollback()

	if th.verbose {
		log.Printf("[conn %d] ROLLBACK: staged deltas discarded", th.connID)
	}
	th.logger.Event(th.connID, "txn", "ROLLBACK")

	return nil
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
