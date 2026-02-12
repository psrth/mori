package proxy

import (
	"fmt"
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/logging"
)

// TxnHandler manages transaction state for a single connection.
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
	default:
		// SAVEPOINT, RELEASE, etc. — forward to both backends without state change.
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

	// Discard staged entries.
	th.deltaMap.Rollback()
	th.tombstones.Rollback()

	if th.verbose {
		log.Printf("[conn %d] ROLLBACK: staged deltas discarded", th.connID)
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
