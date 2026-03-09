package proxy

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/redis/classify"
)

// redisTxnState tracks transaction state for MULTI/EXEC/DISCARD.
type redisTxnState struct {
	inMulti         bool
	cmdQueue        []*RESPValue           // buffered raw RESP commands
	cmdInlines      []string               // inline forms for classification
	classifications []*core.Classification // per-command classifications
	writeKeys       map[string]bool        // keys that need hydration before EXEC
}

// handleMulti begins a transaction: sends MULTI to shadow only, returns +OK.
func (p *Proxy) handleMulti(
	txn *redisTxnState,
	clientConn net.Conn,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	// Send MULTI to shadow only (prod gets nothing).
	multiCmd := BuildCommandArray("MULTI")
	shadowConn.Write(multiCmd.Bytes())
	_, err := ReadRESPValue(shadowReader)
	if err != nil {
		log.Printf("[conn %d] shadow MULTI error: %v", connID, err)
		clientConn.Write(BuildErrorReply("ERR shadow MULTI failed").Bytes())
		return
	}

	txn.inMulti = true
	txn.cmdQueue = nil
	txn.cmdInlines = nil
	txn.classifications = nil
	txn.writeKeys = make(map[string]bool)

	if p.verbose {
		log.Printf("[conn %d] MULTI — transaction started (shadow-only)", connID)
	}

	// Return +OK to client (matching Redis MULTI behavior).
	clientConn.Write(BuildSimpleString("OK").Bytes())
}

// handleQueuedCommand buffers a command during a MULTI transaction.
func (p *Proxy) handleQueuedCommand(
	txn *redisTxnState,
	clientConn net.Conn,
	cmdVal *RESPValue,
	inline string,
	connID int64,
) {
	// Classify the command to extract write keys for pre-EXEC hydration.
	cl, _ := p.classifier.Classify(inline)

	// Buffer the command.
	txn.cmdQueue = append(txn.cmdQueue, cmdVal)
	txn.cmdInlines = append(txn.cmdInlines, inline)
	txn.classifications = append(txn.classifications, cl)

	// Extract write keys for hydration.
	if cl != nil && (cl.OpType == core.OpWrite || cl.OpType == core.OpDDL) {
		args := strings.Fields(inline)
		for _, key := range extractWriteKeys(args) {
			txn.writeKeys[key] = true
		}
		// Also extract hydration keys (source keys for multi-key operations).
		for _, key := range extractHydrationKeys(args) {
			txn.writeKeys[key] = true
		}
	}

	if p.verbose {
		log.Printf("[conn %d] QUEUED: %s", connID, truncateCmd(inline, 100))
	}

	// Return +QUEUED to client (matching Redis behavior).
	clientConn.Write(BuildSimpleString("QUEUED").Bytes())
}

// handleExec executes a buffered transaction: hydrate, forward to shadow, commit deltas.
func (p *Proxy) handleExec(
	txn *redisTxnState,
	clientConn net.Conn,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	// 1. Hydrate all write keys from prod to shadow.
	for key := range txn.writeKeys {
		prefix := classify.KeyPrefix(key)
		if p.deltaMap != nil && p.deltaMap.IsDelta(prefix, key) {
			continue
		}
		p.hydrateKey(key, prodConn, prodReader, shadowConn, shadowReader, connID)
	}

	// 2. Forward each buffered command to shadow (within the MULTI context).
	for _, cmdVal := range txn.cmdQueue {
		if _, err := shadowConn.Write(cmdVal.Bytes()); err != nil {
			log.Printf("[conn %d] shadow write error in txn: %v", connID, err)
			clientConn.Write(BuildErrorReply("ERR shadow transaction failed").Bytes())
			return
		}
		// Shadow returns +QUEUED for each command within MULTI.
		_, err := ReadRESPValue(shadowReader)
		if err != nil {
			log.Printf("[conn %d] shadow read error in txn: %v", connID, err)
			clientConn.Write(BuildErrorReply("ERR shadow transaction failed").Bytes())
			return
		}
	}

	// 3. Forward EXEC to shadow.
	execCmd := BuildCommandArray("EXEC")
	if _, err := shadowConn.Write(execCmd.Bytes()); err != nil {
		log.Printf("[conn %d] shadow EXEC write error: %v", connID, err)
		clientConn.Write(BuildErrorReply("ERR shadow EXEC failed").Bytes())
		return
	}

	execResp, err := ReadRESPValue(shadowReader)
	if err != nil {
		log.Printf("[conn %d] shadow EXEC read error: %v", connID, err)
		clientConn.Write(BuildErrorReply("ERR shadow EXEC failed").Bytes())
		return
	}

	// 4. If EXEC returned null array, the transaction was aborted (WATCH invalidation).
	//    Roll back staged deltas and return the null response without committing.
	if execResp.IsNull {
		p.deltaMap.Rollback()
		p.tombstones.Rollback()
		if p.verbose {
			log.Printf("[conn %d] EXEC — transaction aborted (WATCH invalidation)", connID)
		}
		clientConn.Write(execResp.Bytes())
		return
	}

	// 5. Track write effects using staged deltas for each command.
	for i, cl := range txn.classifications {
		if cl == nil || cl.OpType != core.OpWrite {
			continue
		}
		args := strings.Fields(txn.cmdInlines[i])
		keys := extractWriteKeys(args)

		switch cl.SubType {
		case core.SubInsert:
			for _, table := range cl.Tables {
				p.deltaMap.MarkInserted(table)
			}
			for _, key := range keys {
				prefix := classify.KeyPrefix(key)
				p.deltaMap.Stage(prefix, key)
			}
		case core.SubUpdate:
			for _, key := range keys {
				prefix := classify.KeyPrefix(key)
				p.deltaMap.Stage(prefix, key)
			}
		case core.SubDelete:
			for _, key := range keys {
				prefix := classify.KeyPrefix(key)
				p.tombstones.Stage(prefix, key)
			}
		case core.SubTruncate:
			for _, key := range keys {
				prefix := classify.KeyPrefix(key)
				p.deltaMap.Stage(prefix, key)
			}
		}
	}

	// 6. Commit staged deltas.
	p.deltaMap.Commit()
	p.tombstones.Commit()

	// 7. Persist state.
	if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
		if p.verbose {
			log.Printf("[conn %d] failed to persist delta map after EXEC: %v", connID, err)
		}
	}
	if err := delta.WriteTombstoneSet(p.moriDir, p.tombstones); err != nil {
		if p.verbose {
			log.Printf("[conn %d] failed to persist tombstone set after EXEC: %v", connID, err)
		}
	}

	if p.verbose {
		log.Printf("[conn %d] EXEC — transaction committed (%d commands)", connID, len(txn.cmdQueue))
	}

	// 8. Return EXEC response to client.
	clientConn.Write(execResp.Bytes())
}

// handleDiscard aborts a transaction: DISCARD on shadow, rollback staged deltas.
func (p *Proxy) handleDiscard(
	txn *redisTxnState,
	clientConn net.Conn,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	// Forward DISCARD to shadow.
	discardCmd := BuildCommandArray("DISCARD")
	shadowConn.Write(discardCmd.Bytes())
	ReadRESPValue(shadowReader) // consume response

	// Rollback any staged deltas.
	p.deltaMap.Rollback()
	p.tombstones.Rollback()

	if p.verbose {
		log.Printf("[conn %d] DISCARD — transaction rolled back", connID)
	}

	// Return +OK to client.
	clientConn.Write(BuildSimpleString("OK").Bytes())
}

// hydrateKey copies a single key from prod to shadow using DUMP/RESTORE.
func (p *Proxy) hydrateKey(
	key string,
	prodConn net.Conn, prodReader *bufio.Reader,
	shadowConn net.Conn, shadowReader *bufio.Reader,
	connID int64,
) {
	// DUMP from prod.
	dumpCmd := BuildCommandArray("DUMP", key)
	prodConn.Write(dumpCmd.Bytes())
	dumpResp, err := ReadRESPValue(prodReader)
	if err != nil || dumpResp.IsNull {
		return // Key doesn't exist in prod — skip.
	}

	// Get TTL.
	pttlCmd := BuildCommandArray("PTTL", key)
	prodConn.Write(pttlCmd.Bytes())
	pttlResp, err := ReadRESPValue(prodReader)
	if err != nil {
		return
	}
	ttl := "0"
	if pttlResp.Type == ':' && pttlResp.Int > 0 {
		ttl = fmt.Sprintf("%d", pttlResp.Int)
	}

	// RESTORE to shadow (REPLACE if exists).
	restoreCmd := BuildCommandArray("RESTORE", key, ttl, dumpResp.Str, "REPLACE")
	shadowConn.Write(restoreCmd.Bytes())
	ReadRESPValue(shadowReader)

	if p.verbose {
		log.Printf("[conn %d] hydrated key %q to shadow", connID, key)
	}
}
