package proxy

import (
	"fmt"
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// ExtHandler processes extended query protocol batches for a single connection.
// It accumulates messages until Sync, then classifies, routes, and dispatches.
type ExtHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	classifier     core.Classifier
	router         *core.Router
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger

	txnHandler   *TxnHandler
	writeHandler *WriteHandler
	readHandler  *ReadHandler

	// stmtCache maps statement names to SQL text, persisting across batches.
	// Populated on Parse, evicted on Close('S').
	stmtCache map[string]string

	// Batch accumulator, cleared after each Sync.
	batch    []*pgMsg
	batchRaw []byte

	// Extracted info from the current batch.
	batchSQL      string   // SQL from Parse or stmtCache
	batchParams   [][]byte // parameter values from Bind
	batchHasParse     bool
	batchHasBind      bool
	batchHasDesc      bool
	batchHasExec      bool
	batchBinaryParams bool // true if Bind uses binary-format parameters
}

// Accumulate adds an extended protocol message to the current batch.
// It extracts SQL from Parse and params from Bind for later classification.
func (eh *ExtHandler) Accumulate(msg *pgMsg) {
	eh.batch = append(eh.batch, msg)
	eh.batchRaw = append(eh.batchRaw, msg.Raw...)

	switch msg.Type {
	case 'P':
		eh.batchHasParse = true
		stmtName, sql, _, err := parseParseMsgPayload(msg.Payload)
		if err != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: failed to parse Parse payload: %v", eh.connID, err)
			}
			return
		}
		eh.batchSQL = sql
		eh.stmtCache[stmtName] = sql

	case 'B':
		eh.batchHasBind = true
		eh.batchBinaryParams = hasBinaryParams(msg.Payload)
		_, stmtName, params, err := parseBindMsgPayload(msg.Payload)
		if err != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: failed to parse Bind payload: %v", eh.connID, err)
			}
			return
		}
		eh.batchParams = params
		// If no Parse in this batch, look up SQL from cache.
		if eh.batchSQL == "" {
			if cached, ok := eh.stmtCache[stmtName]; ok {
				eh.batchSQL = cached
			}
		}

	case 'D':
		eh.batchHasDesc = true

	case 'E':
		eh.batchHasExec = true

	case 'C':
		closeType, name, err := parseCloseMsgPayload(msg.Payload)
		if err == nil && closeType == 'S' {
			delete(eh.stmtCache, name)
		}
	}
}

// FlushBatch processes the accumulated batch: classifies, routes, and dispatches.
// Called when Sync ('S') is received.
func (eh *ExtHandler) FlushBatch(clientConn net.Conn) error {
	defer eh.clearBatch()

	batchRaw := eh.batchRaw

	// If no SQL found or no Execute, forward to Prod as a safe default.
	// Covers Describe-only batches, Close-only batches, etc.
	// When the batch includes a Parse, also forward to Shadow so the prepared
	// statement exists on both backends for future Bind+Execute batches.
	if eh.batchSQL == "" || !eh.batchHasExec {
		if eh.batchHasParse && eh.shadowConn != nil {
			eh.shadowConn.Write(batchRaw) //nolint: errcheck
			if err := drainUntilReady(eh.shadowConn); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: shadow drain error (parse-only): %v", eh.connID, err)
				}
			}
		}
		return forwardAndRelay(batchRaw, eh.prodConn, clientConn)
	}

	// Classify with parameters if available.
	var cl *core.Classification
	var err error
	if len(eh.batchParams) > 0 {
		cl, err = eh.classifier.ClassifyWithParams(eh.batchSQL, resolveParams(eh.batchParams))
	} else {
		cl, err = eh.classifier.Classify(eh.batchSQL)
	}
	if err != nil {
		if eh.verbose {
			log.Printf("[conn %d] ext: classify error, forwarding to prod: %v", eh.connID, err)
		}
		return forwardAndRelay(batchRaw, eh.prodConn, clientConn)
	}

	strategy := eh.router.Route(cl)

	if eh.verbose {
		log.Printf("[conn %d] ext: %s/%s tables=%v → %s | %s",
			eh.connID, cl.OpType, cl.SubType,
			cl.Tables, strategy, truncateSQL(eh.batchSQL, 80))
	}

	eh.logger.Query(eh.connID, eh.batchSQL, cl, strategy, 0)

	switch strategy {
	case core.StrategyProdDirect:
		return forwardAndRelay(batchRaw, eh.prodConn, clientConn)

	case core.StrategyShadowWrite:
		return eh.handleExtInsert(clientConn, batchRaw, cl)

	case core.StrategyHydrateAndWrite:
		return eh.handleExtUpdate(clientConn, batchRaw, cl)

	case core.StrategyShadowDelete:
		return eh.handleExtDelete(clientConn, batchRaw, cl)

	case core.StrategyMergedRead:
		return eh.handleExtMergedRead(clientConn, cl)

	case core.StrategyJoinPatch:
		return eh.handleExtJoinPatch(clientConn, cl)

	case core.StrategyShadowDDL:
		return eh.handleExtDDL(clientConn, batchRaw, cl)

	case core.StrategyTransaction:
		return eh.handleExtTxn(clientConn, batchRaw, cl)

	default:
		return forwardAndRelay(batchRaw, eh.prodConn, clientConn)
	}
}

// clearBatch resets the batch accumulator for the next Sync cycle.
func (eh *ExtHandler) clearBatch() {
	eh.batch = nil
	eh.batchRaw = nil
	eh.batchSQL = ""
	eh.batchParams = nil
	eh.batchHasParse = false
	eh.batchHasBind = false
	eh.batchHasDesc = false
	eh.batchHasExec = false
	eh.batchBinaryParams = false
}

// inTxn reports whether this connection is inside an explicit transaction.
func (eh *ExtHandler) inTxn() bool {
	return eh.txnHandler != nil && eh.txnHandler.InTxn()
}

// --- Strategy handlers ---

// handleExtInsert forwards batch to Shadow and tracks the insert in deltaMap.
func (eh *ExtHandler) handleExtInsert(clientConn net.Conn, batchRaw []byte, cl *core.Classification) error {
	if err := forwardAndRelay(batchRaw, eh.shadowConn, clientConn); err != nil {
		return err
	}
	if eh.deltaMap != nil {
		for _, table := range cl.Tables {
			eh.deltaMap.MarkInserted(table)
		}
	}
	return nil
}

// handleExtUpdate hydrates missing rows then forwards batch to Shadow.
func (eh *ExtHandler) handleExtUpdate(clientConn net.Conn, batchRaw []byte, cl *core.Classification) error {
	// Hydrate missing rows before forwarding the batch (uses simple queries).
	if eh.writeHandler != nil && len(cl.PKs) > 0 {
		for _, pk := range cl.PKs {
			if eh.deltaMap != nil && eh.deltaMap.IsDelta(pk.Table, pk.PK) {
				continue
			}
			if err := eh.writeHandler.hydrateRow(pk.Table, pk.PK); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: hydration failed for (%s, %s): %v",
						eh.connID, pk.Table, pk.PK, err)
				}
			}
		}
	}

	// Forward batch to Shadow, relay response.
	if err := forwardAndRelay(batchRaw, eh.shadowConn, clientConn); err != nil {
		return err
	}

	// Track deltas.
	if eh.deltaMap != nil {
		for _, pk := range cl.PKs {
			if eh.inTxn() {
				eh.deltaMap.Stage(pk.Table, pk.PK)
			} else {
				eh.deltaMap.Add(pk.Table, pk.PK)
			}
		}
		if !eh.inTxn() {
			if err := delta.WriteDeltaMap(eh.moriDir, eh.deltaMap); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: failed to persist delta map: %v", eh.connID, err)
				}
			}
		}
	}
	return nil
}

// handleExtDelete forwards batch to Shadow and tracks tombstones.
func (eh *ExtHandler) handleExtDelete(clientConn net.Conn, batchRaw []byte, cl *core.Classification) error {
	if err := forwardAndRelay(batchRaw, eh.shadowConn, clientConn); err != nil {
		return err
	}

	if len(cl.PKs) == 0 || eh.tombstones == nil {
		return nil
	}

	for _, pk := range cl.PKs {
		if eh.inTxn() {
			eh.tombstones.Stage(pk.Table, pk.PK)
		} else {
			eh.tombstones.Add(pk.Table, pk.PK)
			if eh.deltaMap != nil {
				eh.deltaMap.Remove(pk.Table, pk.PK)
			}
		}
	}

	if !eh.inTxn() {
		if err := delta.WriteTombstoneSet(eh.moriDir, eh.tombstones); err != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: failed to persist tombstones: %v", eh.connID, err)
			}
		}
		if eh.deltaMap != nil {
			if err := delta.WriteDeltaMap(eh.moriDir, eh.deltaMap); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: failed to persist delta map: %v", eh.connID, err)
				}
			}
		}
	}
	return nil
}

// handleExtMergedRead constructs full SQL, runs merged read, and synthesizes
// an extended protocol response.
func (eh *ExtHandler) handleExtMergedRead(clientConn net.Conn, cl *core.Classification) error {
	if eh.readHandler == nil || eh.batchBinaryParams {
		// Binary-format parameters can't be safely substituted into SQL text.
		// Fall back to forwarding the batch to Prod as-is.
		return forwardAndRelay(eh.batchRaw, eh.prodConn, clientConn)
	}

	// Construct full SQL with parameters substituted.
	fullSQL := eh.batchSQL
	if len(eh.batchParams) > 0 {
		fullSQL = reconstructSQL(eh.batchSQL, eh.batchParams)
	}

	// Create a classification copy with the full SQL.
	clCopy := *cl
	clCopy.RawSQL = fullSQL

	columns, values, nulls, err := eh.readHandler.mergedReadCore(&clCopy, fullSQL)
	if err != nil {
		if re, ok := err.(*relayError); ok {
			// Build error response with extended protocol framing.
			var resp []byte
			if eh.batchHasParse {
				resp = append(resp, buildParseCompleteMsg()...)
			}
			if eh.batchHasBind {
				resp = append(resp, buildBindCompleteMsg()...)
			}
			resp = append(resp, re.rawMsgs...)
			_, writeErr := clientConn.Write(resp)
			return writeErr
		}
		return err
	}

	resp := buildExtSelectResponse(eh.batchHasParse, eh.batchHasBind, columns, values, nulls)
	_, err = clientConn.Write(resp)
	return err
}

// handleExtJoinPatch constructs full SQL, runs join patch, and synthesizes
// an extended protocol response.
func (eh *ExtHandler) handleExtJoinPatch(clientConn net.Conn, cl *core.Classification) error {
	if eh.readHandler == nil || eh.batchBinaryParams {
		return forwardAndRelay(eh.batchRaw, eh.prodConn, clientConn)
	}

	fullSQL := eh.batchSQL
	if len(eh.batchParams) > 0 {
		fullSQL = reconstructSQL(eh.batchSQL, eh.batchParams)
	}

	clCopy := *cl
	clCopy.RawSQL = fullSQL

	columns, values, nulls, err := eh.readHandler.joinPatchCore(&clCopy, fullSQL)
	if err != nil {
		if re, ok := err.(*relayError); ok {
			var resp []byte
			if eh.batchHasParse {
				resp = append(resp, buildParseCompleteMsg()...)
			}
			if eh.batchHasBind {
				resp = append(resp, buildBindCompleteMsg()...)
			}
			resp = append(resp, re.rawMsgs...)
			_, writeErr := clientConn.Write(resp)
			return writeErr
		}
		return err
	}

	resp := buildExtSelectResponse(eh.batchHasParse, eh.batchHasBind, columns, values, nulls)
	_, err = clientConn.Write(resp)
	return err
}

// handleExtDDL forwards batch to Shadow and updates schema registry.
func (eh *ExtHandler) handleExtDDL(clientConn net.Conn, batchRaw []byte, cl *core.Classification) error {
	hadError, err := forwardAndRelayDDL(batchRaw, eh.shadowConn, clientConn)
	if err != nil {
		return fmt.Errorf("ext DDL forward: %w", err)
	}

	if hadError || eh.schemaRegistry == nil {
		return nil
	}

	// Parse and apply DDL changes (same as DDLHandler).
	changes, err := parseDDLChanges(cl.RawSQL)
	if err != nil {
		if eh.verbose {
			log.Printf("[conn %d] ext: DDL parse warning: %v", eh.connID, err)
		}
		return nil
	}

	for _, ch := range changes {
		eh.applyDDLChange(ch)
	}

	if err := coreSchema.WriteRegistry(eh.moriDir, eh.schemaRegistry); err != nil {
		if eh.verbose {
			log.Printf("[conn %d] ext: failed to persist schema registry: %v", eh.connID, err)
		}
	}
	return nil
}

// applyDDLChange records a single schema change in the registry.
// Mirrors DDLHandler.applyChange.
func (eh *ExtHandler) applyDDLChange(ch ddlChange) {
	switch ch.Kind {
	case ddlAddColumn:
		col := coreSchema.Column{Name: ch.Column, Type: ch.ColType, Default: ch.Default}
		eh.schemaRegistry.RecordAddColumn(ch.Table, col)
	case ddlDropColumn:
		eh.schemaRegistry.RecordDropColumn(ch.Table, ch.Column)
	case ddlRenameColumn:
		eh.schemaRegistry.RecordRenameColumn(ch.Table, ch.OldName, ch.NewName)
	case ddlAlterType:
		eh.schemaRegistry.RecordTypeChange(ch.Table, ch.Column, ch.OldType, ch.NewType)
	case ddlDropTable:
		// Informational only.
	}
}

// handleExtTxn coordinates transaction control across both backends via extended protocol.
// Reuses TxnHandler logic by converting the batch into a simple query.
func (eh *ExtHandler) handleExtTxn(clientConn net.Conn, batchRaw []byte, cl *core.Classification) error {
	if eh.txnHandler == nil {
		// No TxnHandler — forward to both backends.
		eh.shadowConn.Write(batchRaw) //nolint: errcheck
		if err := drainUntilReady(eh.shadowConn); err != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: shadow drain error (txn): %v", eh.connID, err)
			}
		}
		return forwardAndRelay(batchRaw, eh.prodConn, clientConn)
	}

	// Delegate to TxnHandler using a simple query message.
	// This ensures correct BEGIN (REPEATABLE READ on Prod), COMMIT (promote deltas),
	// and ROLLBACK (discard deltas) semantics.
	simpleMsg := buildQueryMsg(eh.batchSQL)
	return eh.txnHandler.HandleTxn(clientConn, simpleMsg, cl)
}
