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

	// shadowOnlyStmts tracks prepared statements that were only sent to Shadow
	// (because the query references tables with schema diffs or Shadow-only tables).
	// When these statements are used in later batches, the batch must be routed
	// through the read handler rather than forwarded to Prod.
	shadowOnlyStmts map[string]bool

	// Batch accumulator, cleared after each Sync.
	batch    []*pgMsg
	batchRaw []byte

	// Extracted info from the current batch.
	batchSQL          string   // SQL from Parse or stmtCache
	batchParams       [][]byte // parameter values from Bind
	batchFormatCodes  []int16  // parameter format codes from Bind (0=text, 1=binary)
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
		_, stmtName, fmtCodes, params, err := parseBindMsgPayload(msg.Payload)
		if err != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: failed to parse Bind payload: %v", eh.connID, err)
			}
			return
		}
		eh.batchParams = params
		eh.batchFormatCodes = fmtCodes
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
			delete(eh.shadowOnlyStmts, name)
		}
	}
}

// FlushBatch processes the accumulated batch: classifies, routes, and dispatches.
// Called when Sync ('S') is received.
func (eh *ExtHandler) FlushBatch(clientConn net.Conn) error {
	defer eh.clearBatch()

	batchRaw := eh.batchRaw

	// If no SQL found or no Execute, forward as a safe default.
	// Covers Describe-only batches, Close-only batches, etc.
	// When the batch includes a Parse, also forward to Shadow so the prepared
	// statement exists on both backends for future Bind+Execute batches.
	if eh.batchSQL == "" || !eh.batchHasExec {
		if eh.batchHasParse && eh.shadowConn != nil {
			// If the query references a Shadow-only table or a table with schema
			// diffs (DDL changes), route to Shadow only — Prod may not be able
			// to parse the query due to missing columns, renamed columns, etc.
			if eh.hasShadowOnlyTable() || eh.hasSchemaModifiedTable() {
				// Track that this statement was prepared on Shadow only.
				if stmtName := eh.parsedStmtName(); stmtName != "" {
					eh.shadowOnlyStmts[stmtName] = true
				}
				return forwardAndRelay(batchRaw, eh.shadowConn, clientConn)
			}
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
		cl, err = eh.classifier.ClassifyWithParams(eh.batchSQL, resolveParams(eh.batchParams, eh.batchFormatCodes))
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

	// WRITE GUARD L1: validate routing decision.
	if err := validateRouteDecision(cl, strategy, eh.connID, eh.logger); err != nil {
		strategy = core.StrategyShadowWrite
	}

	if eh.verbose {
		log.Printf("[conn %d] ext: %s/%s tables=%v → %s | %s",
			eh.connID, cl.OpType, cl.SubType,
			cl.Tables, strategy, truncateSQL(eh.batchSQL, 80))
	}

	eh.logger.Query(eh.connID, eh.batchSQL, cl, strategy, 0)

	switch strategy {
	case core.StrategyProdDirect:
		// If this batch uses a prepared statement that was only sent to Shadow
		// (due to schema diffs), handle it as a merged read instead — but only
		// if at least one table still has diffs. After DROP TABLE or schema
		// resolution, the statement is stale; forward to Prod to get a proper error.
		if eh.isShadowOnlyStmt() && eh.readHandler != nil && cl.OpType == core.OpRead {
			if eh.anyTableStillHasDiffs(cl.Tables) {
				return eh.handleExtMergedRead(clientConn, cl)
			}
			// Table diffs resolved — clean up shadow-only status.
			if name := eh.boundStmtName(); name != "" {
				delete(eh.shadowOnlyStmts, name)
			}
		}
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
	eh.batchFormatCodes = nil
	eh.batchHasParse = false
	eh.batchHasBind = false
	eh.batchHasDesc = false
	eh.batchHasExec = false
	eh.batchBinaryParams = false
}

// hasShadowOnlyTable returns true if the current batch SQL references a table
// that only exists in Shadow (created via DDL, not present in Prod metadata).
func (eh *ExtHandler) hasShadowOnlyTable() bool {
	if eh.batchSQL == "" || eh.schemaRegistry == nil {
		return false
	}
	cl, err := eh.classifier.Classify(eh.batchSQL)
	if err != nil || cl == nil {
		return false
	}
	for _, table := range cl.Tables {
		if _, exists := eh.tables[table]; !exists {
			if eh.schemaRegistry.HasDiff(table) {
				return true
			}
		}
	}
	return false
}

// parsedStmtName returns the statement name from the first Parse message
// in the current batch, or "" if no Parse is present.
func (eh *ExtHandler) parsedStmtName() string {
	for _, msg := range eh.batch {
		if msg.Type == 'P' {
			name, _, _, err := parseParseMsgPayload(msg.Payload)
			if err == nil {
				return name
			}
		}
	}
	return ""
}

// boundStmtName returns the statement name from the Bind message
// in the current batch, or "" if no Bind is present.
func (eh *ExtHandler) boundStmtName() string {
	for _, msg := range eh.batch {
		if msg.Type == 'B' {
			_, stmtName, _, _, err := parseBindMsgPayload(msg.Payload)
			if err == nil {
				return stmtName
			}
		}
	}
	return ""
}

// isShadowOnlyStmt returns true if the current batch references a prepared
// statement that was only prepared on Shadow (not on Prod).
func (eh *ExtHandler) isShadowOnlyStmt() bool {
	if len(eh.shadowOnlyStmts) == 0 {
		return false
	}
	name := eh.boundStmtName()
	return eh.shadowOnlyStmts[name]
}

// anyTableStillHasDiffs returns true if any of the given tables currently has
// schema diffs, data deltas, or tombstones. Used to validate that shadow-only
// prepared statements are still relevant (e.g., after DROP TABLE clears all state).
func (eh *ExtHandler) anyTableStillHasDiffs(tables []string) bool {
	if eh.schemaRegistry != nil {
		for _, t := range tables {
			if eh.schemaRegistry.HasDiff(t) {
				return true
			}
		}
	}
	if eh.deltaMap != nil && eh.deltaMap.AnyTableDelta(tables) {
		return true
	}
	if eh.tombstones != nil && eh.tombstones.AnyTableTombstone(tables) {
		return true
	}
	return false
}

// hasSchemaModifiedTable returns true if the current batch SQL references any
// table that has schema diffs (DDL changes like ADD COLUMN, RENAME COLUMN).
// Used to route Parse-only batches to Shadow when Prod can't parse the query.
func (eh *ExtHandler) hasSchemaModifiedTable() bool {
	if eh.batchSQL == "" || eh.schemaRegistry == nil {
		return false
	}
	cl, err := eh.classifier.Classify(eh.batchSQL)
	if err != nil || cl == nil {
		return false
	}
	for _, table := range cl.Tables {
		if eh.schemaRegistry.HasDiff(table) {
			return true
		}
	}
	return false
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
// For point deletes, corrects CommandComplete to reflect tombstone count.
// For RETURNING clauses, hydrates row data from Prod.
func (eh *ExtHandler) handleExtDelete(clientConn net.Conn, batchRaw []byte, cl *core.Classification) error {
	if len(cl.PKs) == 0 || eh.tombstones == nil {
		// Bulk delete — relay directly.
		return forwardAndRelay(batchRaw, eh.shadowConn, clientConn)
	}

	// Check for RETURNING clause — need to hydrate from Prod.
	fullSQL := eh.batchSQL
	if len(eh.batchParams) > 0 {
		fullSQL = reconstructSQL(eh.batchSQL, eh.batchParams, eh.batchFormatCodes)
	}
	if hasReturning(fullSQL) && len(cl.Tables) > 0 {
		return eh.handleExtDeleteReturning(clientConn, batchRaw, cl, fullSQL)
	}

	// Point delete: capture and correct the response.
	msgs, err := forwardAndCapture(batchRaw, eh.shadowConn)
	if err != nil {
		return err
	}

	tombstoneCount := len(cl.PKs)
	for _, msg := range msgs {
		if msg.Type == 'C' {
			tag := fmt.Sprintf("DELETE %d", tombstoneCount)
			corrected := buildCommandCompleteMsg(tag)
			if _, writeErr := clientConn.Write(corrected); writeErr != nil {
				return fmt.Errorf("relaying corrected CommandComplete: %w", writeErr)
			}
		} else {
			if _, writeErr := clientConn.Write(msg.Raw); writeErr != nil {
				return fmt.Errorf("relaying to client: %w", writeErr)
			}
		}
	}

	eh.extAddTombstones(cl)
	return nil
}

// handleExtDeleteReturning handles DELETE ... RETURNING in extended protocol
// by hydrating from Prod and synthesizing an extended protocol response.
func (eh *ExtHandler) handleExtDeleteReturning(
	clientConn net.Conn,
	batchRaw []byte,
	cl *core.Classification,
	fullSQL string,
) error {
	table := cl.Tables[0]

	// Build SELECT from RETURNING and query Prod.
	selectSQL := buildReturningSelect(fullSQL, table)
	var prodResult *QueryResult
	if selectSQL != "" {
		var err error
		prodResult, err = execQuery(eh.prodConn, selectSQL)
		if err != nil || prodResult.Error != "" {
			prodResult = nil
		}
	}

	// Forward DELETE to Shadow.
	_, err := forwardAndCapture(batchRaw, eh.shadowConn)
	if err != nil {
		return err
	}

	if prodResult != nil && len(prodResult.Columns) > 0 && len(prodResult.RowValues) > 0 {
		tombstoneCount := len(cl.PKs)
		// Build response manually (can't use buildExtSelectResponse — it uses SELECT tag).
		var buf []byte
		if eh.batchHasParse {
			buf = append(buf, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			buf = append(buf, buildBindCompleteMsg()...)
		}
		buf = append(buf, buildRowDescMsg(prodResult.Columns)...)
		for i := range prodResult.RowValues {
			buf = append(buf, buildDataRowMsg(prodResult.RowValues[i], prodResult.RowNulls[i])...)
		}
		tag := fmt.Sprintf("DELETE %d", tombstoneCount)
		buf = append(buf, buildCommandCompleteMsg(tag)...)
		buf = append(buf, buildReadyForQueryMsg()...)
		if _, err := clientConn.Write(buf); err != nil {
			return fmt.Errorf("relaying ext RETURNING response: %w", err)
		}
	} else {
		// Fallback: synthesize corrected response from Shadow.
		tombstoneCount := len(cl.PKs)
		var buf []byte
		if eh.batchHasParse {
			buf = append(buf, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			buf = append(buf, buildBindCompleteMsg()...)
		}
		tag := fmt.Sprintf("DELETE %d", tombstoneCount)
		buf = append(buf, buildCommandCompleteMsg(tag)...)
		buf = append(buf, buildReadyForQueryMsg()...)
		if _, err := clientConn.Write(buf); err != nil {
			return fmt.Errorf("relaying ext delete response: %w", err)
		}
	}

	eh.extAddTombstones(cl)
	return nil
}

// extAddTombstones records tombstones and persists state for the ext handler.
func (eh *ExtHandler) extAddTombstones(cl *core.Classification) {
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
}

// handleExtMergedRead constructs full SQL, runs merged read, and synthesizes
// an extended protocol response.
func (eh *ExtHandler) handleExtMergedRead(clientConn net.Conn, cl *core.Classification) error {
	if eh.readHandler == nil {
		return forwardAndRelay(eh.batchRaw, eh.prodConn, clientConn)
	}

	// Construct full SQL with parameters substituted (binary params decoded).
	fullSQL := eh.batchSQL
	if len(eh.batchParams) > 0 {
		fullSQL = reconstructSQL(eh.batchSQL, eh.batchParams, eh.batchFormatCodes)
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
	if eh.readHandler == nil {
		return forwardAndRelay(eh.batchRaw, eh.prodConn, clientConn)
	}

	fullSQL := eh.batchSQL
	if len(eh.batchParams) > 0 {
		fullSQL = reconstructSQL(eh.batchSQL, eh.batchParams, eh.batchFormatCodes)
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
		eh.schemaRegistry.RemoveTable(ch.Table)
	case ddlCreateTable:
		eh.schemaRegistry.RecordNewTable(ch.Table)
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
