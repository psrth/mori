package proxy

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

// ExtHandler processes extended query protocol batches for a single DuckDB connection.
// It accumulates Parse/Bind/Describe/Execute messages until Sync, then classifies,
// routes, and dispatches using database/sql.
type ExtHandler struct {
	proxy      *Proxy
	connID     int64
	txh        *TxnHandler
	fkEnforcer *FKEnforcer

	// stmtCache maps statement names to SQL text and classification, persisting across batches.
	stmtCache    map[string]string
	stmtClCache  map[string]*core.Classification

	// Batch accumulator, cleared after each Sync.
	batch         []*pgMsg
	batchSQL      string
	batchParams   []string
	batchHasParse bool
	batchHasBind  bool
	batchHasDesc  bool
	batchHasExec  bool
	describeType  byte // 'S' for statement, 'P' for portal

	// drainError: when true, discard messages until next Sync, then send error.
	drainError    bool
	drainErrorMsg string
}

// DrainUntilSync discards accumulated messages (called after an error in the pipeline).
// After calling this, the next Sync will send ErrorResponse + ReadyForQuery.
func (eh *ExtHandler) DrainUntilSync() {
	eh.drainError = true
}

// Accumulate adds an extended protocol message to the current batch.
func (eh *ExtHandler) Accumulate(msg *pgMsg) {
	// If draining after error, discard until Sync.
	if eh.drainError && msg.Type != 'S' {
		return
	}

	eh.batch = append(eh.batch, msg)

	switch msg.Type {
	case 'P':
		eh.batchHasParse = true
		stmtName, sqlText, err := parseParseMsgPayload(msg.Payload)
		if err != nil {
			if eh.proxy.verbose {
				log.Printf("[conn %d] ext: failed to parse Parse payload: %v", eh.connID, err)
			}
			return
		}
		eh.batchSQL = sqlText
		eh.stmtCache[stmtName] = sqlText

	case 'B':
		eh.batchHasBind = true
		stmtName, params, err := parseBindMsgPayload(msg.Payload)
		if err != nil {
			if eh.proxy.verbose {
				log.Printf("[conn %d] ext: failed to parse Bind payload: %v", eh.connID, err)
			}
			return
		}
		eh.batchParams = params
		if eh.batchSQL == "" {
			if cached, ok := eh.stmtCache[stmtName]; ok {
				eh.batchSQL = cached
			}
		}

	case 'D':
		eh.batchHasDesc = true
		// Capture describe target type for Describe response.
		if len(msg.Payload) >= 1 {
			eh.describeType = msg.Payload[0] // 'S' for statement, 'P' for portal
		}

	case 'E':
		eh.batchHasExec = true

	case 'C':
		closeType, name, err := parseCloseMsgPayload(msg.Payload)
		if err == nil && closeType == 'S' {
			// Clean up both SQL and classification caches.
			if sql, ok := eh.stmtCache[name]; ok {
				delete(eh.stmtClCache, sql)
			}
			delete(eh.stmtCache, name)
		}
	}
}

// FlushBatch processes the accumulated batch: classifies, routes, and dispatches.
// Called when Sync ('S') is received.
func (eh *ExtHandler) FlushBatch(clientConn net.Conn) {
	defer eh.clearBatch()

	p := eh.proxy

	// If draining after an error, send ErrorResponse + ReadyForQuery and reset.
	if eh.drainError {
		errMsg := eh.drainErrorMsg
		if errMsg == "" {
			errMsg = "mori: previous error in extended protocol pipeline"
		}
		var resp []byte
		resp = append(resp, buildSQLErrorResponseBytes(errMsg)...)
		state := byte('I')
		if eh.txh != nil {
			state = eh.txh.TxnState()
		}
		resp = append(resp, buildReadyForQueryMsgState(state)...)
		clientConn.Write(resp)
		eh.drainError = false
		eh.drainErrorMsg = ""
		return
	}

	// If no SQL found or no Execute, handle as passthrough.
	if eh.batchSQL == "" || !eh.batchHasExec {
		if eh.batchHasParse {
			clientConn.Write(buildParseCompleteMsg())
		}
		if eh.batchHasBind {
			clientConn.Write(buildBindCompleteMsg())
		}
		if eh.batchHasDesc {
			// Try to provide a proper RowDescription by running LIMIT 0 on Shadow.
			if eh.batchSQL != "" && p.shadowDB != nil {
				if desc := eh.buildDescribeResponse(p.shadowDB, eh.batchSQL); desc != nil {
					clientConn.Write(desc)
				} else {
					clientConn.Write(buildNoDataMsg())
				}
			} else {
				clientConn.Write(buildNoDataMsg())
			}
		}
		clientConn.Write(buildReadyForQueryMsg())
		return
	}

	// Construct full SQL with parameters substituted.
	fullSQL := eh.batchSQL
	if len(eh.batchParams) > 0 {
		fullSQL = reconstructSQL(eh.batchSQL, eh.batchParams)
	}

	// Classify the query.
	var cl *core.Classification
	var err error
	if len(eh.batchParams) > 0 {
		iparams := make([]any, len(eh.batchParams))
		for i, v := range eh.batchParams {
			iparams[i] = v
		}
		cl, err = p.classifier.ClassifyWithParams(eh.batchSQL, iparams)
	} else {
		cl, err = p.classifier.Classify(eh.batchSQL)
	}
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] ext: classify error, executing on prod: %v", eh.connID, err)
		}
		eh.extExecOnProd(clientConn, fullSQL)
		return
	}

	// Cache the classification for future Bind-only batches.
	if eh.batchHasParse && eh.batchSQL != "" {
		eh.stmtClCache[eh.batchSQL] = cl
	}

	strategy := p.router.Route(cl)

	// Shadow-dirty promotion: if this is a cached statement (Bind-only, no Parse)
	// routed to ProdDirect, check if any of its tables have deltas/tombstones.
	// If so, promote to MergedRead for correctness.
	if strategy == core.StrategyProdDirect && !eh.batchHasParse && cl.OpType == core.OpRead {
		if eh.tablesTouchedAreDirty(cl.Tables) {
			strategy = core.StrategyMergedRead
			if p.verbose {
				log.Printf("[conn %d] ext: promoting cached statement to MergedRead (dirty tables)", eh.connID)
			}
		}
	}

	// WRITE GUARD L1.
	if err := validateRouteDecision(cl, strategy, eh.connID, p.logger); err != nil {
		strategy = core.StrategyShadowWrite
	}

	if p.verbose {
		log.Printf("[conn %d] ext: %s/%s tables=%v → %s | %s",
			eh.connID, cl.OpType, cl.SubType,
			cl.Tables, strategy, truncateSQL(eh.batchSQL, 80))
	}

	p.logger.Query(eh.connID, eh.batchSQL, cl, strategy, 0)

	switch strategy {
	case core.StrategyProdDirect:
		eh.extExecOnProd(clientConn, fullSQL)

	case core.StrategyShadowWrite:
		// FK enforcement before shadow writes.
		if eh.fkEnforcer != nil {
			if errMsg := eh.fkEnforcer.CheckWriteFK(cl, fullSQL); errMsg != "" {
				var resp []byte
				if eh.batchHasParse {
					resp = append(resp, buildParseCompleteMsg()...)
				}
				if eh.batchHasBind {
					resp = append(resp, buildBindCompleteMsg()...)
				}
				resp = append(resp, buildSQLErrorResponseBytes(errMsg)...)
				resp = append(resp, buildReadyForQueryMsg()...)
				clientConn.Write(resp)
				return
			}
		}
		eh.extExecOnShadow(clientConn, fullSQL)
		if p.deltaMap != nil {
			for _, table := range cl.Tables {
				p.deltaMap.MarkInserted(table)
			}
		}

	case core.StrategyHydrateAndWrite:
		// FK enforcement before hydrate-and-write.
		if eh.fkEnforcer != nil {
			if errMsg := eh.fkEnforcer.CheckWriteFK(cl, fullSQL); errMsg != "" {
				var resp []byte
				if eh.batchHasParse {
					resp = append(resp, buildParseCompleteMsg()...)
				}
				if eh.batchHasBind {
					resp = append(resp, buildBindCompleteMsg()...)
				}
				resp = append(resp, buildSQLErrorResponseBytes(errMsg)...)
				resp = append(resp, buildReadyForQueryMsg()...)
				clientConn.Write(resp)
				return
			}
		}
		// Handle bulk UPDATE (no PKs) via handleBulkUpdate.
		if len(cl.PKs) == 0 && cl.SubType == core.SubUpdate && len(cl.Tables) > 0 {
			resp := p.handleBulkUpdate(fullSQL, cl, eh.connID, eh.txh)
			var result []byte
			if eh.batchHasParse {
				result = append(result, buildParseCompleteMsg()...)
			}
			if eh.batchHasBind {
				result = append(result, buildBindCompleteMsg()...)
			}
			result = append(result, resp...)
			clientConn.Write(result)
			return
		}
		if len(cl.PKs) > 0 {
			p.hydrateBeforeUpdate(cl, eh.connID)
		}
		eh.extExecOnShadow(clientConn, fullSQL)
		if p.deltaMap != nil {
			inTxn := eh.txh != nil && eh.txh.InTxn()
			for _, pk := range cl.PKs {
				if inTxn {
					p.deltaMap.Stage(pk.Table, pk.PK)
				} else {
					p.deltaMap.Add(pk.Table, pk.PK)
				}
			}
			if !inTxn {
				delta.WriteDeltaMap(p.moriDir, p.deltaMap)
			}
		}

	case core.StrategyShadowDelete:
		// Handle bulk DELETE (no PKs) via handleBulkDelete.
		if len(cl.PKs) == 0 && !cl.IsJoin && len(cl.Tables) == 1 {
			if meta, ok := p.tables[cl.Tables[0]]; ok && len(meta.PKColumns) > 0 {
				resp := p.handleBulkDelete(fullSQL, cl, eh.connID, eh.txh)
				var result []byte
				if eh.batchHasParse {
					result = append(result, buildParseCompleteMsg()...)
				}
				if eh.batchHasBind {
					result = append(result, buildBindCompleteMsg()...)
				}
				result = append(result, resp...)
				clientConn.Write(result)
				return
			}
		}
		eh.extExecOnShadow(clientConn, fullSQL)
		if p.tombstones != nil {
			inTxn := eh.txh != nil && eh.txh.InTxn()
			for _, pk := range cl.PKs {
				if inTxn {
					p.tombstones.Stage(pk.Table, pk.PK)
				} else {
					p.tombstones.Add(pk.Table, pk.PK)
					if p.deltaMap != nil {
						p.deltaMap.Remove(pk.Table, pk.PK)
					}
				}
			}
			if !inTxn {
				delta.WriteTombstoneSet(p.moriDir, p.tombstones)
				if p.deltaMap != nil {
					delta.WriteDeltaMap(p.moriDir, p.deltaMap)
				}
			}
		}

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		// Dispatch to specialized handlers for complex patterns.
		var advancedResp []byte
		switch {
		case cl.HasWindowFunc:
			advancedResp = p.executeWindowRead(fullSQL, cl, eh.connID)
		case cl.HasSetOp:
			advancedResp = p.executeSetOpRead(fullSQL, cl, eh.connID)
		case cl.IsComplexRead:
			advancedResp = p.executeComplexRead(fullSQL, cl, eh.connID)
		}
		if advancedResp != nil {
			var result []byte
			if eh.batchHasParse {
				result = append(result, buildParseCompleteMsg()...)
			}
			if eh.batchHasBind {
				result = append(result, buildBindCompleteMsg()...)
			}
			result = append(result, advancedResp...)
			clientConn.Write(result)
			return
		}
		eh.extMergedRead(clientConn, fullSQL, cl)

	case core.StrategyShadowDDL:
		eh.extExecOnShadow(clientConn, fullSQL)
		p.trackDDLEffects(cl, eh.connID)

	case core.StrategyTruncate:
		resp := p.handleTruncate(fullSQL, cl, eh.connID)
		var result []byte
		if eh.batchHasParse {
			result = append(result, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			result = append(result, buildBindCompleteMsg()...)
		}
		result = append(result, resp...)
		clientConn.Write(result)

	case core.StrategyNotSupported:
		msg := core.UnsupportedTransactionMsg
		if cl.NotSupportedMsg != "" {
			msg = cl.NotSupportedMsg
		}
		var resp []byte
		if eh.batchHasParse {
			resp = append(resp, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			resp = append(resp, buildBindCompleteMsg()...)
		}
		resp = append(resp, buildSQLErrorResponseBytes(msg)...)
		resp = append(resp, buildReadyForQueryMsg()...)
		clientConn.Write(resp)

	case core.StrategyForwardBoth:
		eh.extExecOnShadow(clientConn, fullSQL)
		// Also execute on prod (discard result).
		p.prodDB.Exec(fullSQL)

	case core.StrategyTransaction:
		if eh.txh != nil {
			eh.txh.HandleTxn(clientConn, cl)
		} else {
			eh.extExecOnProd(clientConn, fullSQL)
		}

	default:
		msg := core.UnsupportedTransactionMsg
		var resp []byte
		if eh.batchHasParse {
			resp = append(resp, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			resp = append(resp, buildBindCompleteMsg()...)
		}
		resp = append(resp, buildSQLErrorResponseBytes(msg)...)
		resp = append(resp, buildReadyForQueryMsg()...)
		clientConn.Write(resp)
	}
}

// extExecOnProd executes a query on prod and sends the extended protocol response.
// Includes L2 write guard check.
func (eh *ExtHandler) extExecOnProd(clientConn net.Conn, sqlStr string) {
	p := eh.proxy
	// WRITE GUARD L2: block writes to Prod.
	if blocked, ok := safeProdExec(p.prodDB, sqlStr, eh.connID, p.logger, p.verbose); ok {
		var resp []byte
		if eh.batchHasParse {
			resp = append(resp, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			resp = append(resp, buildBindCompleteMsg()...)
		}
		resp = append(resp, blocked...)
		clientConn.Write(resp)
		return
	}
	eh.extExecOn(clientConn, p.prodDB, sqlStr)
}

// extExecOnShadow executes a query on shadow and sends the extended protocol response.
func (eh *ExtHandler) extExecOnShadow(clientConn net.Conn, sqlStr string) {
	eh.extExecOn(clientConn, eh.proxy.shadowDB, sqlStr)
}

// extExecOn executes SQL on a db and sends a proper extended protocol response.
func (eh *ExtHandler) extExecOn(clientConn net.Conn, db *sql.DB, sqlStr string) {
	var resp []byte
	if eh.batchHasParse {
		resp = append(resp, buildParseCompleteMsg()...)
	}
	if eh.batchHasBind {
		resp = append(resp, buildBindCompleteMsg()...)
	}

	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		(strings.HasPrefix(upper, "WITH") && !strings.Contains(upper, "INSERT") &&
			!strings.Contains(upper, "UPDATE") && !strings.Contains(upper, "DELETE"))

	if isSelect {
		resp = append(resp, eh.extSelectQuery(db, sqlStr)...)
	} else {
		resp = append(resp, eh.extExecQuery(db, sqlStr)...)
	}

	resp = append(resp, buildReadyForQueryMsg()...)
	clientConn.Write(resp)
}

// extSelectQuery executes a SELECT on db and returns RowDescription + DataRows + CommandComplete.
func (eh *ExtHandler) extSelectQuery(db *sql.DB, sqlStr string) []byte {
	rows, err := db.Query(sqlStr)
	if err != nil {
		return buildSQLErrorResponseBytes(err.Error())
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return buildSQLErrorResponseBytes(err.Error())
	}

	// Map DuckDB column types to PostgreSQL OIDs.
	colOIDs := resolveColumnOIDs(rows)
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	rowCount := 0
	scanDest := make([]any, len(columns))
	for i := range scanDest {
		scanDest[i] = new(sql.NullString)
	}

	for rows.Next() {
		if err := rows.Scan(scanDest...); err != nil {
			return buildSQLErrorResponseBytes(err.Error())
		}
		values := make([][]byte, len(columns))
		nulls := make([]bool, len(columns))
		for i, dest := range scanDest {
			ns := dest.(*sql.NullString)
			if !ns.Valid {
				nulls[i] = true
			} else {
				values[i] = []byte(ns.String)
			}
		}
		resp = append(resp, buildDataRowMsg(values, nulls)...)
		rowCount++
	}

	tag := fmt.Sprintf("SELECT %d", rowCount)
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	return resp
}

// extExecQuery executes a non-SELECT on db and returns CommandComplete.
func (eh *ExtHandler) extExecQuery(db *sql.DB, sqlStr string) []byte {
	result, err := db.Exec(sqlStr)
	if err != nil {
		return buildSQLErrorResponseBytes(err.Error())
	}

	rowsAffected, _ := result.RowsAffected()
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	var tag string
	switch {
	case strings.HasPrefix(upper, "INSERT"):
		tag = fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case strings.HasPrefix(upper, "UPDATE"):
		tag = fmt.Sprintf("UPDATE %d", rowsAffected)
	case strings.HasPrefix(upper, "DELETE"):
		tag = fmt.Sprintf("DELETE %d", rowsAffected)
	case strings.HasPrefix(upper, "CREATE"):
		tag = "CREATE TABLE"
	case strings.HasPrefix(upper, "ALTER"):
		tag = "ALTER TABLE"
	case strings.HasPrefix(upper, "DROP"):
		tag = "DROP TABLE"
	case strings.HasPrefix(upper, "TRUNCATE"):
		tag = "TRUNCATE TABLE"
	case strings.HasPrefix(upper, "BEGIN"), strings.HasPrefix(upper, "START"):
		tag = "BEGIN"
	case strings.HasPrefix(upper, "COMMIT"), strings.HasPrefix(upper, "END"):
		tag = "COMMIT"
	case strings.HasPrefix(upper, "ROLLBACK"):
		tag = "ROLLBACK"
	default:
		tag = "OK"
	}

	return buildCommandCompleteMsg(tag)
}

// extMergedRead executes a merged read and returns the result as an extended protocol response.
func (eh *ExtHandler) extMergedRead(clientConn net.Conn, sqlStr string, cl *core.Classification) {
	p := eh.proxy

	clCopy := *cl
	clCopy.RawSQL = sqlStr

	columns, rows, nulls, err := p.mergedReadRows(sqlStr, &clCopy, eh.connID)
	if err != nil {
		var resp []byte
		if eh.batchHasParse {
			resp = append(resp, buildParseCompleteMsg()...)
		}
		if eh.batchHasBind {
			resp = append(resp, buildBindCompleteMsg()...)
		}
		resp = append(resp, buildSQLErrorResponse(err.Error())...)
		clientConn.Write(resp)
		return
	}

	var resp []byte
	if eh.batchHasParse {
		resp = append(resp, buildParseCompleteMsg()...)
	}
	if eh.batchHasBind {
		resp = append(resp, buildBindCompleteMsg()...)
	}
	resp = append(resp, buildMergedResponse(columns, rows, nulls)...)
	clientConn.Write(resp)
}

// buildSQLErrorResponseBytes returns just the error message bytes without ReadyForQuery.
func buildSQLErrorResponseBytes(message string) []byte {
	var errPayload []byte
	errPayload = append(errPayload, 'S')
	errPayload = append(errPayload, []byte("ERROR")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'C')
	errPayload = append(errPayload, []byte("42000")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'M')
	errPayload = append(errPayload, []byte(message)...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 0)
	return buildPGMsg('E', errPayload)
}

// buildDescribeResponse runs a LIMIT 0 query on the given database to obtain
// column metadata and returns a RowDescription message, or nil on failure.
func (eh *ExtHandler) buildDescribeResponse(db *sql.DB, sqlStr string) []byte {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		(strings.HasPrefix(upper, "WITH") && !strings.Contains(upper, "INSERT") &&
			!strings.Contains(upper, "UPDATE") && !strings.Contains(upper, "DELETE"))
	if !isSelect {
		// Non-SELECT statements: return ParameterDescription (empty) + NoData.
		return buildNoDataMsg()
	}

	// Run with LIMIT 0 to get column metadata without data.
	limitSQL := sqlStr + " LIMIT 0"
	rows, err := db.Query(limitSQL)
	if err != nil {
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil || len(columns) == 0 {
		return nil
	}

	colOIDs := resolveColumnOIDs(rows)
	return buildRowDescMsg(columns, colOIDs)
}

// tablesTouchedAreDirty checks if any of the given tables have deltas or tombstones,
// meaning reads from Prod alone would return stale data.
func (eh *ExtHandler) tablesTouchedAreDirty(tables []string) bool {
	p := eh.proxy
	if p.deltaMap == nil && p.tombstones == nil {
		return false
	}
	for _, table := range tables {
		if p.deltaMap != nil && (p.deltaMap.CountForTable(table) > 0 || p.deltaMap.InsertCountForTable(table) > 0) {
			return true
		}
		if p.tombstones != nil && p.tombstones.CountForTable(table) > 0 {
			return true
		}
		if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
			return true
		}
		if p.schemaRegistry != nil && p.schemaRegistry.IsFullyShadowed(table) {
			return true
		}
	}
	return false
}

// clearBatch resets the batch accumulator for the next Sync cycle.
func (eh *ExtHandler) clearBatch() {
	eh.batch = nil
	eh.batchSQL = ""
	eh.batchParams = nil
	eh.batchHasParse = false
	eh.batchHasBind = false
	eh.batchHasDesc = false
	eh.batchHasExec = false
	eh.describeType = 0
	eh.drainError = false
	eh.drainErrorMsg = ""
}
