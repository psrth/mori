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

// ExtHandler processes extended query protocol batches for a single SQLite connection.
// It accumulates Parse/Bind/Describe/Execute messages until Sync, then classifies,
// routes, and dispatches using database/sql.
type ExtHandler struct {
	proxy  *Proxy
	connID int64
	txh    *TxnHandler

	// stmtCache maps statement names to SQL text, persisting across batches.
	stmtCache map[string]string

	// shadowOnlyStmts tracks prepared statements that reference shadow-only tables
	// or tables with schema diffs. When these statements are used in later batches,
	// routes must go through merged read rather than Prod.
	shadowOnlyStmts map[string]bool

	// stmtClassifications caches the Classification for prepared statements
	// so that Execute can route correctly without re-classifying.
	stmtClassifications map[string]*core.Classification

	// Batch accumulator, cleared after each Sync.
	batch        []*pgMsg
	batchSQL     string
	batchParams  []string
	batchHasParse bool
	batchHasBind  bool
	batchHasDesc  bool
	batchHasExec  bool

	// errorDrain: when true, discard remaining messages until Sync.
	errorDrain bool

	// P2 3.8: Portal-based cursor support.
	// batchMaxRows holds the maxRows value from the Execute message.
	// 0 means "return all rows". Non-zero means portal-based cursor limiting.
	batchMaxRows int32

	// portalResults caches materialized results for portal-based cursors.
	// Keyed by portal name. When a client sends Execute with maxRows > 0,
	// the full result is materialized and cached; subsequent Executes on the
	// same portal return the next batch.
	portalResults map[string]*portalResultSet
}

// portalResultSet holds a materialized result set for portal-based cursor iteration.
type portalResultSet struct {
	columns []string
	colOIDs []uint32
	rows    [][]sql.NullString
	nulls   [][]bool
	offset  int // next row to return
}

// Accumulate adds an extended protocol message to the current batch.
func (eh *ExtHandler) Accumulate(msg *pgMsg) {
	// P3 4.9: Connection drain on errors — discard messages until Sync.
	if eh.errorDrain && msg.Type != 'S' {
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

		// P3 4.10: Cache classification for this prepared statement.
		if cl, err := eh.classifySQL(sqlText); err == nil {
			if eh.stmtClassifications == nil {
				eh.stmtClassifications = make(map[string]*core.Classification)
			}
			eh.stmtClassifications[stmtName] = cl

			// Track shadow-only statements.
			if eh.isShadowOnly(cl) {
				if eh.shadowOnlyStmts == nil {
					eh.shadowOnlyStmts = make(map[string]bool)
				}
				eh.shadowOnlyStmts[stmtName] = true
			}
		}

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

	case 'E':
		eh.batchHasExec = true
		// P2 3.8: Parse Execute message to extract portal name and maxRows.
		if _, maxRows, err := parseExecuteMsgPayload(msg.Payload); err == nil {
			eh.batchMaxRows = maxRows
		}

	case 'C':
		closeType, name, err := parseCloseMsgPayload(msg.Payload)
		if err == nil && closeType == 'S' {
			delete(eh.stmtCache, name)
			delete(eh.shadowOnlyStmts, name)
			if eh.stmtClassifications != nil {
				delete(eh.stmtClassifications, name)
			}
		}
	}
}

// classifySQL classifies a SQL string using the proxy's classifier.
func (eh *ExtHandler) classifySQL(sqlStr string) (*core.Classification, error) {
	if eh.proxy.classifier == nil {
		return nil, fmt.Errorf("no classifier")
	}
	return eh.proxy.classifier.Classify(sqlStr)
}

// isShadowOnly checks if a classification references tables that should be
// queried from shadow only (new tables, schema diffs, fully shadowed).
func (eh *ExtHandler) isShadowOnly(cl *core.Classification) bool {
	if eh.proxy.schemaRegistry == nil {
		return false
	}
	for _, table := range cl.Tables {
		diff := eh.proxy.schemaRegistry.GetDiff(table)
		if diff != nil && (diff.IsNewTable || diff.IsFullyShadowed || len(diff.Added) > 0 || len(diff.Dropped) > 0 || len(diff.Renamed) > 0) {
			return true
		}
	}
	return false
}

// txnState returns the current transaction state byte for ReadyForQuery.
func (eh *ExtHandler) txnState() byte {
	if eh.txh != nil {
		return eh.txh.TxnState()
	}
	return 'I'
}

// FlushBatch processes the accumulated batch: classifies, routes, and dispatches.
// Called when Sync ('S') is received.
func (eh *ExtHandler) FlushBatch(clientConn net.Conn) {
	defer eh.clearBatch()

	p := eh.proxy

	// P3 4.9: If in error drain mode, just send error + ReadyForQuery and reset.
	if eh.errorDrain {
		eh.errorDrain = false
		clientConn.Write(buildReadyForQueryMsgState(eh.txnState()))
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
			// P3 4.7: Describe response accuracy — return proper RowDescription
			// for SELECT queries, ParameterDescription for others.
			descResp := eh.buildDescribeResponse()
			clientConn.Write(descResp)
		}
		clientConn.Write(buildReadyForQueryMsgState(eh.txnState()))
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

	cl.RawSQL = fullSQL
	strategy := p.router.Route(cl)

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
		// Use specialized handler for INSERT.
		if cl.SubType == core.SubInsert && cl.HasOnConflict {
			eh.extHandleUpsert(clientConn, fullSQL, cl)
		} else if cl.SubType == core.SubInsert {
			eh.extHandleInsert(clientConn, fullSQL, cl)
		} else {
			eh.extExecOnShadow(clientConn, fullSQL)
			if p.deltaMap != nil {
				for _, table := range cl.Tables {
					p.deltaMap.MarkInserted(table)
				}
			}
		}

	case core.StrategyHydrateAndWrite:
		if cl.SubType == core.SubInsert && cl.HasOnConflict {
			eh.extHandleUpsert(clientConn, fullSQL, cl)
		} else {
			eh.extHandleUpdateWithHydration(clientConn, fullSQL, cl)
		}

	case core.StrategyShadowDelete:
		eh.extHandleDelete(clientConn, fullSQL, cl)

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		eh.extMergedRead(clientConn, fullSQL, cl)

	case core.StrategyShadowDDL:
		eh.extExecOnShadow(clientConn, fullSQL)
		p.trackDDLEffects(cl, eh.connID)

	case core.StrategyTransaction:
		if eh.txh != nil {
			eh.txh.HandleTxn(clientConn, cl)
		} else {
			eh.extExecOnProd(clientConn, fullSQL)
		}

	case core.StrategyTruncate:
		eh.extHandleTruncate(clientConn, cl)

	case core.StrategyNotSupported:
		eh.extSendError(clientConn, "mori: this statement is not supported in shadow mode")

	default:
		eh.extExecOnProd(clientConn, fullSQL)
	}
}

// extHandleInsert handles INSERT via extended protocol with row-level PK tracking.
func (eh *ExtHandler) extHandleInsert(clientConn net.Conn, sqlStr string, cl *core.Classification) {
	p := eh.proxy
	inTxn := eh.txh != nil && eh.txh.InTxn()

	eh.extExecOnShadow(clientConn, sqlStr)

	if len(cl.Tables) == 0 || p.deltaMap == nil {
		return
	}
	table := cl.Tables[0]
	meta, hasMeta := p.tables[table]

	tracked := false
	if hasMeta && len(meta.PKColumns) > 0 {
		pkCol := meta.PKColumns[0]

		if meta.PKType == "serial" || meta.PKType == "bigserial" {
			var lastID int64
			if err := p.shadowDB.QueryRow("SELECT last_insert_rowid()").Scan(&lastID); err == nil && lastID > 0 {
				pk := fmt.Sprintf("%d", lastID)
				if inTxn {
					p.deltaMap.Stage(table, pk)
				} else {
					p.deltaMap.Add(table, pk)
				}
				tracked = true
			}
		}

		if !tracked {
			pks := extractInsertPKValues(sqlStr, table, pkCol)
			if len(pks) > 0 {
				for _, pk := range pks {
					if inTxn {
						p.deltaMap.Stage(table, pk)
					} else {
						p.deltaMap.Add(table, pk)
					}
				}
				tracked = true
			}
		}
	}

	if !tracked {
		p.deltaMap.MarkInserted(table)
	}

	if !inTxn {
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}
}

// extHandleUpsert handles INSERT ON CONFLICT / OR REPLACE via extended protocol.
func (eh *ExtHandler) extHandleUpsert(clientConn net.Conn, sqlStr string, cl *core.Classification) {
	p := eh.proxy
	inTxn := eh.txh != nil && eh.txh.InTxn()

	if len(cl.Tables) != 1 {
		eh.extHandleInsert(clientConn, sqlStr, cl)
		return
	}
	table := cl.Tables[0]
	meta, hasMeta := p.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		eh.extHandleInsert(clientConn, sqlStr, cl)
		return
	}

	pkCol := meta.PKColumns[0]
	conflictValues := extractUpsertConflictValues(sqlStr, table, pkCol)
	skipCols := toSkipSet(meta.GeneratedCols)

	var affectedPKs []string
	for _, val := range conflictValues {
		if p.deltaMap != nil && p.deltaMap.IsDelta(table, val) {
			affectedPKs = append(affectedPKs, val)
			continue
		}
		escapedVal := strings.ReplaceAll(val, "'", "''")
		selectSQL := fmt.Sprintf(`SELECT * FROM %q WHERE %q = '%s'`, table, pkCol, escapedVal)
		if p.maxRowsHydrate > 0 {
			selectSQL += fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
		}
		cols, rows, _, err := p.safeProdQuery(selectSQL, eh.connID)
		if err != nil || len(rows) == 0 {
			continue
		}
		insertSQL := buildHydrateInsert(table, cols, rows[0], skipCols)
		p.shadowDB.Exec(insertSQL)
		affectedPKs = append(affectedPKs, val)
	}

	eh.extExecOnShadow(clientConn, sqlStr)

	if p.deltaMap != nil {
		for _, pk := range affectedPKs {
			if inTxn {
				p.deltaMap.Stage(table, pk)
			} else {
				p.deltaMap.Add(table, pk)
			}
		}
		if !inTxn {
			delta.WriteDeltaMap(p.moriDir, p.deltaMap)
		}
	}
}

// extHandleUpdateWithHydration handles UPDATE with hydration via extended protocol.
func (eh *ExtHandler) extHandleUpdateWithHydration(clientConn net.Conn, sqlStr string, cl *core.Classification) {
	p := eh.proxy
	inTxn := eh.txh != nil && eh.txh.InTxn()

	if len(cl.PKs) > 0 {
		p.hydrateBeforeUpdate(cl, eh.connID)
		eh.extExecOnShadow(clientConn, sqlStr)

		if p.deltaMap != nil {
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
		return
	}

	// Bulk update: no extractable PKs. Hydrate from Prod.
	if len(cl.Tables) == 1 {
		table := cl.Tables[0]
		meta, hasMeta := p.tables[table]
		if hasMeta && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			whereClause := extractWhereFromSQL(sqlStr)
			var selectSQL string
			if whereClause != "" {
				selectSQL = fmt.Sprintf(`SELECT %q FROM %q WHERE %s`, pkCol, table, whereClause)
			} else {
				selectSQL = fmt.Sprintf(`SELECT %q FROM %q`, pkCol, table)
			}
			if p.schemaRegistry != nil {
				rewritten, skipProd := rewriteForProd(selectSQL, p.schemaRegistry, cl.Tables)
				if !skipProd {
					selectSQL = rewritten
				}
			}
			if p.maxRowsHydrate > 0 {
				selectSQL += fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
			}
			_, prodRows, _, prodErr := p.safeProdQuery(selectSQL, eh.connID)
			if prodErr == nil {
				skipCols := toSkipSet(meta.GeneratedCols)
				for _, row := range prodRows {
					if len(row) == 0 || !row[0].Valid {
						continue
					}
					pk := row[0].String
					if p.deltaMap != nil && p.deltaMap.IsDelta(table, pk) {
						continue
					}
					escapedPK := strings.ReplaceAll(pk, "'", "''")
					hydrateSQL := fmt.Sprintf(`SELECT * FROM %q WHERE %q = '%s'`, table, pkCol, escapedPK)
					cols, rows, _, err := p.safeProdQuery(hydrateSQL, eh.connID)
					if err != nil || len(rows) == 0 {
						continue
					}
					insertSQL := buildHydrateInsert(table, cols, rows[0], skipCols)
					p.shadowDB.Exec(insertSQL)
					if p.deltaMap != nil {
						if inTxn {
							p.deltaMap.Stage(table, pk)
						} else {
							p.deltaMap.Add(table, pk)
						}
					}
				}
			}
		}
	}

	eh.extExecOnShadow(clientConn, sqlStr)

	if p.deltaMap != nil && !inTxn {
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}
}

// extHandleDelete handles DELETE via extended protocol with proper tombstoning.
func (eh *ExtHandler) extHandleDelete(clientConn net.Conn, sqlStr string, cl *core.Classification) {
	p := eh.proxy
	inTxn := eh.txh != nil && eh.txh.InTxn()

	if len(cl.PKs) > 0 {
		eh.extExecOnShadow(clientConn, sqlStr)

		if p.tombstones != nil {
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
		return
	}

	// Bulk delete: discover PKs from Prod.
	if len(cl.Tables) == 1 {
		table := cl.Tables[0]
		meta, hasMeta := p.tables[table]
		if hasMeta && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			whereClause := extractWhereFromSQL(sqlStr)
			var selectSQL string
			if whereClause != "" {
				selectSQL = fmt.Sprintf(`SELECT %q FROM %q WHERE %s`, pkCol, table, whereClause)
			} else {
				selectSQL = fmt.Sprintf(`SELECT %q FROM %q`, pkCol, table)
			}
			if p.schemaRegistry != nil {
				rewritten, skipProd := rewriteForProd(selectSQL, p.schemaRegistry, cl.Tables)
				if !skipProd {
					selectSQL = rewritten
				}
			}
			if p.maxRowsHydrate > 0 {
				selectSQL += fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
			}
			_, prodRows, _, prodErr := p.safeProdQuery(selectSQL, eh.connID)
			if prodErr == nil && p.tombstones != nil {
				for _, row := range prodRows {
					if len(row) == 0 || !row[0].Valid {
						continue
					}
					pk := row[0].String
					if inTxn {
						p.tombstones.Stage(table, pk)
					} else {
						p.tombstones.Add(table, pk)
						if p.deltaMap != nil {
							p.deltaMap.Remove(table, pk)
						}
					}
				}
			}
		}
	}

	eh.extExecOnShadow(clientConn, sqlStr)

	if !inTxn {
		if p.tombstones != nil {
			delta.WriteTombstoneSet(p.moriDir, p.tombstones)
		}
		if p.deltaMap != nil {
			delta.WriteDeltaMap(p.moriDir, p.deltaMap)
		}
	}
}

// extHandleTruncate handles TRUNCATE via extended protocol.
func (eh *ExtHandler) extHandleTruncate(clientConn net.Conn, cl *core.Classification) {
	p := eh.proxy

	if len(cl.Tables) == 0 {
		eh.extSendError(clientConn, "mori: truncate requires a table name")
		return
	}
	table := cl.Tables[0]

	deleteSQL := fmt.Sprintf(`DELETE FROM %q`, table)
	result, err := p.shadowDB.Exec(deleteSQL)
	if err != nil {
		eh.extSendError(clientConn, err.Error())
		return
	}

	rowsAffected, _ := result.RowsAffected()

	if p.schemaRegistry != nil {
		p.schemaRegistry.MarkFullyShadowed(table)
	}
	if p.deltaMap != nil {
		p.deltaMap.ClearTable(table)
	}
	if p.tombstones != nil {
		p.tombstones.ClearTable(table)
	}

	var resp []byte
	if eh.batchHasParse {
		resp = append(resp, buildParseCompleteMsg()...)
	}
	if eh.batchHasBind {
		resp = append(resp, buildBindCompleteMsg()...)
	}
	tag := fmt.Sprintf("DELETE %d", rowsAffected)
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	resp = append(resp, buildReadyForQueryMsgState(eh.txnState())...)
	clientConn.Write(resp)
}

// extSendError sends an error response via extended protocol.
func (eh *ExtHandler) extSendError(clientConn net.Conn, message string) {
	var resp []byte
	if eh.batchHasParse {
		resp = append(resp, buildParseCompleteMsg()...)
	}
	if eh.batchHasBind {
		resp = append(resp, buildBindCompleteMsg()...)
	}
	resp = append(resp, buildSQLErrorResponseBytes(message)...)
	resp = append(resp, buildReadyForQueryMsgState(eh.txnState())...)
	clientConn.Write(resp)

	// P3 4.9: Set error drain for mid-stream errors.
	if eh.txh != nil && eh.txh.InTxn() {
		eh.txh.SetErrorState()
	}
}

// extExecOnProd executes a query on prod and sends the extended protocol response.
// L2 write guard: inspects SQL and blocks writes from reaching production.
func (eh *ExtHandler) extExecOnProd(clientConn net.Conn, sqlStr string) {
	if looksLikeWrite(sqlStr) {
		log.Printf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write to prod via extExecOnProd: %s",
			eh.connID, truncateSQL(sqlStr, 120))
		eh.extSendError(clientConn, "write guard: write query blocked from reaching production")
		return
	}
	eh.extExecOn(clientConn, eh.proxy.prodDB, sqlStr)
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
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		(strings.HasPrefix(upper, "WITH") && !strings.Contains(upper, "INSERT") &&
			!strings.Contains(upper, "UPDATE") && !strings.Contains(upper, "DELETE"))

	// Also check for RETURNING clause.
	hasReturning := strings.Contains(upper, "RETURNING")

	if isSelect || hasReturning {
		resp = append(resp, eh.extSelectQuery(db, sqlStr)...)
	} else {
		resp = append(resp, eh.extExecQuery(db, sqlStr)...)
	}

	resp = append(resp, buildReadyForQueryMsgState(eh.txnState())...)
	clientConn.Write(resp)
}

// extSelectQuery executes a SELECT on db and returns RowDescription + DataRows + CommandComplete.
// P2 3.8: Respects batchMaxRows for portal-based cursor support.
func (eh *ExtHandler) extSelectQuery(db *sql.DB, sqlStr string) []byte {
	rows, err := db.Query(sqlStr)
	if err != nil {
		if eh.txh != nil && eh.txh.InTxn() {
			eh.txh.SetErrorState()
		}
		return buildSQLErrorResponseBytes(err.Error())
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return buildSQLErrorResponseBytes(err.Error())
	}

	// P3 4.11: Use type-aware OIDs.
	colOIDs := make([]uint32, len(columns))
	colTypes, ctErr := rows.ColumnTypes()
	for i := range colOIDs {
		if ctErr == nil && i < len(colTypes) {
			typeName := strings.ToUpper(colTypes[i].DatabaseTypeName())
			colOIDs[i] = sqliteTypeToOID(typeName)
		} else {
			colOIDs[i] = 25 // text
		}
	}
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	maxRows := int(eh.batchMaxRows)
	rowCount := 0
	scanDest := make([]any, len(columns))
	for i := range scanDest {
		scanDest[i] = new(sql.NullString)
	}

	suspended := false
	for rows.Next() {
		// P2 3.8: Portal-based cursor — stop after maxRows if set.
		if maxRows > 0 && rowCount >= maxRows {
			suspended = true
			break
		}
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

	if suspended {
		// PortalSuspended ('s') — more rows available.
		resp = append(resp, buildPGMsg('s', nil)...)
	} else {
		tag := fmt.Sprintf("SELECT %d", rowCount)
		resp = append(resp, buildCommandCompleteMsg(tag)...)
	}
	return resp
}

// extExecQuery executes a non-SELECT on db and returns CommandComplete.
func (eh *ExtHandler) extExecQuery(db *sql.DB, sqlStr string) []byte {
	result, err := db.Exec(sqlStr)
	if err != nil {
		if eh.txh != nil && eh.txh.InTxn() {
			eh.txh.SetErrorState()
		}
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

	// Use the existing mergedReadRows to get merged data.
	clCopy := *cl
	clCopy.RawSQL = sqlStr

	columns, rows, nulls, err := p.mergedReadRows(sqlStr, &clCopy, eh.connID)
	if err != nil {
		eh.extSendError(clientConn, err.Error())
		return
	}

	// Build extended protocol response.
	var resp []byte
	if eh.batchHasParse {
		resp = append(resp, buildParseCompleteMsg()...)
	}
	if eh.batchHasBind {
		resp = append(resp, buildBindCompleteMsg()...)
	}

	// P2 3.8: Portal-based cursor support — limit rows if maxRows > 0.
	if eh.batchMaxRows > 0 && int(eh.batchMaxRows) < len(rows) {
		limit := int(eh.batchMaxRows)
		resp = append(resp, eh.buildPortalLimitedResponse(columns, rows[:limit], nulls[:limit])...)
		resp = append(resp, buildReadyForQueryMsgState(eh.txnState())...)
		clientConn.Write(resp)
		return
	}

	// Full result — strip ReadyForQuery from buildMergedResponse since we append our own.
	mergedResp := buildMergedResponseWithoutRFQ(columns, rows, nulls)
	resp = append(resp, mergedResp...)
	resp = append(resp, buildReadyForQueryMsgState(eh.txnState())...)
	clientConn.Write(resp)
}

// buildPortalLimitedResponse builds RowDescription + limited DataRows + PortalSuspended.
// Used when Execute specifies maxRows and there are more rows available.
func (eh *ExtHandler) buildPortalLimitedResponse(columns []string, rows [][]sql.NullString, nulls [][]bool) []byte {
	colOIDs := make([]uint32, len(columns))
	for i := range colOIDs {
		colOIDs[i] = 25 // text OID
	}
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	for i, row := range rows {
		values := make([][]byte, len(columns))
		rowNulls := make([]bool, len(columns))
		for j := range columns {
			if j < len(row) {
				if i < len(nulls) && j < len(nulls[i]) && nulls[i][j] {
					rowNulls[j] = true
				} else if row[j].Valid {
					values[j] = []byte(row[j].String)
				} else {
					rowNulls[j] = true
				}
			} else {
				rowNulls[j] = true
			}
		}
		resp = append(resp, buildDataRowMsg(values, rowNulls)...)
	}

	// PortalSuspended ('s') message instead of CommandComplete.
	resp = append(resp, buildPGMsg('s', nil)...)
	return resp
}

// buildMergedResponseWithoutRFQ builds RowDescription + DataRows + CommandComplete
// without the trailing ReadyForQuery message.
func buildMergedResponseWithoutRFQ(columns []string, rows [][]sql.NullString, nulls [][]bool) []byte {
	colOIDs := make([]uint32, len(columns))
	for i := range colOIDs {
		colOIDs[i] = 25 // text OID
	}
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	for i, row := range rows {
		values := make([][]byte, len(columns))
		rowNulls := make([]bool, len(columns))
		for j := range columns {
			if j < len(row) {
				if i < len(nulls) && j < len(nulls[i]) && nulls[i][j] {
					rowNulls[j] = true
				} else if row[j].Valid {
					values[j] = []byte(row[j].String)
				} else {
					rowNulls[j] = true
				}
			} else {
				rowNulls[j] = true
			}
		}
		resp = append(resp, buildDataRowMsg(values, rowNulls)...)
	}

	tag := fmt.Sprintf("SELECT %d", len(rows))
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	return resp
}

// buildDescribeResponse generates a proper Describe response.
// P3 4.7: For portals with result sets, returns ParameterDescription + RowDescription.
// For non-SELECT, returns ParameterDescription + NoData.
func (eh *ExtHandler) buildDescribeResponse() []byte {
	// ParameterDescription: zero parameters (we don't have parameter type info at this level).
	paramDescPayload := make([]byte, 2)
	// 0 parameters
	paramDesc := buildPGMsg('t', paramDescPayload)

	sqlStr := eh.batchSQL
	if sqlStr == "" {
		return append(paramDesc, buildNoDataMsg()...)
	}

	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		(strings.HasPrefix(upper, "WITH") && !strings.Contains(upper, "INSERT") &&
			!strings.Contains(upper, "UPDATE") && !strings.Contains(upper, "DELETE"))

	if !isSelect {
		return append(paramDesc, buildNoDataMsg()...)
	}

	// Run the query with LIMIT 0 on shadow to get column metadata.
	p := eh.proxy
	db := p.shadowDB
	if db == nil {
		db = p.prodDB
	}
	rows, err := db.Query(sqlStr + " LIMIT 0")
	if err != nil {
		return append(paramDesc, buildNoDataMsg()...)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil || len(columns) == 0 {
		return append(paramDesc, buildNoDataMsg()...)
	}

	// Get type-aware OIDs.
	colOIDs := make([]uint32, len(columns))
	colTypes, ctErr := rows.ColumnTypes()
	for i := range colOIDs {
		if ctErr == nil && i < len(colTypes) {
			typeName := strings.ToUpper(colTypes[i].DatabaseTypeName())
			colOIDs[i] = sqliteTypeToOID(typeName)
		} else {
			colOIDs[i] = 25 // text
		}
	}

	return append(paramDesc, buildRowDescMsg(columns, colOIDs)...)
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

// clearBatch resets the batch accumulator for the next Sync cycle.
func (eh *ExtHandler) clearBatch() {
	eh.batch = nil
	eh.batchSQL = ""
	eh.batchParams = nil
	eh.batchHasParse = false
	eh.batchHasBind = false
	eh.batchHasDesc = false
	eh.batchHasExec = false
	eh.batchMaxRows = 0
}
