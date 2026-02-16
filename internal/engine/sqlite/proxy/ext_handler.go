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

	// Batch accumulator, cleared after each Sync.
	batch        []*pgMsg
	batchSQL     string
	batchParams  []string
	batchHasParse bool
	batchHasBind  bool
	batchHasDesc  bool
	batchHasExec  bool
}

// Accumulate adds an extended protocol message to the current batch.
func (eh *ExtHandler) Accumulate(msg *pgMsg) {
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
func (eh *ExtHandler) FlushBatch(clientConn net.Conn) {
	defer eh.clearBatch()

	p := eh.proxy

	// If no SQL found or no Execute, handle as passthrough.
	if eh.batchSQL == "" || !eh.batchHasExec {
		// For Describe-only or Close-only batches, just respond with ReadyForQuery.
		if eh.batchHasParse {
			clientConn.Write(buildParseCompleteMsg())
		}
		if eh.batchHasBind {
			clientConn.Write(buildBindCompleteMsg())
		}
		if eh.batchHasDesc {
			// For Describe on an unknown statement, send NoData.
			clientConn.Write(buildNoDataMsg())
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
		iparams := make([]interface{}, len(eh.batchParams))
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
		eh.extExecOnShadow(clientConn, fullSQL)
		if p.deltaMap != nil {
			for _, table := range cl.Tables {
				p.deltaMap.MarkInserted(table)
			}
		}

	case core.StrategyHydrateAndWrite:
		// Hydrate missing rows before write.
		if len(cl.PKs) > 0 {
			p.hydrateBeforeUpdate(cl, eh.connID)
		}
		eh.extExecOnShadow(clientConn, fullSQL)
		// Track deltas.
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
		eh.extExecOnShadow(clientConn, fullSQL)
		// Track tombstones.
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

	default:
		eh.extExecOnProd(clientConn, fullSQL)
	}
}

// extExecOnProd executes a query on prod and sends the extended protocol response.
func (eh *ExtHandler) extExecOnProd(clientConn net.Conn, sqlStr string) {
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

	colOIDs := make([]uint32, len(columns))
	for i := range colOIDs {
		colOIDs[i] = 25 // text
	}
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	rowCount := 0
	scanDest := make([]interface{}, len(columns))
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

	// Build extended protocol response.
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

// clearBatch resets the batch accumulator for the next Sync cycle.
func (eh *ExtHandler) clearBatch() {
	eh.batch = nil
	eh.batchSQL = ""
	eh.batchParams = nil
	eh.batchHasParse = false
	eh.batchHasBind = false
	eh.batchHasDesc = false
	eh.batchHasExec = false
}
