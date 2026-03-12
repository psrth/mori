package proxy

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
)

type routeTarget int

const (
	targetProd   routeTarget = iota
	targetShadow
	targetBoth
)

type routeDecision struct {
	target         routeTarget
	classification *core.Classification
	strategy       core.RoutingStrategy
}

// handleConn manages a single client connection's lifecycle.
func (p *Proxy) handleConn(clientConn net.Conn, connID int64) {
	defer p.activeConns.Done()

	clientAddr := clientConn.RemoteAddr().String()
	if p.verbose {
		log.Printf("[conn %d] opened from %s", connID, clientAddr)
	}

	// WRITE GUARD: if shadow is unavailable, refuse the connection.
	if !p.canRoute() {
		log.Printf("[conn %d] WRITE GUARD: shadow unavailable, refusing connection", connID)
		clientConn.Close()
		return
	}

	// Perform pgwire startup handshake (we act as the PG server).
	if err := p.handleStartup(clientConn); err != nil {
		log.Printf("[conn %d] startup failed: %v", connID, err)
		clientConn.Close()
		return
	}

	p.routeLoop(clientConn, connID)
}

// routeLoop is the main query routing loop for a connection.
func (p *Proxy) routeLoop(clientConn net.Conn, connID int64) {
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			clientConn.Close()
		})
	}
	defer closeAll()

	// Per-connection handlers.
	var txh *TxnHandler
	if p.deltaMap != nil && p.tombstones != nil {
		txh = &TxnHandler{proxy: p, connID: connID, txnState: 'I'}
	}

	// txnState returns the current transaction state for ReadyForQuery messages.
	txnState := func() byte {
		if txh != nil {
			return txh.TxnState()
		}
		return 'I'
	}
	_ = txnState // used below

	// Per-connection FK enforcer.
	var fkEnforcer *FKEnforcer
	if p.prodDB != nil && p.shadowDB != nil && p.schemaRegistry != nil {
		fkEnforcer = &FKEnforcer{
			prodDB:         p.prodDB,
			shadowDB:       p.shadowDB,
			deltaMap:       p.deltaMap,
			tombstones:     p.tombstones,
			tables:         p.tables,
			schemaRegistry: p.schemaRegistry,
			connID:         connID,
			verbose:        p.verbose,
		}
	}
	// fkEnforcer is used in targetShadow write paths below.

	var eh *ExtHandler
	if p.classifier != nil && p.router != nil {
		eh = &ExtHandler{
			proxy:       p,
			connID:      connID,
			txh:         txh,
			stmtCache:   make(map[string]string),
			stmtClCache: make(map[string]*core.Classification),
			fkEnforcer:  fkEnforcer,
		}
	}

	for {
		msg, err := readMsg(clientConn)
		if err != nil {
			if err != io.EOF && p.verbose {
				log.Printf("[conn %d] client read error: %v", connID, err)
			}
			return
		}

		// Terminate: close connection.
		if msg.Type == 'X' {
			if p.verbose {
				log.Printf("[conn %d] terminated", connID)
			}
			return
		}

		// Extended query protocol: accumulate messages, dispatch on Sync.
		if eh != nil && isExtendedProtocolMsg(msg.Type) {
			eh.Accumulate(msg)
			if msg.Type == 'S' {
				eh.FlushBatch(clientConn)
			}
			continue
		}

		// Simple Query ('Q'): classify and route.
		if msg.Type == 'Q' {
			sqlStr := querySQL(msg.Payload)
			if sqlStr == "" {
				clientConn.Write(buildEmptyQueryResponse())
				continue
			}

			decision := p.classifyAndRoute(sqlStr, connID)

			// Handle transaction control via TxnHandler.
			if txh != nil && decision.strategy == core.StrategyTransaction && decision.classification != nil {
				txh.HandleTxn(clientConn, decision.classification)
				continue
			}

			// Handle not-supported queries.
			if decision.strategy == core.StrategyNotSupported {
				msg := core.UnsupportedTransactionMsg
				if decision.classification != nil && decision.classification.NotSupportedMsg != "" {
					msg = decision.classification.NotSupportedMsg
				}
				clientConn.Write(buildErrorResponse(msg))
				continue
			}

			// Handle JOIN Patch via materialization: materialize dirty tables
			// into temp tables on shadow, rewrite table refs, execute on shadow.
			if decision.strategy == core.StrategyJoinPatch {
				cl := decision.classification
				resp := p.executeJoinPatch(sqlStr, cl, connID)
				clientConn.Write(resp)
				continue
			}

			// Handle merged reads: query both prod and shadow, merge in-process.
			if decision.strategy == core.StrategyMergedRead {
				cl := decision.classification
				var resp []byte
				// Dispatch to specialized handlers for complex read patterns.
				switch {
				case cl != nil && cl.HasWindowFunc:
					resp = p.executeWindowRead(sqlStr, cl, connID)
				case cl != nil && cl.HasSetOp:
					resp = p.executeSetOpRead(sqlStr, cl, connID)
				case cl != nil && cl.IsComplexRead:
					resp = p.executeComplexRead(sqlStr, cl, connID)
				default:
					resp = p.executeMergedRead(sqlStr, cl, connID)
				}
				clientConn.Write(resp)
				continue
			}

			switch decision.target {
			case targetProd:
				// WRITE GUARD L3: final check before prod dispatch.
				if decision.classification != nil &&
					(decision.classification.OpType == core.OpWrite || decision.classification.OpType == core.OpDDL) {
					log.Printf("[CRITICAL] [conn %d] WRITE GUARD L3: %s/%s reached targetProd — BLOCKED",
						connID, decision.classification.OpType, decision.classification.SubType)
					clientConn.Write(buildErrorResponse("mori: write operation blocked — internal routing error detected"))
					continue
				}
				resp := p.executeQuery(p.prodDB, sqlStr, connID)
				clientConn.Write(resp)

			case targetShadow:
				cl := decision.classification

				// Handle TRUNCATE.
				if decision.strategy == core.StrategyTruncate && cl != nil {
					resp := p.handleTruncate(sqlStr, cl, connID)
					clientConn.Write(resp)
					continue
				}

				// FK enforcement before writes (simple query protocol).
				if fkEnforcer != nil && cl != nil &&
					(decision.strategy == core.StrategyShadowWrite ||
						decision.strategy == core.StrategyHydrateAndWrite ||
						decision.strategy == core.StrategyShadowDelete) {
					if errMsg := p.checkFKConstraints(fkEnforcer, cl, sqlStr); errMsg != "" {
						clientConn.Write(buildErrorResponse(errMsg))
						continue
					}
				}

				// For HydrateAndWrite (UPDATE/upsert), handle bulk case if no PKs.
				if decision.strategy == core.StrategyHydrateAndWrite && cl != nil {
					if len(cl.PKs) == 0 && cl.SubType == core.SubUpdate {
						resp := p.handleBulkUpdate(sqlStr, cl, connID, txh)
						clientConn.Write(resp)
						continue
					}
					// Hydrate for INSERT ON CONFLICT (upsert): the conflicting row
					// may only exist in prod (not yet in shadow delta). Hydrate it
					// so the ON CONFLICT clause can detect and update it.
					if cl.SubType == core.SubInsert && cl.HasOnConflict && len(cl.PKs) > 0 {
						p.hydrateBeforeUpdate(cl, connID)
					}
					if cl.SubType == core.SubUpdate {
						p.hydrateBeforeUpdate(cl, connID)
					}
				}

				// For ShadowDelete, handle bulk case if no PKs.
				if decision.strategy == core.StrategyShadowDelete && cl != nil {
					if len(cl.PKs) == 0 && !cl.IsJoin && len(cl.Tables) == 1 {
						if meta, ok := p.tables[cl.Tables[0]]; ok && len(meta.PKColumns) > 0 {
							resp := p.handleBulkDelete(sqlStr, cl, connID, txh)
							clientConn.Write(resp)
							continue
						}
					}
				}

				resp := p.executeQuery(p.shadowDB, sqlStr, connID)
				clientConn.Write(resp)

				// Track deltas/tombstones/schema after successful writes.
				if cl != nil {
					p.trackWriteEffects(cl, decision.strategy, connID, txh)
				}

			case targetBoth:
				// Execute on shadow first (discard result), then prod.
				p.executeQuery(p.shadowDB, sqlStr, connID)
				resp := p.executeQuery(p.prodDB, sqlStr, connID)
				clientConn.Write(resp)
			}
			continue
		}

		// Unknown message: send error.
		clientConn.Write(buildErrorResponse(fmt.Sprintf("mori-duckdb: unsupported message type '%c'", msg.Type)))
	}
}

// classifyAndRoute determines which backend should handle a query.
func (p *Proxy) classifyAndRoute(sqlStr string, connID int64) routeDecision {
	if sqlStr == "" {
		return routeDecision{target: targetProd}
	}

	classification, err := p.classifier.Classify(sqlStr)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] classify error, forwarding to prod: %v", connID, err)
		}
		return routeDecision{target: targetProd}
	}

	strategy := p.router.Route(classification)

	// WRITE GUARD L1: validate routing decision.
	if err := validateRouteDecision(classification, strategy, connID, p.logger); err != nil {
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       core.StrategyShadowWrite,
		}
	}

	if p.verbose {
		log.Printf("[conn %d] %s/%s tables=%v → %s | %s",
			connID, classification.OpType, classification.SubType,
			classification.Tables, strategy, truncateSQL(sqlStr, 100))
	}

	p.logger.Query(connID, sqlStr, classification, strategy, 0)

	switch strategy {
	case core.StrategyShadowWrite,
		core.StrategyHydrateAndWrite,
		core.StrategyShadowDelete,
		core.StrategyTruncate:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyShadowDDL:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyTransaction:
		return routeDecision{target: targetBoth, classification: classification, strategy: strategy}

	case core.StrategyForwardBoth:
		return routeDecision{target: targetBoth, classification: classification, strategy: strategy}

	case core.StrategyNotSupported:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		return routeDecision{
			target:         targetProd,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyProdDirect:
		return routeDecision{target: targetProd, classification: classification, strategy: strategy}

	default:
		return routeDecision{target: targetProd, classification: classification, strategy: core.StrategyNotSupported}
	}
}

// executeQuery runs a SQL query against a database and returns the pgwire response bytes.
func (p *Proxy) executeQuery(db *sql.DB, sqlStr string, connID int64) []byte {
	// WRITE GUARD L2: if this is the Prod database, block writes.
	if db == p.prodDB {
		if blocked, ok := safeProdExec(p.prodDB, sqlStr, connID, p.logger, p.verbose); ok {
			return blocked
		}
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
		return p.executeSelectQuery(db, sqlStr, connID)
	}
	return p.executeExecQuery(db, sqlStr, connID)
}

// executeSelectQuery handles queries that return rows.
func (p *Proxy) executeSelectQuery(db *sql.DB, sqlStr string, connID int64) []byte {
	rows, err := db.Query(sqlStr)
	if err != nil {
		return buildSQLErrorResponse(err.Error())
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return buildSQLErrorResponse(err.Error())
	}

	// Map DuckDB column types to PostgreSQL OIDs.
	colOIDs := resolveColumnOIDs(rows)
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	rowCount := 0
	scanDest := make([]interface{}, len(columns))
	for i := range scanDest {
		scanDest[i] = new(sql.NullString)
	}

	for rows.Next() {
		if err := rows.Scan(scanDest...); err != nil {
			return buildSQLErrorResponse(err.Error())
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
	if err := rows.Err(); err != nil {
		return buildSQLErrorResponse(err.Error())
	}

	tag := fmt.Sprintf("SELECT %d", rowCount)
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	resp = append(resp, buildReadyForQueryMsg()...)
	return resp
}

// executeExecQuery handles queries that don't return rows.
func (p *Proxy) executeExecQuery(db *sql.DB, sqlStr string, connID int64) []byte {
	result, err := db.Exec(sqlStr)
	if err != nil {
		return buildSQLErrorResponse(err.Error())
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
	case strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "START"):
		tag = "BEGIN"
	case strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "END"):
		tag = "COMMIT"
	case strings.HasPrefix(upper, "ROLLBACK"):
		tag = "ROLLBACK"
	default:
		tag = "OK"
	}

	var resp []byte
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	resp = append(resp, buildReadyForQueryMsg()...)
	return resp
}

// executeMergedRead queries both prod and shadow databases and merges results.
func (p *Proxy) executeMergedRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if cl == nil || len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}
	table := cl.Tables[0]

	if cl.HasAggregate {
		return p.executeAggregateRead(sqlStr, cl, connID)
	}

	// Check if table only exists in shadow.
	if p.schemaRegistry != nil {
		diff := p.schemaRegistry.GetDiff(table)
		if diff != nil && diff.IsNewTable {
			return p.executeQuery(p.shadowDB, sqlStr, connID)
		}
		if _, exists := p.tables[table]; !exists {
			return p.executeQuery(p.shadowDB, sqlStr, connID)
		}
	}

	// PK injection — also supports rowid for PK-less tables (DuckDB implicit column).
	injectedPK := ""
	useRowID := false
	effectiveSQL := sqlStr
	if !cl.IsJoin {
		if meta, ok := p.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(sqlStr, pkCol) {
				effectiveSQL = injectPKColumn(sqlStr, pkCol)
				injectedPK = pkCol
			}
		} else if _, ok := p.tables[table]; ok {
			// PK-less table: inject DuckDB's implicit rowid for dedup.
			if needsPKInjection(sqlStr, "rowid") {
				effectiveSQL = injectPKColumn(sqlStr, "rowid")
				injectedPK = "rowid"
				useRowID = true
			}
		}
	}
	_ = useRowID // rowid is used as dedup key below

	// Query shadow.
	shadowCols, shadowRows, shadowNulls, shadowErr := queryToRows(p.shadowDB, effectiveSQL)
	if shadowErr != nil {
		if p.verbose {
			log.Printf("[conn %d] merged read: shadow query error: %v", connID, shadowErr)
		}
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Rewrite Prod query for schema diffs (DDL changes).
	prodSQL := effectiveSQL
	skipProd := false
	if p.schemaRegistry != nil {
		rewritten, shouldSkip := rewriteSQLForProd(prodSQL, p.schemaRegistry, cl.Tables)
		if shouldSkip {
			skipProd = true
		} else {
			prodSQL = rewritten
		}
	}

	// Check if table is fully shadowed (e.g., after TRUNCATE) — skip Prod entirely.
	if !skipProd && p.schemaRegistry != nil && p.schemaRegistry.IsFullyShadowed(table) {
		skipProd = true
	}

	if skipProd {
		return buildMergedResponse(shadowCols, shadowRows, shadowNulls)
	}

	// Query prod (with overfetch for LIMIT queries).
	if cl.HasLimit && cl.Limit > 0 {
		deltaCount := p.deltaMap.CountForTable(table)
		tombstoneCount := p.tombstones.CountForTable(table)
		overfetch := deltaCount + tombstoneCount
		if overfetch > 0 {
			prodSQL = rewriteLimit(prodSQL, cl.Limit+overfetch)
		}
	}

	prodCols, prodRows, prodNulls, prodErr := queryToRows(p.prodDB, prodSQL)
	if prodErr != nil {
		if p.verbose {
			log.Printf("[conn %d] merged read: prod query error: %v", connID, prodErr)
		}
		if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
			return buildMergedResponse(shadowCols, shadowRows, shadowNulls)
		}
		return buildSQLErrorResponse(prodErr.Error())
	}

	// Find PK column index — for PK-less tables, use rowid if injected.
	meta, hasMeta := p.tables[table]
	pkIdx := -1
	if hasMeta && len(meta.PKColumns) > 0 {
		pkCol := meta.PKColumns[0]
		for i, col := range shadowCols {
			if col == pkCol {
				pkIdx = i
				break
			}
		}
		if pkIdx < 0 {
			for i, col := range prodCols {
				if col == pkCol {
					pkIdx = i
					break
				}
			}
		}
	} else if injectedPK == "rowid" {
		// PK-less table: use injected rowid for dedup.
		for i, col := range shadowCols {
			if col == "rowid" {
				pkIdx = i
				break
			}
		}
	}

	// Filter prod rows.
	var filteredProd [][]sql.NullString
	var filteredProdNulls [][]bool
	if pkIdx >= 0 {
		for i, row := range prodRows {
			if pkIdx < len(row) {
				pk := ""
				if row[pkIdx].Valid {
					pk = row[pkIdx].String
				}
				if p.deltaMap.IsDelta(table, pk) {
					continue
				}
				if p.tombstones.IsTombstoned(table, pk) {
					continue
				}
			}
			filteredProd = append(filteredProd, row)
			filteredProdNulls = append(filteredProdNulls, prodNulls[i])
		}
	} else {
		filteredProd = prodRows
		filteredProdNulls = prodNulls
	}

	// Schema adaptation: adjust Prod columns/rows for schema diffs.
	adaptedProdCols := adaptColumns(p.schemaRegistry, table, prodCols)
	filteredProd, filteredProdNulls = adaptRows(p.schemaRegistry, table, prodCols, filteredProd, filteredProdNulls)
	_ = adaptedProdCols // used for column alignment below

	// Merge — shadow first, then filtered prod.
	merged := append(shadowRows, filteredProd...)
	mergedNulls := append(shadowNulls, filteredProdNulls...)

	// Dedup by PK, or by full-row hash for PK-less tables.
	if pkIdx >= 0 {
		seen := make(map[string]bool)
		var deduped [][]sql.NullString
		var dedupedNulls [][]bool
		for i, row := range merged {
			if pkIdx < len(row) {
				pk := ""
				if row[pkIdx].Valid {
					pk = row[pkIdx].String
				}
				if seen[pk] {
					continue
				}
				seen[pk] = true
			}
			deduped = append(deduped, row)
			dedupedNulls = append(dedupedNulls, mergedNulls[i])
		}
		merged = deduped
		mergedNulls = dedupedNulls
	} else {
		// PK-less table: deduplicate by full-row hash (shadow wins on collision).
		merged, mergedNulls = deduplicateByFullRow(merged, mergedNulls)
	}

	// Re-sort.
	if cl.OrderBy != "" {
		sortMergedRows(shadowCols, prodCols, merged, mergedNulls, cl.OrderBy)
	}

	// Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(merged) > cl.Limit {
		merged = merged[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	columns := shadowCols
	if len(columns) == 0 {
		columns = prodCols
	}

	// Strip injected PK.
	if injectedPK != "" {
		columns, merged, mergedNulls = stripInjectedPKColumn(columns, merged, mergedNulls, injectedPK)
	}

	return buildMergedResponse(columns, merged, mergedNulls)
}

// executeAggregateRead handles aggregate queries (COUNT, SUM, AVG, MIN, MAX,
// GROUP BY, HAVING, and complex aggregates) via materialization.
// It materializes merged base data into a temp table on shadow, then executes
// the original aggregate query against the temp table, letting DuckDB handle
// the aggregation natively.
func (p *Proxy) executeAggregateRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}
	table := cl.Tables[0]

	// If the table has no dirty data, execute on prod directly.
	if !p.isTableAffected(table) {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Build a base query that fetches all rows (no aggregation):
	// SELECT * FROM <table> WHERE <original conditions>
	baseSQL := buildAggregateBaseSQL(sqlStr, table)
	if baseSQL == "" {
		// Can't decompose — fall back to shadow (has merged data from file copy).
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}

	baseCl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{table},
		RawSQL:  baseSQL,
	}

	// Materialize merged base data into a temp table.
	utilName, err := p.materializeToUtilTable(baseSQL, baseCl, connID)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] aggregate materialization failed: %v", connID, err)
		}
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	defer p.dropUtilTable(utilName)

	// Rewrite the original aggregate query to reference the temp table.
	rewrittenSQL := rewriteTableRef(sqlStr, table, utilName)

	if p.verbose {
		log.Printf("[conn %d] aggregate read: materialized %s, executing on shadow", connID, table)
	}

	// Execute the full aggregate query on shadow with DuckDB handling all
	// aggregation (COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING, etc.) natively.
	return p.executeQuery(p.shadowDB, rewrittenSQL, connID)
}

// buildAggregateBaseSQL builds a SELECT * FROM <table> WHERE ... query from an
// aggregate query. Strips SELECT list, GROUP BY, HAVING, ORDER BY, LIMIT —
// keeps only FROM and WHERE to get the base row set.
func buildAggregateBaseSQL(sqlStr, table string) string {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	fromIdx := findOuterFromIndex(upper)
	if fromIdx < 0 {
		return ""
	}

	// Take everything from FROM onwards.
	rest := sqlStr[fromIdx:]

	// Strip GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, FETCH.
	restUpper := strings.ToUpper(rest)
	for _, kw := range []string{"GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET", "FETCH"} {
		if idx := strings.Index(restUpper, " "+kw); idx >= 0 {
			rest = rest[:idx]
			restUpper = restUpper[:idx]
		}
	}

	return fmt.Sprintf("SELECT * %s", strings.TrimSpace(rest))
}

// mergedReadRows is an internal version that returns merged columns, rows, and nulls.
func (p *Proxy) mergedReadRows(sqlStr string, cl *core.Classification, connID int64) (
	columns []string, rows [][]sql.NullString, nulls [][]bool, err error,
) {
	if cl == nil || len(cl.Tables) == 0 {
		cols, r, n, e := queryToRows(p.prodDB, sqlStr)
		return cols, r, n, e
	}
	table := cl.Tables[0]

	if p.schemaRegistry != nil {
		diff := p.schemaRegistry.GetDiff(table)
		if diff != nil && diff.IsNewTable {
			return queryToRows(p.shadowDB, sqlStr)
		}
		if _, exists := p.tables[table]; !exists {
			return queryToRows(p.shadowDB, sqlStr)
		}
	}

	injectedPK := ""
	effectiveSQL := sqlStr
	if !cl.IsJoin {
		if meta, ok := p.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(sqlStr, pkCol) {
				effectiveSQL = injectPKColumn(sqlStr, pkCol)
				injectedPK = pkCol
			}
		} else if _, ok := p.tables[table]; ok {
			// PK-less table: inject rowid for dedup.
			if needsPKInjection(sqlStr, "rowid") {
				effectiveSQL = injectPKColumn(sqlStr, "rowid")
				injectedPK = "rowid"
			}
		}
	}

	shadowCols, shadowRows, shadowNulls, shadowErr := queryToRows(p.shadowDB, effectiveSQL)
	if shadowErr != nil {
		return queryToRows(p.prodDB, sqlStr)
	}

	// Rewrite Prod query for schema diffs.
	prodSQL := effectiveSQL
	skipProdInternal := false
	if p.schemaRegistry != nil {
		rewritten, shouldSkip := rewriteSQLForProd(prodSQL, p.schemaRegistry, cl.Tables)
		if shouldSkip {
			skipProdInternal = true
		} else {
			prodSQL = rewritten
		}
	}
	if !skipProdInternal && p.schemaRegistry != nil && p.schemaRegistry.IsFullyShadowed(table) {
		skipProdInternal = true
	}

	if skipProdInternal {
		if injectedPK != "" {
			shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, injectedPK)
		}
		return shadowCols, shadowRows, shadowNulls, nil
	}

	prodSQL = p.capSQL(prodSQL)
	prodCols, prodRows, prodNulls, prodErr := queryToRows(p.prodDB, prodSQL)
	if prodErr != nil {
		if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
			if injectedPK != "" {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, injectedPK)
			}
			return shadowCols, shadowRows, shadowNulls, nil
		}
		return nil, nil, nil, prodErr
	}

	meta, hasMeta := p.tables[table]
	pkIdx := -1
	if hasMeta && len(meta.PKColumns) > 0 {
		pkCol := meta.PKColumns[0]
		for i, col := range shadowCols {
			if col == pkCol {
				pkIdx = i
				break
			}
		}
		if pkIdx < 0 {
			for i, col := range prodCols {
				if col == pkCol {
					pkIdx = i
					break
				}
			}
		}
	} else if injectedPK == "rowid" {
		// PK-less table: use injected rowid for dedup.
		for i, col := range shadowCols {
			if col == "rowid" {
				pkIdx = i
				break
			}
		}
	}

	var filteredProd [][]sql.NullString
	var filteredProdNulls [][]bool
	if pkIdx >= 0 {
		for i, row := range prodRows {
			if pkIdx < len(row) {
				pk := ""
				if row[pkIdx].Valid {
					pk = row[pkIdx].String
				}
				if p.deltaMap.IsDelta(table, pk) {
					continue
				}
				if p.tombstones.IsTombstoned(table, pk) {
					continue
				}
			}
			filteredProd = append(filteredProd, row)
			filteredProdNulls = append(filteredProdNulls, prodNulls[i])
		}
	} else {
		filteredProd = prodRows
		filteredProdNulls = prodNulls
	}

	// Schema adaptation for Prod rows.
	adaptedProdCols := adaptColumns(p.schemaRegistry, table, prodCols)
	filteredProd, filteredProdNulls = adaptRows(p.schemaRegistry, table, prodCols, filteredProd, filteredProdNulls)
	_ = adaptedProdCols

	merged := append(shadowRows, filteredProd...)
	mergedNulls := append(shadowNulls, filteredProdNulls...)

	if pkIdx >= 0 {
		seen := make(map[string]bool)
		var deduped [][]sql.NullString
		var dedupedNulls [][]bool
		for i, row := range merged {
			if pkIdx < len(row) {
				pk := ""
				if row[pkIdx].Valid {
					pk = row[pkIdx].String
				}
				if seen[pk] {
					continue
				}
				seen[pk] = true
			}
			deduped = append(deduped, row)
			dedupedNulls = append(dedupedNulls, mergedNulls[i])
		}
		merged = deduped
		mergedNulls = dedupedNulls
	} else {
		// PK-less table: deduplicate by full-row hash.
		merged, mergedNulls = deduplicateByFullRow(merged, mergedNulls)
	}

	cols := shadowCols
	if len(cols) == 0 {
		cols = prodCols
	}

	if injectedPK != "" {
		cols, merged, mergedNulls = stripInjectedPKColumn(cols, merged, mergedNulls, injectedPK)
	}

	return cols, merged, mergedNulls, nil
}

// resolveColumnOIDs uses sql.ColumnType metadata to map DuckDB types to PostgreSQL OIDs.
func resolveColumnOIDs(rows *sql.Rows) []uint32 {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		// Fallback to all-text.
		cols, _ := rows.Columns()
		oids := make([]uint32, len(cols))
		for i := range oids {
			oids[i] = 25
		}
		return oids
	}
	oids := make([]uint32, len(colTypes))
	for i, ct := range colTypes {
		oids[i] = duckDBTypeToOID(ct.DatabaseTypeName())
	}
	return oids
}

// queryToRows executes a SELECT query and returns columns, rows, and null flags.
func queryToRows(db *sql.DB, sqlStr string) (columns []string, rows [][]sql.NullString, nulls [][]bool, err error) {
	sqlRows, err := db.Query(sqlStr)
	if err != nil {
		return nil, nil, nil, err
	}
	defer sqlRows.Close()

	columns, err = sqlRows.Columns()
	if err != nil {
		return nil, nil, nil, err
	}

	for sqlRows.Next() {
		dest := make([]sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			return columns, rows, nulls, err
		}
		rowNulls := make([]bool, len(columns))
		for i, d := range dest {
			rowNulls[i] = !d.Valid
		}
		rows = append(rows, dest)
		nulls = append(nulls, rowNulls)
	}
	if err := sqlRows.Err(); err != nil {
		return columns, rows, nulls, err
	}
	return columns, rows, nulls, nil
}

// buildMergedResponse constructs the full pgwire response from merged results.
func buildMergedResponse(columns []string, rows [][]sql.NullString, nulls [][]bool) []byte {
	colOIDs := make([]uint32, len(columns))
	for i := range colOIDs {
		colOIDs[i] = 25
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
	resp = append(resp, buildReadyForQueryMsg()...)
	return resp
}

func rewriteLimit(sqlStr string, newLimit int) string {
	upper := strings.ToUpper(sqlStr)
	idx := strings.LastIndex(upper, "LIMIT")
	if idx < 0 {
		return sqlStr
	}
	afterLimit := idx + 5
	rest := sqlStr[afterLimit:]
	trimmed := strings.TrimLeft(rest, " \t")
	whitespace := len(rest) - len(trimmed)

	numEnd := 0
	for numEnd < len(trimmed) && trimmed[numEnd] >= '0' && trimmed[numEnd] <= '9' {
		numEnd++
	}
	if numEnd == 0 {
		return sqlStr
	}

	return sqlStr[:afterLimit] + rest[:whitespace] + strconv.Itoa(newLimit) + trimmed[numEnd:]
}

type mergedOrderCol struct {
	idx  int
	desc bool
}

func sortMergedRows(shadowCols, prodCols []string, rows [][]sql.NullString, nulls [][]bool, orderBy string) {
	columns := shadowCols
	if len(columns) == 0 {
		columns = prodCols
	}
	if len(columns) == 0 || len(rows) == 0 {
		return
	}

	orderBy = strings.TrimRight(orderBy, "; \t\n")
	parts := strings.Split(orderBy, ",")
	var resolved []mergedOrderCol
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) == 0 {
			continue
		}
		colName := strings.Trim(fields[0], `"`)
		if dotIdx := strings.LastIndex(colName, "."); dotIdx >= 0 {
			colName = colName[dotIdx+1:]
		}
		desc := len(fields) >= 2 && strings.EqualFold(fields[1], "DESC")
		for i, c := range columns {
			if strings.EqualFold(c, colName) {
				resolved = append(resolved, mergedOrderCol{idx: i, desc: desc})
				break
			}
		}
	}
	if len(resolved) == 0 {
		return
	}

	indices := make([]int, len(rows))
	for i := range indices {
		indices[i] = i
	}

	for i := 1; i < len(indices); i++ {
		for j := i; j > 0; j-- {
			if !mergedLess(rows, indices[j], indices[j-1], resolved) {
				break
			}
			indices[j], indices[j-1] = indices[j-1], indices[j]
		}
	}

	sortedRows := make([][]sql.NullString, len(rows))
	sortedNulls := make([][]bool, len(nulls))
	for i, idx := range indices {
		sortedRows[i] = rows[idx]
		sortedNulls[i] = nulls[idx]
	}
	copy(rows, sortedRows)
	copy(nulls, sortedNulls)
}

func mergedLess(rows [][]sql.NullString, a, b int, order []mergedOrderCol) bool {
	for _, oc := range order {
		va, vb := "", ""
		if oc.idx < len(rows[a]) && rows[a][oc.idx].Valid {
			va = rows[a][oc.idx].String
		}
		if oc.idx < len(rows[b]) && rows[b][oc.idx].Valid {
			vb = rows[b][oc.idx].String
		}
		cmp := compareMergedValues(va, vb)
		if cmp == 0 {
			continue
		}
		if oc.desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

func compareMergedValues(a, b string) int {
	na, errA := strconv.ParseFloat(a, 64)
	nb, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		if na < nb {
			return -1
		}
		if na > nb {
			return 1
		}
		return 0
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// extractInsertedPKs attempts to extract inserted PK values from an INSERT statement.
// For auto-increment tables, it queries Shadow for the last inserted row.
// For explicit PK values in the INSERT, it extracts them from the SQL.
func (p *Proxy) extractInsertedPKs(sqlStr, table string, connID int64) []string {
	meta, ok := p.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return nil
	}
	pkCol := meta.PKColumns[0]

	// For serial/bigserial PKs, query shadow for the last inserted PK.
	if meta.PKType == "serial" || meta.PKType == "bigserial" {
		query := fmt.Sprintf(`SELECT MAX("%s") FROM "%s"`, pkCol, table)
		var maxPK sql.NullString
		if err := p.shadowDB.QueryRow(query).Scan(&maxPK); err == nil && maxPK.Valid {
			return []string{maxPK.String}
		}
	}

	// Try to extract PK from VALUES clause.
	pks := extractPKFromInsertValues(sqlStr, table, pkCol, meta.PKColumns)
	return pks
}

// extractPKFromInsertValues tries to extract PK values from an INSERT statement's
// VALUES clause by matching the PK column position.
func extractPKFromInsertValues(sqlStr, table, pkCol string, pkColumns []string) []string {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Find column list: INSERT INTO table (col1, col2, ...) VALUES (...)
	parenStart := strings.Index(upper, "(")
	valuesIdx := strings.Index(upper, "VALUES")
	if parenStart < 0 || valuesIdx < 0 || parenStart > valuesIdx {
		return nil
	}

	// Extract column list.
	parenEnd := strings.Index(upper[:valuesIdx], ")")
	if parenEnd < 0 {
		return nil
	}
	colList := sqlStr[parenStart+1 : parenEnd]
	cols := strings.Split(colList, ",")

	// Find PK column index.
	pkIdx := -1
	for i, col := range cols {
		trimmed := strings.Trim(strings.TrimSpace(col), `"'`)
		if strings.EqualFold(trimmed, pkCol) {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return nil
	}

	// Extract VALUES tuples.
	valuesStart := strings.Index(upper[valuesIdx:], "(")
	if valuesStart < 0 {
		return nil
	}
	valuesPart := sqlStr[valuesIdx+valuesStart:]

	// Parse value tuples (handles multiple VALUES (...), (...))
	var pks []string
	depth := 0
	tupleStart := -1
	for i, ch := range valuesPart {
		if ch == '(' {
			if depth == 0 {
				tupleStart = i + 1
			}
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 && tupleStart >= 0 {
				tuple := valuesPart[tupleStart:i]
				vals := splitValuesTuple(tuple)
				if pkIdx < len(vals) {
					val := strings.TrimSpace(vals[pkIdx])
					val = strings.Trim(val, "'")
					if val != "" && !strings.EqualFold(val, "NULL") && !strings.EqualFold(val, "DEFAULT") {
						pks = append(pks, val)
					}
				}
				tupleStart = -1
			}
		}
	}
	return pks
}

// splitValuesTuple splits a VALUES tuple by commas, respecting parentheses and quotes.
func splitValuesTuple(s string) []string {
	var parts []string
	depth := 0
	inQuote := false
	start := 0
	for i, ch := range s {
		switch {
		case ch == '\'' && !inQuote:
			inQuote = true
		case ch == '\'' && inQuote:
			inQuote = false
		case ch == '(' && !inQuote:
			depth++
		case ch == ')' && !inQuote:
			depth--
		case ch == ',' && depth == 0 && !inQuote:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// handleTruncate executes TRUNCATE on Shadow and marks the table as fully shadowed.
func (p *Proxy) handleTruncate(sqlStr string, cl *core.Classification, connID int64) []byte {
	// Execute TRUNCATE on Shadow.
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)

	// Mark each truncated table as fully shadowed and clear delta/tombstone state.
	for _, table := range cl.Tables {
		if p.schemaRegistry != nil {
			p.schemaRegistry.MarkFullyShadowed(table)
		}
		if p.deltaMap != nil {
			p.deltaMap.ClearTable(table)
		}
		if p.tombstones != nil {
			p.tombstones.ClearTable(table)
		}
		if p.verbose {
			log.Printf("[conn %d] TRUNCATE: table %s marked fully shadowed", connID, table)
		}
	}

	// Persist state.
	if p.schemaRegistry != nil {
		coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry)
	}
	if p.deltaMap != nil {
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}
	if p.tombstones != nil {
		delta.WriteTombstoneSet(p.moriDir, p.tombstones)
	}

	p.logger.Event(connID, "truncate", fmt.Sprintf("tables=%v", cl.Tables))
	return resp
}

// checkFKConstraints enforces FK constraints before a write operation in the simple query path.
// Returns an error string if the FK constraint would be violated, or "" if OK.
func (p *Proxy) checkFKConstraints(fkEnforcer *FKEnforcer, cl *core.Classification, sqlStr string) string {
	if fkEnforcer == nil || cl == nil {
		return ""
	}
	return fkEnforcer.CheckWriteFK(cl, sqlStr)
}

// hydrateBeforeUpdate copies affected rows from prod to shadow before an UPDATE.
func (p *Proxy) hydrateBeforeUpdate(cl *core.Classification, connID int64) {
	for _, pk := range cl.PKs {
		if p.deltaMap.IsDelta(pk.Table, pk.PK) {
			continue
		}
		meta, ok := p.tables[pk.Table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]
		escapedPK := strings.ReplaceAll(pk.PK, "'", "''")
		selectSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" = '%s'`, pk.Table, pkCol, escapedPK)
		cols, rows, _, err := queryToRows(p.prodDB, selectSQL)
		if err != nil || len(rows) == 0 {
			continue
		}
		var genCols []string
		if meta, ok := p.tables[pk.Table]; ok {
			genCols = meta.GeneratedCols
		}
		insertSQL := buildHydrateInsert(pk.Table, cols, rows[0], genCols)
		if _, err := p.shadowDB.Exec(insertSQL); err != nil {
			if p.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", connID, pk.Table, pk.PK, err)
			}
		}
	}
}

// buildHydrateInsert constructs an INSERT OR REPLACE statement.
// DuckDB uses INSERT OR REPLACE syntax for upserts.
// Generated columns are excluded since they cannot be written to directly.
func buildHydrateInsert(table string, cols []string, row []sql.NullString, generatedCols []string) string {
	genSet := make(map[string]bool, len(generatedCols))
	for _, g := range generatedCols {
		genSet[strings.ToLower(g)] = true
	}

	var quotedCols []string
	var values []string
	for i, c := range cols {
		if genSet[strings.ToLower(c)] {
			continue
		}
		quotedCols = append(quotedCols, `"`+c+`"`)
		if i < len(row) && row[i].Valid {
			values = append(values, "'"+strings.ReplaceAll(row[i].String, "'", "''")+"'")
		} else {
			values = append(values, "NULL")
		}
	}
	return fmt.Sprintf(`INSERT OR REPLACE INTO "%s" (%s) VALUES (%s)`,
		table, strings.Join(quotedCols, ", "), strings.Join(values, ", "))
}

// trackWriteEffects updates delta/tombstone state after a write operation.
func (p *Proxy) trackWriteEffects(cl *core.Classification, strategy core.RoutingStrategy, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	switch strategy {
	case core.StrategyShadowWrite:
		// For INSERTs, try to extract individual PKs for row-level tracking.
		if cl.SubType == core.SubInsert && len(cl.Tables) > 0 {
			table := cl.Tables[0]
			inserted := p.extractInsertedPKs(cl.RawSQL, table, connID)
			if len(inserted) > 0 {
				for _, pk := range inserted {
					if inTxn {
						p.deltaMap.Stage(table, pk)
					} else {
						p.deltaMap.Add(table, pk)
					}
				}
				if !inTxn {
					delta.WriteDeltaMap(p.moriDir, p.deltaMap)
				}
			} else {
				// Fallback to table-level tracking.
				for _, table := range cl.Tables {
					p.deltaMap.MarkInserted(table)
				}
			}
		} else {
			for _, table := range cl.Tables {
				p.deltaMap.MarkInserted(table)
			}
		}

	case core.StrategyHydrateAndWrite:
		for _, pk := range cl.PKs {
			if inTxn {
				p.deltaMap.Stage(pk.Table, pk.PK)
			} else {
				p.deltaMap.Add(pk.Table, pk.PK)
			}
		}
		if len(cl.PKs) == 0 {
			for _, table := range cl.Tables {
				p.deltaMap.MarkInserted(table)
			}
		}
		if !inTxn {
			if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
				if p.verbose {
					log.Printf("[conn %d] failed to persist delta map: %v", connID, err)
				}
			}
		}

	case core.StrategyShadowDelete:
		for _, pk := range cl.PKs {
			if inTxn {
				p.tombstones.Stage(pk.Table, pk.PK)
			} else {
				p.tombstones.Add(pk.Table, pk.PK)
				p.deltaMap.Remove(pk.Table, pk.PK)
			}
		}
		if len(cl.PKs) == 0 {
			for _, table := range cl.Tables {
				p.deltaMap.MarkInserted(table)
			}
		}
		if !inTxn {
			if err := delta.WriteTombstoneSet(p.moriDir, p.tombstones); err != nil {
				if p.verbose {
					log.Printf("[conn %d] failed to persist tombstone set: %v", connID, err)
				}
			}
			if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
				if p.verbose {
					log.Printf("[conn %d] failed to persist delta map: %v", connID, err)
				}
			}
		}

	case core.StrategyShadowDDL:
		p.trackDDLEffects(cl, connID)
	}
}

// trackDDLEffects updates the schema registry after a DDL statement.
func (p *Proxy) trackDDLEffects(cl *core.Classification, connID int64) {
	if p.schemaRegistry == nil {
		return
	}
	sqlStr := cl.RawSQL
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	switch {
	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, " RENAME TO ") && !strings.Contains(upper, "RENAME COLUMN"):
		// Table rename: ALTER TABLE <old> RENAME TO <new>
		oldName := ""
		if len(cl.Tables) > 0 {
			oldName = cl.Tables[0]
		} else {
			fields := strings.Fields(sqlStr)
			if len(fields) >= 3 {
				oldName = strings.Trim(fields[2], `"'`)
			}
		}
		newName := ""
		fields := strings.Fields(sqlStr)
		for i, f := range fields {
			if strings.EqualFold(f, "RENAME") && i+2 < len(fields) && strings.EqualFold(fields[i+1], "TO") {
				newName = strings.Trim(fields[i+2], `"';`)
				break
			}
		}
		if oldName != "" && newName != "" {
			p.schemaRegistry.RemoveTable(oldName)
			p.schemaRegistry.RecordNewTable(newName)
			if p.deltaMap != nil {
				p.deltaMap.RenameTable(oldName, newName)
			}
			if p.tombstones != nil {
				p.tombstones.RenameTable(oldName, newName)
			}
			if p.verbose {
				log.Printf("[conn %d] schema registry: RENAME TABLE %s → %s", connID, oldName, newName)
			}
		}

	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "RENAME COLUMN"):
		table, oldName, newName := parseAlterRenameColumn(sqlStr)
		if table != "" && oldName != "" && newName != "" {
			p.schemaRegistry.RecordRenameColumn(table, oldName, newName)
			if p.verbose {
				log.Printf("[conn %d] schema registry: RENAME COLUMN %s.%s → %s", connID, table, oldName, newName)
			}
		}

	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD COLUMN"):
		table, col, colType := parseAlterAddColumn(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordAddColumn(table, coreSchema.Column{Name: col, Type: colType})
			if p.verbose {
				log.Printf("[conn %d] schema registry: ADD COLUMN %s.%s (%s)", connID, table, col, colType)
			}
		}

	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "DROP COLUMN"):
		table, col := parseAlterDropColumn(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordDropColumn(table, col)
			if p.verbose {
				log.Printf("[conn %d] schema registry: DROP COLUMN %s.%s", connID, table, col)
			}
		}

	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "TYPE"):
		table, col, oldType, newType := parseAlterColumnType(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordTypeChange(table, col, oldType, newType)
			if p.verbose {
				log.Printf("[conn %d] schema registry: ALTER COLUMN TYPE %s.%s %s -> %s", connID, table, col, oldType, newType)
			}
		}

	case strings.HasPrefix(upper, "CREATE TABLE"):
		for _, table := range cl.Tables {
			p.schemaRegistry.RecordNewTable(table)
			if p.verbose {
				log.Printf("[conn %d] schema registry: CREATE TABLE %s", connID, table)
			}
		}
		// Extract FK constraints from DDL and record them.
		fks := extractFKFromDDL(sqlStr)
		for _, fk := range fks {
			if len(cl.Tables) > 0 {
				fk.ChildTable = cl.Tables[0]
				p.schemaRegistry.RecordForeignKey(fk.ChildTable, fk)
				if p.verbose {
					log.Printf("[conn %d] schema registry: FK %s.(%s) -> %s.(%s)",
						connID, fk.ChildTable, strings.Join(fk.ChildColumns, ","),
						fk.ParentTable, strings.Join(fk.ParentColumns, ","))
				}
			}
		}

	case strings.HasPrefix(upper, "DROP TABLE"):
		for _, table := range cl.Tables {
			p.schemaRegistry.RemoveTable(table)
			if p.deltaMap != nil {
				p.deltaMap.ClearTable(table)
			}
			if p.tombstones != nil {
				p.tombstones.ClearTable(table)
			}
			if p.verbose {
				log.Printf("[conn %d] schema registry: DROP TABLE %s", connID, table)
			}
		}
	}

	if err := coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry); err != nil {
		if p.verbose {
			log.Printf("[conn %d] failed to persist schema registry: %v", connID, err)
		}
	}
}

func parseAlterRenameColumn(sqlStr string) (table, oldName, newName string) {
	fields := strings.Fields(sqlStr)
	if len(fields) < 6 {
		return "", "", ""
	}
	table = strings.Trim(fields[2], `"'`)
	idx := 3
	if idx < len(fields) && strings.EqualFold(fields[idx], "RENAME") {
		idx++
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "COLUMN") {
		idx++
	}
	if idx < len(fields) {
		oldName = strings.Trim(fields[idx], `"'`)
		idx++
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "TO") {
		idx++
	}
	if idx < len(fields) {
		newName = strings.Trim(fields[idx], `"'`)
	}
	return table, oldName, newName
}

func parseAlterDropColumn(sqlStr string) (table, col string) {
	fields := strings.Fields(sqlStr)
	if len(fields) < 5 {
		return "", ""
	}
	table = strings.Trim(fields[2], `"'`)
	idx := 3
	if idx < len(fields) && strings.EqualFold(fields[idx], "DROP") {
		idx++
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "COLUMN") {
		idx++
	}
	if idx < len(fields) {
		col = strings.Trim(fields[idx], `"'`)
	}
	return table, col
}

func parseAlterColumnType(sqlStr string) (table, col, oldType, newType string) {
	fields := strings.Fields(sqlStr)
	if len(fields) < 6 {
		return "", "", "", ""
	}
	table = strings.Trim(fields[2], `"'`)
	// ALTER TABLE t ALTER COLUMN c TYPE new_type
	idx := 3
	if idx < len(fields) && strings.EqualFold(fields[idx], "ALTER") {
		idx++
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "COLUMN") {
		idx++
	}
	if idx < len(fields) {
		col = strings.Trim(fields[idx], `"'`)
		idx++
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "SET") {
		// ALTER ... SET DATA TYPE
		idx++
		if idx < len(fields) && strings.EqualFold(fields[idx], "DATA") {
			idx++
		}
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "TYPE") {
		idx++
	}
	if idx < len(fields) {
		newType = fields[idx]
	}
	return table, col, oldType, newType
}

func parseAlterAddColumn(sqlStr string) (table, col, colType string) {
	fields := strings.Fields(sqlStr)
	if len(fields) < 5 {
		return "", "", ""
	}
	table = strings.Trim(fields[2], `"'`)
	idx := 3
	if idx < len(fields) && strings.EqualFold(fields[idx], "ADD") {
		idx++
	}
	if idx < len(fields) && strings.EqualFold(fields[idx], "COLUMN") {
		idx++
	}
	if idx < len(fields) {
		col = strings.Trim(fields[idx], `"'`)
		idx++
	}
	if idx < len(fields) {
		colType = fields[idx]
	}
	return table, col, colType
}

func needsPKInjection(sqlStr, pkCol string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	if strings.Contains(upper, " UNION ") || strings.Contains(upper, " INTERSECT ") || strings.Contains(upper, " EXCEPT ") {
		return false
	}
	if strings.HasPrefix(upper, "WITH ") {
		return false
	}

	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return false
	}
	afterSelect := strings.TrimSpace(upper[selectIdx+6:])
	if strings.HasPrefix(afterSelect, "*") || strings.HasPrefix(afterSelect, "DISTINCT *") {
		return false
	}
	fromIdx := findOuterFromIndex(upper)
	if fromIdx < 0 {
		return false
	}
	afterFrom := strings.TrimSpace(upper[fromIdx+6:])
	if strings.HasPrefix(afterFrom, "(") {
		return false
	}

	selectList := strings.ToLower(sqlStr[selectIdx+6 : fromIdx])
	return !selectListContainsColumn(selectList, strings.ToLower(pkCol))
}

func selectListContainsColumn(selectList, col string) bool {
	parts := strings.Split(selectList, ",")
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if asIdx := strings.Index(strings.ToUpper(name), " AS "); asIdx >= 0 {
			name = strings.TrimSpace(name[:asIdx])
		}
		if dotIdx := strings.LastIndex(name, "."); dotIdx >= 0 {
			name = name[dotIdx+1:]
		}
		name = strings.Trim(name, `"'`)
		if strings.ToLower(name) == col {
			return true
		}
	}
	return false
}

// findOuterFromIndex finds the position of the first " FROM " at parenthesis
// depth 0 (not inside a subquery). Returns -1 if not found.
func findOuterFromIndex(upper string) int {
	depth := 0
	target := " FROM "
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+len(target) <= len(upper) && upper[i:i+len(target)] == target {
				return i
			}
		}
	}
	return -1
}

// containsColumnForTable checks if a SELECT list contains a specific column
// for a given table alias. A qualified column (e.g., "u.id") only matches if
// the qualifier matches the expected alias. An unqualified column matches any table.
func containsColumnForTable(selectList, col, tableAlias string) bool {
	lowerCol := strings.ToLower(col)
	lowerAlias := strings.ToLower(tableAlias)

	parts := strings.Split(selectList, ",")
	for _, part := range parts {
		name := strings.TrimSpace(part)
		// Handle aliases: "col AS alias"
		if idx := strings.Index(strings.ToLower(name), " as "); idx >= 0 {
			name = strings.TrimSpace(name[:idx])
		}
		name = strings.Trim(name, `"`)
		lowerName := strings.ToLower(name)

		if dotIdx := strings.LastIndex(lowerName, "."); dotIdx >= 0 {
			// Qualified: only match if qualifier matches expected alias
			qualifier := strings.Trim(lowerName[:dotIdx], `"`)
			colPart := strings.Trim(lowerName[dotIdx+1:], `"`)
			if colPart == lowerCol && qualifier == lowerAlias {
				return true
			}
		} else {
			// Unqualified: matches any table
			if lowerName == lowerCol {
				return true
			}
		}
	}
	return false
}

func injectPKColumn(sqlStr, pkCol string) string {
	upper := strings.ToUpper(sqlStr)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return sqlStr
	}
	insertPos := selectIdx + 6
	afterSelect := strings.TrimSpace(sqlStr[insertPos:])
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT") {
		insertPos += (len(sqlStr[insertPos:]) - len(afterSelect)) + 8
	}
	return sqlStr[:insertPos] + " " + `"` + pkCol + `",` + sqlStr[insertPos:]
}

func stripInjectedPKColumn(columns []string, rows [][]sql.NullString, nulls [][]bool, pkCol string) ([]string, [][]sql.NullString, [][]bool) {
	pkIdx := -1
	for i, col := range columns {
		if col == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return columns, rows, nulls
	}

	newCols := make([]string, 0, len(columns)-1)
	newCols = append(newCols, columns[:pkIdx]...)
	newCols = append(newCols, columns[pkIdx+1:]...)

	newRows := make([][]sql.NullString, len(rows))
	newNulls := make([][]bool, len(nulls))
	for i, row := range rows {
		newRow := make([]sql.NullString, 0, len(row)-1)
		newRow = append(newRow, row[:pkIdx]...)
		newRow = append(newRow, row[pkIdx+1:]...)
		newRows[i] = newRow

		newNull := make([]bool, 0, len(nulls[i])-1)
		newNull = append(newNull, nulls[i][:pkIdx]...)
		newNull = append(newNull, nulls[i][pkIdx+1:]...)
		newNulls[i] = newNull
	}

	return newCols, newRows, newNulls
}
