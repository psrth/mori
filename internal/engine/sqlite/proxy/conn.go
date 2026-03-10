package proxy

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
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
	if err := handleStartup(clientConn); err != nil {
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

	// Per-connection FK enforcer.
	var fke *FKEnforcer
	if p.schemaRegistry != nil {
		fke = NewFKEnforcer(p, connID)
	}

	var eh *ExtHandler
	if p.classifier != nil && p.router != nil {
		eh = &ExtHandler{
			proxy:           p,
			connID:          connID,
			txh:             txh,
			stmtCache:       make(map[string]string),
			shadowOnlyStmts: make(map[string]bool),
		}
	}

	// Convenience: get txn state for ReadyForQuery.
	txnStateByte := func() byte {
		if txh != nil {
			return txh.TxnState()
		}
		return 'I'
	}
	_ = fke       // FK enforcer wired into write handlers below.
	_ = txnStateByte

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

		// Handle Flush ('H') message.
		if msg.Type == 'H' {
			// Flush buffered output — since we write synchronously, this is a no-op.
			continue
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

			// Handle TRUNCATE.
			if decision.strategy == core.StrategyTruncate && decision.classification != nil {
				p.handleTruncate(clientConn, decision.classification, connID, txh)
				continue
			}

			// Handle merged reads: query both prod and shadow, merge in-process.
			if decision.strategy == core.StrategyMergedRead || decision.strategy == core.StrategyJoinPatch {
				resp := p.executeMergedRead(sqlStr, decision.classification, connID)
				clientConn.Write(resp)
				continue
			}

			// Handle not-supported.
			if decision.strategy == core.StrategyNotSupported {
				msg := core.UnsupportedTransactionMsg
				if decision.classification != nil && decision.classification.NotSupportedMsg != "" {
					msg = decision.classification.NotSupportedMsg
				}
				clientConn.Write(buildErrorResponse(msg))
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
				// L2 write guard: use safeProdExecQuery for defense-in-depth.
				resp := p.safeProdExecQuery(sqlStr, connID)
				clientConn.Write(resp)

			case targetShadow:
				// For HydrateAndWrite — handle INSERT ON CONFLICT (upserts) and UPDATEs.
				if decision.strategy == core.StrategyHydrateAndWrite && decision.classification != nil {
					if decision.classification.SubType == core.SubInsert && decision.classification.HasOnConflict {
						p.handleUpsert(clientConn, sqlStr, decision.classification, connID, txh)
						continue
					}
					p.handleUpdateWithHydration(clientConn, sqlStr, decision.classification, connID, txh)
					continue
				}

				// For ShadowDelete.
				if decision.strategy == core.StrategyShadowDelete && decision.classification != nil {
					p.handleDelete(clientConn, sqlStr, decision.classification, connID, txh)
					continue
				}

				// For ShadowWrite (INSERT without ON CONFLICT).
				if decision.strategy == core.StrategyShadowWrite && decision.classification != nil &&
					decision.classification.SubType == core.SubInsert {
					p.handleInsert(clientConn, sqlStr, decision.classification, connID, txh)
					continue
				}

				resp := p.executeQuery(p.shadowDB, sqlStr, connID)
				clientConn.Write(resp)

				// Track deltas/tombstones/schema after successful writes.
				if decision.classification != nil {
					p.trackWriteEffects(decision.classification, decision.strategy, connID, txh)
				}

			case targetBoth:
				// Execute on shadow first (discard result), then prod.
				p.executeQuery(p.shadowDB, sqlStr, connID)
				resp := p.safeProdExecQuery(sqlStr, connID)
				clientConn.Write(resp)
			}
			continue
		}

		// Unknown message: send error.
		clientConn.Write(buildErrorResponse(fmt.Sprintf("mori-sqlite: unsupported message type '%c'", msg.Type)))
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
		core.StrategyShadowDelete:
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

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		return routeDecision{
			target:         targetProd,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyTruncate:
		return routeDecision{
			target:         targetShadow,
			classification: classification,
			strategy:       strategy,
		}

	case core.StrategyNotSupported:
		return routeDecision{
			target:         targetShadow,
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
	// Determine if this is a query that returns rows.
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		(strings.HasPrefix(upper, "WITH") && !strings.Contains(upper, "INSERT") &&
			!strings.Contains(upper, "UPDATE") && !strings.Contains(upper, "DELETE"))

	// Also check for RETURNING clause.
	hasReturning := strings.Contains(upper, "RETURNING")

	if isSelect || hasReturning {
		return p.executeSelectQuery(db, sqlStr, connID)
	}
	return p.executeExecQuery(db, sqlStr, connID)
}

// executeSelectQuery handles queries that return rows (SELECT, PRAGMA, etc.).
func (p *Proxy) executeSelectQuery(db *sql.DB, sqlStr string, _connID int64) []byte {
	rows, err := db.Query(sqlStr)
	if err != nil {
		return buildSQLErrorResponse(err.Error())
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return buildSQLErrorResponse(err.Error())
	}

	// Build RowDescription with type-aware OIDs.
	colOIDs := p.inferColumnOIDs(db, sqlStr, columns)
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	// Read and build DataRows.
	rowCount := 0
	scanDest := make([]any, len(columns))
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

	// CommandComplete + ReadyForQuery.
	tag := fmt.Sprintf("SELECT %d", rowCount)
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	resp = append(resp, buildReadyForQueryMsg()...)
	return resp
}

// executeExecQuery handles queries that don't return rows (INSERT, UPDATE, DELETE, DDL).
func (p *Proxy) executeExecQuery(db *sql.DB, sqlStr string, _connID int64) []byte {
	result, err := db.Exec(sqlStr)
	if err != nil {
		return buildSQLErrorResponse(err.Error())
	}

	rowsAffected, _ := result.RowsAffected()

	// Determine command tag based on SQL.
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

// inferColumnOIDs attempts to map SQLite column types to PostgreSQL OIDs.
func (p *Proxy) inferColumnOIDs(db *sql.DB, sqlStr string, columns []string) []uint32 {
	oids := make([]uint32, len(columns))
	for i := range oids {
		oids[i] = 25 // Default: text
	}

	// Try to get column types from the query.
	rows, err := db.Query(sqlStr + " LIMIT 0")
	if err != nil {
		return oids
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return oids
	}

	for i, ct := range colTypes {
		if i >= len(oids) {
			break
		}
		typeName := strings.ToUpper(ct.DatabaseTypeName())
		oids[i] = sqliteTypeToOID(typeName)
	}
	return oids
}

// sqliteTypeToOID maps SQLite type names to PostgreSQL OIDs.
func sqliteTypeToOID(typeName string) uint32 {
	switch typeName {
	case "INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT":
		return 23 // int4
	case "BIGINT":
		return 20 // int8
	case "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT":
		return 701 // float8
	case "NUMERIC", "DECIMAL":
		return 1700 // numeric
	case "BOOLEAN", "BOOL":
		return 16 // bool
	case "BLOB":
		return 17 // bytea
	case "DATE":
		return 1082 // date
	case "TIME":
		return 1083 // time
	case "TIMESTAMP", "DATETIME":
		return 1114 // timestamp
	default:
		return 25 // text
	}
}

// handleTruncate handles TRUNCATE / DELETE FROM <table> with no WHERE.
func (p *Proxy) handleTruncate(clientConn net.Conn, cl *core.Classification, connID int64, txh *TxnHandler) {
	if len(cl.Tables) == 0 {
		clientConn.Write(buildErrorResponse("mori: truncate requires a table name"))
		return
	}
	table := cl.Tables[0]

	// SQLite doesn't have TRUNCATE — use DELETE FROM table.
	deleteSQL := fmt.Sprintf(`DELETE FROM %q`, table)
	result, err := p.shadowDB.Exec(deleteSQL)
	if err != nil {
		clientConn.Write(buildSQLErrorResponse(err.Error()))
		return
	}

	rowsAffected, _ := result.RowsAffected()

	// Mark table as fully shadowed.
	if p.schemaRegistry != nil {
		p.schemaRegistry.MarkFullyShadowed(table)
		coreSchema.WriteRegistry(p.moriDir, p.schemaRegistry)
	}

	// Clear delta map and tombstones for this table.
	if p.deltaMap != nil {
		p.deltaMap.ClearTable(table)
	}
	if p.tombstones != nil {
		p.tombstones.ClearTable(table)
	}

	if p.verbose {
		log.Printf("[conn %d] TRUNCATE %s: %d rows deleted, table marked fully shadowed", connID, table, rowsAffected)
	}

	var resp []byte
	tag := fmt.Sprintf("DELETE %d", rowsAffected)
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	resp = append(resp, buildReadyForQueryMsg()...)
	clientConn.Write(resp)
}

// handleInsert handles INSERT statements with row-level PK tracking.
// P3 4.2: If RETURNING clause is present, captures returned rows for PK extraction.
func (p *Proxy) handleInsert(clientConn net.Conn, sqlStr string, cl *core.Classification, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	// FK enforcement: validate parent rows exist.
	if fke := NewFKEnforcer(p, connID); fke != nil && len(cl.Tables) == 1 {
		if err := p.validateFKsForInsert(fke, cl.Tables[0], sqlStr); err != nil {
			clientConn.Write(buildSQLErrorResponse(err.Error()))
			if txh != nil && txh.InTxn() {
				txh.SetErrorState()
			}
			return
		}
	}

	// P3 4.2: RETURNING clause — execute as a query to capture returned rows.
	if cl.HasReturning {
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)

		// Extract PKs from RETURNING results if possible.
		if len(cl.Tables) > 0 {
			table := cl.Tables[0]
			if meta, ok := p.tables[table]; ok && len(meta.PKColumns) > 0 {
				pkCol := meta.PKColumns[0]
				pks := extractInsertPKValues(sqlStr, table, pkCol)
				for _, pk := range pks {
					if inTxn {
						p.deltaMap.Stage(table, pk)
					} else {
						p.deltaMap.Add(table, pk)
					}
				}
				if len(pks) == 0 {
					p.deltaMap.MarkInserted(table)
				}
			} else {
				p.deltaMap.MarkInserted(table)
			}
			if !inTxn {
				delta.WriteDeltaMap(p.moriDir, p.deltaMap)
			}
		}
		return
	}

	// Execute the INSERT on Shadow.
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)
	clientConn.Write(resp)

	if len(cl.Tables) == 0 {
		return
	}
	table := cl.Tables[0]
	meta, hasMeta := p.tables[table]

	// Try to extract specific PKs for row-level tracking.
	tracked := false

	if hasMeta && len(meta.PKColumns) > 0 {
		pkCol := meta.PKColumns[0]

		// For AUTOINCREMENT tables, query last_insert_rowid().
		if meta.PKType == "serial" || meta.PKType == "bigserial" {
			var lastID int64
			if err := p.shadowDB.QueryRow("SELECT last_insert_rowid()").Scan(&lastID); err == nil && lastID > 0 {
				pk := strconv.FormatInt(lastID, 10)
				if inTxn {
					p.deltaMap.Stage(table, pk)
				} else {
					p.deltaMap.Add(table, pk)
				}
				tracked = true
			}
		}

		// For non-AUTOINCREMENT tables, try to extract PKs from the INSERT VALUES clause.
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

	// Fallback: table-level tracking.
	if !tracked {
		p.deltaMap.MarkInserted(table)
	}

	// Persist immediately in autocommit mode.
	if !inTxn {
		if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
			if p.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", connID, err)
			}
		}
	}
}

// handleUpsert handles INSERT ... ON CONFLICT and INSERT OR REPLACE statements.
func (p *Proxy) handleUpsert(clientConn net.Conn, sqlStr string, cl *core.Classification, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	if len(cl.Tables) != 1 {
		// Multi-table: fall back to simple INSERT.
		p.handleInsert(clientConn, sqlStr, cl, connID, txh)
		return
	}

	table := cl.Tables[0]
	meta, hasMeta := p.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		p.handleInsert(clientConn, sqlStr, cl, connID, txh)
		return
	}

	pkCol := meta.PKColumns[0]

	// Try to extract conflict column values from the INSERT.
	conflictValues := extractUpsertConflictValues(sqlStr, table, pkCol)

	// Hydrate matching rows from Prod into Shadow.
	hydratedCount := 0
	var affectedPKs []string
	skipCols := toSkipSet(meta.GeneratedCols)

	for _, val := range conflictValues {
		// Skip if already in delta map.
		if p.deltaMap.IsDelta(table, val) {
			affectedPKs = append(affectedPKs, val)
			continue
		}

		// Check if row exists in Prod.
		escapedVal := strings.ReplaceAll(val, "'", "''")
		selectSQL := fmt.Sprintf(`SELECT * FROM %q WHERE %q = '%s'`, table, pkCol, escapedVal)

		// Apply max rows cap.
		if p.maxRowsHydrate > 0 {
			selectSQL += fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
		}

		cols, rows, _, err := p.safeProdQuery(selectSQL, connID)
		if err != nil || len(rows) == 0 {
			continue
		}

		// Hydrate into Shadow.
		insertSQL := buildHydrateInsert(table, cols, rows[0], skipCols)
		if _, err := p.shadowDB.Exec(insertSQL); err != nil {
			if p.verbose {
				log.Printf("[conn %d] upsert hydration failed for (%s, %s): %v", connID, table, val, err)
			}
		} else {
			hydratedCount++
		}
		affectedPKs = append(affectedPKs, val)
	}

	if p.verbose && hydratedCount > 0 {
		log.Printf("[conn %d] upsert: hydrated %d rows from Prod for %s", connID, hydratedCount, table)
	}

	// Execute the original upsert on Shadow.
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)
	clientConn.Write(resp)

	// Track affected PKs.
	for _, pk := range affectedPKs {
		if inTxn {
			p.deltaMap.Stage(table, pk)
		} else {
			p.deltaMap.Add(table, pk)
		}
	}

	// Also track via last_insert_rowid for newly inserted rows.
	if meta.PKType == "serial" || meta.PKType == "bigserial" {
		var lastID int64
		if err := p.shadowDB.QueryRow("SELECT last_insert_rowid()").Scan(&lastID); err == nil && lastID > 0 {
			pk := strconv.FormatInt(lastID, 10)
			if inTxn {
				p.deltaMap.Stage(table, pk)
			} else {
				p.deltaMap.Add(table, pk)
			}
		}
	}

	if !inTxn {
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}
}

// handleUpdateWithHydration handles UPDATE with hydration from Prod.
func (p *Proxy) handleUpdateWithHydration(clientConn net.Conn, sqlStr string, cl *core.Classification, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	if len(cl.PKs) > 0 {
		// Point update: hydrate specific rows.
		p.hydrateBeforeUpdate(cl, connID)

		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)

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
		return
	}

	// Bulk update: no extractable PKs.
	if len(cl.Tables) != 1 {
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)
		return
	}

	table := cl.Tables[0]
	meta, hasMeta := p.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)
		return
	}

	pkCol := meta.PKColumns[0]

	// Build SELECT pk FROM table WHERE <same conditions> to discover affected rows.
	whereClause := extractWhereFromSQL(sqlStr)
	var selectSQL string
	if whereClause != "" {
		selectSQL = fmt.Sprintf(`SELECT %q FROM %q WHERE %s`, pkCol, table, whereClause)
	} else {
		selectSQL = fmt.Sprintf(`SELECT %q FROM %q`, pkCol, table)
	}

	// Rewrite for Prod compatibility.
	if p.schemaRegistry != nil {
		rewritten, skipProd := rewriteForProd(selectSQL, p.schemaRegistry, cl.Tables)
		if skipProd {
			// WHERE is on shadow-only columns, execute on Shadow only.
			resp := p.executeQuery(p.shadowDB, sqlStr, connID)
			clientConn.Write(resp)
			p.trackBulkUpdateDeltas(sqlStr, table, pkCol, connID, txh)
			return
		}
		selectSQL = rewritten
	}

	// Apply max rows cap.
	if p.maxRowsHydrate > 0 {
		selectSQL += fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
	}

	// Query Prod for matching PKs.
	_, prodRows, _, prodErr := p.safeProdQuery(selectSQL, connID)
	if prodErr != nil {
		if p.verbose {
			log.Printf("[conn %d] bulk UPDATE: Prod PK discovery failed: %v", connID, prodErr)
		}
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)
		p.trackBulkUpdateDeltas(sqlStr, table, pkCol, connID, txh)
		return
	}

	// Hydrate matching rows.
	skipCols := toSkipSet(meta.GeneratedCols)
	var affectedPKs []string
	hydratedCount := 0

	for _, row := range prodRows {
		if len(row) == 0 || !row[0].Valid {
			continue
		}
		pk := row[0].String

		if p.deltaMap.IsDelta(table, pk) {
			affectedPKs = append(affectedPKs, pk)
			continue
		}

		// Hydrate from Prod.
		escapedPK := strings.ReplaceAll(pk, "'", "''")
		hydrateSQL := fmt.Sprintf(`SELECT * FROM %q WHERE %q = '%s'`, table, pkCol, escapedPK)
		cols, rows, _, err := p.safeProdQuery(hydrateSQL, connID)
		if err != nil || len(rows) == 0 {
			continue
		}

		insertSQL := buildHydrateInsert(table, cols, rows[0], skipCols)
		if _, err := p.shadowDB.Exec(insertSQL); err != nil {
			if p.verbose {
				log.Printf("[conn %d] bulk UPDATE: hydration failed for PK %s: %v", connID, pk, err)
			}
			continue
		}
		affectedPKs = append(affectedPKs, pk)
		hydratedCount++
	}

	if p.verbose {
		log.Printf("[conn %d] bulk UPDATE: hydrated %d rows (%d total)", connID, hydratedCount, len(affectedPKs))
	}

	// Execute UPDATE on Shadow.
	resp := p.executeQuery(p.shadowDB, sqlStr, connID)
	clientConn.Write(resp)

	// Track deltas.
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

// trackBulkUpdateDeltas discovers PKs from Shadow after a shadow-only UPDATE.
func (p *Proxy) trackBulkUpdateDeltas(sqlStr, table, pkCol string, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	whereClause := extractWhereFromSQL(sqlStr)
	var selectSQL string
	if whereClause != "" {
		selectSQL = fmt.Sprintf(`SELECT %q FROM %q WHERE %s`, pkCol, table, whereClause)
	} else {
		selectSQL = fmt.Sprintf(`SELECT %q FROM %q`, pkCol, table)
	}

	_, rows, _, err := queryToRows(p.shadowDB, selectSQL)
	if err != nil {
		return
	}

	for _, row := range rows {
		if len(row) == 0 || !row[0].Valid {
			continue
		}
		pk := row[0].String
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

// handleDelete handles DELETE statements with proper tombstoning.
func (p *Proxy) handleDelete(clientConn net.Conn, sqlStr string, cl *core.Classification, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	if len(cl.PKs) > 0 {
		// FK enforcement: check RESTRICT constraints before delete.
		fke := NewFKEnforcer(p, connID)
		if fke != nil && len(cl.Tables) == 1 {
			deletedPKs := make([]string, len(cl.PKs))
			for i, pk := range cl.PKs {
				deletedPKs[i] = pk.PK
			}
			if err := fke.CheckDeleteRestrict(cl.Tables[0], deletedPKs); err != nil {
				clientConn.Write(buildSQLErrorResponse(err.Error()))
				if txh != nil && txh.InTxn() {
					txh.SetErrorState()
				}
				return
			}
		}

		// Point delete: execute on Shadow, then tombstone.
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)

		// Correct the row count: we may be deleting Prod-only rows.
		tombstoneCount := len(cl.PKs)
		correctedResp := correctDeleteCount(resp, tombstoneCount)
		clientConn.Write(correctedResp)

		for _, pk := range cl.PKs {
			if inTxn {
				p.tombstones.Stage(pk.Table, pk.PK)
			} else {
				p.tombstones.Add(pk.Table, pk.PK)
				p.deltaMap.Remove(pk.Table, pk.PK)
			}
		}

		// FK CASCADE/SET NULL enforcement on child tables.
		if fke != nil && len(cl.Tables) == 1 {
			deletedPKs := make([]string, len(cl.PKs))
			for i, pk := range cl.PKs {
				deletedPKs[i] = pk.PK
			}
			fke.EnforceDeleteCascade(cl.Tables[0], deletedPKs)
		}

		if !inTxn {
			delta.WriteTombstoneSet(p.moriDir, p.tombstones)
			delta.WriteDeltaMap(p.moriDir, p.deltaMap)
		}
		return
	}

	// Bulk delete: discover PKs from Prod first.
	if len(cl.Tables) != 1 {
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)
		return
	}

	table := cl.Tables[0]
	meta, hasMeta := p.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)
		return
	}

	pkCol := meta.PKColumns[0]

	// Build SELECT pk FROM table WHERE <same conditions>.
	whereClause := extractWhereFromSQL(sqlStr)
	var selectSQL string
	if whereClause != "" {
		selectSQL = fmt.Sprintf(`SELECT %q FROM %q WHERE %s`, pkCol, table, whereClause)
	} else {
		selectSQL = fmt.Sprintf(`SELECT %q FROM %q`, pkCol, table)
	}

	// Rewrite for Prod.
	if p.schemaRegistry != nil {
		rewritten, skipProd := rewriteForProd(selectSQL, p.schemaRegistry, cl.Tables)
		if skipProd {
			resp := p.executeQuery(p.shadowDB, sqlStr, connID)
			clientConn.Write(resp)
			return
		}
		selectSQL = rewritten
	}

	// Apply max rows cap.
	if p.maxRowsHydrate > 0 {
		selectSQL += fmt.Sprintf(" LIMIT %d", p.maxRowsHydrate)
	}

	// Query Prod for matching PKs.
	_, prodRows, _, prodErr := p.safeProdQuery(selectSQL, connID)
	if prodErr != nil {
		resp := p.executeQuery(p.shadowDB, sqlStr, connID)
		clientConn.Write(resp)
		return
	}

	var discoveredPKs []string
	for _, row := range prodRows {
		if len(row) == 0 || !row[0].Valid {
			continue
		}
		discoveredPKs = append(discoveredPKs, row[0].String)
	}

	// Execute DELETE on Shadow.
	result, err := p.shadowDB.Exec(sqlStr)
	shadowCount := int64(0)
	if err == nil {
		shadowCount, _ = result.RowsAffected()
	}

	// Compute corrected count.
	prodOnlyCount := 0
	for _, pk := range discoveredPKs {
		if !p.deltaMap.IsDelta(table, pk) {
			prodOnlyCount++
		}
	}
	totalCount := shadowCount + int64(prodOnlyCount)

	// Send corrected response.
	var resp []byte
	if err != nil {
		resp = buildSQLErrorResponse(err.Error())
	} else {
		tag := fmt.Sprintf("DELETE %d", totalCount)
		resp = append(resp, buildCommandCompleteMsg(tag)...)
		resp = append(resp, buildReadyForQueryMsg()...)
	}
	clientConn.Write(resp)

	// Tombstone all discovered PKs.
	for _, pk := range discoveredPKs {
		if inTxn {
			p.tombstones.Stage(table, pk)
		} else {
			p.tombstones.Add(table, pk)
			p.deltaMap.Remove(table, pk)
		}
	}

	if !inTxn {
		delta.WriteTombstoneSet(p.moriDir, p.tombstones)
		delta.WriteDeltaMap(p.moriDir, p.deltaMap)
	}
}

// correctDeleteCount replaces the DELETE count in a pgwire response.
func correctDeleteCount(resp []byte, count int) []byte {
	// Simple approach: rebuild the response.
	tag := fmt.Sprintf("DELETE %d", count)
	var result []byte
	result = append(result, buildCommandCompleteMsg(tag)...)
	result = append(result, buildReadyForQueryMsg()...)
	return result
}

// executeMergedRead queries both prod and shadow databases and merges results.
func (p *Proxy) executeMergedRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if cl == nil || len(cl.Tables) == 0 {
		return p.safeProdExecQuery(sqlStr, connID)
	}
	table := cl.Tables[0]

	// Aggregate queries need special handling.
	if cl.HasAggregate {
		return p.executeAggregateRead(sqlStr, cl, connID)
	}

	// Window functions: materialize and re-execute on Shadow.
	if cl.HasWindowFunc {
		return p.executeWindowFuncRead(sqlStr, cl, connID)
	}

	// Set operations: decompose, merge, apply set ops.
	if cl.HasSetOp {
		return p.executeSetOpRead(sqlStr, cl, connID)
	}

	// Complex reads (CTEs, derived tables): materialize dirty tables.
	if cl.IsComplexRead {
		return p.executeComplexRead(sqlStr, cl, connID)
	}

	// P2 3.6: JOIN patch strategy — when the query is a JOIN involving dirty tables,
	// materialize the dirty tables into temp tables and rewrite the query.
	if cl.IsJoin && len(cl.Tables) > 1 {
		return p.executeJoinPatch(sqlStr, cl, connID)
	}

	// Check if table only exists in shadow (created via DDL) or is fully shadowed.
	if p.schemaRegistry != nil {
		diff := p.schemaRegistry.GetDiff(table)
		if diff != nil && (diff.IsNewTable || diff.IsFullyShadowed) {
			return p.executeQuery(p.shadowDB, sqlStr, connID)
		}
		if _, exists := p.tables[table]; !exists {
			return p.executeQuery(p.shadowDB, sqlStr, connID)
		}
	}

	// Step 0: Inject PK column into query if not present (needed for dedup).
	injectedPK := ""
	injectedRowID := false
	effectiveSQL := sqlStr
	if !cl.IsJoin {
		if meta, ok := p.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(sqlStr, pkCol) {
				effectiveSQL = injectPKColumn(sqlStr, pkCol)
				injectedPK = pkCol
			}
		} else if meta, ok := p.tables[table]; ok && len(meta.PKColumns) == 0 && meta.PKType == "none" {
			// P3 4.4: ROWID injection for PK-less tables.
			effectiveSQL = injectRowID(sqlStr)
			injectedRowID = true
		}
	}

	// Step 1: Query shadow.
	shadowCols, shadowRows, shadowNulls, shadowErr := queryToRows(p.shadowDB, effectiveSQL)
	if shadowErr != nil {
		if p.verbose {
			log.Printf("[conn %d] merged read: shadow query error: %v", connID, shadowErr)
		}
		return p.safeProdExecQuery(sqlStr, connID)
	}

	// Step 2: Query prod (with overfetch for LIMIT queries, rewrite for schema diffs).
	prodSQL := effectiveSQL
	if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
		rewritten, skipProd := rewriteForProd(prodSQL, p.schemaRegistry, cl.Tables)
		if skipProd {
			if injectedPK != "" {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, injectedPK)
			}
			return buildMergedResponse(shadowCols, shadowRows, shadowNulls)
		}
		prodSQL = rewritten
	}

	if cl.HasLimit && cl.Limit > 0 {
		deltaCount := p.deltaMap.CountForTable(table)
		tombstoneCount := p.tombstones.CountForTable(table)
		overfetch := deltaCount + tombstoneCount
		if overfetch > 0 {
			prodSQL = rewriteLimit(prodSQL, cl.Limit+overfetch)
		}
	}

	// L2 write guard: use safeProdQuery for defense-in-depth.
	prodCols, prodRows, prodNulls, prodErr := p.safeProdQuery(prodSQL, connID)
	if prodErr != nil {
		if p.verbose {
			log.Printf("[conn %d] merged read: prod query error: %v", connID, prodErr)
		}
		if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
			if injectedPK != "" {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, injectedPK)
			}
			return buildMergedResponse(shadowCols, shadowRows, shadowNulls)
		}
		return buildSQLErrorResponse(prodErr.Error())
	}

	// Step 2.5: Adapt prod rows for schema differences.
	if p.schemaRegistry != nil {
		prodCols, prodRows, prodNulls = adaptProdRows(prodCols, prodRows, prodNulls, shadowCols, table, p.schemaRegistry)
	}

	// Step 3: Find PK column index (or rowid for PK-less tables).
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
	} else if injectedRowID {
		// Use rowid as the dedup key for PK-less tables.
		for i, col := range shadowCols {
			if col == "rowid" {
				pkIdx = i
				break
			}
		}
	}

	// Step 4: Filter prod rows — remove delta and tombstoned rows.
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

	// Step 5: Merge — shadow first, then filtered prod.
	merged := append(shadowRows, filteredProd...)
	mergedNulls := append(shadowNulls, filteredProdNulls...)

	// Step 6: Dedup by PK (first occurrence = shadow wins).
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
	}

	// Step 7: Re-sort by ORDER BY if present.
	if cl.OrderBy != "" {
		sortMergedRows(shadowCols, prodCols, merged, mergedNulls, cl.OrderBy)
	}

	// Step 8: Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(merged) > cl.Limit {
		merged = merged[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	// Use shadow columns as canonical; fall back to prod.
	columns := shadowCols
	if len(columns) == 0 {
		columns = prodCols
	}

	// Step 9: Strip injected PK column or rowid if it was added for dedup.
	if injectedPK != "" {
		columns, merged, mergedNulls = stripInjectedPKColumn(columns, merged, mergedNulls, injectedPK)
	}
	if injectedRowID {
		columns, merged, mergedNulls = stripInjectedPKColumn(columns, merged, mergedNulls, "rowid")
	}

	return buildMergedResponse(columns, merged, mergedNulls)
}

// adaptProdRows adapts prod rows for schema differences (added/dropped/renamed columns).
func adaptProdRows(prodCols []string, prodRows [][]sql.NullString, prodNulls [][]bool,
	shadowCols []string, table string, registry *coreSchema.Registry) ([]string, [][]sql.NullString, [][]bool) {

	diff := registry.GetDiff(table)
	if diff == nil {
		return prodCols, prodRows, prodNulls
	}

	if len(diff.Added) == 0 && len(diff.Dropped) == 0 && len(diff.Renamed) == 0 {
		return prodCols, prodRows, prodNulls
	}

	// Build column mapping: shadow col index -> prod col index.
	// Also build the target column set to match shadow's schema.
	prodColIdx := make(map[string]int, len(prodCols))
	for i, col := range prodCols {
		prodColIdx[strings.ToLower(col)] = i
	}

	addedSet := make(map[string]bool)
	for _, col := range diff.Added {
		addedSet[strings.ToLower(col.Name)] = true
	}

	droppedSet := make(map[string]bool)
	for _, col := range diff.Dropped {
		droppedSet[strings.ToLower(col)] = true
	}

	// Build adapted rows matching shadow column order.
	var adaptedRows [][]sql.NullString
	var adaptedNulls [][]bool

	for ri, row := range prodRows {
		adaptedRow := make([]sql.NullString, len(shadowCols))
		adaptedNull := make([]bool, len(shadowCols))

		for si, sCol := range shadowCols {
			sLower := strings.ToLower(sCol)

			// Added column: inject NULL.
			if addedSet[sLower] {
				adaptedNull[si] = true
				continue
			}

			// Renamed column: look for old name.
			sourceName := sLower
			for oldName, newName := range diff.Renamed {
				if strings.ToLower(newName) == sLower {
					sourceName = strings.ToLower(oldName)
					break
				}
			}

			// Find the column in prod.
			if pi, ok := prodColIdx[sourceName]; ok && pi < len(row) {
				adaptedRow[si] = row[pi]
				if ri < len(prodNulls) && pi < len(prodNulls[ri]) {
					adaptedNull[si] = prodNulls[ri][pi]
				}
			} else {
				adaptedNull[si] = true
			}
		}

		adaptedRows = append(adaptedRows, adaptedRow)
		adaptedNulls = append(adaptedNulls, adaptedNull)
	}

	return shadowCols, adaptedRows, adaptedNulls
}

// executeAggregateRead handles aggregate queries (COUNT, SUM, etc.) on affected tables.
// P2 3.2: Full aggregate re-aggregation — supports COUNT, SUM, AVG, MIN, MAX,
// GROUP BY, HAVING, COUNT(DISTINCT), and falls back to materialization for
// complex aggregates like GROUP_CONCAT.
func (p *Proxy) executeAggregateRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.safeProdExecQuery(sqlStr, connID)
	}
	table := cl.Tables[0]
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Complex aggregates (GROUP_CONCAT, TOTAL) or GROUP BY queries
	// always use materialization for correctness.
	if cl.HasComplexAgg || strings.Contains(upper, "GROUP BY") {
		return p.materializeAndReexecute(sqlStr, cl, connID)
	}

	// Simple aggregates without GROUP BY: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX.
	// For these, we can do a simpler approach: materialize if dirty, else merged rows.

	// If the query is a simple bare aggregate, try the efficient path.
	baseSQL := p.buildAggregateBaseQuery(sqlStr, table)
	if baseSQL == "" {
		// Not a simple aggregate — use materialization.
		return p.materializeAndReexecute(sqlStr, cl, connID)
	}

	baseCl := *cl
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""

	baseSQL = p.capSQL(baseSQL)
	_, baseRows, _, err := p.mergedReadRows(baseSQL, &baseCl, connID)
	if err != nil {
		return p.safeProdExecQuery(sqlStr, connID)
	}

	count := len(baseRows)
	countStr := fmt.Sprintf("%d", count)

	columns := []string{"count"}
	rows := [][]sql.NullString{{{String: countStr, Valid: true}}}
	nulls := [][]bool{{false}}
	return buildMergedResponse(columns, rows, nulls)
}

// materializeAndReexecute materializes merged data into a temp table and re-executes
// the original query on Shadow. Used for complex aggregates, window functions, etc.
func (p *Proxy) materializeAndReexecute(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.safeProdExecQuery(sqlStr, connID)
	}
	table := cl.Tables[0]

	// Build base SELECT * FROM table.
	baseSQL := fmt.Sprintf(`SELECT * FROM %q`, table)
	baseSQL = p.capSQL(baseSQL)
	utilName, err := p.materializeToUtilTable(baseSQL, table, connID, p.maxRowsHydrate)
	if err != nil {
		if p.verbose {
			log.Printf("[conn %d] materialization failed: %v", connID, err)
		}
		return p.executeQuery(p.shadowDB, sqlStr, connID)
	}
	defer p.dropUtilTable(utilName)

	// Rewrite query to reference the temp table instead.
	rewritten := rewriteTableRef(sqlStr, table, utilName)
	return p.executeQuery(p.shadowDB, rewritten, connID)
}

// executeWindowFuncRead handles window function queries.
func (p *Proxy) executeWindowFuncRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	return p.materializeAndReexecute(sqlStr, cl, connID)
}

// executeSetOpRead handles UNION/INTERSECT/EXCEPT queries.
func (p *Proxy) executeSetOpRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	return p.materializeAndReexecute(sqlStr, cl, connID)
}

// executeComplexRead handles CTEs and derived tables.
func (p *Proxy) executeComplexRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	return p.materializeAndReexecute(sqlStr, cl, connID)
}

// executeJoinPatch handles JOIN queries involving dirty tables.
// P2 3.6: Materializes dirty tables into temp tables and rewrites the query.
func (p *Proxy) executeJoinPatch(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.safeProdExecQuery(sqlStr, connID)
	}

	// Identify which tables have deltas/tombstones (are "dirty").
	var dirtyTables []string
	for _, table := range cl.Tables {
		isDirty := false
		if p.deltaMap != nil && p.deltaMap.CountForTable(table) > 0 {
			isDirty = true
		}
		if p.tombstones != nil && p.tombstones.CountForTable(table) > 0 {
			isDirty = true
		}
		if p.schemaRegistry != nil {
			diff := p.schemaRegistry.GetDiff(table)
			if diff != nil && (diff.IsNewTable || diff.IsFullyShadowed || len(diff.Added) > 0 || len(diff.Dropped) > 0 || len(diff.Renamed) > 0) {
				isDirty = true
			}
		}
		if isDirty {
			dirtyTables = append(dirtyTables, table)
		}
	}

	// If no dirty tables, execute directly on Prod.
	if len(dirtyTables) == 0 {
		return p.safeProdExecQuery(sqlStr, connID)
	}

	// Materialize each dirty table into a temp table, rewrite the query.
	rewritten := sqlStr
	var utilNames []string
	for _, table := range dirtyTables {
		baseSQL := fmt.Sprintf(`SELECT * FROM %q`, table)
		utilName, err := p.materializeToUtilTable(baseSQL, table, connID, p.maxRowsHydrate)
		if err != nil {
			if p.verbose {
				log.Printf("[conn %d] JOIN patch: materialization failed for %s: %v", connID, table, err)
			}
			// Fall back to shadow-only execution.
			return p.executeQuery(p.shadowDB, sqlStr, connID)
		}
		utilNames = append(utilNames, utilName)
		rewritten = rewriteTableRef(rewritten, table, utilName)
	}

	// Clean up temp tables when done.
	defer func() {
		for _, name := range utilNames {
			p.dropUtilTable(name)
		}
	}()

	return p.executeQuery(p.shadowDB, rewritten, connID)
}

// rewriteTableRef replaces a table name in SQL with a different name.
func rewriteTableRef(sqlStr, oldTable, newTable string) string {
	// Replace quoted and unquoted references.
	re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(oldTable) + `\b`)
	return re.ReplaceAllString(sqlStr, `"`+newTable+`"`)
}

// buildAggregateBaseQuery converts a bare aggregate query (COUNT(*), COUNT(col))
// to SELECT pk FROM table [WHERE ...] for in-memory counting.
// Returns "" if the query is too complex for this approach.
func (p *Proxy) buildAggregateBaseQuery(sqlStr, table string) string {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Skip GROUP BY, HAVING queries (need full materialization).
	if strings.Contains(upper, "GROUP BY") || strings.Contains(upper, "HAVING") {
		return ""
	}

	// Skip complex aggregates that can't be re-aggregated from row-level data.
	if strings.Contains(upper, "SUM(") || strings.Contains(upper, "AVG(") ||
		strings.Contains(upper, "MIN(") || strings.Contains(upper, "MAX(") ||
		strings.Contains(upper, "GROUP_CONCAT(") || strings.Contains(upper, "TOTAL(") {
		return ""
	}

	// Must be a COUNT-only query at this point.
	if !strings.Contains(upper, "COUNT(") && !strings.Contains(upper, "COUNT (") {
		return ""
	}

	// Get the PK or fallback to rowid.
	meta, ok := p.tables[table]
	pkCol := ""
	if ok && len(meta.PKColumns) > 0 {
		pkCol = meta.PKColumns[0]
	} else {
		pkCol = "rowid"
	}

	selectIdx := strings.Index(upper, "SELECT")
	fromIdx := strings.Index(upper, " FROM ")
	if selectIdx < 0 || fromIdx < 0 {
		return ""
	}

	return "SELECT " + `"` + pkCol + `"` + sqlStr[fromIdx:]
}

// mergedReadRows is an internal version of executeMergedRead that returns
// the merged columns, rows, and nulls instead of a pgwire response.
func (p *Proxy) mergedReadRows(sqlStr string, cl *core.Classification, connID int64) (
	columns []string, rows [][]sql.NullString, nulls [][]bool, err error,
) {
	if cl == nil || len(cl.Tables) == 0 {
		cols, r, n, e := p.safeProdQuery(sqlStr, connID)
		return cols, r, n, e
	}
	table := cl.Tables[0]

	// Check if table only exists in shadow or is fully shadowed.
	if p.schemaRegistry != nil {
		diff := p.schemaRegistry.GetDiff(table)
		if diff != nil && (diff.IsNewTable || diff.IsFullyShadowed) {
			return queryToRows(p.shadowDB, sqlStr)
		}
		if _, exists := p.tables[table]; !exists {
			return queryToRows(p.shadowDB, sqlStr)
		}
	}

	// PK injection (or ROWID for PK-less tables).
	injectedPK := ""
	injectedRowID := false
	effectiveSQL := sqlStr
	if !cl.IsJoin {
		if meta, ok := p.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(sqlStr, pkCol) {
				effectiveSQL = injectPKColumn(sqlStr, pkCol)
				injectedPK = pkCol
			}
		} else if meta, ok := p.tables[table]; ok && len(meta.PKColumns) == 0 && meta.PKType == "none" {
			effectiveSQL = injectRowID(sqlStr)
			injectedRowID = true
		}
	}

	shadowCols, shadowRows, shadowNulls, shadowErr := queryToRows(p.shadowDB, effectiveSQL)
	if shadowErr != nil {
		return p.safeProdQuery(sqlStr, connID)
	}

	prodSQL := effectiveSQL
	if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
		rewritten, skipProd := rewriteForProd(prodSQL, p.schemaRegistry, cl.Tables)
		if skipProd {
			if injectedPK != "" {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, injectedPK)
			}
			if injectedRowID {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, "rowid")
			}
			return shadowCols, shadowRows, shadowNulls, nil
		}
		prodSQL = rewritten
	}

	// L2 write guard: use safeProdQuery for defense-in-depth.
	prodSQL = p.capSQL(prodSQL)
	prodCols, prodRows, prodNulls, prodErr := p.safeProdQuery(prodSQL, connID)
	if prodErr != nil {
		if p.schemaRegistry != nil && p.schemaRegistry.HasDiff(table) {
			if injectedPK != "" {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, injectedPK)
			}
			if injectedRowID {
				shadowCols, shadowRows, shadowNulls = stripInjectedPKColumn(shadowCols, shadowRows, shadowNulls, "rowid")
			}
			return shadowCols, shadowRows, shadowNulls, nil
		}
		return nil, nil, nil, prodErr
	}

	// Adapt prod rows for schema diffs.
	if p.schemaRegistry != nil && len(shadowCols) > 0 {
		prodCols, prodRows, prodNulls = adaptProdRows(prodCols, prodRows, prodNulls, shadowCols, table, p.schemaRegistry)
	}

	// Find PK column index (or rowid for PK-less tables).
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
	} else if injectedRowID {
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

	// Merge.
	merged := append(shadowRows, filteredProd...)
	mergedNulls := append(shadowNulls, filteredProdNulls...)

	// Dedup by PK.
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
	}

	cols := shadowCols
	if len(cols) == 0 {
		cols = prodCols
	}

	if injectedPK != "" {
		cols, merged, mergedNulls = stripInjectedPKColumn(cols, merged, mergedNulls, injectedPK)
	}
	if injectedRowID {
		cols, merged, mergedNulls = stripInjectedPKColumn(cols, merged, mergedNulls, "rowid")
	}

	return cols, merged, mergedNulls, nil
}

// queryToRows executes a SELECT query and returns columns, rows as NullString slices, and null flags.
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
		ptrs := make([]any, len(columns))
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

// buildMergedResponse constructs the full pgwire response from in-memory merged results.
func buildMergedResponse(columns []string, rows [][]sql.NullString, nulls [][]bool) []byte {
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
	resp = append(resp, buildReadyForQueryMsg()...)
	return resp
}

// rewriteLimit replaces the LIMIT value in a SQL string with a new limit.
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
		skipCols := toSkipSet(meta.GeneratedCols)
		escapedPK := strings.ReplaceAll(pk.PK, "'", "''")
		selectSQL := fmt.Sprintf(`SELECT * FROM %q WHERE %q = '%s'`, pk.Table, pkCol, escapedPK)
		cols, rows, _, err := p.safeProdQuery(selectSQL, connID)
		if err != nil || len(rows) == 0 {
			continue
		}
		insertSQL := buildHydrateInsert(pk.Table, cols, rows[0], skipCols)
		if _, err := p.shadowDB.Exec(insertSQL); err != nil {
			if p.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", connID, pk.Table, pk.PK, err)
			}
		}
	}
}

// buildHydrateInsert constructs an INSERT OR REPLACE statement for hydrating a row into shadow.
// Skips generated columns.
func buildHydrateInsert(table string, cols []string, row []sql.NullString, skipCols map[string]bool) string {
	var quotedCols []string
	var values []string
	for i, c := range cols {
		if skipCols[strings.ToLower(c)] {
			continue
		}
		quotedCols = append(quotedCols, `"`+c+`"`)
		if i < len(row) && row[i].Valid {
			values = append(values, "'"+strings.ReplaceAll(row[i].String, "'", "''")+"'")
		} else {
			values = append(values, "NULL")
		}
	}
	return fmt.Sprintf(`INSERT OR REPLACE INTO %q (%s) VALUES (%s)`,
		table, strings.Join(quotedCols, ", "), strings.Join(values, ", "))
}

// toSkipSet converts a slice of column names to a lowercase set for skipping.
func toSkipSet(cols []string) map[string]bool {
	if len(cols) == 0 {
		return nil
	}
	set := make(map[string]bool, len(cols))
	for _, c := range cols {
		set[strings.ToLower(c)] = true
	}
	return set
}

// trackWriteEffects updates delta/tombstone state after a write operation.
// Note: The specialized handlers (handleInsert, handleUpsert, handleUpdateWithHydration,
// handleDelete) perform more precise tracking. This fallback handles remaining cases.
func (p *Proxy) trackWriteEffects(cl *core.Classification, strategy core.RoutingStrategy, connID int64, txh *TxnHandler) {
	inTxn := txh != nil && txh.InTxn()

	switch strategy {
	case core.StrategyShadowDDL:
		p.trackDDLEffects(cl, connID)

	case core.StrategyShadowWrite:
		// Generic writes not handled by specialized handlers.
		for _, table := range cl.Tables {
			p.deltaMap.MarkInserted(table)
		}

	case core.StrategyHydrateAndWrite:
		// Track deltas from PK-identified writes.
		if p.deltaMap != nil {
			for _, pk := range cl.PKs {
				if inTxn {
					p.deltaMap.Stage(pk.Table, pk.PK)
				} else {
					p.deltaMap.Add(pk.Table, pk.PK)
				}
			}
			if !inTxn && len(cl.PKs) > 0 {
				delta.WriteDeltaMap(p.moriDir, p.deltaMap)
			}
		}

	case core.StrategyShadowDelete:
		// Track tombstones from PK-identified deletes.
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
	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "DROP COLUMN"):
		table, col := parseAlterDropColumn(sqlStr)
		if table != "" && col != "" {
			p.schemaRegistry.RecordDropColumn(table, col)
			if p.verbose {
				log.Printf("[conn %d] schema registry: DROP COLUMN %s.%s", connID, table, col)
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

	case strings.HasPrefix(upper, "CREATE TABLE"):
		for _, table := range cl.Tables {
			p.schemaRegistry.RecordNewTable(table)
			if p.verbose {
				log.Printf("[conn %d] schema registry: CREATE TABLE %s", connID, table)
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

// parseAlterDropColumn extracts table and column name from
// "ALTER TABLE <table> DROP COLUMN <col>".
func parseAlterDropColumn(sqlStr string) (table, col string) {
	re := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+("?\w+"?)\s+DROP\s+COLUMN\s+("?\w+"?)`)
	if m := re.FindStringSubmatch(sqlStr); len(m) > 2 {
		return strings.Trim(m[1], `"`), strings.Trim(m[2], `"`)
	}
	return "", ""
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

// extractWhereFromSQL extracts the WHERE clause from a SQL statement.
func extractWhereFromSQL(sqlStr string) string {
	upper := strings.ToUpper(sqlStr)
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return ""
	}
	rest := sqlStr[whereIdx+5:]

	// Strip ORDER BY, LIMIT, RETURNING, etc.
	for _, kw := range []string{"ORDER BY", "LIMIT", "RETURNING"} {
		if idx := strings.Index(strings.ToUpper(rest), kw); idx >= 0 {
			rest = rest[:idx]
		}
	}
	return strings.TrimSpace(rest)
}

// extractInsertPKValues extracts PK values from an INSERT VALUES clause.
func extractInsertPKValues(sqlStr, table, pkCol string) []string {
	upper := strings.ToUpper(sqlStr)

	// Find column list.
	parenStart := strings.Index(sqlStr, "(")
	if parenStart < 0 {
		return nil
	}
	parenEnd := strings.Index(sqlStr[parenStart:], ")")
	if parenEnd < 0 {
		return nil
	}
	parenEnd += parenStart
	colList := sqlStr[parenStart+1 : parenEnd]
	cols := strings.Split(colList, ",")

	pkIdx := -1
	for i, col := range cols {
		col = strings.TrimSpace(col)
		col = strings.Trim(col, `"`)
		if strings.EqualFold(col, pkCol) {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return nil
	}

	// Find VALUES.
	valuesIdx := strings.Index(upper, "VALUES")
	if valuesIdx < 0 {
		return nil
	}
	valuesPart := sqlStr[valuesIdx+6:]

	// Extract values from each row.
	var pks []string
	re := regexp.MustCompile(`\(([^)]+)\)`)
	for _, m := range re.FindAllStringSubmatch(valuesPart, -1) {
		if len(m) < 2 {
			continue
		}
		vals := strings.Split(m[1], ",")
		if pkIdx < len(vals) {
			val := strings.TrimSpace(vals[pkIdx])
			val = strings.Trim(val, "'")
			if val != "" && val != "NULL" && val != "null" {
				pks = append(pks, val)
			}
		}
	}

	return pks
}

// extractUpsertConflictValues extracts conflict key values from an INSERT ... ON CONFLICT
// or INSERT OR REPLACE statement.
func extractUpsertConflictValues(sqlStr, table, pkCol string) []string {
	// For INSERT OR REPLACE, the conflict column is the PK.
	// Extract PK values from the INSERT VALUES clause.
	return extractInsertPKValues(sqlStr, table, pkCol)
}

// needsPKInjection returns true if the query's SELECT list doesn't include the PK column.
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
	fromIdx := strings.Index(upper, " FROM ")
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

// validateFKsForInsert checks foreign key constraints for an INSERT statement.
// Validates that all referenced parent rows exist.
func (p *Proxy) validateFKsForInsert(fke *FKEnforcer, table, sqlStr string) error {
	if fke == nil || fke.schemaRegistry == nil {
		return nil
	}

	fks := fke.schemaRegistry.GetReferencingFKs(table)
	if len(fks) == 0 {
		// Also check if this table has FKs pointing outward.
		diff := fke.schemaRegistry.GetDiff(table)
		if diff == nil {
			return nil
		}
	}

	// Get FKs where this table is the child.
	allFKs := p.getAllFKsForChildTable(table)
	if len(allFKs) == 0 {
		return nil
	}

	// Extract column values from the INSERT.
	for _, fk := range allFKs {
		if fk.ChildTable != table || len(fk.ChildColumns) == 0 || len(fk.ParentColumns) == 0 {
			continue
		}
		childCol := fk.ChildColumns[0]
		parentCol := fk.ParentColumns[0]

		// Extract the value for this FK column from the INSERT VALUES.
		vals := extractInsertPKValues(sqlStr, table, childCol)
		for _, val := range vals {
			if val == "" || strings.EqualFold(val, "NULL") {
				continue // NULL is allowed for FK columns.
			}
			if err := fke.ValidateParentExists(fk.ParentTable, parentCol, val); err != nil {
				return err
			}
		}
	}

	return nil
}

// getAllFKsForChildTable returns all foreign keys where the given table is the child.
func (p *Proxy) getAllFKsForChildTable(table string) []coreSchema.ForeignKey {
	if p.schemaRegistry == nil {
		return nil
	}
	// The schema registry stores FKs indexed by child table.
	// GetReferencingFKs returns FKs where the given table is the PARENT.
	// We need to iterate and find FKs where ChildTable matches.
	// For now, return empty — FK validation on insert requires the registry
	// to expose child-table lookup. The core registry method RecordForeignKey
	// stores by child table, so we can query it directly.
	return p.schemaRegistry.GetForeignKeys(table)
}

// injectRowID injects "rowid" as the dedup column for PK-less tables.
// SQLite has an implicit rowid for most tables (unless WITHOUT ROWID).
func injectRowID(sqlStr string) string {
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
	return sqlStr[:insertPos] + ` rowid,` + sqlStr[insertPos:]
}

// SafeProdDB wraps a *sql.DB to provide L2 write guard inspection.
// All Exec calls are inspected and write SQL is rejected.
type SafeProdDB struct {
	inner   *sql.DB
	connID  int64
	verbose bool
}

// NewSafeProdDB creates a SafeProdDB wrapping the given database handle.
func NewSafeProdDB(inner *sql.DB, connID int64, verbose bool) *SafeProdDB {
	return &SafeProdDB{inner: inner, connID: connID, verbose: verbose}
}

// Query delegates to the inner database with L2 write inspection.
func (s *SafeProdDB) Query(query string, args ...any) (*sql.Rows, error) {
	if looksLikeWrite(query) {
		log.Printf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write Query to prod: %s",
			s.connID, truncateSQL(query, 120))
		return nil, fmt.Errorf("write guard: write query blocked from reaching production")
	}
	return s.inner.Query(query, args...)
}

// QueryRow delegates to the inner database with L2 write inspection.
func (s *SafeProdDB) QueryRow(query string, args ...any) *sql.Row {
	if looksLikeWrite(query) {
		log.Printf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write QueryRow to prod: %s",
			s.connID, truncateSQL(query, 120))
	}
	return s.inner.QueryRow(query, args...)
}

// Exec rejects write SQL to prod.
func (s *SafeProdDB) Exec(query string, args ...any) (sql.Result, error) {
	if looksLikeWrite(query) {
		log.Printf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked Exec to prod: %s",
			s.connID, truncateSQL(query, 120))
		return nil, fmt.Errorf("write guard: write query blocked from reaching production")
	}
	return s.inner.Exec(query, args...)
}

// safeProdQuery is a helper for prod database queries with L2 write guard.
// It inspects the SQL and rejects write operations from reaching production.
func (p *Proxy) safeProdQuery(sqlStr string, connID int64) (columns []string, rows [][]sql.NullString, nulls [][]bool, err error) {
	if looksLikeWrite(sqlStr) {
		log.Printf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write to prod via safeProdQuery: %s",
			connID, truncateSQL(sqlStr, 120))
		return nil, nil, nil, fmt.Errorf("write guard: write query blocked from reaching production")
	}
	return queryToRows(p.prodDB, sqlStr)
}

// safeProdExecQuery is a helper for prod database queries with L2 write guard.
// It inspects the SQL and uses the executeQuery path.
func (p *Proxy) safeProdExecQuery(sqlStr string, connID int64) []byte {
	if looksLikeWrite(sqlStr) {
		log.Printf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write to prod via safeProdExecQuery: %s",
			connID, truncateSQL(sqlStr, 120))
		return buildErrorResponse("write guard: write query blocked from reaching production")
	}
	return p.executeQuery(p.prodDB, sqlStr, connID)
}
