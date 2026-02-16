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
		txh = &TxnHandler{proxy: p, connID: connID}
	}

	var eh *ExtHandler
	if p.classifier != nil && p.router != nil {
		eh = &ExtHandler{
			proxy:     p,
			connID:    connID,
			txh:       txh,
			stmtCache: make(map[string]string),
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

			// Handle merged reads: query both prod and shadow, merge in-process.
			if decision.strategy == core.StrategyMergedRead || decision.strategy == core.StrategyJoinPatch {
				resp := p.executeMergedRead(sqlStr, decision.classification, connID)
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
				// For HydrateAndWrite (UPDATE), hydrate from prod first.
				if decision.strategy == core.StrategyHydrateAndWrite && decision.classification != nil {
					p.hydrateBeforeUpdate(decision.classification, connID)
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

	default:
		return routeDecision{target: targetProd, strategy: strategy}
	}
}

// executeQuery runs a SQL query against a database and returns the pgwire response bytes.
func (p *Proxy) executeQuery(db *sql.DB, sqlStr string, connID int64) []byte {
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

	colOIDs := make([]uint32, len(columns))
	for i := range colOIDs {
		colOIDs[i] = 25 // text OID
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

	// PK injection.
	injectedPK := ""
	effectiveSQL := sqlStr
	if !cl.IsJoin {
		if meta, ok := p.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(sqlStr, pkCol) {
				effectiveSQL = injectPKColumn(sqlStr, pkCol)
				injectedPK = pkCol
			}
		}
	}

	// Query shadow.
	shadowCols, shadowRows, shadowNulls, shadowErr := queryToRows(p.shadowDB, effectiveSQL)
	if shadowErr != nil {
		if p.verbose {
			log.Printf("[conn %d] merged read: shadow query error: %v", connID, shadowErr)
		}
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	// Query prod (with overfetch for LIMIT queries).
	prodSQL := effectiveSQL
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

	// Find PK column index.
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

	// Merge — shadow first, then filtered prod.
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

// executeAggregateRead handles aggregate queries.
func (p *Proxy) executeAggregateRead(sqlStr string, cl *core.Classification, connID int64) []byte {
	if len(cl.Tables) == 0 {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}
	table := cl.Tables[0]

	baseSQL := p.buildAggregateBaseQuery(sqlStr, table)
	if baseSQL == "" {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	baseCl := *cl
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""

	_, baseRows, _, err := p.mergedReadRows(baseSQL, &baseCl, connID)
	if err != nil {
		return p.executeQuery(p.prodDB, sqlStr, connID)
	}

	count := len(baseRows)
	countStr := fmt.Sprintf("%d", count)

	columns := []string{"count"}
	rows := [][]sql.NullString{{sql.NullString{String: countStr, Valid: true}}}
	nulls := [][]bool{{false}}
	return buildMergedResponse(columns, rows, nulls)
}

func (p *Proxy) buildAggregateBaseQuery(sqlStr, table string) string {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	if strings.Contains(upper, "GROUP BY") {
		return ""
	}

	meta, ok := p.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return ""
	}
	pkCol := meta.PKColumns[0]

	selectIdx := strings.Index(upper, "SELECT")
	fromIdx := strings.Index(upper, " FROM ")
	if selectIdx < 0 || fromIdx < 0 {
		return ""
	}

	return "SELECT " + `"` + pkCol + `"` + sqlStr[fromIdx:]
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
		}
	}

	shadowCols, shadowRows, shadowNulls, shadowErr := queryToRows(p.shadowDB, effectiveSQL)
	if shadowErr != nil {
		return queryToRows(p.prodDB, sqlStr)
	}

	prodSQL := effectiveSQL
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
		insertSQL := buildHydrateInsert(pk.Table, cols, rows[0])
		if _, err := p.shadowDB.Exec(insertSQL); err != nil {
			if p.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", connID, pk.Table, pk.PK, err)
			}
		}
	}
}

// buildHydrateInsert constructs an INSERT OR REPLACE statement.
// DuckDB uses INSERT OR REPLACE syntax for upserts.
func buildHydrateInsert(table string, cols []string, row []sql.NullString) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = `"` + c + `"`
	}
	values := make([]string, len(row))
	for i, v := range row {
		if !v.Valid {
			values[i] = "NULL"
		} else {
			values[i] = "'" + strings.ReplaceAll(v.String, "'", "''") + "'"
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
		for _, table := range cl.Tables {
			p.deltaMap.MarkInserted(table)
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
