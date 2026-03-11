package proxy

import (
	"fmt"
	"log"
	"net"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/psrth/mori/internal/core"
)

// handleCursor handles DECLARE CURSOR statements by materializing the cursor's
// query through the merged read pipeline, then rewriting the DECLARE to use
// a temp table on shadow. FETCH and CLOSE are forwarded to shadow directly.
func (rh *ReadHandler) handleCursor(clientConn net.Conn, cl *core.Classification) error {
	sql := cl.RawSQL

	// Parse to identify the cursor operation.
	result, err := pg_query.Parse(sql)
	if err != nil {
		// Can't parse — forward to shadow.
		return forwardAndRelay(buildQueryMsg(sql), rh.shadowConn, clientConn)
	}

	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return forwardAndRelay(buildQueryMsg(sql), rh.shadowConn, clientConn)
	}

	stmt := stmts[0].GetStmt()

	// DECLARE CURSOR
	if decl := stmt.GetDeclareCursorStmt(); decl != nil {
		return rh.handleDeclareCursor(clientConn, cl, decl)
	}

	// FETCH and CLOSE — forward to shadow directly.
	if stmt.GetFetchStmt() != nil || stmt.GetClosePortalStmt() != nil {
		return forwardAndRelay(buildQueryMsg(sql), rh.shadowConn, clientConn)
	}

	// Unknown cursor operation — forward to shadow.
	return forwardAndRelay(buildQueryMsg(sql), rh.shadowConn, clientConn)
}

// handleDeclareCursor materializes the cursor's query through merged read,
// then rewrites the DECLARE to reference the temp table.
func (rh *ReadHandler) handleDeclareCursor(
	clientConn net.Conn,
	cl *core.Classification,
	decl *pg_query.DeclareCursorStmt,
) error {
	cursorName := decl.GetPortalname()
	cursorQuery := decl.GetQuery()

	if cursorQuery == nil {
		return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.shadowConn, clientConn)
	}

	// Deparse the cursor's query.
	queryResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{Stmt: cursorQuery},
		},
	}
	querySQL, err := pg_query.Deparse(queryResult)
	if err != nil {
		return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.shadowConn, clientConn)
	}

	// Check if the cursor query references dirty tables.
	// If not, just forward to shadow as-is.
	dirty := false
	for _, table := range cl.Tables {
		if rh.deltaMap != nil && rh.deltaMap.CountForTable(table) > 0 {
			dirty = true
			break
		}
		if rh.tombstones != nil && rh.tombstones.CountForTable(table) > 0 {
			dirty = true
			break
		}
		if rh.schemaRegistry != nil && rh.schemaRegistry.HasDiff(table) {
			dirty = true
			break
		}
	}

	if !dirty {
		return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.shadowConn, clientConn)
	}

	// Run the cursor's query through merged read.
	cappedSQL := rh.capSQL(querySQL)
	queryCl := &core.Classification{
		Tables: cl.Tables,
		RawSQL: cappedSQL,
	}
	columns, values, nulls, err := rh.mergedReadCore(queryCl, cappedSQL)
	if err != nil {
		if rh.verbose {
			log.Printf("[conn %d] cursor materialization failed, forwarding to shadow: %v", rh.connID, err)
		}
		return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.shadowConn, clientConn)
	}

	// Materialize into temp table.
	utilName := utilTableName("cursor_" + cursorName + "_" + querySQL)
	createSQL := buildCreateTempTableSQL(utilName, columns)
	createResult, err := execQuery(rh.shadowConn, createSQL)
	if err != nil || createResult.Error != "" {
		// Table might already exist — drop and recreate.
		dropUtilTable(rh.shadowConn, utilName)
		if _, err := execQuery(rh.shadowConn, createSQL); err != nil {
			return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.shadowConn, clientConn)
		}
	}
	// Note: don't drop the temp table here — it needs to persist for FETCH.
	// It will be cleaned up on connection close.

	if len(values) > 0 {
		if err := bulkInsertToUtilTable(rh.shadowConn, utilName, columns, values, nulls); err != nil {
			dropUtilTable(rh.shadowConn, utilName)
			return forwardAndRelay(buildQueryMsg(cl.RawSQL), rh.shadowConn, clientConn)
		}
	}

	if rh.verbose {
		log.Printf("[conn %d] cursor %s: materialized %d rows into %s", rh.connID, cursorName, len(values), utilName)
	}

	// Rewrite the DECLARE to use the temp table.
	// Build: DECLARE cursorName CURSOR FOR SELECT * FROM utilName
	rewrittenSQL := fmt.Sprintf("DECLARE %s CURSOR FOR SELECT * FROM %s",
		quoteIdent(cursorName), quoteIdent(utilName))

	// Forward the rewritten DECLARE to shadow.
	return forwardAndRelay(buildQueryMsg(rewrittenSQL), rh.shadowConn, clientConn)
}
