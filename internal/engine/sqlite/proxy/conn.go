package proxy

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/mori-dev/mori/internal/core"
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

		// Simple Query ('Q'): classify and route.
		if msg.Type == 'Q' {
			sqlStr := querySQL(msg.Payload)
			if sqlStr == "" {
				clientConn.Write(buildEmptyQueryResponse())
				continue
			}

			decision := p.classifyAndRoute(sqlStr, connID)

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
				resp := p.executeQuery(p.shadowDB, sqlStr, connID)
				clientConn.Write(resp)

			case targetBoth:
				// Execute on shadow first (discard result), then prod.
				p.executeQuery(p.shadowDB, sqlStr, connID)
				resp := p.executeQuery(p.prodDB, sqlStr, connID)
				clientConn.Write(resp)
			}
			continue
		}

		// Extended query protocol (Parse/Bind/Describe/Execute/Sync/Close):
		// For v1, return an error for unsupported message types.
		if msg.Type == 'P' || msg.Type == 'B' || msg.Type == 'D' || msg.Type == 'E' || msg.Type == 'H' {
			clientConn.Write(buildErrorResponse("mori-sqlite: extended query protocol not supported, use simple query mode"))
			continue
		}

		// Sync ('S'): respond with ReadyForQuery.
		if msg.Type == 'S' {
			clientConn.Write(buildReadyForQueryMsg())
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
		// For v1, fall back to prod for merged reads.
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
	// Determine if this is a query that returns rows.
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))
	isSelect := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "PRAGMA") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		(strings.HasPrefix(upper, "WITH") && !strings.Contains(upper, "INSERT") &&
			!strings.Contains(upper, "UPDATE") && !strings.Contains(upper, "DELETE"))

	if isSelect {
		return p.executeSelectQuery(db, sqlStr, connID)
	}
	return p.executeExecQuery(db, sqlStr, connID)
}

// executeSelectQuery handles queries that return rows (SELECT, PRAGMA, etc.).
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

	// Build RowDescription.
	colOIDs := make([]uint32, len(columns))
	for i := range colOIDs {
		colOIDs[i] = 25 // text OID
	}
	var resp []byte
	resp = append(resp, buildRowDescMsg(columns, colOIDs)...)

	// Read and build DataRows.
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

	// CommandComplete + ReadyForQuery.
	tag := fmt.Sprintf("SELECT %d", rowCount)
	resp = append(resp, buildCommandCompleteMsg(tag)...)
	resp = append(resp, buildReadyForQueryMsg()...)
	return resp
}

// executeExecQuery handles queries that don't return rows (INSERT, UPDATE, DELETE, DDL).
func (p *Proxy) executeExecQuery(db *sql.DB, sqlStr string, connID int64) []byte {
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
