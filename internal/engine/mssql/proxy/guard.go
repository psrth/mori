package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/logging"
)

// ---------------------------------------------------------------------------
// L1: validateRouteDecision — routing assertion
// ---------------------------------------------------------------------------

// validateRouteDecision checks that write/DDL operations are never routed to
// production via StrategyProdDirect. Returns an error if the routing decision
// would send a mutating operation to production.
func validateRouteDecision(cl *core.Classification, strategy core.RoutingStrategy, connID int64, logger *logging.Logger) error {
	if cl == nil {
		return nil
	}
	if strategy != core.StrategyProdDirect {
		return nil
	}
	if cl.OpType != core.OpWrite && cl.OpType != core.OpDDL {
		return nil
	}

	msg := fmt.Sprintf("[CRITICAL] [conn %d] WRITE GUARD L1: %s/%s routed to PROD_DIRECT — overriding to SHADOW_WRITE",
		connID, cl.OpType, cl.SubType)
	log.Printf("%s", msg)

	if logger != nil {
		logger.Log(logging.LogEntry{
			Level:  "critical",
			ConnID: connID,
			Event:  "write_guard_l1",
			Detail: msg,
		})
	}

	return fmt.Errorf("write guard: %s/%s must not route to prod", cl.OpType, cl.SubType)
}

// ---------------------------------------------------------------------------
// L2: SafeProdConn — connection wrapper that inspects outgoing writes
// ---------------------------------------------------------------------------

// SafeProdConn wraps a net.Conn to the production database and blocks any
// SQL_BATCH messages that contain write SQL from being sent.
type SafeProdConn struct {
	inner   net.Conn
	connID  int64
	verbose bool
	logger  *logging.Logger
}

// NewSafeProdConn creates a new SafeProdConn wrapping the given connection.
func NewSafeProdConn(inner net.Conn, connID int64, verbose bool, logger *logging.Logger) *SafeProdConn {
	return &SafeProdConn{
		inner:   inner,
		connID:  connID,
		verbose: verbose,
		logger:  logger,
	}
}

// Write inspects outgoing data for SQL_BATCH and RPC messages containing write SQL.
// P3 §4.7: Extended to also inspect RPC (sp_executesql) packets.
func (s *SafeProdConn) Write(b []byte) (int, error) {
	// TDS packet: 8-byte header + payload.
	if len(b) >= tdsHeaderSize+4 {
		pktType := b[0]
		if pktType == typeSQLBatch {
			payload := b[tdsHeaderSize:]
			sql := extractSQLFromBatch(payload)
			if looksLikeWrite(sql) {
				msg := fmt.Sprintf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write to prod: %s",
					s.connID, truncateSQL(sql, 120))
				log.Printf("%s", msg)

				if s.logger != nil {
					s.logger.Log(logging.LogEntry{
						Level:  "critical",
						ConnID: s.connID,
						Event:  "write_guard_l2",
						Detail: msg,
					})
				}

				return 0, fmt.Errorf("write guard: write query blocked from reaching production")
			}
		}

		// P3 §4.7: Inspect RPC packets for sp_executesql containing writes.
		if pktType == typeRPC {
			payload := b[tdsHeaderSize:]
			_, sqlText, ok := parseRPCPayload(payload)
			if ok && sqlText != "" && looksLikeWrite(sqlText) {
				msg := fmt.Sprintf("[CRITICAL] [conn %d] WRITE GUARD L2 (RPC): blocked write to prod: %s",
					s.connID, truncateSQL(sqlText, 120))
				log.Printf("%s", msg)

				if s.logger != nil {
					s.logger.Log(logging.LogEntry{
						Level:  "critical",
						ConnID: s.connID,
						Event:  "write_guard_l2_rpc",
						Detail: msg,
					})
				}

				return 0, fmt.Errorf("write guard: write RPC blocked from reaching production")
			}
		}
	}
	return s.inner.Write(b)
}

func (s *SafeProdConn) Read(b []byte) (int, error)         { return s.inner.Read(b) }
func (s *SafeProdConn) Close() error                        { return s.inner.Close() }
func (s *SafeProdConn) LocalAddr() net.Addr                 { return s.inner.LocalAddr() }
func (s *SafeProdConn) RemoteAddr() net.Addr                { return s.inner.RemoteAddr() }
func (s *SafeProdConn) SetDeadline(t time.Time) error       { return s.inner.SetDeadline(t) }
func (s *SafeProdConn) SetReadDeadline(t time.Time) error   { return s.inner.SetReadDeadline(t) }
func (s *SafeProdConn) SetWriteDeadline(t time.Time) error  { return s.inner.SetWriteDeadline(t) }

// ---------------------------------------------------------------------------
// looksLikeWrite — T-SQL version
// ---------------------------------------------------------------------------

// looksLikeWrite returns true if the SQL appears to be a mutating T-SQL statement.
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	safePrefixes := []string{
		"SELECT", "SET", "EXEC", "EXECUTE", "PRINT",
		"BEGIN", "COMMIT", "ROLLBACK",
		"DECLARE", "USE", "DBCC", "WAITFOR",
		"RAISERROR", "THROW",
	}
	for _, prefix := range safePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return false
		}
	}

	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
		"CREATE", "ALTER", "DROP",
		"GRANT", "REVOKE",
		"BULK INSERT",
	}
	for _, prefix := range writePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	// CTE writes.
	if strings.HasPrefix(upper, "WITH") {
		if strings.Contains(upper, "INSERT") ||
			strings.Contains(upper, "UPDATE") ||
			strings.Contains(upper, "DELETE") ||
			strings.Contains(upper, "MERGE") {
			return true
		}
	}

	return false
}

func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}
