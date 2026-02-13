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
// TNS Data messages that contain write SQL from being sent.
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

// Write inspects outgoing data for TNS Data messages containing write SQL.
func (s *SafeProdConn) Write(b []byte) (int, error) {
	// TNS packet: 2-byte length (BE) + 2-byte checksum + 1-byte type + 3 bytes reserved/checksum.
	// Type 6 = Data packet.
	if len(b) >= 9 && b[4] == tnsData {
		sql := extractQueryFromTNSData(b[8:])
		if sql != "" && looksLikeWrite(sql) {
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
	return s.inner.Write(b)
}

func (s *SafeProdConn) Read(b []byte) (int, error)         { return s.inner.Read(b) }
func (s *SafeProdConn) Close() error                        { return s.inner.Close() }
func (s *SafeProdConn) LocalAddr() net.Addr                 { return s.inner.LocalAddr() }
func (s *SafeProdConn) RemoteAddr() net.Addr                { return s.inner.RemoteAddr() }
func (s *SafeProdConn) SetDeadline(t time.Time) error       { return s.inner.SetDeadline(t) }
func (s *SafeProdConn) SetReadDeadline(t time.Time) error   { return s.inner.SetReadDeadline(t) }
func (s *SafeProdConn) SetWriteDeadline(t time.Time) error  { return s.inner.SetWriteDeadline(t) }

// looksLikeWrite returns true if the SQL appears to be a mutating statement.
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	safePrefixes := []string{
		"SELECT", "SET", "SHOW", "EXPLAIN",
		"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT",
		"ALTER SESSION",
	}
	for _, prefix := range safePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return false
		}
	}

	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
		"CREATE", "ALTER", "DROP",
		"GRANT", "REVOKE", "RENAME",
	}
	for _, prefix := range writePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	// PL/SQL blocks with DML.
	if strings.HasPrefix(upper, "DECLARE") {
		return true
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
