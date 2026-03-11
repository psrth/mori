package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/logging"
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

// findSemicolonOutsideQuotes returns the index of the first semicolon
// that is not inside a single-quoted string literal or a comment, or -1
// if none found.
func findSemicolonOutsideQuotes(s string) int {
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++ // skip escaped ''
				} else {
					inQuote = false
				}
			}
			continue
		}
		switch c {
		case '\'':
			inQuote = true
		case '-':
			// Line comment: skip to newline
			if i+1 < len(s) && s[i+1] == '-' {
				nl := strings.Index(s[i:], "\n")
				if nl >= 0 {
					i += nl // loop will increment past \n
				} else {
					return -1 // rest is a comment
				}
			}
		case '/':
			// Block comment: skip to */
			if i+1 < len(s) && s[i+1] == '*' {
				end := strings.Index(s[i+2:], "*/")
				if end >= 0 {
					i += 2 + end + 1 // skip past */
				} else {
					return -1 // unclosed block comment
				}
			}
		case ';':
			return i
		}
	}
	return -1
}

// stripLeadingComments removes leading SQL line comments (-- ...\n)
// and block comments (/* ... */) from a string.
func stripLeadingComments(s string) string {
	for {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "--") {
			if nl := strings.Index(s, "\n"); nl >= 0 {
				s = s[nl+1:]
			} else {
				return ""
			}
		} else if strings.HasPrefix(s, "/*") {
			if end := strings.Index(s, "*/"); end >= 0 {
				s = s[end+2:]
			} else {
				return ""
			}
		} else {
			return s
		}
	}
}

// looksLikeWrite returns true if the SQL appears to be a mutating T-SQL statement.
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
		"CREATE", "ALTER", "DROP",
		"GRANT", "REVOKE",
		"BULK INSERT",
		"EXEC", "EXECUTE",
	}

	safePrefixes := []string{
		"SELECT", "SET", "PRINT",
		"BEGIN", "COMMIT", "ROLLBACK",
		"DECLARE", "USE", "DBCC", "WAITFOR",
		"RAISERROR", "THROW",
	}
	for _, prefix := range safePrefixes {
		if strings.HasPrefix(upper, prefix) {
			// Multi-statement: even if first statement is safe, check
			// subsequent statements after semicolons.
			if idx := findSemicolonOutsideQuotes(upper); idx >= 0 {
				if multiStmtHasWrite(upper[idx+1:], writePrefixes) {
					return true
				}
			}
			return false
		}
	}

	for _, prefix := range writePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	// CTE writes.
	if strings.HasPrefix(upper, "WITH") {
		if containsKeyword(upper, "INSERT") ||
			containsKeyword(upper, "UPDATE") ||
			containsKeyword(upper, "DELETE") ||
			containsKeyword(upper, "MERGE") {
			return true
		}
	}

	return false
}

// multiStmtHasWrite checks whether any segment in the semicolon-separated
// remainder contains a write prefix or a CTE write.
func multiStmtHasWrite(rest string, writePrefixes []string) bool {
	for rest != "" {
		idx := findSemicolonOutsideQuotes(rest)
		var seg string
		if idx >= 0 {
			seg = rest[:idx]
			rest = rest[idx+1:]
		} else {
			seg = rest
			rest = ""
		}
		seg = strings.ToUpper(stripLeadingComments(strings.TrimSpace(seg)))
		if seg == "" {
			continue
		}
		for _, wp := range writePrefixes {
			if strings.HasPrefix(seg, wp) {
				return true
			}
		}
		if strings.HasPrefix(seg, "WITH") {
			if containsKeyword(seg, "INSERT") ||
				containsKeyword(seg, "UPDATE") ||
				containsKeyword(seg, "DELETE") ||
				containsKeyword(seg, "MERGE") {
				return true
			}
		}
	}
	return false
}

// containsKeyword checks if s contains keyword as a standalone word
// (not as a substring of an identifier like "insert_log").
func containsKeyword(s, keyword string) bool {
	for idx := 0; idx < len(s); {
		pos := strings.Index(s[idx:], keyword)
		if pos < 0 {
			return false
		}
		pos += idx
		leftOK := pos == 0 || !isIdentChar(s[pos-1])
		end := pos + len(keyword)
		rightOK := end >= len(s) || !isIdentChar(s[end])
		if leftOK && rightOK {
			return true
		}
		idx = pos + len(keyword)
	}
	return false
}

func isIdentChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'
}

func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}
