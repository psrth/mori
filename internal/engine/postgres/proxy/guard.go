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
// simple-query ('Q') messages that contain write SQL from being sent.
// It implements the full net.Conn interface so it can be used as a drop-in
// replacement anywhere prodConn is used.
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

// Write inspects outgoing data for simple-query messages containing write SQL.
// If a write is detected, it logs a CRITICAL message and returns an error
// instead of forwarding to production.
func (s *SafeProdConn) Write(b []byte) (int, error) {
	if len(b) >= 5 && b[0] == 'Q' {
		sql := querySQL(b[5:])
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
	return s.inner.Write(b)
}

// Read delegates to the inner connection.
func (s *SafeProdConn) Read(b []byte) (int, error) {
	return s.inner.Read(b)
}

// Close delegates to the inner connection.
func (s *SafeProdConn) Close() error {
	return s.inner.Close()
}

// LocalAddr delegates to the inner connection.
func (s *SafeProdConn) LocalAddr() net.Addr {
	return s.inner.LocalAddr()
}

// RemoteAddr delegates to the inner connection.
func (s *SafeProdConn) RemoteAddr() net.Addr {
	return s.inner.RemoteAddr()
}

// SetDeadline delegates to the inner connection.
func (s *SafeProdConn) SetDeadline(t time.Time) error {
	return s.inner.SetDeadline(t)
}

// SetReadDeadline delegates to the inner connection.
func (s *SafeProdConn) SetReadDeadline(t time.Time) error {
	return s.inner.SetReadDeadline(t)
}

// SetWriteDeadline delegates to the inner connection.
func (s *SafeProdConn) SetWriteDeadline(t time.Time) error {
	return s.inner.SetWriteDeadline(t)
}

// ---------------------------------------------------------------------------
// looksLikeWrite — heuristic SQL write detection
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

// looksLikeWrite returns true if the given SQL string appears to be a mutating
// statement (INSERT, UPDATE, DELETE, DDL, etc.). It uses prefix matching and
// handles CTE writes (WITH ... INSERT/UPDATE/DELETE).
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	// Write prefixes — these are always writes.
	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE", "TRUNCATE",
		"CREATE", "ALTER", "DROP",
		"GRANT", "REVOKE",
		"COPY", "DO", "CALL",
	}

	// Safe prefixes — these are never writes.
	safePrefixes := []string{
		"SELECT", "SET", "SHOW", "EXPLAIN", "BEGIN", "COMMIT", "ROLLBACK",
		"SAVEPOINT", "RELEASE", "DEALLOCATE", "CLOSE", "FETCH", "DECLARE",
		"DISCARD", "LISTEN", "NOTIFY", "UNLISTEN", "RESET",
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

	// CTE writes: WITH ... INSERT/UPDATE/DELETE.
	if strings.HasPrefix(upper, "WITH") {
		if containsKeyword(upper, "INSERT") ||
			containsKeyword(upper, "UPDATE") ||
			containsKeyword(upper, "DELETE") {
			return true
		}
	}

	// Default: not a write.
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
				containsKeyword(seg, "DELETE") {
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

// ---------------------------------------------------------------------------
// buildGuardErrorResponse — pgwire error + ReadyForQuery
// ---------------------------------------------------------------------------

// buildGuardErrorResponse constructs a pgwire ErrorResponse followed by a
// ReadyForQuery message. The error uses SQLSTATE code "MR001" to identify
// it as a Mori write guard error.
func buildGuardErrorResponse(message string) []byte {
	var errPayload []byte
	errPayload = append(errPayload, 'S')
	errPayload = append(errPayload, []byte("ERROR")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'C')
	errPayload = append(errPayload, []byte("MR001")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'M')
	errPayload = append(errPayload, []byte(message)...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 0) // terminator

	var result []byte
	result = append(result, buildPGMsg('E', errPayload)...)
	result = append(result, buildReadyForQueryMsg()...)
	return result
}
