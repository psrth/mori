package proxy

import (
	"fmt"
	"log"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/logging"
)

// ---------------------------------------------------------------------------
// L1: validateRouteDecision — routing assertion
// ---------------------------------------------------------------------------

// validateRouteDecision checks that write/DDL operations are never routed to
// production via StrategyProdDirect.
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

// looksLikeWrite returns true if the given SQL string appears to be a mutating statement.
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE", "REPLACE",
		"CREATE", "ALTER", "DROP",
		"VACUUM", "REINDEX",
		"CALL",
	}

	safePrefixes := []string{
		"SELECT", "PRAGMA", "EXPLAIN", "BEGIN", "COMMIT", "END",
		"ROLLBACK", "SAVEPOINT", "RELEASE", "ANALYZE", "ATTACH", "DETACH",
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

	if strings.HasPrefix(upper, "WITH") {
		if containsKeyword(upper, "INSERT") ||
			containsKeyword(upper, "UPDATE") ||
			containsKeyword(upper, "DELETE") {
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

func isIdentChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'
}

func truncateSQL(sql string, maxLen int) string {
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}
