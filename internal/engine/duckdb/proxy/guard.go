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

// looksLikeWrite returns true if the given SQL string appears to be a mutating statement.
func looksLikeWrite(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	safePrefixes := []string{
		"SELECT", "EXPLAIN", "DESCRIBE", "SHOW", "PRAGMA",
		"BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE",
		"SET", "RESET", "CALL",
	}
	for _, prefix := range safePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return false
		}
	}

	writePrefixes := []string{
		"INSERT", "UPDATE", "DELETE",
		"CREATE", "ALTER", "DROP",
		"COPY", "EXPORT", "IMPORT",
	}
	for _, prefix := range writePrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	if strings.HasPrefix(upper, "WITH") {
		if strings.Contains(upper, "INSERT") ||
			strings.Contains(upper, "UPDATE") ||
			strings.Contains(upper, "DELETE") {
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
