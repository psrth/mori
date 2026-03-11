package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/engine/redis/classify"
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

// SafeProdConn wraps a net.Conn to the production Redis and blocks any
// RESP commands that are write operations from being sent.
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

// Write inspects outgoing data for RESP commands containing write operations.
// If a write is detected, it logs a CRITICAL message and returns an error
// instead of forwarding to production.
func (s *SafeProdConn) Write(b []byte) (int, error) {
	// Try to extract the command name from RESP data.
	cmd := extractCommandFromRESP(b)
	if cmd != "" && classify.IsWriteCommand(cmd) {
		msg := fmt.Sprintf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write to prod: %s",
			s.connID, cmd)
		log.Printf("%s", msg)

		if s.logger != nil {
			s.logger.Log(logging.LogEntry{
				Level:  "critical",
				ConnID: s.connID,
				Event:  "write_guard_l2",
				Detail: msg,
			})
		}

		return 0, fmt.Errorf("write guard: write command %q blocked from reaching production", cmd)
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
// extractCommandFromRESP — extract command name from RESP bytes
// ---------------------------------------------------------------------------

// extractCommandFromRESP attempts to extract the Redis command name from
// raw RESP protocol bytes. Returns empty string if parsing fails.
func extractCommandFromRESP(data []byte) string {
	if len(data) < 4 {
		return ""
	}

	// RESP array: *N\r\n$M\r\ncmd\r\n...
	if data[0] != '*' {
		return ""
	}

	// Skip past *N\r\n
	idx := 1
	for idx < len(data) && data[idx] != '\n' {
		idx++
	}
	idx++ // skip \n

	if idx >= len(data) || data[idx] != '$' {
		return ""
	}

	// Skip past $M\r\n
	idx++
	cmdLen := 0
	for idx < len(data) && data[idx] >= '0' && data[idx] <= '9' {
		cmdLen = cmdLen*10 + int(data[idx]-'0')
		idx++
	}
	// Skip \r\n
	for idx < len(data) && (data[idx] == '\r' || data[idx] == '\n') {
		idx++
	}

	if idx+cmdLen > len(data) {
		return ""
	}

	return strings.ToUpper(string(data[idx : idx+cmdLen]))
}

// buildGuardErrorResponse constructs a RESP error response for write guard violations.
func buildGuardErrorResponse(message string) []byte {
	return BuildErrorReply("MORI " + message).Bytes()
}
