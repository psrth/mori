package proxy

import (
	"fmt"
	"log"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/firestore/classify"
	"github.com/mori-dev/mori/internal/logging"
)

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

// guardProdMethod checks whether a gRPC method being sent to prod is a write
// method. If so, it blocks the call and logs a critical message.
// This is L2 of the write guard.
func guardProdMethod(method string, connID int64, verbose bool, logger *logging.Logger) error {
	if !classify.IsWriteMethod(method) {
		return nil
	}

	msg := fmt.Sprintf("[CRITICAL] [conn %d] WRITE GUARD L2: blocked write method %q from reaching prod",
		connID, method)
	log.Printf("%s", msg)

	if logger != nil {
		logger.Log(logging.LogEntry{
			Level:  "critical",
			ConnID: connID,
			Event:  "write_guard_l2",
			Detail: msg,
		})
	}

	return fmt.Errorf("write guard: write method %q blocked from reaching production", method)
}
