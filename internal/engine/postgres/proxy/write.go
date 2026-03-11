package proxy

import (
	"fmt"
	"net"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/engine/postgres/schema"
	"github.com/psrth/mori/internal/logging"
)

// WriteHandler encapsulates write path logic for a single connection.
// It holds references to the backend connections and shared state.
type WriteHandler struct {
	prodConn        net.Conn
	shadowConn      net.Conn
	deltaMap        *delta.Map
	tombstones      *delta.TombstoneSet
	tables          map[string]schema.TableMeta
	schemaRegistry  *coreSchema.Registry
	moriDir         string
	connID          int64
	verbose         bool
	logger          *logging.Logger
	txnHandler      *TxnHandler // nil when no TxnHandler; used to check inTxn state
	maxRowsHydrate  int         // cap for cross-table hydration row count; 0 = unlimited
	fkEnforcer      *FKEnforcer // nil if FK enforcement is disabled
}

// inTxn reports whether this connection is inside an explicit transaction.
func (w *WriteHandler) inTxn() bool {
	return w.txnHandler != nil && w.txnHandler.InTxn()
}

// HandleWrite dispatches a write operation based on the routing strategy.
// On success, the response has already been relayed to the client.
func (w *WriteHandler) HandleWrite(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyShadowWrite:
		return w.handleInsert(clientConn, rawMsg, cl)
	case core.StrategyHydrateAndWrite:
		if cl.SubType == core.SubInsert {
			return w.handleUpsert(clientConn, rawMsg, cl)
		}
		return w.handleUpdate(clientConn, rawMsg, cl)
	case core.StrategyShadowDelete:
		return w.handleDelete(clientConn, rawMsg, cl)
	default:
		return fmt.Errorf("unsupported write strategy: %s", strategy)
	}
}
