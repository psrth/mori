package proxy

import (
	"fmt"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// WriteHandler encapsulates write path logic for a single connection.
// It holds references to the backend connections and shared state.
type WriteHandler struct {
	prodConn   net.Conn
	shadowConn net.Conn
	deltaMap   *delta.Map
	tombstones *delta.TombstoneSet
	tables     map[string]schema.TableMeta
	moriDir    string
	connID     int64
	verbose    bool
	txnHandler *TxnHandler // nil when no TxnHandler; used to check inTxn state
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
		return w.handleUpdate(clientConn, rawMsg, cl)
	case core.StrategyShadowDelete:
		return w.handleDelete(clientConn, rawMsg, cl)
	default:
		return fmt.Errorf("unsupported write strategy: %s", strategy)
	}
}
