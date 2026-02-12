package proxy

import (
	"fmt"
	"log"
	"net"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// ReadHandler encapsulates merged read logic for a single connection.
// It mirrors WriteHandler: one instance per connection, holds references
// to both backend connections and shared state.
type ReadHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	connID         int64
	verbose        bool
	logger         *logging.Logger
}

// HandleRead dispatches a read operation based on the routing strategy.
// On success, the synthesized response has been written to clientConn.
func (rh *ReadHandler) HandleRead(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyMergedRead:
		return rh.handleMergedRead(clientConn, cl)
	case core.StrategyJoinPatch:
		return rh.handleJoinPatch(clientConn, cl)
	default:
		return fmt.Errorf("unsupported read strategy: %s", strategy)
	}
}

func (rh *ReadHandler) logf(format string, args ...interface{}) {
	if rh.verbose {
		prefix := fmt.Sprintf("[conn %d] ", rh.connID)
		log.Printf(prefix+format, args...)
	}
}
