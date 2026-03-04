package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

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
	prodConn        net.Conn
	shadowConn      net.Conn
	deltaMap        *delta.Map
	tombstones      *delta.TombstoneSet
	tables          map[string]schema.TableMeta
	schemaRegistry  *coreSchema.Registry
	connID          int64
	verbose         bool
	logger          *logging.Logger
	maxRowsHydrate  int // cap on rows hydrated from Prod during materialization; 0 = unlimited
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
		// Set operations (UNION/INTERSECT/EXCEPT) need decomposition.
		if cl.HasSetOp {
			return rh.handleSetOperation(clientConn, cl)
		}
		// Cursor operations (DECLARE/FETCH/CLOSE) need materialization.
		if cl.HasCursor || cl.SubType == core.SubCursor {
			return rh.handleCursor(clientConn, cl)
		}
		// Complex reads (CTEs, derived tables) need decomposition.
		if cl.IsComplexRead {
			return rh.handleComplexRead(clientConn, cl)
		}
		if cl.HasWindowFunc {
			return rh.handleWindowRead(clientConn, cl)
		}
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

// capSQL appends a LIMIT clause to a SQL query for materialization reads
// when maxRowsHydrate is configured. If the query already contains a LIMIT
// clause, the SQL is returned unchanged to avoid double-limiting.
func (rh *ReadHandler) capSQL(sql string) string {
	if rh.maxRowsHydrate <= 0 {
		return sql
	}
	if strings.Contains(strings.ToUpper(sql), " LIMIT ") {
		return sql
	}
	return sql + fmt.Sprintf(" LIMIT %d", rh.maxRowsHydrate)
}
