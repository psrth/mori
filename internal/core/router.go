package core

import (
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
)

// Router decides execution strategy based on classification and delta state.
type Router struct {
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	schemaRegistry *coreSchema.Registry
}

// NewRouter creates a new Router with the given delta, tombstone, and schema state.
// If deltaMap, tombstones, or schemaRegistry is nil, the router treats the
// corresponding dimension as clean.
func NewRouter(deltaMap *delta.Map, tombstones *delta.TombstoneSet, schemaRegistry *coreSchema.Registry) *Router {
	return &Router{
		deltaMap:       deltaMap,
		tombstones:     tombstones,
		schemaRegistry: schemaRegistry,
	}
}

// Route returns the execution strategy for a classified query.
func (r *Router) Route(c *Classification) RoutingStrategy {
	// Not-supported features always return an error.
	if c.SubType == SubNotSupported {
		return StrategyNotSupported
	}

	switch c.OpType {
	case OpRead:
		if r.anyTableAffected(c.Tables) {
			// Set operations (UNION/INTERSECT/EXCEPT) on affected tables need
			// decomposition — each sub-SELECT runs through merged read, then
			// the set operation is applied in memory.
			if c.HasSetOp {
				return StrategyMergedRead
			}
			// Complex reads (CTEs, derived tables) on affected tables use
			// temp table materialization via merged read path.
			if c.IsComplexRead {
				return StrategyMergedRead
			}
			// Aggregate queries (COUNT, SUM, GROUP BY, etc.) on affected tables
			// need row-level merge then re-aggregation. Route through MergedRead
			// which handles aggregates via Shadow-only execution.
			if c.HasAggregate {
				return StrategyMergedRead
			}
			if c.IsJoin {
				return StrategyJoinPatch
			}
			return StrategyMergedRead
		}
		return StrategyProdDirect

	case OpWrite:
		switch c.SubType {
		case SubInsert:
			// INSERT ... ON CONFLICT needs hydration like UPDATE.
			if c.HasOnConflict {
				return StrategyHydrateAndWrite
			}
			return StrategyShadowWrite
		case SubUpdate:
			return StrategyHydrateAndWrite
		case SubDelete:
			return StrategyShadowDelete
		case SubTruncate:
			return StrategyTruncate
		case SubNotify:
			return StrategyNotSupported
		default:
			return StrategyShadowWrite
		}

	case OpDDL:
		return StrategyShadowDDL

	case OpTransaction:
		return StrategyTransaction

	case OpOther:
		switch c.SubType {
		case SubSet:
			return StrategyForwardBoth
		case SubShow:
			return StrategyProdDirect
		case SubExplain:
			// EXPLAIN on dirty tables is unreliable — error out.
			if r.anyTableAffected(c.Tables) {
				return StrategyNotSupported
			}
			return StrategyProdDirect
		case SubCursor:
			// Cursor operations (DECLARE/FETCH/CLOSE) on affected tables need
			// materialization through merged read. FETCH/CLOSE have no tables
			// and will be forwarded to shadow by the handler.
			if c.HasCursor && r.anyTableAffected(c.Tables) {
				return StrategyMergedRead
			}
			// Clean cursor or FETCH/CLOSE — forward to shadow where the cursor lives.
			return StrategyMergedRead
		case SubListen:
			return StrategyListenOnly
		case SubNotSupported:
			return StrategyNotSupported
		case SubPrepare:
			return StrategyShadowWrite // PREPARE forwarded to Shadow; proxy caches the SQL
		case SubDeallocate:
			return StrategyForwardBoth
		case SubOther:
			return StrategyProdDirect
		case SubExecute:
			return StrategyProdDirect
		default:
			return StrategyNotSupported
		}

	default:
		return StrategyNotSupported
	}
}

// anyTableSchemaModified reports whether any of the given tables have schema diffs
// (DDL changes applied to Shadow but not Prod).
func (r *Router) anyTableSchemaModified(tables []string) bool {
	if r.schemaRegistry != nil {
		for _, t := range tables {
			if r.schemaRegistry.HasDiff(t) {
				return true
			}
		}
	}
	return false
}

// anyTableAffected reports whether any of the given tables have deltas, tombstones,
// or schema diffs (DDL changes applied to Shadow but not Prod).
func (r *Router) anyTableAffected(tables []string) bool {
	if r.deltaMap != nil && r.deltaMap.AnyTableDelta(tables) {
		return true
	}
	if r.tombstones != nil && r.tombstones.AnyTableTombstone(tables) {
		return true
	}
	if r.schemaRegistry != nil {
		for _, t := range tables {
			if r.schemaRegistry.HasDiff(t) {
				return true
			}
		}
	}
	return false
}

// anyTableFullyShadowed reports whether any of the given tables are fully shadowed
// (e.g., after TRUNCATE), meaning all reads should go to Shadow only.
func (r *Router) anyTableFullyShadowed(tables []string) bool {
	if r.schemaRegistry != nil {
		for _, t := range tables {
			if r.schemaRegistry.IsFullyShadowed(t) {
				return true
			}
		}
	}
	return false
}
