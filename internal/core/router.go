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
	switch c.OpType {
	case OpRead:
		if r.anyTableAffected(c.Tables) {
			// Set operations (UNION/INTERSECT/EXCEPT) and complex reads (CTEs,
			// derived tables) cannot be safely merged or patched at the row level.
			// Route to Prod for structurally correct results.
			if c.HasSetOp || c.IsComplexRead {
				return StrategyProdDirect
			}
			// Aggregate queries (COUNT, SUM, GROUP BY, etc.) on affected tables
			// need row-level merge then re-aggregation. Route through MergedRead
			// which handles aggregates via Shadow-only execution.
			if c.HasAggregate {
				return StrategyMergedRead
			}
			if c.IsJoin {
				// JoinPatch sends the original query to Prod — if any table has
				// schema diffs (added/renamed columns), Prod will reject it.
				// Route through MergedRead which falls back to Shadow-only on Prod error.
				if r.anyTableSchemaModified(c.Tables) {
					return StrategyMergedRead
				}
				return StrategyJoinPatch
			}
			return StrategyMergedRead
		}
		return StrategyProdDirect

	case OpWrite:
		switch c.SubType {
		case SubInsert:
			return StrategyShadowWrite
		case SubUpdate:
			return StrategyHydrateAndWrite
		case SubDelete:
			return StrategyShadowDelete
		default:
			return StrategyShadowWrite
		}

	case OpDDL:
		return StrategyShadowDDL

	case OpTransaction:
		return StrategyTransaction

	default:
		return StrategyProdDirect
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
