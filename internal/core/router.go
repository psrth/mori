package core

import "github.com/mori-dev/mori/internal/core/delta"

// Router decides execution strategy based on classification and delta state.
type Router struct {
	deltaMap   *delta.Map
	tombstones *delta.TombstoneSet
}

// NewRouter creates a new Router with the given delta and tombstone state.
// If deltaMap or tombstones is nil, the router treats all tables as clean
// (no deltas, no tombstones), meaning all reads go to Prod Direct.
func NewRouter(deltaMap *delta.Map, tombstones *delta.TombstoneSet) *Router {
	return &Router{
		deltaMap:   deltaMap,
		tombstones: tombstones,
	}
}

// Route returns the execution strategy for a classified query.
func (r *Router) Route(c *Classification) RoutingStrategy {
	switch c.OpType {
	case OpRead:
		if r.anyTableAffected(c.Tables) {
			if c.IsJoin {
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

// anyTableAffected reports whether any of the given tables have deltas or tombstones.
func (r *Router) anyTableAffected(tables []string) bool {
	if r.deltaMap != nil && r.deltaMap.AnyTableDelta(tables) {
		return true
	}
	if r.tombstones != nil && r.tombstones.AnyTableTombstone(tables) {
		return true
	}
	return false
}
