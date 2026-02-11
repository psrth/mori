package core

// Router decides execution strategy based on classification and delta state.
type Router struct {
	// deltaMap and tombstones will be added in Phase 5.
}

// NewRouter creates a new Router instance.
func NewRouter() *Router {
	return &Router{}
}

// Route returns the execution strategy for a classified query.
// Phase 1 stub: always returns StrategyProdDirect.
func (r *Router) Route(c *Classification) RoutingStrategy {
	return StrategyProdDirect
}
