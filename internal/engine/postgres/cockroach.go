package postgres

import (
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/registry"
)

// crdbEngine is a thin CockroachDB adapter that reuses the PostgreSQL engine
// implementation (CockroachDB speaks the pgwire protocol).
type crdbEngine struct {
	pgEngine
}

// Compile-time interface check.
var _ engine.Engine = (*crdbEngine)(nil)

func init() {
	engine.Register(&crdbEngine{})
}

func (e *crdbEngine) ID() registry.EngineID {
	return registry.CockroachDB
}
