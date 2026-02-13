package mysql

import (
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/registry"
)

// mariadbEngine is a thin MariaDB adapter that reuses the MySQL engine
// implementation (MariaDB speaks the MySQL wire protocol).
type mariadbEngine struct {
	mysqlEngine
}

// Compile-time interface check.
var _ engine.Engine = (*mariadbEngine)(nil)

func init() {
	engine.Register(&mariadbEngine{})
}

func (e *mariadbEngine) ID() registry.EngineID {
	return registry.MariaDB
}
