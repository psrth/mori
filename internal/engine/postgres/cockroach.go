package postgres

import (
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/registry"
)

// crdbDefaultPort is the default CockroachDB SQL port.
const crdbDefaultPort = 26257

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

// ParseConnStr overrides the embedded pgEngine method so that CockroachDB
// connections default to port 26257 instead of PostgreSQL's 5432.
func (e *crdbEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	dsn, err := connstr.ParseWithDefaultPort(cs, crdbDefaultPort)
	if err != nil {
		return nil, err
	}
	return &engine.ConnInfo{
		Addr:     dsn.Address(),
		Host:     dsn.Host,
		Port:     dsn.Port,
		DBName:   dsn.DBName,
		User:     dsn.User,
		Password: dsn.Password,
		SSLMode:  dsn.SSLMode,
		ConnStr:  dsn.ConnString(),
	}, nil
}
