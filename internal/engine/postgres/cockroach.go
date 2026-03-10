package postgres

import (
	"context"

	"github.com/mori-dev/mori/internal/core/tlsutil"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/engine/postgres/proxy"
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

// Init overrides pgEngine.Init with a CockroachDB-specific initialization
// pipeline that uses SHOW CREATE ALL TABLES instead of pg_dump.
func (e *crdbEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := CRDBInit(ctx, InitOptions{
		ProdConnStr:   opts.ProdConnStr,
		ImageOverride: opts.ImageOverride,
		ProjectRoot:   opts.ProjectRoot,
		ConnName:      opts.ConnName,
		EngineID:      string(registry.CockroachDB),
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: result.Container.ContainerName,
		ContainerPort: result.Container.HostPort,
		TableCount:    len(result.Dump.Tables),
		Extensions:    nil, // CockroachDB has no extensions
	}, nil
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

// NewProxy overrides pgEngine.NewProxy to pass isCockroachDB=true,
// which enables SERIALIZABLE isolation and rowid-based PK-less dedup.
func (e *crdbEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
	return proxy.New(
		deps.ProdAddr,
		deps.ShadowAddr,
		deps.DBName,
		deps.ListenPort,
		deps.Verbose,
		deps.Classifier,
		deps.Router,
		deps.DeltaMap,
		deps.Tombstones,
		convertTablesToPG(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
		deps.MaxRowsHydrate,
		tlsutil.TLSParams{
			ServerName: deps.ProdHost,
			SSLMode:    deps.SSLMode,
			CACertPath: deps.CACertPath,
			CertPath:   deps.CertPath,
			KeyPath:    deps.KeyPath,
		},
		true, // isCockroachDB
	)
}
