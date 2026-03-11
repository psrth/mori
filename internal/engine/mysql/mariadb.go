package mysql

import (
	"context"

	"github.com/psrth/mori/internal/core/tlsutil"
	"github.com/psrth/mori/internal/engine"
	"github.com/psrth/mori/internal/engine/mysql/proxy"
	"github.com/psrth/mori/internal/registry"
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

// Init overrides the MySQL Init to use a MariaDB Docker image for the shadow
// container. The image version is auto-detected from SELECT VERSION() to match
// production. MySQL 8.0's mysqldump queries COLUMN_STATISTICS which MariaDB
// does not support, so we use the mariadb image for both shadow and schema dump.
func (e *mariadbEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
		ConnName:    opts.ConnName,
		EngineName:  "mariadb",
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: result.Container.ContainerName,
		ContainerPort: result.Container.HostPort,
		TableCount:    len(result.Dump.Tables),
	}, nil
}

// NewProxy overrides the MySQL NewProxy to enable RETURNING-based PK tracking.
// MariaDB 10.5+ supports INSERT ... RETURNING which provides PG-level delta
// fidelity for all INSERT patterns (composite PKs, UUIDs, INSERT...SELECT).
func (e *mariadbEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
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
		convertTablesToMySQL(tables),
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
		true, // useReturning: MariaDB 10.5+ supports INSERT ... RETURNING
	)
}
