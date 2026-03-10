package postgres

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/tlsutil"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/postgres/classify"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/engine/postgres/proxy"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/registry"
)

// pgEngine is the PostgreSQL implementation of engine.Engine.
type pgEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*pgEngine)(nil)

func init() {
	engine.Register(&pgEngine{})
}

func (e *pgEngine) ID() registry.EngineID {
	return registry.Postgres
}

func (e *pgEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr:   opts.ProdConnStr,
		ImageOverride: opts.ImageOverride,
		ProjectRoot:   opts.ProjectRoot,
		ConnName:      opts.ConnName,
		EngineID:      string(registry.Postgres),
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: result.Container.ContainerName,
		ContainerPort: result.Container.HostPort,
		TableCount:    len(result.Dump.Tables),
		Extensions:    extensionNames(result.Dump.Extensions),
	}, nil
}

func (e *pgEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	dsn, err := connstr.Parse(cs)
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

func (e *pgEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	pgTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromPG(pgTables), nil
}

func (e *pgEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToPG(tables))
}

func (e *pgEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
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
		false, // isCockroachDB
	)
}

// convertTablesFromPG converts postgres-specific TableMeta to engine-agnostic TableMeta.
func convertTablesFromPG(pgTables map[string]schema.TableMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(pgTables))
	for name, t := range pgTables {
		out[name] = engine.TableMeta{
			PKColumns:     t.PKColumns,
			PKType:        t.PKType,
			GeneratedCols: t.GeneratedCols,
		}
	}
	return out
}

// convertTablesToPG converts engine-agnostic TableMeta to postgres-specific TableMeta.
func convertTablesToPG(tables map[string]engine.TableMeta) map[string]schema.TableMeta {
	out := make(map[string]schema.TableMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.TableMeta{
			PKColumns:     t.PKColumns,
			PKType:        t.PKType,
			GeneratedCols: t.GeneratedCols,
		}
	}
	return out
}
