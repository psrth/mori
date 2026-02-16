package duckdb

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/duckdb/classify"
	"github.com/mori-dev/mori/internal/engine/duckdb/connstr"
	"github.com/mori-dev/mori/internal/engine/duckdb/proxy"
	"github.com/mori-dev/mori/internal/engine/duckdb/schema"
	"github.com/mori-dev/mori/internal/engine/duckdb/shadow"
	"github.com/mori-dev/mori/internal/registry"
)

// duckdbEngine is the DuckDB implementation of engine.Engine.
type duckdbEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*duckdbEngine)(nil)

func init() {
	engine.Register(&duckdbEngine{})
}

func (e *duckdbEngine) ID() registry.EngineID {
	return registry.DuckDB
}

func (e *duckdbEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: "", // No Docker container for DuckDB.
		ContainerPort: 0,
		TableCount:    len(result.Tables),
	}, nil
}

func (e *duckdbEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	info, err := connstr.Parse(cs)
	if err != nil {
		return nil, err
	}
	return &engine.ConnInfo{
		Addr:    info.FilePath, // For DuckDB, "addr" is the file path.
		Host:    "",            // Embedded — no host.
		Port:    0,             // Embedded — no port.
		DBName:  info.FilePath,
		ConnStr: info.DSN(),
	}, nil
}

func (e *duckdbEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	duckdbTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromDuckDB(duckdbTables), nil
}

func (e *duckdbEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToDuckDB(tables))
}

func (e *duckdbEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
	prodDSN := deps.ProdAddr
	shadowDSN := shadow.ShadowPath(deps.MoriDir)

	return proxy.New(
		prodDSN,
		shadowDSN,
		deps.ListenPort,
		deps.Verbose,
		deps.Classifier,
		deps.Router,
		deps.DeltaMap,
		deps.Tombstones,
		convertTablesToDuckDB(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
	)
}

func convertTablesFromDuckDB(duckdbTables map[string]schema.TableMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(duckdbTables))
	for name, t := range duckdbTables {
		out[name] = engine.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}

func convertTablesToDuckDB(tables map[string]engine.TableMeta) map[string]schema.TableMeta {
	out := make(map[string]schema.TableMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}
