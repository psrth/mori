package mssql

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/mssql/classify"
	"github.com/mori-dev/mori/internal/engine/mssql/connstr"
	"github.com/mori-dev/mori/internal/engine/mssql/proxy"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/registry"
)

// mssqlEngine is the MSSQL implementation of engine.Engine.
type mssqlEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*mssqlEngine)(nil)

func init() {
	engine.Register(&mssqlEngine{})
}

func (e *mssqlEngine) ID() registry.EngineID {
	return registry.MSSQL
}

func (e *mssqlEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
		ConnName:    opts.ConnName,
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

func (e *mssqlEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
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
		ConnStr:  dsn.GoDSN(),
	}, nil
}

func (e *mssqlEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	msTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromMSSQL(msTables), nil
}

func (e *mssqlEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToMSSQL(tables))
}

func (e *mssqlEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
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
		convertTablesToMSSQL(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
		deps.MaxRowsHydrate,
	)
}

// convertTablesFromMSSQL converts mssql-specific TableMeta to engine-agnostic TableMeta.
func convertTablesFromMSSQL(msTables map[string]schema.TableMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(msTables))
	for name, t := range msTables {
		out[name] = engine.TableMeta{
			PKColumns:     t.PKColumns,
			PKType:        t.PKType,
			GeneratedCols: t.GeneratedCols,
		}
	}
	return out
}

// convertTablesToMSSQL converts engine-agnostic TableMeta to mssql-specific TableMeta.
func convertTablesToMSSQL(tables map[string]engine.TableMeta) map[string]schema.TableMeta {
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
