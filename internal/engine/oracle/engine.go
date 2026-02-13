package oracle

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/oracle/classify"
	"github.com/mori-dev/mori/internal/engine/oracle/connstr"
	"github.com/mori-dev/mori/internal/engine/oracle/proxy"
	"github.com/mori-dev/mori/internal/engine/oracle/schema"
	"github.com/mori-dev/mori/internal/registry"
)

// oracleEngine is the Oracle implementation of engine.Engine.
type oracleEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*oracleEngine)(nil)

func init() {
	engine.Register(&oracleEngine{})
}

func (e *oracleEngine) ID() registry.EngineID {
	return registry.Oracle
}

func (e *oracleEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
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

func (e *oracleEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	dsn, err := connstr.Parse(cs)
	if err != nil {
		return nil, err
	}
	return &engine.ConnInfo{
		Addr:     dsn.Address(),
		Host:     dsn.Host,
		Port:     dsn.Port,
		DBName:   dsn.ServiceName,
		User:     dsn.User,
		Password: dsn.Password,
		ConnStr:  dsn.GoOraDSN(),
	}, nil
}

func (e *oracleEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	oraTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromOracle(oraTables), nil
}

func (e *oracleEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToOracle(tables))
}

func (e *oracleEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
	return proxy.New(
		deps.ProdAddr,
		deps.ShadowAddr,
		deps.ListenPort,
		deps.Verbose,
		deps.Classifier,
		deps.Router,
		deps.DeltaMap,
		deps.Tombstones,
		convertTablesToOracle(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
	)
}

// convertTablesFromOracle converts oracle-specific TableMeta to engine-agnostic TableMeta.
func convertTablesFromOracle(oraTables map[string]schema.TableMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(oraTables))
	for name, t := range oraTables {
		out[name] = engine.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}

// convertTablesToOracle converts engine-agnostic TableMeta to oracle-specific TableMeta.
func convertTablesToOracle(tables map[string]engine.TableMeta) map[string]schema.TableMeta {
	out := make(map[string]schema.TableMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}
