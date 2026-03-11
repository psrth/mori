package mysql

import (
	"context"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/tlsutil"
	"github.com/psrth/mori/internal/engine"
	"github.com/psrth/mori/internal/engine/mysql/classify"
	"github.com/psrth/mori/internal/engine/mysql/connstr"
	"github.com/psrth/mori/internal/engine/mysql/proxy"
	"github.com/psrth/mori/internal/engine/mysql/schema"
	"github.com/psrth/mori/internal/registry"
)

// mysqlEngine is the MySQL implementation of engine.Engine.
type mysqlEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*mysqlEngine)(nil)

func init() {
	engine.Register(&mysqlEngine{})
}

func (e *mysqlEngine) ID() registry.EngineID {
	return registry.MySQL
}

func (e *mysqlEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
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

func (e *mysqlEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
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
		ConnStr:  dsn.GoDSN(),
	}, nil
}

func (e *mysqlEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	myTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromMySQL(myTables), nil
}

func (e *mysqlEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToMySQL(tables))
}

func (e *mysqlEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
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
		false, // useReturning: MySQL does not support RETURNING
	)
}

// convertTablesFromMySQL converts mysql-specific TableMeta to engine-agnostic TableMeta.
func convertTablesFromMySQL(myTables map[string]schema.TableMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(myTables))
	for name, t := range myTables {
		out[name] = engine.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}

// convertTablesToMySQL converts engine-agnostic TableMeta to mysql-specific TableMeta.
func convertTablesToMySQL(tables map[string]engine.TableMeta) map[string]schema.TableMeta {
	out := make(map[string]schema.TableMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}
