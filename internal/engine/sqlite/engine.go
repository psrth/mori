package sqlite

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/sqlite/classify"
	"github.com/mori-dev/mori/internal/engine/sqlite/connstr"
	"github.com/mori-dev/mori/internal/engine/sqlite/proxy"
	"github.com/mori-dev/mori/internal/engine/sqlite/schema"
	"github.com/mori-dev/mori/internal/engine/sqlite/shadow"
	"github.com/mori-dev/mori/internal/registry"
)

// sqliteEngine is the SQLite implementation of engine.Engine.
type sqliteEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*sqliteEngine)(nil)

func init() {
	engine.Register(&sqliteEngine{})
}

func (e *sqliteEngine) ID() registry.EngineID {
	return registry.SQLite
}

func (e *sqliteEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: "", // No Docker container for SQLite.
		ContainerPort: 0,
		TableCount:    len(result.Tables),
	}, nil
}

func (e *sqliteEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	info, err := connstr.Parse(cs)
	if err != nil {
		return nil, err
	}
	return &engine.ConnInfo{
		Addr:    info.FilePath, // For SQLite, "addr" is the file path.
		Host:    "",            // Embedded — no host.
		Port:    0,             // Embedded — no port.
		DBName:  info.FilePath,
		ConnStr: info.DSN(),
	}, nil
}

func (e *sqliteEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	sqliteTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromSQLite(sqliteTables), nil
}

func (e *sqliteEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToSQLite(tables))
}

func (e *sqliteEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
	// For SQLite, ProdAddr is the file path and ShadowAddr is the shadow file path.
	// Build DSNs from these paths.
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
		convertTablesToSQLite(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
	)
}

func convertTablesFromSQLite(sqliteTables map[string]schema.TableMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(sqliteTables))
	for name, t := range sqliteTables {
		out[name] = engine.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}

func convertTablesToSQLite(tables map[string]engine.TableMeta) map[string]schema.TableMeta {
	out := make(map[string]schema.TableMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}
