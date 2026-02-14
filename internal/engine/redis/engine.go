package redis

import (
	"context"
	"fmt"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/redis/classify"
	"github.com/mori-dev/mori/internal/engine/redis/connstr"
	"github.com/mori-dev/mori/internal/engine/redis/proxy"
	"github.com/mori-dev/mori/internal/engine/redis/schema"
	"github.com/mori-dev/mori/internal/registry"
)

// redisEngine is the Redis implementation of engine.Engine.
type redisEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*redisEngine)(nil)

func init() {
	engine.Register(&redisEngine{})
}

func (e *redisEngine) ID() registry.EngineID {
	return registry.Redis
}

func (e *redisEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: result.Config.ShadowContainer,
		ContainerPort: result.Config.ShadowPort,
		TableCount:    len(result.Tables),
	}, nil
}

func (e *redisEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	info, err := connstr.Parse(cs)
	if err != nil {
		return nil, err
	}
	return &engine.ConnInfo{
		Addr:     info.Addr(),
		Host:     info.Host,
		Port:     info.Port,
		DBName:   fmt.Sprintf("db%d", info.DB),
		Password: info.Password,
		ConnStr:  info.Raw,
	}, nil
}

func (e *redisEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	redisTables, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertTablesFromRedis(redisTables), nil
}

func (e *redisEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertTablesToRedis(tables))
}

func (e *redisEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
	// Parse the prod connection to get password and db number.
	info, _ := connstr.Parse(deps.ProdAddr)
	prodPass := ""
	prodDB := 0
	if info != nil {
		prodPass = info.Password
		prodDB = info.DB
		// Use the actual addr for connecting.
		deps.ProdAddr = info.Addr()
	}

	return proxy.New(
		deps.ProdAddr,
		deps.ShadowAddr,
		prodPass,
		prodDB,
		deps.ListenPort,
		deps.Verbose,
		deps.Classifier,
		deps.Router,
		deps.DeltaMap,
		deps.Tombstones,
		convertTablesToRedis(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
	)
}

func convertTablesFromRedis(redisTables map[string]schema.KeyMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(redisTables))
	for name, t := range redisTables {
		out[name] = engine.TableMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}

func convertTablesToRedis(tables map[string]engine.TableMeta) map[string]schema.KeyMeta {
	out := make(map[string]schema.KeyMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.KeyMeta{
			Prefix:    name,
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}
