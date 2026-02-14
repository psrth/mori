package firestore

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/engine/firestore/classify"
	"github.com/mori-dev/mori/internal/engine/firestore/connstr"
	"github.com/mori-dev/mori/internal/engine/firestore/proxy"
	"github.com/mori-dev/mori/internal/engine/firestore/schema"
	"github.com/mori-dev/mori/internal/engine/firestore/shadow"
	"github.com/mori-dev/mori/internal/registry"
)

// firestoreEngine is the Firestore implementation of engine.Engine.
type firestoreEngine struct{}

// Compile-time interface check.
var _ engine.Engine = (*firestoreEngine)(nil)

func init() {
	engine.Register(&firestoreEngine{})
}

func (e *firestoreEngine) ID() registry.EngineID {
	return registry.Firestore
}

func (e *firestoreEngine) Init(ctx context.Context, opts engine.InitOptions) (*engine.InitResult, error) {
	result, err := Init(ctx, InitOptions{
		ProdConnStr: opts.ProdConnStr,
		ProjectRoot: opts.ProjectRoot,
	})
	if err != nil {
		return nil, err
	}
	return &engine.InitResult{
		Config:        result.Config,
		ContainerName: result.ContainerName,
		ContainerPort: result.ContainerPort,
		TableCount:    len(result.Collections),
	}, nil
}

func (e *firestoreEngine) ParseConnStr(cs string) (*engine.ConnInfo, error) {
	info, err := connstr.Parse(cs)
	if err != nil {
		return nil, err
	}
	prodAddr := info.ProdAddr()
	host := "firestore.googleapis.com"
	port := 443
	if info.IsEmulator() {
		host = "127.0.0.1"
		port = 0
	}
	return &engine.ConnInfo{
		Addr:    prodAddr,
		Host:    host,
		Port:    port,
		DBName:  info.ProjectID,
		ConnStr: info.Raw,
	}, nil
}

func (e *firestoreEngine) LoadTableMeta(moriDir string) (map[string]engine.TableMeta, error) {
	collections, err := schema.ReadTables(moriDir)
	if err != nil {
		return nil, err
	}
	return convertCollectionsToEngine(collections), nil
}

func (e *firestoreEngine) NewClassifier(tables map[string]engine.TableMeta) core.Classifier {
	return classify.New(convertEngineToCollections(tables))
}

func (e *firestoreEngine) NewProxy(deps engine.ProxyDeps, tables map[string]engine.TableMeta) engine.Proxy {
	// Extract credentials file from the connection string if present.
	credentialsFile := ""
	if info, err := connstr.Parse(deps.ProdAddr); err == nil {
		credentialsFile = info.CredentialsFile
	}

	return proxy.New(
		deps.ProdAddr,
		deps.ShadowAddr,
		credentialsFile,
		deps.DBName,
		deps.ListenPort,
		deps.Verbose,
		deps.Classifier,
		deps.Router,
		deps.DeltaMap,
		deps.Tombstones,
		convertEngineToCollections(tables),
		deps.MoriDir,
		deps.SchemaReg,
		deps.Logger,
	)
}

func convertCollectionsToEngine(collections map[string]schema.CollectionMeta) map[string]engine.TableMeta {
	out := make(map[string]engine.TableMeta, len(collections))
	for name, c := range collections {
		out[name] = engine.TableMeta{
			PKColumns: c.PKColumns,
			PKType:    c.PKType,
		}
	}
	return out
}

func convertEngineToCollections(tables map[string]engine.TableMeta) map[string]schema.CollectionMeta {
	out := make(map[string]schema.CollectionMeta, len(tables))
	for name, t := range tables {
		out[name] = schema.CollectionMeta{
			PKColumns: t.PKColumns,
			PKType:    t.PKType,
		}
	}
	return out
}

// ShadowEmulatorAddr returns the emulator address for a given host port.
func ShadowEmulatorAddr(port int) string {
	return shadow.EmulatorAddr(port)
}
