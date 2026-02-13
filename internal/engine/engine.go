package engine

import (
	"context"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/logging"
	"github.com/mori-dev/mori/internal/registry"
)

// TableMeta holds engine-agnostic table metadata.
type TableMeta struct {
	PKColumns []string
	PKType    string // "serial", "bigserial", "uuid", "composite", "none"
}

// ConnInfo holds parsed connection string components.
type ConnInfo struct {
	Addr     string // "host:port" for TCP dialing
	Host     string // hostname only
	Port     int    // port number
	DBName   string
	User     string
	Password string
	SSLMode  string // e.g. "disable", "require"
	ConnStr  string // full connection string for drivers
}

// InitOptions holds engine-agnostic init parameters.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
}

// InitResult holds engine-agnostic init results.
type InitResult struct {
	Config        *config.Config
	ContainerName string
	ContainerPort int
	TableCount    int
	Extensions    []string
}

// ProxyDeps bundles all dependencies needed to create a proxy.
type ProxyDeps struct {
	ProdAddr   string
	ShadowAddr string
	DBName     string
	ListenPort int
	Verbose    bool
	Classifier core.Classifier
	Router     *core.Router
	DeltaMap   *delta.Map
	Tombstones *delta.TombstoneSet
	SchemaReg  *coreSchema.Registry
	MoriDir    string
	Logger     *logging.Logger
}

// Engine defines the contract for a database engine implementation.
type Engine interface {
	ID() registry.EngineID
	Init(ctx context.Context, opts InitOptions) (*InitResult, error)
	ParseConnStr(connStr string) (*ConnInfo, error)
	LoadTableMeta(moriDir string) (map[string]TableMeta, error)
	NewClassifier(tables map[string]TableMeta) core.Classifier
	NewProxy(deps ProxyDeps, tables map[string]TableMeta) Proxy
}

// Proxy is the minimal interface for a running proxy server.
type Proxy interface {
	ListenAndServe(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Addr() string
}
