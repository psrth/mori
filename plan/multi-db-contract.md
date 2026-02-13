## Mori Engine Interface — Subagent Implementation Guide

### Architecture Overview

Mori uses an **Engine interface** + **runtime registry** pattern. Each database engine lives in its own isolated folder under `internal/engine/<name>/` and self-registers via a Go `init()` function. The CLI (`cmd/mori/start.go`) is completely database-agnostic — it calls `engine.Lookup(id)` at startup and dispatches everything through the interface.

```
internal/engine/
├── engine.go            # Engine + Proxy interfaces, bridge types
├── registry.go          # Register(), Lookup(), IsRegistered()
├── engine_test.go       # Registry unit tests
└── postgres/            # Reference implementation
    ├── engine.go        # pgEngine implementing engine.Engine
    ├── cockroach.go     # CockroachDB (thin wrapper over pgEngine)
    ├── init.go          # Shadow setup, schema dump
    ├── connstr/         # Connection string parsing
    ├── classify/        # SQL classifier (→ core.Classifier)
    ├── proxy/           # Wire-protocol proxy (→ engine.Proxy)
    ├── schema/          # Schema dump/persist/versioning
    └── shadow/          # Docker container management
```

### The Engine Interface

Every engine must implement this 6-method interface defined in `internal/engine/engine.go`:

```go
type Engine interface {
    ID() registry.EngineID
    Init(ctx context.Context, opts InitOptions) (*InitResult, error)
    ParseConnStr(connStr string) (*ConnInfo, error)
    LoadTableMeta(moriDir string) (map[string]TableMeta, error)
    NewClassifier(tables map[string]TableMeta) core.Classifier
    NewProxy(deps ProxyDeps, tables map[string]TableMeta) Proxy
}
```

And return a `Proxy` that satisfies:

```go
type Proxy interface {
    ListenAndServe(ctx context.Context) error
    Shutdown(ctx context.Context) error
    Addr() string
}
```

### What Each Method Must Do

| Method            | Purpose                                                                                                     | Called When                                |
| ----------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `ID()`            | Return the `registry.EngineID` constant (e.g. `registry.MySQL`)                                             | Registration + lookup                      |
| `Init()`          | First-time setup: spin up a shadow container, dump prod schema into it, return config                       | `mori start` (first run only)              |
| `ParseConnStr()`  | Parse a connection string into `engine.ConnInfo` (host, port, user, password, dbname, sslmode, raw connstr) | Every `mori start` to extract prod address |
| `LoadTableMeta()` | Read persisted table metadata from `.mori/` dir, return `map[string]TableMeta` with PK info                 | Every `mori start` to feed the classifier  |
| `NewClassifier()` | Return a `core.Classifier` that parses queries in this engine's dialect                                     | Every `mori start`                         |
| `NewProxy()`      | Create and return a `Proxy` that speaks this engine's wire protocol                                         | Every `mori start`                         |

### Bridge Types

These types in `engine.go` are what your engine communicates through — they're engine-agnostic:

- **`TableMeta`** — `PKColumns []string`, `PKType string` ("serial", "bigserial", "uuid", "composite", "none")
- **`ConnInfo`** — Parsed connection: `Addr`, `Host`, `Port`, `DBName`, `User`, `Password`, `SSLMode`, `ConnStr`
- **`InitOptions`** — Input to Init: `ProdConnStr`, `ProjectRoot`
- **`InitResult`** — Output from Init: `Config`, `ContainerName`, `ContainerPort`, `TableCount`, `Extensions`
- **`ProxyDeps`** — Everything the proxy needs: addresses, classifier, router, delta state, logger, etc.

### The core.Classifier Interface

Your classifier must implement (defined in `internal/core/classifier.go`):

```go
type Classifier interface {
    Classify(query string) (*Classification, error)
    ClassifyWithParams(query string, params []interface{}) (*Classification, error)
}
```

A `Classification` contains: `OpType` (OpRead/OpWrite/OpDDL/OpTransaction/OpMeta), `SubType`, `Tables []string`, and flags like `HasAggregate`, `IsJoin`, `IsComplexRead`, `HasSetOp`. The **Router** (shared, engine-agnostic) uses this to decide routing strategy. Your classifier just needs to parse your engine's query language into these fields.

### How to Add a New Engine

#### 1. Create the isolated folder

```
internal/engine/<name>/
├── engine.go        # Main adapter (implements engine.Engine)
├── init.go          # Shadow setup logic (Docker container, schema dump)
├── connstr/         # Connection string parsing
│   └── connstr.go
├── classify/        # Query classifier
│   └── classifier.go
├── proxy/           # Wire protocol proxy
│   └── proxy.go
└── schema/          # Schema introspection + persistence
    └── schema.go
```

#### 2. Implement the engine adapter

`internal/engine/<name>/engine.go`:

```go
package <name>

import (
    "github.com/mori-dev/mori/internal/engine"
    "github.com/mori-dev/mori/internal/registry"
)

type myEngine struct{}

var _ engine.Engine = (*myEngine)(nil)  // compile-time check

func init() {
    engine.Register(&myEngine{})
}

func (e *myEngine) ID() registry.EngineID {
    return registry.<Name>  // must match constant in registry/engine.go
}

// Implement remaining 5 methods...
```

#### 3. Add the blank import

In `cmd/mori/start.go`, add a side-effect import:

```go
import (
    _ "github.com/mori-dev/mori/internal/engine/<name>"
)
```

This triggers the `init()` function, registering the engine at startup.

#### 4. Mark as supported

In `internal/registry/engine.go`, flip `Supported: true` for your engine's entry. The entry already exists — the EngineID constants and catalog are pre-defined for all planned engines:

```
Postgres, CockroachDB, MySQL, MariaDB, MSSQL, Oracle, SQLite,
Redis, MongoDB, Elasticsearch, DynamoDB, Firestore, Cassandra,
ClickHouse, Neo4j
```

### Subpackage Responsibilities

| Subpackage  | What it does                                                                                               | Key output                    |
| ----------- | ---------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `connstr/`  | Parse engine-specific connection strings (DSN, URI, key-value)                                             | `engine.ConnInfo`             |
| `classify/` | Parse queries in this engine's dialect into `core.Classification`                                          | `core.Classifier`             |
| `proxy/`    | Accept client connections, speak the wire protocol, route queries to prod/shadow based on router decisions | `engine.Proxy`                |
| `schema/`   | Connect to prod, introspect tables/columns/PKs, dump into shadow container, persist metadata to `.mori/`   | `map[string]engine.TableMeta` |
| `shadow/`   | Manage the Docker container for the shadow database (pull image, create, start, health-check)              | Container name + port         |

### Write Guard

The PostgreSQL engine includes a 4-layer production write guard (`proxy/guard.go`). For SQL-based engines, you should implement equivalent guards in your proxy. The layers are:

1. **Strict mode** — Refuse to serve if shadow DB is unavailable (no fallback to prod-only)
2. **L1 (routing assertion)** — After the router returns a strategy, verify that write/DDL ops never got `StrategyProdDirect`. Override to `StrategyShadowWrite` if violated.
3. **L2 (connection wrapper)** — Wrap the production connection so outbound bytes are inspected. Block any wire-protocol messages containing write SQL.
4. **L3 (dispatch gate)** — Final check in the route loop: if a write/DDL classification somehow reaches `targetProd`, block it and return an error to the client.

For NoSQL engines, adapt the guard concept to your protocol (e.g., inspect Redis commands for write ops like SET/DEL/HSET, or inspect MongoDB wire protocol for insert/update/delete ops).

### Testing When Done

#### Unit Tests

Each subpackage should have `_test.go` files. At minimum:

- `classify/classifier_test.go` — Table-driven tests covering reads, writes, DDL, transactions, edge cases in your dialect
- `connstr/connstr_test.go` — Parse various connection string formats
- `proxy/guard_test.go` — Verify write guard blocks writes and allows reads (if SQL-based)
- `schema/` tests for dump/persist round-trips

Run engine-specific unit tests:

```bash
go test ./internal/engine/<name>/...
```

#### E2E Tests

The full E2E suite lives in `tests/e2e/` and runs against a live Mori proxy + real database. The existing 202 tests target PostgreSQL.

To validate your engine doesn't break anything:

```bash
# Run all existing tests (should still pass)
go test ./tests/e2e/... -v -count=1

# Run only your engine's unit tests
go test ./internal/engine/<name>/... -v -count=1
```

To add E2E tests for your engine, create test files in `tests/e2e/` that:

1. Start Mori with a connection using your engine type
2. Connect a client through the proxy
3. Execute reads, writes, DDL through the proxy
4. Verify writes go to shadow, reads merge correctly, DDL is shadow-only

#### Build Verification

```bash
go build ./cmd/mori/...
go vet ./internal/engine/<name>/...
```

#### Quick Smoke Test

```bash
# 1. Build
go build -o mori ./cmd/mori

# 2. Init a connection with your engine
./mori init

# 3. Start the proxy
./mori start --verbose

# 4. Connect your engine's CLI client through the proxy
#    e.g. mysql -h 127.0.0.1 -P 9002 -u user -p dbname
#    Run a SELECT, INSERT, CREATE TABLE — verify routing behavior
```

---

That's the full contract. Each engine is completely isolated in `internal/engine/<name>/` — no engine should import another engine's packages. The only shared code is `internal/engine/` (interfaces + registry), `internal/core/` (classifier interface, router, delta), and `internal/registry/` (EngineID constants).
