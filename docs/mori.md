# Mori

**Database virtualization with copy-on-write semantics.**

Mori is a transparent proxy that sits between an application and a production database. The application connects to Mori using a standard database driver — same wire protocol, no code changes beyond swapping a connection string. Reads are served from the real production database. Writes are captured in a local database instance (the Shadow) that starts as a schema-only clone of production.

The result: developers test against real production data, mutate freely, and never risk the production database. Break something, reset, start over.

---

## 1. Core Invariants

These hold at all times, without exception.

1. **Production is read-only.** Mori never issues a write to the upstream database. The Prod connection is opened in read-only mode. There is no code path that writes to Prod.

2. **Shadow is the mutation layer.** All writes go to the Shadow database — a local instance (same engine as Prod) holding all local mutations. It starts schema-only with zero rows.

3. **The application sees one unified database.** When the app issues a SELECT, Mori transparently merges results from Prod and Shadow. Locally-modified rows come from Shadow. Untouched rows come from Prod. The app never knows the difference.

4. **Reset is instant.** Wiping the Shadow and its metadata restores a clean view of production. No migration, no rollback — just a clean slate.

---

## 2. Terminology

| Term | Definition |
|------|-----------|
| **Prod** | The upstream production database. Read-only from Mori's perspective. Source of truth for all unmodified data. |
| **Shadow** | A local database instance (same engine and version as Prod) holding all local mutations. Starts schema-only with zero rows — a transparent overlay on Prod. |
| **Proxy** | The Mori process. Intercepts wire protocol traffic, classifies queries, routes them, merges results, manages state. |
| **Delta Map** | Set of `(table, primary_key)` pairs identifying locally modified rows. When a row is delta, Shadow holds the authoritative version. |
| **Tombstone Set** | Set of `(table, primary_key)` pairs identifying locally deleted rows. Tombstoned rows are filtered from Prod results. |
| **Schema Registry** | Record of structural differences between Shadow and Prod schemas, introduced by local DDL. Tracks added/dropped/renamed columns and type changes per table. |
| **Hydration** | Copying a row from Prod into Shadow so it can be mutated locally. Happens transparently on UPDATE/DELETE of rows that only exist in Prod. |
| **Classification** | Parsing an incoming statement to determine operation type (READ, WRITE, DDL, TRANSACTION), affected tables, and extractable PKs. |
| **Merged Read** | A read where results from both Prod and Shadow are combined, with delta/tombstoned rows filtered from Prod results. |

---

## 3. Supported Engines

### 3.1 Engine Matrix

| Engine | Display Name | Category | Default Port | Status |
|--------|-------------|----------|-------------|--------|
| `postgres` | PostgreSQL | SQL / pgwire | 5432 | Stable |
| `cockroachdb` | CockroachDB | SQL / pgwire | 26257 | Stable |
| `mysql` | MySQL | SQL / MySQL | 3306 | Supported |
| `mariadb` | MariaDB | SQL / MySQL | 3306 | Supported |
| `mssql` | MS SQL Server | SQL / TDS | 1433 | Supported |
| `sqlite` | SQLite | SQL / Embedded | N/A | Supported |
| `duckdb` | DuckDB | SQL / Embedded | N/A | Supported |
| `redis` | Redis | NoSQL | 6379 | Supported |
| `firestore` | Firestore | NoSQL | N/A | Supported |
| `mongodb` | MongoDB | NoSQL | 27017 | Planned |
| `elasticsearch` | Elasticsearch | NoSQL | 9200 | Planned |
| `dynamodb` | DynamoDB | NoSQL | N/A | Planned |
| `cassandra` | Cassandra / ScyllaDB | Specialized | 9042 | Planned |
| `clickhouse` | ClickHouse | Specialized | 9000 | Planned |
| `neo4j` | Neo4j | Specialized | 7687 | Planned |

### 3.2 Auth Providers

| Provider | Compatible Engines | SSL Default |
|----------|-------------------|-------------|
| **Direct / Self-Hosted** | All | Engine default |
| **GCP Cloud SQL** | Postgres, MySQL, MSSQL | require |
| **AWS RDS / Aurora** | Postgres, MySQL, MariaDB, MSSQL | require |
| **Neon** | Postgres | require |
| **Supabase** | Postgres | require |
| **Azure Database** | Postgres, MySQL, MariaDB, MSSQL | require |
| **PlanetScale** | MySQL | require |
| **Vercel Postgres** | Postgres | require |
| **MongoDB Atlas** | MongoDB | true |
| **DigitalOcean** | Postgres, MySQL, Redis, MongoDB | require |
| **Railway** | Postgres, MySQL, Redis, MongoDB | require |
| **Upstash** | Redis | true |
| **Cloudflare D1 / KV** | SQLite, Redis | -- |
| **Firebase** | Firestore | -- |

### 3.3 Connection Fields

Each engine requires specific connection parameters:

**PostgreSQL / CockroachDB**: host, port, user, password, database, ssl_mode

**MySQL / MariaDB**: host, port, user, password, database, ssl_mode

**MS SQL Server**: host, port, user, password, database, encrypt, trust_server_cert

**SQLite**: file_path

**DuckDB**: file_path (also supports URI format `duckdb:///path/to/db.duckdb` and `:memory:`)

**Redis**: host, port, password, db_number, ssl

**Firestore**: project_id, credentials_file

Connection string formats vary by engine (URI, DSN, key-value). Each engine's `connstr/` package handles parsing.

---

## 4. How It Works

### 4.1 Architecture

```
+--------------+
| Application  |
+------+-------+
       | DB wire protocol (PG v3, MySQL, TDS, RESP, gRPC, etc.)
       v
+----------------------------------------------------------+
|                       MORI PROXY                          |
|                                                           |
|  +-------------+  +-------------+  +------------------+  |
|  |  Protocol   |  | Classifier  |  |      Router      |  |
|  |  Handler    |--|             |--|                  |  |
|  |             |  | op_type     |  | Prod Direct      |  |
|  | accept      |  | tables      |  | Merged Read      |  |
|  | parse       |  | pks         |  | Shadow Write     |  |
|  | serialize   |  | join?       |  | Hydrate+Write    |  |
|  +-------------+  +-------------+  | Shadow Delete    |  |
|                                     | Shadow DDL       |  |
|  +-------------+  +-------------+  | Transaction Ctrl |  |
|  |   Delta     |  |  Schema     |  +------------------+  |
|  |  Manager    |  | Registry    |                         |
|  |             |  |             |  +------------------+  |
|  | delta_map   |  | added_cols  |  |   Merge Engine   |  |
|  | tombstones  |  | dropped     |  |                  |  |
|  | staging     |  | renamed     |  | single_table     |  |
|  +-------------+  | type_casts  |  | join_patch       |  |
|                    +-------------+  | over_fetch       |  |
|  +-------------+                    | adapt + filter   |  |
|  | Transaction |                    +------------------+  |
|  |  Manager    |                                          |
|  |             |  +-------------+  +------------------+  |
|  | per-conn    |  |  Engine     |  |   Write Engine   |  |
|  | staging     |  |  Manager    |  |                  |  |
|  | commit      |  |             |  | insert           |  |
|  | rollback    |  | prod_pool   |  | update+hydrate   |  |
|  +-------------+  | shadow_pool |  | delete+tombstone |  |
|                    +-------------+  | ddl+registry     |  |
|                                     +------------------+  |
+---------------+-----------------------------+-------------+
                | read-only                   | read-write
                v                             v
         +-------------+             +-------------+
         |    Prod      |             |   Shadow    |
         |  Database    |             |  Database   |
         |  (upstream)  |             |  (local)    |
         +-------------+             +-------------+
```

### 4.2 Components

**Protocol Handler** — Speaks the target database's native wire protocol. Accepts connections, performs handshake/authentication, parses incoming messages, serializes outgoing results. Indistinguishable from a real database server to the application.

**Classifier** — Parses each query to determine operation type (`READ`, `WRITE`, `DDL`, `TRANSACTION`), affected tables, extractable primary keys, and whether it's a JOIN. Lightweight syntactic analysis — fast, sits in the hot path.

**Router** — Takes classifier output + delta/tombstone state and selects an execution strategy. The core decision point.

**Delta Manager** — Maintains the Delta Map and Tombstone Set. Supports transaction staging: additions are staged within a transaction, promoted on COMMIT, discarded on ROLLBACK.

**Schema Registry** — Tracks per-table schema divergence. Used to adapt Prod rows during reads (inject NULLs for new columns, strip dropped columns, rename, cast types) and during hydration.

**Transaction Manager** — Per-connection transaction state. Coordinates transactions across Prod (read-only) and Shadow (read-write). Enforces staged delta semantics.

**Engine Manager** — Connection pools to Prod and Shadow. Each app connection gets one dedicated Prod connection (read-only) and one dedicated Shadow connection (read-write).

**Merge Engine** — Combines Prod and Shadow results during reads. Handles single-table merges, JOIN patching, over-fetching for LIMIT queries, row filtering, and schema adaptation.

**Write Engine** — INSERT (Shadow-only), UPDATE (hydrate + Shadow), DELETE (Shadow + tombstone). Coordinates with Delta Manager on every mutation.

### 4.3 Routing Table

| Operation | Condition | Strategy |
|-----------|-----------|----------|
| `SELECT` (single table) | No deltas/tombstones for table | **Prod Direct** — forward to Prod |
| `SELECT` (single table) | Table has deltas or tombstones | **Merged Read** — query both, filter, adapt, merge |
| `SELECT` (JOIN) | No table has deltas/tombstones | **Prod Direct** — forward to Prod |
| `SELECT` (JOIN) | Any table has deltas/tombstones | **Join Patch** — execute on Prod, patch from Shadow |
| `INSERT` | -- | **Shadow Write** — execute on Shadow only |
| `UPDATE` | -- | **Hydrate + Shadow Write** — hydrate if needed, update Shadow |
| `DELETE` | -- | **Shadow Delete** — delete from Shadow, add tombstone |
| `DDL` | -- | **Shadow DDL** — execute on Shadow, update Schema Registry |
| `BEGIN` | -- | Open transactions on both Prod and Shadow |
| `COMMIT` | -- | Commit Shadow, close Prod, promote staged deltas |
| `ROLLBACK` | -- | Rollback Shadow, close Prod, discard staged deltas |

### 4.4 Read Path

#### Prod Direct

When a SELECT touches only tables with no local mutations, queries pass straight through to Prod. Zero overhead beyond the proxy hop.

#### Merged Read (Single Table)

When a SELECT touches a table with delta or tombstone entries:

1. **Shadow Query** — Execute the original query against Shadow. Returns the local subset.
2. **Prod Query (over-fetched)** — Execute against Prod with adjusted LIMIT: `LIMIT + |deltas| + |tombstones|`. If schema divergence exists, sanitize the query first (remove WHERE predicates on Shadow-only columns).
3. **Filter Prod Results** — Discard rows where `(table, pk)` exists in Delta Map (Shadow has authoritative version) or Tombstone Set (locally deleted).
4. **Adapt Prod Results** — Per Schema Registry: inject NULLs for added columns, strip dropped columns, apply renames/type casts.
5. **Merge** — Combine Shadow + filtered/adapted Prod results.
6. **Re-apply Ordering and Limits** — Sort by original ORDER BY, apply LIMIT/OFFSET.

Over-fetching caps at 3 iterations to bound latency.

#### Merged Read (JOINs)

When a SELECT joins tables and at least one has local mutations:

1. **Execute on Prod** — Run as-is. Produces correct results for non-delta rows.
2. **Identify affected rows** — For each row, check if any PK from a delta table is in the Delta Map or Tombstone Set. Partition into clean, delta, and dead rows.
3. **Discard tombstoned rows.**
4. **Patch delta rows** — Fetch Shadow versions, replace delta columns in joined result.
5. **Execute on Shadow** — Catches locally-inserted rows that should appear.
6. **Merge and deduplicate** — Combine, dedup by composite key, re-apply ORDER BY and LIMIT.

### 4.5 Write Path

#### INSERT

1. Execute on Shadow only.
2. Shadow's offset sequence assigns a PK in the local range (e.g., `10,000,001`). Offset guarantees no collision with Prod PKs.
3. Return result (including generated PK) to app.

For UUIDs, Shadow generates locally — statistically guaranteed unique, no collision risk.

#### UPDATE

**Point update** (`WHERE id = 42`):
1. Check Shadow — does the row exist?
   - **Yes**: proceed to step 2.
   - **No**: **Hydrate** — fetch from Prod, adapt for schema differences, insert into Shadow.
2. Apply UPDATE on Shadow.
3. Add `(table, pk)` to Delta Map.
4. Return result.

**Bulk update** (no PK in WHERE):
1. Identify affected rows via SELECT on both Prod and Shadow.
2. Hydrate missing Prod rows into Shadow.
3. Apply UPDATE on Shadow.
4. Add all affected PKs to Delta Map.

#### DELETE

1. If row exists in Shadow, delete it.
2. Add `(table, pk)` to Tombstone Set (uniform path — works whether row was in Shadow or Prod-only).
3. Return result.

### 4.6 DDL Path

DDL runs on Shadow only. The Schema Registry tracks what changed:

| DDL | Registry Records | Read Adaptation | Hydration Adaptation |
|-----|-----------------|----------------|---------------------|
| `ADD COLUMN` | Column name, type, default | Prod rows get NULL/default injected | Hydrated rows get NULL/default |
| `DROP COLUMN` | Dropped column name | Dropped column stripped from Prod | Dropped column excluded |
| `RENAME COLUMN` | `old_name -> new_name` | Column renamed in result | Column renamed on insert |
| `ALTER TYPE` | `old_type -> new_type` | Value cast in result | Value cast on insert |
| `CREATE TABLE` | Shadow-only table | N/A | N/A |
| `DROP TABLE` | Marked as dropped | "table not found" | N/A |
| `CREATE INDEX` | Shadow only, silent | N/A | N/A |

### 4.7 Transaction Model

- **BEGIN**: Open read-only transaction on Prod (REPEATABLE READ) and read-write transaction on Shadow.
- **COMMIT**: Commit Shadow. Close Prod. Promote staged deltas/tombstones to persistent state.
- **ROLLBACK**: Rollback Shadow. Close Prod. Discard staged deltas/tombstones.

Within a transaction, all delta/tombstone additions are **staged** — held in a per-transaction buffer. This prevents phantom deltas from failed transactions: if a transaction rolls back, the Delta Map remains unchanged.

Autocommit mode (implicit single-statement transactions) applies deltas immediately — no staging needed.

### 4.8 Primary Key Model

| PK Type | Handling | Routing |
|---------|---------|---------|
| **Serial / bigserial** | Sequence offset guarantees no collision | Range-based fast-path: PK above offset threshold = local |
| **UUID** | Local generation, statistically unique | Delta Map lookup for all operations |
| **Composite** | Serialized as tuple in Delta Map | Standard delta lookup |
| **No PK** | Table treated as read-only with warning | Prod Direct only |

Sequence offset formula: `max(prod_max * 10, prod_max + 10,000,000)`

---

## 5. Usage Guide

### 5.1 Prerequisites

- Go 1.21+
- Docker (for Shadow containers — all engines except SQLite and DuckDB)

### 5.2 Quick Start

```bash
# 1. Build
go build -o mori ./cmd/mori

# 2. Initialize — connect to your production database
./mori init

# 3. Start the proxy
./mori start

# 4. Point your app at the proxy (same port as your engine's default)
# Your app reads from prod, writes to shadow — transparently

# 5. Reset when done (wipes all local state)
./mori reset
```

### 5.3 CLI Reference

| Command | Description |
|---------|------------|
| `mori init` | Interactive setup: select engine, provider, enter credentials |
| `mori init --from <conn_string>` | Non-interactive init with connection string |
| `mori init --image <image>` | Use a custom Docker image for the Shadow container |
| `mori start [connection-name]` | Start proxy (first run creates Shadow) |
| `mori start --port <port>` | Start on a specific port |
| `mori start --verbose` | Log all queries and routing decisions |
| `mori stop` | Graceful shutdown, persist state |
| `mori reset` | Wipe all local state |
| `mori reset --hard` | Wipe + re-sync schema from Prod |
| `mori status` | Display delta rows, tombstones, schema diffs, sequence offsets |
| `mori log` | Stream proxy activity log |
| `mori log --tail <n>` | Show last n log entries |
| `mori inspect <table>` | Detailed state for a table |
| `mori ls` | List configured connections |
| `mori rm <connection-name>` | Remove a connection |
| `mori dash` | Launch TUI dashboard |
| `mori config` | View/edit configuration |

### 5.4 Lifecycle

```
mori init         Connect to Prod, dump schema, spin up Shadow, offset sequences
     |
mori start        Start proxy, load state, accept connections
     |
  [app runs]      CRUD operations routed transparently
     |
mori stop         Persist state, shut down proxy
     |
mori reset        Wipe Shadow data + metadata, clean slate
```

### 5.5 Configuration

Mori stores state in a `.mori/` directory at the project root:

```
.mori/
  mori.yaml             # Connection config (engine, addresses, ports)
  shadow/               # Shadow container metadata
  state/
    delta.json           # Delta Map (locally modified rows)
    tombstones.json      # Tombstone Set (locally deleted rows)
    schema_registry.json # Schema divergence tracking
    tables.json          # Table metadata (PKs, types)
    sequences.json       # Sequence offsets
  log/                   # Query and routing logs
```

### 5.6 MCP Server

Mori includes an MCP (Model Context Protocol) server for AI agent integration that supports all 9 engines. AI coding agents can programmatically query databases — reads from Prod, writes to Shadow — as part of their test-verify loop.

Tools by engine type:
- **SQL engines** (PostgreSQL, CockroachDB, MySQL, MariaDB, MSSQL, SQLite, DuckDB): `db_query` tool
- **Redis**: `redis_command`, `redis_get`, `redis_hgetall`, `redis_keys`
- **Firestore**: `firestore_get`, `firestore_list`, `firestore_query`

---

## 6. Design Decisions

### FK Constraints Stripped from Shadow

Shadow starts empty. INSERTs referencing Prod rows via FK would fail (the referenced row doesn't exist in Shadow). Hydrating referenced rows transitively is a rabbit hole. The app's code already ensures referential integrity.

### Production Data Drift Accepted

Prod reads are live, not pinned to a snapshot. Pinning holds resources on Prod that interfere with vacuuming and replication. For a dev tool with short sessions, live reads are pragmatic. Shadow overrides are consistent; Prod reads are best-effort recent.

### 1:1:1 Connection Model

Each app connection to Mori gets one dedicated Prod connection and one dedicated Shadow connection. Simple, no multiplexing complexity. Per-connection transaction isolation comes for free.

### Sequence Offset

`max(prod_max * 10, prod_max + 10,000,000)` per table. Large enough that Prod's ongoing inserts don't catch up during a session. Configurable for high-volume tables.

### Parameterized Queries: Two-Phase Classification

Classify at template parse time (tables, operation type); resolve PK values at parameter bind time. Necessary because wire protocols separate SQL templates from parameter values.

### Known Limitations

| Limitation | Impact | Mitigation |
|-----------|--------|-----------|
| Multi-column ORDER BY | Only single-column sort re-applied after merge | Multi-column ORDER BY returns in backend order |
| Mutating CTEs only see Shadow data | CTE reads from clean tables return empty | v1.1: split into write + read |
| Stored procedures with side effects | `SELECT my_function()` routes as READ to Prod | v2: function introspection or config allowlist |
| Tables without PKs | Read-only, writes produce warning | Rare in practice |
| Bulk ops on large tables | Hydrating thousands of rows is slow | Acceptable for dev tool |
| JOIN PK patching | Requires PK in SELECT list | Patching skipped when PK not selected |
| Aggregate queries | COUNT/SUM on delta tables not merged | Returns separate rows from Prod/Shadow |
| Parameterized LIMIT | `LIMIT $1` not rewritten for over-fetching | Uncommon in practice |

---

## 7. Roadmap

### 7.1 Current State (v1)

- All SQL engines at parity: merged reads, write hydration, transactions (TxnHandler), extended query protocol (ExtHandler), DDL tracking, schema adaptation
- 9 engine implementations (PostgreSQL, CockroachDB, MySQL, MariaDB, MSSQL, SQLite, DuckDB, Redis, Firestore)
- MySQL/MariaDB: vitess AST classifier, COM_STMT_PREPARE support, version-matched shadow images
- MSSQL: TDS RPC support (sp_executesql/sp_prepare), enhanced T-SQL classifier, version-matched shadow images
- SQLite: all handlers implemented (Read, Write, DDL, Txn, Ext), FK stripping, RENAME COLUMN support
- DuckDB: new embedded OLAP engine, file-based shadow, pgwire protocol, full handler parity with SQLite
- Redis: Pub/Sub fan-in, EVAL/EVALSHA support, merged SCAN, version-matched shadow images
- Firestore: SDK-based architecture, document-level merged reads, delta/tombstone tracking, seed cap removed
- PostgreSQL: extension auto-install via apt-get, `--image` flag for custom Docker images
- Multi-engine MCP server (SQL db_query, Redis tools, Firestore tools)
- 14 auth providers
- 202+ E2E tests (PostgreSQL)
- TUI dashboard

### 7.2 v1.1 — Fast Follows

- **Mutating CTE splitting** — Split `WITH updated AS (UPDATE ... RETURNING *) SELECT ...` into separate write and read operations for correct clean-table reads
- **Hydrate command** — `mori hydrate <table> [--where <condition>]` to explicitly pull rows from Prod into Shadow
- **Improved diagnostics** — Query timing, delta map diffs, warning aggregation
- **Better error messages** — Clear guidance for PK-less tables, schema divergence, Shadow container failures
- **Config validation** — `mori doctor` command to validate config, test connectivity, verify Shadow health

### 7.3 v2 — Platform

- **Snapshots** — Save/restore complete Shadow state as named snapshots (`mori snapshot save/restore/list/delete`)
- **Team sharing** — Push/pull snapshots to shared store (S3, GCS)
- **Stored procedure support** — Config allowlist for mutating functions, later function introspection
- **Snapshot pinning** — `mori start --snapshot` to pin Prod reads to a point-in-time
- **AI agent HTTP API** — REST API for programmatic control
- **Partial schema sync** — `mori init --tables users,orders` to shadow a subset
- **Read replica support** — Point reads at a replica instead of primary

### 7.4 Future Engines

MongoDB, Elasticsearch, DynamoDB, Cassandra, ClickHouse, Neo4j — registered in the engine catalog, not yet implemented.

---

## 8. Technical Reference

### 8.1 Project Structure

```
cmd/mori/                        # CLI entry point (Cobra)
  main.go, init.go, start.go, stop.go, reset.go,
  status.go, inspect.go, log.go, ls.go, rm.go,
  dash.go, config_cmd.go

internal/
  core/                          # Engine-agnostic core
    types.go                     # Classification, RoutingStrategy, OpType
    classifier.go                # Classifier interface
    router.go                    # Routing logic
    delta/                       # Delta Map + Tombstone Set (persistent)
      map.go, store.go, tombstone.go, pkset.go
    schema/                      # Schema Registry
      registry.go
    merge/                       # Merge engine interface
      merge.go, result.go
    config/                      # .mori/ directory & config
      config.go, project.go

  engine/                        # Engine-specific adapters (pluggable)
    engine.go, registry.go       # Engine + Proxy interfaces, registry
    postgres/                    # Reference implementation
      engine.go, init.go
      connstr/, classify/, proxy/, schema/, shadow/
    mysql/                       # MySQL + MariaDB
    mssql/                       # MS SQL Server
    sqlite/                      # SQLite (file-based)
    duckdb/                      # DuckDB (file-based, embedded OLAP)
    redis/                       # Redis
    firestore/                   # Google Firestore

  auth/                          # Database auth providers
    provider.go
    providers/                   # 14 provider implementations

  registry/                      # Engine/provider catalogs
    engine.go, provider.go, field.go

  tui/                           # Terminal UI dashboard
  mcp/                           # MCP server
  logging/                       # Structured logging
  ui/                            # CLI formatting

e2e/                             # End-to-end test suite
  e2e_test.go, helpers_test.go, seed.sql
  01-06_*_test.go                # PostgreSQL tests
  redis/, firestore/, sqlite/, mssql/, duckdb/
```

### 8.2 Engine Interface

Every engine implements this contract (`internal/engine/engine.go`):

```go
type Engine interface {
    ID() registry.EngineID
    Init(ctx context.Context, opts InitOptions) (*InitResult, error)
    ParseConnStr(connStr string) (*ConnInfo, error)
    LoadTableMeta(moriDir string) (map[string]TableMeta, error)
    NewClassifier(tables map[string]TableMeta) core.Classifier
    NewProxy(deps ProxyDeps, tables map[string]TableMeta) Proxy
}

type Proxy interface {
    ListenAndServe(ctx context.Context) error
    Shutdown(ctx context.Context) error
    Addr() string
}
```

| Method | Purpose | Called When |
|--------|---------|-----------|
| `ID()` | Return `registry.EngineID` constant | Registration + lookup |
| `Init()` | First-time setup: spin up shadow, dump prod schema, return config | `mori start` (first run) |
| `ParseConnStr()` | Parse connection string into `ConnInfo` | Every `mori start` |
| `LoadTableMeta()` | Read persisted table metadata from `.mori/` | Every `mori start` |
| `NewClassifier()` | Return a `core.Classifier` for this engine's query dialect | Every `mori start` |
| `NewProxy()` | Create a `Proxy` speaking this engine's wire protocol | Every `mori start` |

Bridge types: `TableMeta` (PKColumns, PKType), `ConnInfo` (Addr, Host, Port, DBName, User, Password, SSLMode, ConnStr), `InitOptions`, `InitResult`, `ProxyDeps`.

### 8.3 Adding a New Engine

1. **Create the folder** — `internal/engine/<name>/` with subpackages:

```
internal/engine/<name>/
  engine.go        # Main adapter (implements engine.Engine)
  init.go          # Shadow setup (Docker container, schema dump)
  connstr/         # Connection string parsing
  classify/        # Query classifier -> core.Classification
  proxy/           # Wire protocol proxy -> engine.Proxy
  schema/          # Schema introspection + persistence
  shadow/          # Shadow container management
```

2. **Implement the adapter** — `engine.go` with `init()` self-registration:

```go
func init() {
    engine.Register(&myEngine{})
}
```

3. **Add blank import** — In `cmd/mori/start.go`:

```go
import _ "github.com/psrth/mori/internal/engine/<name>"
```

4. **Mark as supported** — In `internal/registry/engine.go`, set `Supported: true`.

Each engine is completely isolated — no engine imports another engine's packages. Shared code lives in `internal/core/` (router, delta, merge) and `internal/registry/` (engine/provider catalogs).

### 8.4 Write Guard

SQL-based engines implement a 4-layer production write guard:

1. **Strict mode** — Refuse to serve if Shadow is unavailable
2. **L1 (routing assertion)** — Verify write/DDL ops never get `StrategyProdDirect`
3. **L2 (connection wrapper)** — Inspect outbound bytes on Prod connection, block write SQL
4. **L3 (dispatch gate)** — Final check: if write/DDL reaches `targetProd`, block and return error

For NoSQL engines, adapt the guard to the protocol (e.g., inspect Redis commands for SET/DEL/HSET).

### 8.5 Classifier Interface

```go
type Classifier interface {
    Classify(query string) (*Classification, error)
    ClassifyWithParams(query string, params []interface{}) (*Classification, error)
}
```

A `Classification` contains: `OpType` (OpRead/OpWrite/OpDDL/OpTransaction/OpMeta), `SubType`, `Tables []string`, and flags (`HasAggregate`, `IsJoin`, `IsComplexRead`, `HasSetOp`). The shared Router uses this to decide strategy. Each engine's classifier only needs to parse its own query language into these fields.

### 8.6 Testing

**Unit tests** — Each subpackage should have `_test.go` files:
- `classify/classifier_test.go` — Reads, writes, DDL, transactions, edge cases
- `connstr/connstr_test.go` — Connection string format variants
- `proxy/guard_test.go` — Write guard blocks writes, allows reads
- `schema/` — Dump/persist round-trips

**E2E tests** — Full suite in `e2e/` with engine-specific subdirectories:

```bash
# Run engine unit tests
go test ./internal/engine/<name>/...

# Run E2E tests (engine-specific build tags)
go test -tags e2e -v -count=1 -timeout 15m ./e2e/
go test -tags e2e_redis -v -count=1 -timeout 10m ./e2e/redis/

# Build verification
go build ./cmd/mori/...
go vet ./internal/engine/<name>/...
```
