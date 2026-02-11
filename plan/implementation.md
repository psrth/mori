# Mori Implementation Plan

**Go + PostgreSQL, modular for multi-engine future.**

---

## 1. Dependencies

| Dependency | Purpose | Notes |
|-----------|---------|-------|
| `jackc/pgx/v5` | PostgreSQL driver + connection pooling | Includes `pgproto3` for wire protocol message parsing |
| `pgx/v5/pgproto3` | PG wire protocol message types | Parse/Bind/Execute/Query/etc. Already part of pgx |
| `pganalyze/pg_query_go/v5` | SQL parser | Wraps PostgreSQL's actual parser. Produces a full AST. Battle-tested. |
| `docker/docker/client` | Docker SDK | Shadow container lifecycle (create, start, stop, remove) |
| `spf13/cobra` | CLI framework | Command structure, flags, help text |
| `encoding/json` (stdlib) | State persistence | Taint map, tombstone set, schema registry → JSON files |
| `net` (stdlib) | TCP listener | Proxy accepts connections |
| `sync` (stdlib) | Concurrency | Mutexes for taint map, goroutine-per-connection |

---

## 2. Project Structure

The architecture separates **engine-agnostic core** from **engine-specific adapters**. When we add Redis, MySQL, or MongoDB, we add a new package under `internal/engine/` and a new protocol handler — the core routing, taint, and merge logic is shared.

```
columbus-v1/
├── cmd/
│   └── mori/
│       ├── main.go              # Cobra root command, version, global flags
│       ├── init.go              # mori init
│       ├── start.go             # mori start
│       ├── stop.go              # mori stop
│       ├── reset.go             # mori reset
│       ├── status.go            # mori status
│       ├── log.go               # mori log
│       └── inspect.go           # mori inspect <table>
│
├── internal/
│   ├── core/                    # ◄ ENGINE-AGNOSTIC CORE
│   │   ├── types.go             # Shared types: Classification, RoutingDecision, TaintEntry, etc.
│   │   ├── classifier.go        # Classifier interface
│   │   ├── router.go            # Router: classification + taint state → strategy
│   │   ├── taint/
│   │   │   ├── map.go           # TaintMap: (table, pk) → tainted, with txn staging
│   │   │   ├── tombstone.go     # TombstoneSet: (table, pk) → deleted, with txn staging
│   │   │   └── store.go         # Persistence: JSON ↔ in-memory
│   │   ├── schema/
│   │   │   └── registry.go      # SchemaRegistry: per-table diffs (added/dropped/renamed cols)
│   │   ├── txn/
│   │   │   └── manager.go       # Per-connection transaction state, staging, commit/rollback
│   │   ├── merge/
│   │   │   ├── merge.go         # MergeEngine interface: combine results from two sources
│   │   │   └── result.go        # ResultSet abstraction (rows, columns, ordering)
│   │   └── config/
│   │       └── config.go        # .mori/ directory structure, read/write config + state
│   │
│   ├── engine/                  # ◄ ENGINE-SPECIFIC ADAPTERS
│   │   └── postgres/
│   │       ├── proxy/
│   │       │   ├── listener.go  # TCP listener, PG startup handshake
│   │       │   └── connection.go # Per-connection loop: receive → classify → route → respond
│   │       ├── classify/
│   │       │   └── classifier.go # SQL → Classification using pg_query_go AST
│   │       ├── merge/
│   │       │   ├── single.go    # Merged reads for single-table SELECTs
│   │       │   └── join.go      # Execute-on-Prod-Patch-from-Shadow for JOINs
│   │       ├── write/
│   │       │   ├── insert.go    # Shadow-only INSERT
│   │       │   ├── update.go    # Hydrate + Shadow UPDATE
│   │       │   └── delete.go    # Shadow DELETE + tombstone
│   │       ├── ddl/
│   │       │   └── handler.go   # Shadow DDL + schema registry update
│   │       ├── shadow/
│   │       │   └── container.go # Docker container lifecycle for PG Shadow
│   │       ├── schema/
│   │       │   └── dumper.go    # pg_dump --schema-only, FK stripping, schema apply
│   │       ├── pool/
│   │       │   └── pools.go     # pgx connection pools for Prod + Shadow
│   │       └── adapter.go       # Implements core.EngineAdapter interface, wires everything together
│   │
│   └── logging/
│       └── logger.go            # Structured logging: queries, routing decisions, hydrations, timing
│
├── plan/                        # Design documents (you are here)
│   ├── v1.md
│   ├── protocol.md
│   ├── roadmap.md
│   └── implementation.md
│
├── go.mod
└── go.sum
```

### Why This Structure

- **`internal/core/`** contains everything that is database-agnostic: the taint map, tombstone set, router logic, schema registry interface, transaction manager, and config. When we add Redis or MySQL, none of this changes.
- **`internal/engine/postgres/`** contains everything PostgreSQL-specific: the PG wire protocol handler, SQL classifier (using `pg_query_go`), PG-specific merge logic, write paths, DDL handling, and Docker container management for PG.
- Adding a new engine means creating `internal/engine/redis/`, `internal/engine/mysql/`, etc. Each implements the same core interfaces with engine-specific behavior.

---

## 3. Core Interfaces

These interfaces live in `internal/core/` and are implemented by each engine adapter.

```go
// internal/core/types.go

// Classification is the result of parsing a query.
type Classification struct {
    OpType    OpType            // READ, WRITE, DDL, TRANSACTION, OTHER
    SubType   SubType           // SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP, BEGIN, COMMIT, ROLLBACK
    Tables    []string          // All table names referenced
    PKs       []TablePK         // Extractable (table, pk) pairs from WHERE clauses
    IsJoin    bool              // Whether the query involves multiple tables
    HasLimit  bool              // Whether the query has a LIMIT clause
    Limit     int               // The LIMIT value, if present
    OrderBy   string            // Raw ORDER BY clause, for re-application after merge
    RawSQL    string            // Original SQL text
}

// TablePK identifies a row in a specific table.
type TablePK struct {
    Table string
    PK    string    // Serialized PK value (scalar or JSON-encoded composite)
}

// RoutingStrategy is the decision of where to execute a query.
type RoutingStrategy int

const (
    StrategyProdDirect       RoutingStrategy = iota // Forward to Prod, return result
    StrategyMergedRead                              // Query both, filter, merge
    StrategyJoinPatch                               // Execute on Prod, patch from Shadow
    StrategyShadowWrite                             // Execute on Shadow only
    StrategyHydrateAndWrite                         // Hydrate from Prod, then write to Shadow
    StrategyShadowDelete                            // Delete from Shadow, add tombstone
    StrategyShadowDDL                               // Execute DDL on Shadow, update schema registry
    StrategyTransaction                             // Transaction control (BEGIN/COMMIT/ROLLBACK)
)

// Row represents a single database row as a map of column names to values.
type Row map[string]interface{}

// ResultSet is a collection of rows with column metadata.
type ResultSet struct {
    Columns []string
    Rows    []Row
}
```

```go
// internal/core/classifier.go

// Classifier parses a query into a Classification.
// Each engine implements this interface with its own parser.
type Classifier interface {
    // Classify parses a SQL/command string and returns its classification.
    Classify(query string) (*Classification, error)

    // ClassifyWithParams classifies a parameterized query with bound values.
    // Used for the extended query protocol (Parse/Bind/Execute).
    ClassifyWithParams(query string, params []interface{}) (*Classification, error)
}
```

```go
// internal/core/router.go

// Router decides execution strategy based on classification and taint state.
// This is engine-agnostic — it only looks at classification metadata and taint state.
type Router struct {
    taintMap    *taint.Map
    tombstones  *taint.TombstoneSet
}

// Route returns the execution strategy for a classified query.
func (r *Router) Route(c *Classification) RoutingStrategy {
    switch c.OpType {
    case OpRead:
        if c.IsJoin {
            if r.anyTableTainted(c.Tables) {
                return StrategyJoinPatch
            }
            return StrategyProdDirect
        }
        if r.anyTableTainted(c.Tables) {
            return StrategyMergedRead
        }
        return StrategyProdDirect
    case OpWrite:
        switch c.SubType {
        case SubInsert:
            return StrategyShadowWrite
        case SubUpdate:
            return StrategyHydrateAndWrite
        case SubDelete:
            return StrategyShadowDelete
        }
    case OpDDL:
        return StrategyShadowDDL
    case OpTransaction:
        return StrategyTransaction
    }
    return StrategyProdDirect // default: treat as read
}
```

```go
// internal/core/merge/merge.go

// MergeEngine combines results from Prod and Shadow.
// Each engine implements this with its own row serialization and comparison.
type MergeEngine interface {
    // MergeSingleTable performs a merged read for a single-table SELECT.
    MergeSingleTable(ctx context.Context, query string, class *core.Classification) (*core.ResultSet, error)

    // MergeJoin performs the execute-on-Prod-patch-from-Shadow strategy for JOINs.
    MergeJoin(ctx context.Context, query string, class *core.Classification) (*core.ResultSet, error)
}
```

```go
// EngineAdapter is the top-level interface that each database engine implements.
// It wires together the classifier, merge engine, write engine, and protocol handler.
type EngineAdapter interface {
    // Init performs engine-specific initialization (schema dump, container setup, etc.)
    Init(ctx context.Context, prodConnStr string) error

    // Start starts the proxy and begins accepting connections.
    Start(ctx context.Context, port int) error

    // Stop gracefully shuts down the proxy.
    Stop(ctx context.Context) error

    // Reset wipes all local state.
    Reset(ctx context.Context, hard bool) error

    // Classifier returns the engine's query classifier.
    Classifier() Classifier

    // Status returns the current state of the engine.
    Status() (*EngineStatus, error)
}
```

---

## 4. Build Phases

Each phase produces something testable. Checkpoint at each phase before moving on.

---

### Phase 1: Project Skeleton + CLI Framework
**Goal:** `mori` binary builds, subcommands are wired, `--help` works.

**Build:**
- `go mod init` with module path
- `cmd/mori/main.go` — Cobra root command with version flag
- Stub all subcommands: `init`, `start`, `stop`, `reset`, `status`, `log`, `inspect`
- `internal/core/types.go` — all shared type definitions
- `internal/core/config/config.go` — `.mori/` directory creation, config read/write

**Checkpoint:** `go build ./cmd/mori && ./mori --help` prints usage. `./mori init --help` prints init flags. `./mori status` prints "not initialized" (no `.mori/` directory).

---

### Phase 2: Shadow Container Lifecycle
**Goal:** `mori init` connects to Prod, dumps schema, spins up a PG Shadow container, applies schema.

**Build:**
- `internal/engine/postgres/shadow/container.go` — Docker SDK:
  - Pull PG image (matching Prod version, or user-specified `--image`)
  - Create container with random port mapping
  - Start container, wait for health check (`pg_isready`)
  - Stop and remove container
- `internal/engine/postgres/schema/dumper.go`:
  - Query Prod for installed extensions: `SELECT extname, extversion FROM pg_extension`
  - Shell out to `pg_dump --schema-only` against Prod
  - Parse dump to strip FK constraints (regex on `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`)
  - Install extensions on Shadow (attempt `CREATE EXTENSION` for each). If any fail, abort with clear message: "Prod uses `<ext>` but Shadow image doesn't have it — re-run with `--image <image>`"
  - Apply filtered schema to Shadow via `psql` or direct pgx connection
  - Query Prod for max PK values per table, set Shadow sequences
- `cmd/mori/init.go` — wire init command to container + schema logic, accept `--image` flag
- Persist config to `.mori/config.json`

**Checkpoint:** `mori init --from postgres://prod:5432/mydb` creates `.mori/`, starts a Docker PG container, applies schema. `docker ps` shows the container. Connect directly to Shadow and verify tables exist, FKs are gone, sequences are offset, extensions are installed.

---

### Phase 3: Transparent Pass-Through Proxy
**Goal:** `mori start` listens on a port, proxies all traffic to Prod unmodified. App connects to Mori, gets Prod data.

**Build:**
- `internal/engine/postgres/proxy/listener.go`:
  - TCP listener on configured port
  - Accept connections, spawn goroutine per connection
  - PG startup message handling (SSL negotiation, startup parameters)
- `internal/engine/postgres/proxy/connection.go`:
  - Read PG wire protocol messages (using `pgproto3.Backend`)
  - Forward all messages to Prod (using `pgproto3.Frontend`)
  - Relay Prod responses back to app
  - Handle connection close/error
- `internal/engine/postgres/pool/pools.go`:
  - Create pgx connection pool to Prod (read-only)
  - Per-app-connection: allocate one Prod connection
- `cmd/mori/start.go` — wire start command
- `cmd/mori/stop.go` — drain connections, shutdown

**Checkpoint:** `mori start --port 5433`, then `psql -h localhost -p 5433 -d mydb`. Run `SELECT * FROM users LIMIT 5` — get Prod data. Run `\dt` — see all tables. App driver (pgx, psycopg2) connects and works. All queries pass through unmodified.

---

### Phase 4: SQL Classifier
**Goal:** Every incoming query is classified by operation type, affected tables, and extractable PKs.

**Build:**
- `internal/engine/postgres/classify/classifier.go`:
  - Parse SQL with `pg_query_go` → AST
  - Walk AST to extract:
    - **Operation type**: check top-level node type (SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, AlterTableStmt, CreateStmt, DropStmt, TransactionStmt)
    - **Tables**: walk `FROM`, `INTO`, `UPDATE`, `JOIN` clauses
    - **PKs**: for `WHERE id = <value>` patterns, extract the value (requires knowing PK column names per table — from config/schema)
    - **JOIN detection**: check if multiple tables in FROM/JOIN clauses
    - **LIMIT/ORDER BY**: extract from SelectStmt
  - Handle edge cases: subqueries, CTEs (check for mutating CTEs), UNION, INSERT ... SELECT

**Checkpoint:** Unit test suite with 50+ SQL patterns:
- Simple CRUD: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- JOINs: inner, left, right, cross, self-join
- Subqueries: `WHERE id IN (SELECT ...)`
- CTEs: `WITH ... SELECT`, `WITH ... UPDATE ... RETURNING`
- DDL: `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`
- Edge cases: `SELECT 1`, `SET`, `SHOW`, `EXPLAIN`

---

### Phase 5: State Stores (Taint Map, Tombstone Set, Schema Registry)
**Goal:** In-memory state management with persistence and transaction staging.

**Build:**
- `internal/core/taint/map.go`:
  - Core data structure: `map[string]map[string]bool` (table → serialized_pk → exists)
  - Methods: `Add(table, pk)`, `Remove(table, pk)`, `IsTainted(table, pk) bool`, `TaintedPKs(table) []string`, `CountForTable(table) int`
  - Thread-safe (sync.RWMutex)
- `internal/core/taint/tombstone.go`:
  - Same structure and API as TaintMap
  - Methods: `Add(table, pk)`, `IsTombstoned(table, pk) bool`, `TombstonedPKs(table) []string`
- Transaction staging (in both):
  - `Stage(table, pk)` — add to per-transaction buffer
  - `Commit()` — promote staged entries to persistent state
  - `Rollback()` — discard staged entries
- `internal/core/taint/store.go`:
  - Serialize to JSON: `{"users": ["42", "108"], "orders": ["7"]}`
  - Deserialize from JSON on startup
  - Persist to `.mori/taint.json` and `.mori/tombstones.json`
- `internal/core/schema/registry.go`:
  - Per-table diffs: `map[string]TableDiff`
  - `TableDiff`: `Added []Column`, `Dropped []string`, `Renamed map[string]string`, `TypeChanged map[string][2]string`
  - Methods: `RecordAddColumn(table, col)`, `RecordDropColumn(table, col)`, `GetDiff(table) *TableDiff`, `HasDiff(table) bool`
  - Persist to `.mori/schema_registry.json`

**Checkpoint:** Unit tests for all operations. Persistence round-trip test: create state → persist → reload → verify identical. Transaction staging test: stage → commit → verify promoted; stage → rollback → verify discarded. Concurrent access test: multiple goroutines read/write taint map.

---

### Phase 6: Router + Shadow Connection
**Goal:** The proxy classifies queries and routes writes to Shadow. Reads still go to Prod.

**Build:**
- `internal/core/router.go` — implement routing logic (see §3)
- `internal/engine/postgres/pool/pools.go` — add Shadow connection pool
- Update `internal/engine/postgres/proxy/connection.go`:
  - After receiving a message, classify the query
  - Route based on classification:
    - READ + no taints → forward to Prod (existing behavior)
    - WRITE → forward to Shadow
    - DDL → forward to Shadow
    - TRANSACTION → handle (next phase)
  - Return result from appropriate backend to app

**Checkpoint:** Connect via `psql`. `SELECT * FROM users` → Prod data. `INSERT INTO users (name) VALUES ('test')` → goes to Shadow. `SELECT * FROM users WHERE id = <new_id>` → still goes to Prod (returns nothing, because routing doesn't merge yet). Verify row is in Shadow by connecting directly.

---

### Phase 7: Write Paths (INSERT, UPDATE, DELETE)
**Goal:** Full write path with hydration, taint tracking, and tombstoning.

**Build:**
- `internal/engine/postgres/write/insert.go`:
  - Execute INSERT on Shadow
  - Return result (including RETURNING if present) to app
  - No taint map update (row is Shadow-only)
- `internal/engine/postgres/write/update.go`:
  - **Point update** (`WHERE pk = value`):
    1. Check if `(table, pk)` exists in Shadow
    2. If not: fetch row from Prod (`SELECT * WHERE pk = value`), insert into Shadow (adapt for schema diffs)
    3. Execute UPDATE on Shadow
    4. Add `(table, pk)` to Taint Map
  - **Bulk update** (no PK in WHERE):
    1. `SELECT pk FROM table WHERE <condition>` on both Prod and Shadow
    2. For each Prod PK not in Shadow: hydrate
    3. Execute UPDATE on Shadow
    4. Add all affected PKs to Taint Map
- `internal/engine/postgres/write/delete.go`:
  - If `(table, pk)` exists in Shadow: `DELETE FROM table WHERE pk = value`
  - Add `(table, pk)` to Tombstone Set
  - For bulk deletes: identify affected PKs, delete from Shadow, add all to Tombstone Set

**Checkpoint:** Integration test sequence:
1. `INSERT INTO users (name) VALUES ('alice')` → success, returns new ID in local range
2. `UPDATE users SET name = 'bob' WHERE id = 42` (Prod row) → hydrates user 42 into Shadow, updates, taint map shows `(users, 42)`
3. `DELETE FROM users WHERE id = 43` → tombstone added
4. Verify Shadow state directly: new row exists, user 42 exists with modified name, user 43 absent
5. Verify taint map and tombstone set on disk

---

### Phase 8: Merged Reads (Single Table)
**Goal:** SELECTs on tainted tables merge Shadow + Prod results.

**Build:**
- `internal/engine/postgres/merge/single.go`:
  1. Execute query on Shadow → `R_shadow`
  2. Execute query on Prod (with over-fetching for LIMIT queries) → `R_prod`
  3. Filter `R_prod`: remove rows where PK is in taint map or tombstone set
  4. Adapt `R_prod` for schema diffs (via Schema Registry): inject NULLs, strip columns, rename, cast
  5. Merge `R_shadow` + filtered `R_prod`
  6. Re-sort by original ORDER BY
  7. Apply LIMIT/OFFSET to merged set
  8. Return to app
- Over-fetching logic:
  - First attempt: `LIMIT N + |taints| + |tombstones|`
  - If result too short: retry with OFFSET, cap at 3 iterations
- Update proxy connection to use MergeEngine when StrategyMergedRead

**Checkpoint:**
1. Insert user locally, UPDATE another, DELETE a third
2. `SELECT * FROM users ORDER BY id` → see Prod users + local insert + updated user from Shadow + deleted user absent
3. `SELECT * FROM users WHERE active = true LIMIT 10` → merged results with correct filtering, ordering, and limit
4. `SELECT count(*) FROM users` → correct count (Prod count - tombstones + local inserts)

---

### Phase 9: Merged Reads (JOINs)
**Goal:** JOINs across tainted and clean tables produce correct results.

**Build:**
- `internal/engine/postgres/merge/join.go`:
  1. Execute JOIN on Prod → `R_prod`
  2. For each row in `R_prod`, extract PKs for all tainted tables
  3. Check PKs against taint map and tombstone set
  4. Remove rows with tombstoned PKs
  5. For rows with tainted PKs: fetch Shadow versions, replace tainted columns
  6. Re-evaluate WHERE clause for patched rows (drop rows that no longer match)
  7. Execute same JOIN on Shadow → `R_shadow`
  8. Merge patched `R_prod` + `R_shadow`, dedup by composite PK
  9. Re-apply ORDER BY + LIMIT
- PK extraction from JOIN results: need to know which columns in the result map to which table's PK (use table alias tracking from classifier)

**Checkpoint:**
1. Taint a user (update name)
2. `SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE u.id = <tainted_id>` → shows updated name from Shadow
3. Insert a new user, insert an order for that user
4. Same JOIN query → new user+order appears in results
5. Tombstone a user → their orders disappear from JOIN results

---

### Phase 10: DDL + Schema Registry Integration
**Goal:** ALTER TABLE runs on Shadow, Schema Registry tracks diffs, reads adapt Prod rows.

**Build:**
- `internal/engine/postgres/ddl/handler.go`:
  - Parse DDL with `pg_query_go` to identify operation (ADD COLUMN, DROP COLUMN, RENAME COLUMN, ALTER TYPE, CREATE TABLE, DROP TABLE)
  - Execute DDL on Shadow
  - Update Schema Registry with the change
- Wire Schema Registry into merged reads: when `registry.HasDiff(table)`, adapt Prod rows
- Wire Schema Registry into hydration: when copying a Prod row to Shadow, adapt for diffs

**Checkpoint:**
1. `ALTER TABLE users ADD COLUMN phone TEXT` → executes on Shadow
2. `SELECT * FROM users` → Prod rows have `phone = NULL` injected
3. `INSERT INTO users (name, phone) VALUES ('alice', '555-0100')` → Shadow has phone value
4. `ALTER TABLE users DROP COLUMN fax` → Prod rows have fax stripped
5. Verify schema registry on disk reflects both changes

---

### Phase 11: Transaction Management
**Goal:** BEGIN/COMMIT/ROLLBACK with staged taints and coordinated Prod/Shadow transactions.

**Build:**
- `internal/core/txn/manager.go`:
  - Per-connection state: `inTransaction bool`, `stagedTaints []TablePK`, `stagedTombstones []TablePK`
  - `Begin()`: open Prod transaction (REPEATABLE READ), open Shadow transaction
  - `Commit()`: commit Shadow, close Prod, promote staged taints/tombstones
  - `Rollback()`: rollback Shadow, close Prod, discard staged taints/tombstones
- Update proxy connection: detect TRANSACTION operations, delegate to txn manager
- Update write paths: within a transaction, stage taint/tombstone additions instead of applying immediately

**Checkpoint:**
1. `BEGIN; INSERT INTO users (name) VALUES ('test'); ROLLBACK;` → taint map unchanged, row absent from Shadow
2. `BEGIN; UPDATE users SET name = 'x' WHERE id = 42; COMMIT;` → taint map has (users, 42), Shadow has updated row
3. `BEGIN; DELETE FROM users WHERE id = 43; ROLLBACK;` → tombstone set unchanged
4. Within a transaction, reads see transaction-local changes (Shadow's transaction isolation handles this)

---

### Phase 12: Extended Query Protocol
**Goal:** Parameterized queries (Parse/Bind/Execute) work correctly.

**Build:**
- Extend `proxy/connection.go` to handle extended query protocol messages:
  - **Parse**: receive SQL template, classify (tables, op type), store as prepared statement metadata
  - **Bind**: receive parameter values, resolve PKs from bound parameters, finalize classification
  - **Execute**: route using finalized classification
  - **Describe**: return column descriptions (may need to query appropriate backend)
  - **Close**: clean up prepared statement state
  - **Sync**: flush response buffer
- Per-connection prepared statement registry: `map[string]*PreparedStatement` mapping statement name to classification + metadata

**Checkpoint:** Connect with a real application driver:
1. pgx Go client with parameterized queries → CRUD operations work
2. psycopg2 Python client with prepared statements → CRUD operations work
3. An ORM (e.g., GORM, SQLAlchemy) connects and basic operations succeed
4. `PREPARE stmt AS SELECT * FROM users WHERE id = $1; EXECUTE stmt(42);` → works

---

### Phase 13: CLI Polish
**Goal:** All CLI commands produce useful output.

**Build:**
- `cmd/mori/status.go`:
  ```
  Engine:       PostgreSQL 16.2
  Prod:         postgres://prod-host:5432/mydb (read-only)
  Shadow:       postgres://localhost:54320/mori_shadow
  Proxy:        localhost:5432

  Tainted Rows:
    users           4 rows
    orders          1 row

  Tombstones:
    users           1 row

  Schema Diffs:
    users           +phone (TEXT), -fax

  Sequence Offsets:
    users.id        start=10,000,001 (prod max: 834,219)
    orders.id       start=10,000,001 (prod max: 612,003)
  ```
- `cmd/mori/reset.go`: truncate Shadow, re-sync schema, clear state. `--hard` re-dumps from Prod.
- `cmd/mori/log.go`: stream from log file, `--tail N` for last N entries
- `cmd/mori/inspect.go`: per-table detail (taint count, tombstone count, schema diffs, sequence offset, sample local rows)
- `internal/logging/logger.go`: structured JSON log with query text, classification, routing decision, timing, hydration events

**Checkpoint:** All commands produce correct, well-formatted output. `mori status` after a session shows accurate state. `mori log --tail 20` shows recent activity. `mori inspect users` shows detailed table state. `mori reset` clears everything and `mori status` confirms clean state.

---

### Phase 14: Hardening + End-to-End Tests
**Goal:** Robust error handling, edge cases covered, full E2E test suite.

**Build:**
- **E2E test harness:**
  - Spin up a test Prod PG (Docker container with seed data)
  - Run `mori init`, `mori start`
  - Execute scripted workload (mix of reads, writes, JOINs, DDL, transactions)
  - Verify all results match expected output
  - Run `mori reset`, verify clean state
  - Run `mori stop`, verify graceful shutdown
- **Edge cases:**
  - Tables without PKs → warning, read-only behavior
  - UUID PKs → correct routing and taint tracking
  - Composite PKs → correct serialization and lookup
  - Empty tables → no errors
  - Very large result sets → over-fetching works, performance acceptable
  - Concurrent app connections → no race conditions
- **Error handling:**
  - Prod connection drops mid-query → clear error to app, reconnect
  - Shadow container dies → clear error, suggest `mori reset`
  - Malformed SQL → pass through to appropriate backend, return DB error
  - Auth failures → clear error message
  - `.mori/` corrupted → clear error, suggest `mori reset --hard`
- **Graceful shutdown:**
  - Drain in-flight queries before stopping
  - Persist all state before exit
  - Handle SIGINT/SIGTERM

**Checkpoint:** Full E2E test suite passes. Manual testing with a real application (simple CRUD app) works end-to-end. `mori init → start → [use app] → reset → [use app again] → stop` cycle works reliably.

---

## 5. State Persistence Format

All state lives in `.mori/` as human-readable JSON:

```
.mori/
├── config.json              # Connection metadata, engine, version, port
│   {
│     "prod_connection": "postgres://...",
│     "shadow_port": 54320,
│     "shadow_container": "mori-shadow-abc123",
│     "shadow_image": "postgres:16.2",
│     "engine": "postgres",
│     "engine_version": "16.2",
│     "proxy_port": 5432,
│     "extensions": ["uuid-ossp", "hstore", "pg_trgm"],
│     "initialized_at": "2025-01-15T10:30:00Z"
│   }
│
├── sequences.json           # Per-table sequence offsets
│   {
│     "users": {"column": "id", "type": "serial", "prod_max": 834219, "shadow_start": 10000001},
│     "orders": {"column": "id", "type": "bigserial", "prod_max": 612003, "shadow_start": 10000001}
│   }
│
├── taint.json               # Taint map
│   {
│     "users": ["42", "108"],
│     "orders": ["7"]
│   }
│
├── tombstones.json          # Tombstone set
│   {
│     "users": ["99"]
│   }
│
├── schema_registry.json     # Schema diffs
│   {
│     "users": {
│       "added": [{"name": "phone", "type": "TEXT", "default": null}],
│       "dropped": ["fax"],
│       "renamed": {},
│       "type_changed": {}
│     }
│   }
│
├── tables.json              # Table metadata (PK columns, PK types)
│   {
│     "users": {"pk_columns": ["id"], "pk_type": "serial"},
│     "orders": {"pk_columns": ["id"], "pk_type": "bigserial"},
│     "user_roles": {"pk_columns": ["user_id", "role_id"], "pk_type": "composite"}
│   }
│
└── log/
    └── mori.log             # Query log (append-only, structured JSON lines)
```

---

## 6. Testing Strategy

| Layer | Approach | Tools |
|-------|----------|-------|
| **Classifier** | Unit tests with SQL corpus (50+ patterns) | `go test`, table-driven tests |
| **Taint/Tombstone** | Unit tests for CRUD, staging, persistence | `go test` |
| **Schema Registry** | Unit tests for all DDL operations | `go test` |
| **Router** | Unit tests with mock taint state | `go test` |
| **Write paths** | Integration tests with real PG (Shadow) | `go test` + Docker test container |
| **Merge engine** | Integration tests with real PG (Prod + Shadow) | `go test` + Docker test containers |
| **Proxy** | Integration tests with real PG client | `go test` + `pgx` test client |
| **E2E** | Full lifecycle test harness | Custom Go test + Docker |

Integration and E2E tests use Docker containers (same as production use). The test harness:
1. Spins up a "Prod" PG container with seed data
2. Runs `mori init` against it
3. Runs `mori start`
4. Executes test workload via pgx client
5. Asserts results
6. Tears everything down

---

## 7. Verification Plan

After each phase, verify:

1. **Build:** `go build ./cmd/mori` succeeds with no errors
2. **Tests:** `go test ./...` passes for all existing tests
3. **Manual:** Run the checkpoint for that phase (documented above)
4. **State:** Verify `.mori/` files are correct after operations

Final verification (after Phase 14):
1. Full E2E test suite passes
2. Connect a real application (e.g., a simple Go/Python CRUD app) to Mori
3. Exercise: read, insert, update, delete, JOIN, ALTER TABLE, transaction, reset
4. Verify all operations produce correct results
5. `mori reset` restores clean state
6. `mori stop` + `mori start` preserves state across restarts
