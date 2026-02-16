# Changes for Docs

Temporary file tracking code changes that need documentation updates. Delete after docs update is complete.

---

## Oracle Engine Removal

**What changed**: Oracle engine support has been completely removed from Mori.

**Why**: Oracle was a stub implementation with no ReadHandler, WriteHandler, DDLHandler, or TxnHandler. It only functioned as a pass-through proxy. The proprietary Oracle Net8 protocol complexity makes it not worth investing in.

**Code changes**:
- Removed `Oracle` EngineID constant from `internal/registry/engine.go`
- Removed Oracle entry from engines array in `internal/registry/engine.go`
- Removed Oracle connection fields from `internal/registry/field.go`
- Removed Oracle from AWS RDS `CompatibleEngines` in `internal/registry/provider.go`
- Removed Oracle side-effect import from `cmd/mori/start.go`
- Removed `toOracleConnString()` and its switch case from `internal/core/config/project.go`
- Deleted `internal/engine/oracle/` directory (15 files)
- Deleted `e2e/oracle/` directory (8 files including seed.sql)

**Doc implications**:
- Engine count changed from 9 to 8 supported engines
- Oracle removed from all engine matrices, connection field tables, provider compatibility lists
- AWS RDS compatible engines no longer include Oracle
- Updated: `docs/mintlify/engines/overview.mdx`, `docs/mintlify/engines/providers.mdx`, `docs/mintlify/roadmap.mdx`, `docs/testing.md`, `docs/mori.md`, `docs/SKILL.md`

---

## PostgreSQL Extension Auto-Install

**What changed**: When `CREATE EXTENSION` fails on the Shadow container, Mori now automatically attempts to install the extension package via `apt-get` inside the container.

**How it works**:
1. During `mori init` (first start) or `mori reset --hard`, Mori detects Prod extensions and runs `CREATE EXTENSION IF NOT EXISTS` on Shadow.
2. If that fails (extension not available in the image), Mori runs `docker exec <container> apt-get update && apt-get install -y postgresql-<major>-<extension>` inside the Shadow container.
3. Official Postgres Docker images ship with PGDG apt repos pre-configured, so packages like `postgresql-16-pgvector`, `postgresql-16-postgis`, etc. are available.
4. After installing, Mori retries `CREATE EXTENSION`. If it still fails, the user is told to use `--image`.

**New `--image` flag**:
- Added to `mori init` command: `mori init --image myregistry/postgres-custom:16`
- Persisted in `mori.yaml` under the connection's `extra.image` field
- Read during `mori start` and passed to the engine init as `ImageOverride`
- Escape hatch for custom Docker images where apt-get won't work (e.g., distroless images, proprietary extensions)

**Code changes**:
- `internal/engine/engine.go`: Added `ImageOverride` field to `InitOptions`
- `internal/engine/postgres/schema/dumper.go`: Added `ExtInstallOptions` struct, `aptInstallExtension()` function, updated `InstallExtensions()` to auto-install on failure, updated `ApplyToShadow()` signature
- `internal/engine/postgres/init.go`: Passes container ID and PG major version to `ApplyToShadow`
- `internal/engine/postgres/engine.go`: Threads `ImageOverride` through to postgres-level `InitOptions`
- `cmd/mori/init.go`: Added `--image` flag, passes it to both interactive and non-interactive init
- `cmd/mori/init_interactive.go`: Accepts and persists `imageOverride` in connection's Extra map
- `cmd/mori/start.go`: Reads `image` from connection Extra map, passes as `ImageOverride`
- `cmd/mori/reset.go`: Passes container info to `ApplyToShadow` for hard reset

**Doc implications**:
- CLI reference: `mori init` now has `--image` flag
- PostgreSQL engine docs should mention extension auto-install behavior
- Troubleshooting: if extension install fails, user can use `--image` as escape hatch

---

## Redis Engine Parity

### 5a. Pub/Sub Routing Fix
- **SUBSCRIBE/PSUBSCRIBE**: Now forwarded to BOTH Prod and Shadow backends with a fan-in multiplexer that merges messages from both sources to the client
- **PUBLISH**: Remains Shadow-only (write guard preserved)
- **UNSUBSCRIBE/PUNSUBSCRIBE**: Forwarded to both backends, tears down listeners
- Files: `internal/engine/redis/classify/classifier.go`, `internal/engine/redis/proxy/proxy.go`

### 5b. Lua Scripting (EVAL/EVALSHA)
- Added EVAL, EVALSHA, EVALRO, EVALSHA_RO, SCRIPT to classifier
- EVAL/EVALSHA classified as OpWrite; keys extracted from numkeys argument
- Before execution: each declared KEY is hydrated from Prod to Shadow via DUMP/RESTORE if not already in deltaMap
- After execution: all declared KEYS added to deltaMap
- Files: `internal/engine/redis/classify/classifier.go`, `internal/engine/redis/proxy/proxy.go`

### 5c. SCAN Consistency
- When deltaMap has entries, SCAN runs on both Prod and Shadow
- Results merged, deduplicated, tombstoned keys filtered out
- Cursor management: uses prod cursor, falls back to shadow cursor if prod is exhausted
- File: `internal/engine/redis/proxy/proxy.go`

### 5d. Version Detection
- `INFO server` output already parsed during `mori init` (existed before)
- Now uses parsed `redis_version` to select matching Docker image tag (e.g., `redis:7.2` for version `7.2.4`)
- Falls back to `redis:7` (default) if version-matched image is unavailable
- Files: `internal/engine/redis/shadow/shadow.go` (new `ImageForVersion`), `internal/engine/redis/init.go`

### 5e. Docs Update
- `e2e/PENDING-E2E.md`: Marked MSET proxy bug as FIXED, added note about new Redis features

### New delta.Map Method
- Added `HasAnyDelta()` to `internal/core/delta/map.go` -- reports whether any delta entries or inserts exist at all (used by merged SCAN)

**Doc implications**:
- Redis feature matrix should show: Pub/Sub fan-in, EVAL/EVALSHA support, merged SCAN, version-matched shadow images
- Redis MSET bug is resolved
- Redis command count in classifier increased (EVAL, EVALSHA, EVALRO, EVALSHA_RO, SCRIPT added)

---

## SQLite Engine Parity

### TxnHandler (new: `internal/engine/sqlite/proxy/txn.go`)
- Added transaction coordination for SQLite connections (BEGIN/COMMIT/ROLLBACK)
- Staged delta/tombstone pattern: writes inside a transaction are staged, promoted on COMMIT, discarded on ROLLBACK
- BEGIN executes on both prod and shadow databases
- SAVEPOINT/RELEASE forwarded to both databases

### ExtHandler (new: `internal/engine/sqlite/proxy/ext_handler.go`)
- Implemented extended query protocol (Parse/Bind/Describe/Execute/Sync) for SQLite
- Previously returned "extended query protocol not supported" error; now fully handled
- Statement cache persists across batches (populated on Parse, evicted on Close)
- Parameters resolved from Bind and substituted into SQL for classification and execution
- All routing strategies supported: merged reads, shadow writes, hydrate-and-write, shadow delete, DDL, transactions

### DDL RENAME COLUMN (updated: `internal/engine/sqlite/proxy/conn.go`)
- Extended DDL tracking to support `ALTER TABLE ... RENAME COLUMN` (SQLite 3.25+)
- Schema registry records column renames for merged read column mapping
- New parser: `parseAlterRenameColumn()` extracts table, old name, new name

### FK Stripping (updated: `internal/engine/sqlite/proxy/proxy.go`)
- Changed shadow connection from `PRAGMA foreign_keys=ON` to `PRAGMA foreign_keys=OFF`
- Prevents foreign key constraint violations when hydrating individual rows from prod to shadow
- Matches the pattern used by other engines (Postgres uses `session_replication_role = replica`)

### Extended Protocol Message Helpers (updated: `internal/engine/sqlite/proxy/pgmsg.go`)
- Added `buildParseCompleteMsg()`, `buildBindCompleteMsg()`, `buildNoDataMsg()`
- Added `isExtendedProtocolMsg()` for message type detection
- Added `parseParseMsgPayload()`, `parseBindMsgPayload()`, `parseCloseMsgPayload()` for protocol parsing
- Added `reconstructSQL()` for parameter substitution in prepared statements

### Write Effects with Transaction Awareness (updated: `internal/engine/sqlite/proxy/conn.go`)
- `trackWriteEffects()` now accepts optional `TxnHandler` parameter
- When inside a transaction, deltas and tombstones are staged instead of immediately committed
- Persistence is deferred until transaction commit

### Doc implications
- SQLite now supports all core proxy features: merged reads, write hydration, DDL tracking, transactions, extended query protocol
- Feature matrix should show SQLite at parity with PostgreSQL for core operations
- Extended query protocol (used by default in pgx, node-postgres, etc.) now works with SQLite

---

## Firestore Engine Parity

### 6a. SDK-Based Architecture (replaces raw gRPC byte proxy)
- **Before**: `rawCodec` opaque gRPC forwarding — all requests/responses treated as opaque byte frames, no content inspection
- **After**: SDK-based clients (`cloud.google.com/go/firestore/apiv1`) for both Prod and Shadow, with `firestorepb` protobuf deserialization
- Prod SDK client: connects with real credentials (or ADC) to `firestore.googleapis.com:443`
- Shadow SDK client: connects to local emulator via `FIRESTORE_EMULATOR_HOST`
- Raw gRPC forwarding retained as fallback for unrecognized methods or when SDK init fails
- New file: `internal/engine/firestore/proxy/sdk.go` — SDK client lifecycle management
- Files modified: `internal/engine/firestore/proxy/proxy.go` (added `sdk *sdkClients` field), `internal/engine/firestore/proxy/handler.go` (SDK-aware routing)

### 6b. Document-Level Merged Reads
- **GetDocument**: checks delta map first — if doc path is modified, reads from Shadow; if tombstoned, returns NOT_FOUND; otherwise reads from Prod
- **ListDocuments**: queries both backends via SDK iterators, merges by document `Name` (path), Shadow wins on conflicts, tombstoned docs filtered out
- **RunQuery**: queries both backends via SDK streaming, merges results by document path, Shadow wins, tombstoned docs removed
- Dedup by document path (Firestore equivalent of PK dedup in SQL engines)
- New file: `internal/engine/firestore/proxy/read.go` — merged read handler with `readHandler` struct
- New file: `internal/engine/firestore/proxy/read_test.go` — tests for merge logic, path parsing, tombstone filtering

### 6c. Delta/Tombstone Tracking
- **CreateDocument**: adds doc path to delta map + marks collection as having inserts
- **UpdateDocument**: adds doc path to delta map
- **DeleteDocument**: adds doc path to tombstone set, removes from delta map
- **Commit/BatchWrite**: iterates all `Write` protos, tracks Update/Delete/Transform operations
- Persists to `.mori/delta.json` and `.mori/tombstones.json` after each mutation
- New file: `internal/engine/firestore/proxy/write.go` — write handler with `writeHandler` struct
- New file: `internal/engine/firestore/proxy/write_test.go` — tests for delta/tombstone tracking

### 6d. Seed Cap Removed
- **Before**: hardcoded 100-document limit per collection during `mori init` seeding
- **After**: `DefaultSeedLimit = 0` (unlimited) — all documents in each collection are seeded by default
- `SeedShadow` now accepts `maxDocs <= 0` to mean "no limit" (omits `.Limit()` on query)
- Configurable: callers can pass any positive integer to limit seeding
- Files: `internal/engine/firestore/schema/schema.go` (new `DefaultSeedLimit` const, updated `seedCollection`), `internal/engine/firestore/init.go` (uses `DefaultSeedLimit`)

**Doc implications**:
- Firestore feature matrix should show: SDK-based merged reads, delta/tombstone tracking, write guard (3 layers preserved)
- Firestore no longer has 100-doc seed limit — seeds all documents by default
- Architecture: Firestore proxy now uses `cloud.google.com/go/firestore/apiv1` SDK alongside raw gRPC forwarding
- New routing targets: `targetMerged` (SDK merged reads) and `targetSDKWrite` (SDK writes with delta tracking)

---

## MSSQL Engine Parity

### 3a. TxnHandler (new: `internal/engine/mssql/proxy/txn.go`)
- Added transaction coordination for MSSQL TDS connections (BEGIN TRAN/COMMIT/ROLLBACK)
- Staged delta/tombstone pattern: writes inside a transaction are staged, promoted on COMMIT, discarded on ROLLBACK
- BEGIN TRAN: sends to Shadow first, then Prod; sets inTxn flag
- COMMIT: sends to both; promotes staged deltas on success, discards on failure
- ROLLBACK: sends to both; discards staged deltas
- Error detection via TDS token scanning (token 0xAA = error)
- Wired into `conn.go` routeLoop: dispatches StrategyTransaction before WriteHandler
- New test file: `internal/engine/mssql/proxy/txn_test.go`

### 3b. ExtHandler — TDS RPC (new: `internal/engine/mssql/proxy/ext_handler.go`)
- Handles `sp_executesql`, `sp_prepare`, `sp_execute`, `sp_unprepare` via TDS RPC packets
- Parses TDS RPC payloads: supports both well-known procedure IDs (uint16) and named procedures (UTF-16LE)
- Well-known proc IDs: 10=sp_executesql, 11=sp_prepare, 12=sp_execute, 15=sp_unprepare
- `sp_executesql`: extracts SQL text from first NVARCHAR param, classifies, routes through full pipeline
- `sp_prepare`: forwards to Prod, extracts int32 handle from RETURNVALUE token, caches SQL for later sp_execute calls
- `sp_execute`: looks up cached SQL by handle, classifies, routes accordingly
- `sp_unprepare`: evicts handle from cache
- Supports PLP (Partially Length-Prefixed) encoding for NVARCHAR(MAX) parameters
- Wired into `conn.go`: ExtHandler tried first for RPC packets; unrecognized RPCs pass through to Prod
- New test file: `internal/engine/mssql/proxy/ext_handler_test.go`

### 3c. Classifier Upgrade (updated: `internal/engine/mssql/classify/classifier.go`)
- Added window function detection: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, CUME_DIST, PERCENT_RANK
- Added PIVOT/UNPIVOT detection (T-SQL-specific)
- Added CROSS APPLY / OUTER APPLY detection (T-SQL-specific)
- Added GRANT/REVOKE/DENY as OpOther (permission statements)
- Added IF conditional statement handling — classifies body of IF block to determine routing
- Added additional aggregate functions: STDEV, STDEVP, VAR, VARP, COUNT_BIG
- Note: bytebase/tsql-parser (ANTLR4) was archived Nov 2025; enhanced existing regex approach instead
- New tests: `TestClassify_WindowFunctions`, `TestClassify_Pivot`, `TestClassify_CrossApply`, `TestClassify_PermissionStatements`, `TestClassify_ConditionalStatements`, `TestClassify_SchemaQualifiedTables`, `TestClassify_AdditionalAggregates`, `TestClassify_MergeTableExtraction`

### 3d. Version Matching (updated: `internal/engine/mssql/init.go`)
- `SELECT @@VERSION` output is now parsed to select matching Docker image tag
- Maps version strings to Docker images: 2017 -> `mcr.microsoft.com/mssql/server:2017-latest`, 2019 -> `2019-latest`, 2022 -> `2022-latest`
- Falls back to `2022-latest` for unknown versions
- Replaced hardcoded image with version-matched image in Init sequence
- New test file: `internal/engine/mssql/init_test.go` (TestVersionToImage, TestExtractShortVersion)

### Files Created
- `internal/engine/mssql/proxy/txn.go` — TxnHandler for transaction coordination
- `internal/engine/mssql/proxy/txn_test.go` — TxnHandler tests
- `internal/engine/mssql/proxy/ext_handler.go` — ExtHandler for TDS RPC packets
- `internal/engine/mssql/proxy/ext_handler_test.go` — ExtHandler tests
- `internal/engine/mssql/init_test.go` — Version matching tests

### Files Modified
- `internal/engine/mssql/proxy/conn.go` — Added TxnHandler + ExtHandler creation and dispatch
- `internal/engine/mssql/classify/classifier.go` — Enhanced T-SQL classification
- `internal/engine/mssql/classify/classifier_test.go` — Added comprehensive T-SQL-specific tests
- `internal/engine/mssql/init.go` — Added versionToImage(), replaced hardcoded image

**Doc implications**:
- MSSQL feature matrix should show: TxnHandler (transaction coordination with staged deltas), ExtHandler (sp_executesql/sp_prepare/sp_execute), enhanced classifier (window functions, PIVOT, APPLY, conditionals), version-matched shadow images
- MSSQL engine is now at near-parity with PostgreSQL for core proxy operations
- TDS RPC support means ORM-generated queries (Entity Framework, Dapper, etc.) using sp_executesql are properly classified and routed

---

## MySQL/MariaDB Engine Parity

### 2a. TxnHandler (new: `internal/engine/mysql/proxy/txn.go`)
- Added transaction coordination for MySQL wire protocol connections (BEGIN/START TRANSACTION, COMMIT, ROLLBACK)
- Staged delta/tombstone pattern: writes inside a transaction are staged, promoted on COMMIT, discarded on ROLLBACK
- BEGIN: sends to Shadow first (drain OK), then Prod (relay to client); sets inTxn flag
- COMMIT: sends to both; promotes staged deltas on success, discards on failure
- ROLLBACK: sends to both; discards staged deltas
- MySQL-specific: drains OK/ERR/result-set responses with `drainWithErrorCheck()`
- Wired into `conn.go` routeLoop: dispatches StrategyTransaction to TxnHandler before WriteHandler

### 2b. ExtHandler (new: `internal/engine/mysql/proxy/ext_handler.go`)
- Handles COM_STMT_PREPARE (0x16), COM_STMT_EXECUTE (0x17), COM_STMT_CLOSE (0x19)
- COM_STMT_PREPARE: forwards to Prod, captures 4-byte stmtID from PREPARE_OK response, caches stmtID -> SQL mapping
- COM_STMT_EXECUTE: looks up cached SQL by stmtID, parses binary protocol parameters, classifies, routes through full pipeline
- COM_STMT_CLOSE: evicts from cache, forwards to Prod
- Binary protocol parameter parsing: TINY, SHORT, LONG, LONGLONG, FLOAT, DOUBLE, STRING types
- SQL reconstruction: substitutes `?` placeholders with parsed parameter values for COM_QUERY-based routing
- All routing strategies supported: merged reads, shadow writes, hydrate-and-write, shadow delete, DDL, transactions, JOIN patch
- Wired into `conn.go`: ExtHandler tried first for COM_STMT_* packets

### 2c. Classifier Upgrade (updated: `internal/engine/mysql/classify/classifier.go`)
- Replaced pure-regex classifier with AST-based classification using `vitess.io/vitess/go/vt/sqlparser`
- Primary classification via `parser.Parse()` -> type-switch on AST node types
- Regex fallback: if vitess cannot parse the SQL, falls back to the original regex patterns
- AST handles: Select, Union, Insert, Update, Delete, CreateTable, AlterTable, DropTable, TruncateTable, Begin, Commit, Rollback, Set, Show, Use, Explain, CreateDatabase, DropDatabase
- JOINs detected from AST JoinTableExpr nodes
- CTEs detected from With clause
- Subqueries detected from Subquery expressions
- Aggregates detected from FuncExpr with aggregate function names
- PKs extracted from WHERE clause binary comparison expressions
- CREATE INDEX: vitess parses as AlterTable with AddIndexDefinition; special case detects raw SQL starting with "CREATE" to classify as SubCreate
- New dependency: `vitess.io/vitess v0.23.2` added to go.mod

### 2d. Version Matching (updated: `internal/engine/mysql/init.go`)
- `SELECT VERSION()` output is now parsed to select matching Docker image tag
- MySQL: "8.0.35-0ubuntu0.22.04.1" -> `mysql:8.0`; "5.7.44" -> `mysql:5.7`
- MariaDB: "10.11.6-MariaDB-1:10.11.6+maria~ubu2204" -> `mariadb:10.11`
- Falls back to `mysql:8.0` or `mariadb:11` for unparseable versions
- MariaDB detection: checks version string for "mariadb" OR engine name override
- Helpers: `matchDockerImage()`, `parseMajorMinor()`, `parseDigits()`
- Updated `mariadb.go`: removed hardcoded `Image: "mariadb:11"` to use version detection

### 2e/2f. Docs Updates
- `docs/testing.md`: Updated MariaDB Docker image entry from hardcoded `mariadb:11` to note auto-detection from `SELECT VERSION()`
- No incorrect claims about MySQL merged reads were found in existing docs (the suspected claim did not exist)
- Mintlify feature matrix (`docs/mintlify/engines/overview.mdx`) only shows status columns, no feature-level breakdown to update

### Files Created
- `internal/engine/mysql/proxy/txn.go` -- TxnHandler for transaction coordination
- `internal/engine/mysql/proxy/ext_handler.go` -- ExtHandler for MySQL binary protocol prepared statements

### Files Modified
- `internal/engine/mysql/proxy/conn.go` -- Added TxnHandler + ExtHandler creation and dispatch, transaction-aware write handling
- `internal/engine/mysql/classify/classifier.go` -- Complete rewrite: vitess AST + regex fallback
- `internal/engine/mysql/init.go` -- Added version matching (matchDockerImage, parseMajorMinor, parseDigits)
- `internal/engine/mysql/mariadb.go` -- Removed hardcoded image override, uses version detection
- `docs/testing.md` -- Updated MariaDB Docker image note
- `go.mod` / `go.sum` -- Added vitess.io/vitess v0.23.2 dependency

### Test Results
- classify: 12 tests PASS
- connstr: 9 tests PASS
- proxy: 19 tests PASS
- schema: 6 tests PASS
- shadow: 2 tests PASS

**Doc implications**:
- MySQL/MariaDB feature matrix should show: TxnHandler (transaction coordination with staged deltas), ExtHandler (COM_STMT_PREPARE/EXECUTE/CLOSE), AST-based classifier (vitess sqlparser with regex fallback), version-matched shadow images
- MySQL/MariaDB engine is now at near-parity with PostgreSQL for core proxy operations
- MySQL binary protocol support means ORM-generated queries (JDBC, Go database/sql, PHP PDO, etc.) using prepared statements are properly classified and routed
- MariaDB shadow container image is now version-matched rather than hardcoded to `mariadb:11`

---

## MCP Multi-Engine Support

### Architecture Refactor (`internal/mcp/server.go`)
- **Before**: MCP server was hardcoded to PostgreSQL (pgx only), with a single `db_query` tool
- **After**: Engine-aware MCP server that registers appropriate tools based on `cfg.Engine`
- New `EngineConfig` struct replaces individual connection parameters
- `registerTools()` dispatches to engine-specific tool registration
- Three tool families: SQL engines (db_query), Redis (redis_command/get/hgetall/keys), Firestore (firestore_get/list/query)

### SQL Engines — pgwire (`internal/mcp/pgwire.go`)
- Handles: PostgreSQL, CockroachDB, SQLite, DuckDB (all speak pgwire)
- Uses `pgx` driver (existing behavior preserved)
- Extracted shared helpers: `isReadQuery()`, `mcpError()`, `mcpOK()`, `formatRows()`, `getOptionalInt()`

### SQL Engines — MySQL wire (`internal/mcp/mysql.go`)
- Handles: MySQL, MariaDB
- Uses `database/sql` with `go-sql-driver/mysql` driver
- Connects to proxy at `127.0.0.1:<proxyPort>` using MySQL DSN format
- Same `db_query` tool interface as pgwire engines

### SQL Engines — TDS (`internal/mcp/mssql.go`)
- Handles: MSSQL
- Uses `database/sql` with `microsoft/go-mssqldb` driver
- Connects to proxy at `127.0.0.1:<proxyPort>` using sqlserver:// URL format
- Same `db_query` tool interface as pgwire engines

### Shared SQL helpers (`internal/mcp/sql.go`)
- `execSQLQuery()`: query execution via `database/sql` with column scanning and []byte→string conversion
- `execSQLExec()`: mutation execution via `database/sql` with rows-affected reporting

### Redis (`internal/mcp/redis.go`)
- Tools: `redis_command`, `redis_get`, `redis_hgetall`, `redis_keys`
- Raw RESP protocol client (no new dependency)
- Full RESP protocol implementation: encode commands, parse responses (strings, integers, bulk strings, arrays, errors)
- Hash-like responses (even-length arrays) auto-formatted as JSON objects
- Quote-aware command parser for `redis_command`
- AUTH support when password is configured

### Firestore (`internal/mcp/firestore.go`)
- Tools: `firestore_get`, `firestore_list`, `firestore_query`
- Uses high-level `cloud.google.com/go/firestore` SDK
- Connects to proxy as if it were a Firestore emulator (gRPC, no auth, insecure)
- `firestore_get`: retrieve single document by collection + doc ID
- `firestore_list`: list documents with configurable limit (default 25, max 100)
- `firestore_query`: query with field filter (supports ==, !=, <, <=, >, >=, in, array-contains)
- Auto-detects value types (bool, int, float, string) for query filters

### CLI Update (`cmd/mori/start.go`)
- Updated `morimcp.New()` call to pass `EngineConfig` with engine type from `cfg.Engine`
- Engine type flows from mori.yaml → config.json → MCP server

### Tests (`internal/mcp/server_test.go`)
- 9 unit tests: isReadQuery, parseRedisCommand, encodeRESP, readRESPValue (6 subtests), RESP errors, autoDetectValue, formatRESPValue hash, engine dispatch

**Doc implications**:
- MCP server section should document all three tool families (SQL, Redis, Firestore)
- Redis MCP tools: redis_command (free-form), redis_get, redis_hgetall, redis_keys
- Firestore MCP tools: firestore_get, firestore_list, firestore_query
- SQL engines all share the same db_query tool — no change for existing Postgres/CockroachDB users
- `--mcp` flag works with all 8 supported engines, not just PostgreSQL

---

## DuckDB: New Engine

### Overview
- **New engine**: DuckDB added as a fully supported embedded OLAP database engine
- Reuses ~90% of the SQLite proxy handler patterns (both are embedded, file-based, use `database/sql`)
- Registered as Tier 2 SQL/EMBEDDED engine (same tier as SQLite)
- DuckDB SQL is ~90% PostgreSQL compatible, so pgwire clients (pgx, node-postgres, etc.) work seamlessly

### Engine Scaffold
- New `DuckDB` EngineID constant added to `internal/registry/engine.go`
- Engine entry: `{DuckDB, "DuckDB", EngineTierT2, "SQL/EMBEDDED", 0, true}`
- Implements full `engine.Engine` interface: ID, Init, ParseConnStr, LoadTableMeta, NewClassifier, NewProxy
- Side-effect import registered in `cmd/mori/start.go`
- New dependency: `github.com/marcboeker/go-duckdb` (CGo-based DuckDB driver)

### Connection String Parser (`internal/engine/duckdb/connstr/`)
- Supports plain file paths: `/path/to/db.duckdb`, `./data.duckdb`
- Supports URI format: `duckdb:///path/to/db.duckdb`
- Supports in-memory: `:memory:` (for testing)
- Query parameters parsed (e.g., `?threads=4`)
- `ReadOnlyDSN()` uses `access_mode=READ_ONLY` (DuckDB-specific)

### Shadow Management (`internal/engine/duckdb/shadow/`)
- File copy pattern (same as SQLite): copies prod `.duckdb` file to `.mori/shadow.duckdb`
- Cleans up `.wal` file on shadow removal (DuckDB WAL vs SQLite's WAL/SHM/journal)

### Schema Detection (`internal/engine/duckdb/schema/`)
- Uses `INFORMATION_SCHEMA` (vs SQLite's `PRAGMA table_info`)
- Queries `information_schema.tables` for table discovery (schema='main', type='BASE TABLE')
- Queries `information_schema.table_constraints` + `key_column_usage` + `columns` for PK detection
- PK type classification: INTEGER->serial, BIGINT->bigserial, UUID->uuid, VARCHAR/TEXT->uuid
- Sequence offset detection via MAX(pk) queries
- Offset application via `CREATE SEQUENCE` (DuckDB-specific)

### Classifier (`internal/engine/duckdb/classify/`)
- Regex-based SQL classifier adapted from SQLite with DuckDB extensions
- Handles DuckDB-specific statements as OpOther: DESCRIBE, SHOW, PRAGMA, INSTALL, LOAD, COPY, EXPORT, IMPORT, SET, RESET, CALL
- Handles DuckDB-specific DDL: DROP SEQUENCE/MACRO/TYPE, CREATE OR REPLACE TABLE
- Handles DuckDB-specific joins: FULL JOIN
- Handles DuckDB-specific aggregates: LIST, ARRAY_AGG
- Handles DuckDB-specific clauses: QUALIFY, WINDOW, USING, RETURNING
- Supports PG-style `$N` parameter placeholders (DuckDB is PG-compatible)

### Proxy (`internal/engine/duckdb/proxy/`)
- Full pgwire proxy (same architecture as SQLite proxy)
- Prod opened with `?access_mode=READ_ONLY` (DuckDB-specific read-only mode)
- No PRAGMA setup needed (unlike SQLite's WAL/FK pragmas)
- All handlers ported from SQLite:
  - **ReadHandler**: Merged reads (shadow + prod, delta/tombstone filtering, PK dedup, re-sort, LIMIT overfetch)
  - **WriteHandler**: Shadow writes, hydrate-and-write (INSERT OR REPLACE for hydration), delta/tombstone tracking
  - **DDLHandler**: Schema registry updates (CREATE TABLE, DROP TABLE, ALTER TABLE ADD/RENAME COLUMN)
  - **TxnHandler**: Transaction coordination (BEGIN/COMMIT/ROLLBACK with staged deltas)
  - **ExtHandler**: Extended query protocol (Parse/Bind/Describe/Execute/Sync)
- Write guard (3 layers): L1 routing validation, L2 connection-level, L3 final prod dispatch check

### MCP Support
- Works automatically via pgwire — DuckDB accepts PostgreSQL wire protocol, so the existing pgwire MCP tools (`db_query`) work without changes

### Files Created
- `internal/engine/duckdb/engine.go` — Engine interface implementation + registration
- `internal/engine/duckdb/init.go` — Init sequence (parse connstr, validate, open, detect version, create shadow, detect tables, apply offsets, persist)
- `internal/engine/duckdb/connstr/connstr.go` — Connection string parser
- `internal/engine/duckdb/connstr/connstr_test.go` — 12 tests
- `internal/engine/duckdb/shadow/shadow.go` — Shadow file management
- `internal/engine/duckdb/schema/schema.go` — Schema detection via INFORMATION_SCHEMA
- `internal/engine/duckdb/schema/persist.go` — TableMeta persistence
- `internal/engine/duckdb/classify/classifier.go` — DuckDB SQL classifier
- `internal/engine/duckdb/classify/classifier_test.go` — 23 tests
- `internal/engine/duckdb/proxy/proxy.go` — Proxy struct and ListenAndServe
- `internal/engine/duckdb/proxy/conn.go` — Route loop, merged reads, write tracking, DDL tracking
- `internal/engine/duckdb/proxy/txn.go` — TxnHandler
- `internal/engine/duckdb/proxy/ext_handler.go` — ExtHandler
- `internal/engine/duckdb/proxy/pgmsg.go` — pgwire protocol helpers
- `internal/engine/duckdb/proxy/startup.go` — pgwire startup handshake
- `internal/engine/duckdb/proxy/guard.go` — Write guard
- `internal/engine/duckdb/proxy/txn_test.go` — 12 tests

### Files Modified
- `internal/registry/engine.go` — Added DuckDB EngineID constant and engines array entry
- `cmd/mori/start.go` — Added DuckDB side-effect import
- `go.mod` / `go.sum` — Added `github.com/marcboeker/go-duckdb` dependency

### Test Results
- classify: 23 tests PASS
- connstr: 12 tests PASS
- proxy: 12 tests PASS
- Total: 47 tests PASS

**Doc implications**:
- Engine count increased from 8 to 9 supported engines
- DuckDB should appear in all engine matrices as Tier 2 SQL/EMBEDDED
- Feature parity with SQLite: merged reads, write hydration, DDL tracking, transactions, extended query protocol
- Connection format: file path or `duckdb:///path/to/db.duckdb`
- MCP: `db_query` tool works via pgwire (same as PostgreSQL)
- Use case: OLAP workloads, analytics databases, Parquet/CSV queries

---

## Docs Update Complete

All changes above have been applied to documentation files. The following files were updated:

- `docs/mintlify/engines/overview.mdx` — Added DuckDB to engine matrix and connection fields
- `docs/mintlify/engines/providers.mdx` — No changes needed (Oracle was not listed)
- `docs/mintlify/roadmap.mdx` — Updated engine count to 9, added DuckDB, updated feature descriptions
- `docs/mintlify/concepts/transactions.mdx` — Documented transaction support across all SQL engines with engine-specific details
- `docs/mintlify/integrations/mcp-server.mdx` — Documented multi-engine MCP tools (SQL db_query, Redis tools, Firestore tools)
- `docs/mintlify/cli/reference.mdx` — Added `--image` flag documentation for `mori init` with PostgreSQL extension auto-install context
- `docs/testing.md` — Added DuckDB testing section (47 unit tests, E2E pending)
- `docs/mori.md` — Updated engine matrix, connection fields, project structure, CLI reference, MCP section, current state (v1), prerequisites
- `docs/SKILL.md` — Updated engine support matrix (added DuckDB), prerequisites, CLI flags (`--image`), MCP tools documentation
- `docs/mintlify/quickstart.mdx` — Updated prerequisites to include DuckDB as Docker-free engine

Changes applied: 2026-02-15
