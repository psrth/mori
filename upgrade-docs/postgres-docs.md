# PostgreSQL Engine — Exhaustive Feature Documentation

This document provides a comprehensive reference of all features, strategies, and implementation details in the PostgreSQL engine. It serves as the baseline for identifying gaps in other engine implementations.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [SQL Classification](#2-sql-classification)
3. [Routing Strategies](#3-routing-strategies)
4. [Read Operations](#4-read-operations)
5. [Write Operations](#5-write-operations)
6. [DDL & Schema Tracking](#6-ddl--schema-tracking)
7. [Transaction Management](#7-transaction-management)
8. [Foreign Key Enforcement](#8-foreign-key-enforcement)
9. [Extended Query Protocol](#9-extended-query-protocol)
10. [Guard Rails](#10-guard-rails)
11. [Schema Detection & Initialization](#11-schema-detection--initialization)
12. [Connection & Protocol Handling](#12-connection--protocol-handling)
13. [Cursor Operations](#13-cursor-operations)
14. [WHERE Clause Evaluation](#14-where-clause-evaluation)
15. [Production Query Rewriting](#15-production-query-rewriting)
16. [Utility & Materialization](#16-utility--materialization)
17. [Edge Cases & Special Handling](#17-edge-cases--special-handling)

---

## 1. Architecture Overview

The PostgreSQL engine implements a transparent proxy that sits between the application and the production database. It uses **copy-on-write** semantics:

- **Production (Prod)**: Read-only reference database, never mutated
- **Shadow**: Local Docker-based PostgreSQL clone (schema-only), receives all writes
- **Proxy**: Intercepts pgwire protocol, classifies queries, routes to appropriate backend(s), merges results

### Pipeline Flow

```
Client → Proxy → Classify(SQL) → Route(Classification) → Handler(Strategy) → [Prod | Shadow | Both] → Merge → Client
```

### State Tracking

Three persistent state structures track divergence:
- **Delta Map**: Tracks `(table, pk)` pairs modified in Shadow
- **Tombstone Set**: Tracks `(table, pk)` pairs deleted (logically removed from Prod)
- **Schema Registry**: Tracks DDL changes (added/dropped/renamed columns, new tables, type changes, FK metadata)

---

## 2. SQL Classification

### Classifier Implementation

- Uses `pg_query_go` (actual PostgreSQL parser internals) for AST-based parsing
- Implements `core.Classifier` interface with `Classify()` and `ClassifyWithParams()`
- Returns `core.Classification` with operation type, sub-type, tables, PKs, and feature flags

### Statement Types Classified

| Category | Statements | SubType |
|----------|-----------|---------|
| **Read** | SELECT | SubSelect |
| **Write** | INSERT, UPDATE, DELETE, TRUNCATE | SubInsert, SubUpdate, SubDelete, SubTruncate |
| **DDL** | CREATE TABLE/INDEX, ALTER TABLE, DROP TABLE, RENAME | SubCreate, SubAlter, SubDrop |
| **Transaction** | BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE | SubBegin, SubCommit, SubRollback, SubSavepoint, SubRelease |
| **Cursor** | DECLARE, FETCH, CLOSE | SubCursor |
| **Session** | SET, SHOW | SubSet, SubShow |
| **Explain** | EXPLAIN (rejects ANALYZE) | SubExplain |
| **Prepared** | PREPARE, EXECUTE, DEALLOCATE | SubPrepare, SubExecute, SubDeallocate |
| **Listen** | LISTEN, UNLISTEN | SubListen |
| **Unsupported** | NOTIFY, COPY, LOCK TABLE, DO $$, CALL, EXPLAIN ANALYZE | SubNotSupported |

### Feature Flags Extracted

- `IsJoin`: Multiple tables in FROM/JOIN
- `HasLimit` / `Limit`: LIMIT clause presence and value
- `OrderBy`: Raw ORDER BY clause text
- `HasAggregate`: COUNT/SUM/AVG/MIN/MAX/GROUP BY
- `HasComplexAgg`: array_agg, json_agg, string_agg, jsonb_agg, json_object_agg, jsonb_object_agg
- `HasSetOp`: UNION/INTERSECT/EXCEPT
- `IsComplexRead`: CTEs, derived tables (subqueries in FROM)
- `HasReturning`: RETURNING clause on writes
- `HasOnConflict`: INSERT ... ON CONFLICT (upsert)
- `HasWindowFunc`: Window functions (FUNC OVER)
- `HasDistinct`: SELECT DISTINCT
- `HasCursor`: Cursor operations
- `IsMetadataQuery` / `IntrospectedTable`: information_schema/pg_catalog queries

### PK Extraction

- Walks WHERE clause AST for equality comparisons (`column = literal`)
- Handles both `column = value` and `value = column`
- Resolves unqualified columns to tables via PK metadata
- Unwraps TypeCast nodes (`'value'::uuid`)
- Handles parameterized queries (`$1`, `$2`) with param resolution in `ClassifyWithParams`
- Supports integer, string, float constants and ParamRef nodes

### AST Walking

- Recursively extracts tables from RangeVar, JoinExpr, RangeSubselect nodes
- Detects mutating CTEs (WITH ... INSERT/UPDATE/DELETE)
- Collects CTE names to avoid counting as real tables
- Extracts ORDER BY via regex from raw SQL (handles trailing LIMIT/OFFSET/FOR/FETCH)

---

## 3. Routing Strategies

### Strategy Definitions

| Strategy | Code | Description |
|----------|------|-------------|
| `StrategyProdDirect` | 0 | Forward to Prod unchanged, return result |
| `StrategyMergedRead` | 1 | Query both backends, filter tombstones/deltas, merge |
| `StrategyJoinPatch` | 2 | Execute JOIN on Prod, patch delta rows from Shadow |
| `StrategyShadowWrite` | 3 | Execute INSERT on Shadow only |
| `StrategyHydrateAndWrite` | 4 | Hydrate from Prod → Shadow, then write |
| `StrategyShadowDelete` | 5 | Delete from Shadow, add tombstone |
| `StrategyShadowDDL` | 6 | Execute DDL on Shadow, update schema registry |
| `StrategyTransaction` | 7 | Forward transaction control |
| `StrategyNotSupported` | 8 | Return error to client |
| `StrategyForwardBoth` | 9 | Forward to both backends (SET) |
| `StrategyTruncate` | 10 | Execute on Shadow, mark table fully shadowed |
| `StrategyListenOnly` | 11 | Forward LISTEN to Prod only |

### Routing Decision Matrix

| OpType | Condition | Strategy |
|--------|-----------|----------|
| READ | All tables clean (no deltas/tombstones/schema diffs) | PROD_DIRECT |
| READ | Affected + set operation | MERGED_READ |
| READ | Affected + complex read (CTEs, derived tables) | MERGED_READ |
| READ | Affected + aggregate/GROUP BY | MERGED_READ |
| READ | Affected + JOIN | JOIN_PATCH |
| READ | Affected (simple single-table) | MERGED_READ |
| WRITE (INSERT) | No ON CONFLICT | SHADOW_WRITE |
| WRITE (INSERT) | Has ON CONFLICT | HYDRATE_AND_WRITE |
| WRITE (UPDATE) | Always | HYDRATE_AND_WRITE |
| WRITE (DELETE) | Always | SHADOW_DELETE |
| WRITE (TRUNCATE) | Always | TRUNCATE |
| DDL | Always | SHADOW_DDL |
| TRANSACTION | Always | TRANSACTION |
| OTHER (SET) | Always | FORWARD_BOTH |
| OTHER (SHOW) | Always | PROD_DIRECT |
| OTHER (EXPLAIN) | Clean tables → PROD_DIRECT; affected → NOT_SUPPORTED |
| OTHER (CURSOR) | Always | MERGED_READ |
| OTHER (LISTEN) | Always | LISTEN_ONLY |
| OTHER (PREPARE) | Always | SHADOW_WRITE |
| OTHER (DEALLOCATE) | Always | FORWARD_BOTH |
| NOTIFY | Always | NOT_SUPPORTED |

---

## 4. Read Operations

### 4.1 Single-Table Merged Read (`read_single.go`)

**Pipeline:**
1. Query Shadow database
2. Query Prod database (with over-fetching for LIMIT handling)
3. Filter tombstoned rows from Prod results
4. Adapt schema (handle added/dropped/renamed columns)
5. Merge results (Shadow rows + filtered Prod rows)
6. Deduplicate by PK (Shadow wins on conflict)
7. Re-apply ORDER BY sorting
8. Apply LIMIT

**Key Features:**
- PK injection into SELECT for deduplication (removed from final output)
- CTID injection for PK-less tables (uses system column as identity)
- LIMIT over-fetch: Prod query adds `delta_count + tombstone_count` to LIMIT
- Schema adaptation: dropped cols removed, renamed cols reverted, added cols get NULL
- Column name replacement for renamed columns

### 4.2 Aggregate Queries (`aggregate.go`)

**Three Paths:**

1. **Simple Re-aggregatable** (COUNT, SUM, AVG, MIN, MAX with GROUP BY):
   - Builds row-level base query (strips aggregates, keeps GROUP BY columns)
   - Runs through merged read pipeline to get all rows
   - Materializes into temp table on Shadow
   - Re-aggregates in Go (in-memory grouping + function application)
   - Supports HAVING filter on re-aggregated results
   - Handles COUNT(DISTINCT col) specially

2. **Complex Aggregates** (array_agg, json_agg, string_agg, etc.):
   - Materializes base data into temp table
   - Re-executes original query against temp table on Shadow
   - Leverages PostgreSQL's native aggregate implementation

3. **Fallback**:
   - Prod-only if no schema diffs
   - Shadow-only if schema diffs exist

### 4.3 JOIN Handling (`read_join.go`)

**StrategyJoinPatch Algorithm:**
1. Identify which joined tables have deltas/tombstones
2. Execute JOIN on Prod
3. For each Prod row, classify: Clean (keep) / Delta (patch from Shadow) / Dead (tombstoned → discard)
4. Fetch Shadow data for delta rows
5. Patch Prod rows with Shadow values
6. Re-evaluate original WHERE clause on patched rows
7. Deduplicate by composite key (all joined table PKs)
8. Fall back to full-row dedup if PK extraction is ambiguous

**Schema Diff Handling for JOINs:**
- Materializes dirty tables into temp tables
- Uses scoped predicates for pushdown optimization (only materialize relevant rows)
- Rewrites JOIN to reference temp tables

### 4.4 Complex Queries (`read_complex.go`)

**CTE (WITH clause) Handling:**
- Non-recursive CTEs: Materializes dirty CTEs independently into temp tables
- Recursive CTEs: Materializes dirty base tables, rewrites query to reference temp tables
- Preserves CTE semantics through rewriting

**Derived Tables (subqueries in FROM):**
- Recursively rewrites subqueries, replacing dirty subquery results with temp tables

**LATERAL Joins:**
- Materializes referenced dirty base tables (not the LATERAL body itself)
- Preserves LATERAL semantics through table materialization

### 4.5 Set Operations (`read_setop.go`)

- Recursively decomposes UNION/INTERSECT/EXCEPT into leaf SELECT nodes
- Each leaf runs through merged read pipeline independently
- In-memory set operation application:
  - UNION: Combine + deduplicate
  - UNION ALL: Combine without dedup
  - INTERSECT: Keep rows in both sets
  - EXCEPT: Keep rows only in first set
- Count tracking for ALL variants

### 4.6 Window Functions (`read_window.go`)

- Cannot decompose (window semantics require complete dataset)
- Builds base SELECT (same FROM/WHERE, no window functions)
- Runs base through merged read pipeline
- Materializes result into temp table on Shadow
- Rewrites original query to read from temp table
- Executes rewritten query on Shadow (leverages PostgreSQL's native window implementation)
- Strips table qualifiers from column references in rewritten query

---

## 5. Write Operations

### 5.1 INSERT (`write_insert.go`)

**Simple INSERT (StrategyShadowWrite):**
- FK constraint enforcement before execution
- Executes INSERT on Shadow only
- Captures RETURNING clause to extract inserted PKs for precise delta tracking
- Falls back to insert count tracking when PKs can't be extracted (no PK, no RETURNING)
- Adds all inserted PKs to delta map

**Upsert INSERT ... ON CONFLICT (StrategyHydrateAndWrite):**
- Detects conflict key columns
- Checks Prod for rows matching conflict keys
- Hydrates matching Prod rows into Shadow before execution
- Executes upsert on Shadow
- Merges pre-hydrated PKs with RETURNING-extracted PKs
- Tracks all affected rows in delta map

### 5.2 UPDATE (`write_update.go`)

**Point Updates (PKs extractable from WHERE):**
- Extracts PKs from WHERE clause
- Hydrates missing rows from Prod to Shadow (skips rows already in delta)
- Executes UPDATE on Shadow
- Adds affected PKs to delta map

**Bulk Updates (PKs not extractable):**
- Queries Prod for all matching rows (with schema rewriting for Prod compatibility)
- Hydrates all matching rows into Shadow
- Rewrites UPDATE WHERE clause with concrete PK values
- Executes on Shadow
- Tracks all affected PKs in delta

**Cross-Table Updates (UPDATE ... FROM):**
- Extracts subquery predicates from FROM clause
- Detects correlated subqueries (falls back to full table hydration if correlated)
- Hydrates referenced tables

**Features:**
- WHERE clause evaluation on patched delta rows to filter invalid results
- Generated column filtering during hydration
- Schema rewriting for Prod compatibility on bulk queries
- Max rows cap on hydration

### 5.3 DELETE (`write_delete.go`)

**Point Deletes (PKs extractable):**
- Captures correct row count (including Prod-only rows)
- Tombstones matching PKs
- Executes DELETE on Shadow

**Bulk Deletes (PKs not extractable):**
- Pre-queries Prod for matching PKs
- Computes total count: Shadow actual deletes + Prod-only matches
- Tombstones all matching PKs

**Features:**
- RETURNING support: Hydrates from Prod for return data synthesis
- FK RESTRICT/NO ACTION enforcement before deletion (rejects if child rows exist)
- FK CASCADE enforcement after tombstoning (automatic child deletion/tombstoning)

### 5.4 Hydration System

Core mechanism for bringing Prod rows into Shadow before writes:

- Queries Prod for rows matching PK or WHERE criteria
- Filters out generated columns (GENERATED ALWAYS AS STORED)
- Constructs INSERT SQL for Shadow with proper NULL handling
- Skips rows already in delta map (already in Shadow)
- Respects max rows hydration cap
- Uses schema rewriting for Prod compatibility (handles renamed/dropped columns)
- Handles both simple protocol and extended protocol hydration paths

---

## 6. DDL & Schema Tracking

### DDL Execution (`ddl.go`)

**Strategy: StrategyShadowDDL**
1. Strips FK constraints from DDL before Shadow execution (proxy enforces FKs instead)
2. Extracts and stores FK metadata BEFORE stripping (for proxy FK enforcement)
3. Executes modified DDL on Shadow
4. Updates Schema Registry on success

### Schema Changes Tracked (`ddl_parse.go`)

| Change | Registry Method | Impact |
|--------|----------------|--------|
| ADD COLUMN | `RecordAddColumn(table, col)` | New column gets NULL in Prod reads |
| DROP COLUMN | `RecordDropColumn(table, name)` | Column removed from Prod reads |
| RENAME COLUMN | `RecordRenameColumn(table, old, new)` | Prod queries use old name |
| ALTER TYPE | `RecordTypeChange(table, col, old, new)` | Tracked for compatibility |
| CREATE TABLE | `RecordNewTable(table)` | Table is Shadow-only |
| DROP TABLE | `RemoveTable(table)` | Table removed from registry |

### Query Rewriting for Schema Diffs

- **Dropped columns**: Removed from SELECT, WHERE, ORDER BY, GROUP BY, HAVING
- **Renamed columns**: Reverted to Prod names for Prod queries; new names for Shadow
- **Added columns**: NULL values injected in Prod results to match Shadow schema

---

## 7. Transaction Management

### Transaction Lifecycle (`txn.go`)

**BEGIN:**
- Shadow receives original BEGIN
- Prod receives `BEGIN ISOLATION LEVEL REPEATABLE READ` (for merged read consistency)
- Schema registry snapshot taken for rollback capability

**COMMIT:**
- Both backends COMMIT
- Staged deltas promoted to committed (atomically)
- State persisted to disk

**ROLLBACK:**
- Both backends ROLLBACK
- Staged deltas discarded
- Schema registry restored to pre-BEGIN snapshot

### Savepoint System

- Stack-based snapshot management
- Each SAVEPOINT captures: delta map, tombstone set, insert counts, schema registry state
- RELEASE SAVEPOINT: Pop snapshot from stack (changes persist)
- ROLLBACK TO SAVEPOINT: Restore state from snapshot, keep savepoint on stack for re-use

### Staged Delta System

- In-transaction writes use `Stage()` instead of `Add()`
- Staged entries are visible to reads but not yet committed
- `Commit()` promotes staged → committed
- `Rollback()` discards staged entries

---

## 8. Foreign Key Enforcement

### FK Discovery (`fk.go`)

- FKs discovered from Prod schema during initialization
- Stored in Schema Registry as `ForeignKey` structs
- Additional FK discovery from DDL (new constraints added via ALTER TABLE)

### Parent Row Validation (INSERT/UPDATE)

Validation order (fast-path first):
1. **Delta map check**: If parent PK in delta → exists in Shadow ✓
2. **Tombstone check**: If parent PK tombstoned → REJECT ✗
3. **Shadow query**: Check Shadow for parent row
4. **Prod query**: Fall back to Prod if not found in Shadow
5. Supports multi-column (composite) FKs

### Referential Action Enforcement (DELETE)

- **RESTRICT / NO ACTION**: Rejects parent delete if child rows exist (checks Shadow + Prod)
- **CASCADE**: Tombstones child rows when parent deleted, recursively cascades
- **SET NULL / SET DEFAULT**: Not explicitly handled (falls through to Shadow enforcement)

### FK Caching

- Per-table FK discovery cached to avoid repeated information_schema queries
- Cache populated during initialization and DDL processing

---

## 9. Extended Query Protocol

### Protocol Handling (`extended.go`, `ext_handler.go`)

**Message Types:**
- Parse ('P'): SQL statement preparation
- Bind ('B'): Parameter binding
- Describe ('D'): Statement/portal description
- Execute ('E'): Statement execution
- Close ('C'): Statement/portal closing
- Sync ('S'): Batch completion and error barrier

### Parameter Handling

- Binary parameter decoding: bool (1 byte), int16 (2), int32 (4), int64 (8), UUID (16 bytes)
- Text parameter passthrough
- Format code resolution (per-parameter or global text/binary flag)
- Parameter substitution for classification (`$1` → actual value)

### Statement Cache

- Maps statement names to SQL text
- Persists across Sync boundaries
- Close('S') evicts from cache
- Tracks shadow-only statements (referencing shadow-only or schema-modified tables)

### Batch Processing

- Accumulates messages until Sync
- Parse-only batches: Forward to Shadow if shadow-only; otherwise Prod
- Execute triggers full classify → route → dispatch pipeline
- Flush on Sync with proper error handling

### Extended Protocol Variants for Each Strategy

- `InsertExt`: FK enforcement, tag capture, insert count tracking
- `UpdateExt`: FK enforcement, bulk hydration with full SQL reconstruction
- `DeleteExt`: FK enforcement, bulk PK discovery, tombstone management
- `MergedReadExt` / `JoinPatchExt`: Full pipeline with parameter substitution

---

## 10. Guard Rails

### Three-Layer Write Protection (`guard.go`)

| Layer | Name | Mechanism |
|-------|------|-----------|
| L1 | `validateRouteDecision` | Asserts write/DDL never gets StrategyProdDirect |
| L2 | `SafeProdConn` | Wraps Prod conn, inspects outgoing Query messages for write SQL |
| L3 | `classifyAndRoute` | Final assertion before Prod dispatch; logs CRITICAL and rejects |

### Write Detection (`looksLikeWrite`)

- Prefix matching: INSERT, UPDATE, DELETE, TRUNCATE, CREATE, ALTER, DROP, GRANT, REVOKE
- CTE writes: WITH ... INSERT/UPDATE/DELETE
- Safe prefixes: SELECT, SET, SHOW, EXPLAIN, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, DEALLOCATE, DECLARE, FETCH, CLOSE, LISTEN, UNLISTEN, DISCARD, RESET, NOTIFY

### Error Responses

- SQLSTATE `MR001` for Mori write guard errors
- `GuardErrorResponse` with ErrorResponse + ReadyForQuery messages

---

## 11. Schema Detection & Initialization

### Full Initialization Pipeline (`init.go`)

1. Parse connection string
2. Connect to Prod, detect PostgreSQL version
3. Resolve Docker image (auto-detect or override)
4. Create Shadow container via Docker
5. Full schema dump:
   a. Detect extensions (with retry on read-replica conflicts)
   b. Run pg_dump --schema-only (inside Docker for version parity)
   c. Strip FKs, psql meta, managed extensions
   d. Detect table metadata (PKs, types)
   e. Detect generated columns (pg_attribute.attgenerated)
   f. Detect sequence offsets (MAX(pk) calculation)
   g. Detect foreign keys (pg_catalog query)
6. Apply schema to Shadow (extensions + DDL + sequences)
7. Persist configuration and metadata

### Extension Handling

- Detects all extensions from `pg_extension`
- Filters managed/cloud-provider extensions (Google, AWS, Aiven, Azure, Supabase)
- Auto-installs missing extensions via `apt-get` inside container
- Maps extension names to PGDG packages (e.g., `vector` → `pgvector`)

### Sequence Offset Calculation

- Queries MAX(pk_column) per table for serial/bigserial PKs
- Calculates Shadow start: `max(prod_max * 10, prod_max + 10_000_000)`
- Prevents PK collisions between Prod and Shadow sequences
- Skips UUID, composite, or missing PKs

### Version Detection

- Parses `SHOW server_version` output
- Handles formats: "16.2", "16.2 (Debian ...)", "17beta1"
- Maps to Docker image tag (postgres:X.Y)

### Read-Replica Conflict Handling

- Detects SQLSTATE 40001 (recovery conflict)
- Retries with exponential backoff (200ms increments)
- Max 5 attempts
- Applied to: DetectExtensions, DetectTableMetadata, DetectGeneratedColumns, DetectSequenceOffsets, DetectForeignKeys

---

## 12. Connection & Protocol Handling

### pgwire Protocol (`pgwire.go`, `pgmsg.go`)

**Client → Proxy Messages:**
- Query ('Q'): Simple protocol SQL
- Parse ('P'), Bind ('B'), Describe ('D'), Execute ('E'), Close ('C'), Sync ('S'): Extended protocol
- Terminate ('X'): Connection close

**Proxy → Client Messages Built:**
- RowDescription ('T'): Column metadata with OID types
- DataRow ('D'): Column values with NULL indicators
- CommandComplete ('C'): Operation tag (INSERT 0 N, UPDATE N, DELETE N)
- ReadyForQuery ('Z'): Transaction state (I=idle, T=transaction, E=error)
- ErrorResponse ('E'): Error with SQLSTATE, message, detail, hint

### Connection Management (`conn.go`)

- Dual-backend connections (Prod + Shadow) per client
- SSL/TLS negotiation handled independently with Prod
- SASL PLUS mechanism stripping for plain TCP clients
- Startup handshake relay through Prod
- SQL-level PREPARE/EXECUTE/DEALLOCATE caching with parameter substitution
- TRUNCATE CASCADE tracking

### Result Set Materialization

- `execQuery()`: Sends query, collects response until ReadyForQuery
- Handles mid-stream errors with connection drain to prevent desync
- Parses RowDescription → DataRows → CommandComplete → ErrorResponse

---

## 13. Cursor Operations

### Cursor Handling (`cursor.go`)

- DECLARE CURSOR: Classifies inner query, materializes if affected tables
- FETCH: Reads from cursor on appropriate backend
- CLOSE: Closes cursor on both backends

---

## 14. WHERE Clause Evaluation

### Expression Evaluator (`where_eval.go`)

**Supported Operators:**
- Comparison: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- Pattern: `LIKE` (`~~`, `~~*`, `!~~`, `!~~*`)
- Range: `IN`, `BETWEEN`, `NOT BETWEEN`
- Null: `IS NULL`, `IS NOT NULL`
- Boolean: `AND`, `OR`, `NOT`

**Type Handling:**
- Boolean normalization: 't'/'f' vs 'true'/'false'
- Numeric comparison (parseFloat fallback)
- String comparison
- NULL handling: NULL comparisons → false (SQL three-valued logic)

**Usage:**
- Used for JOIN WHERE re-evaluation on patched rows
- Safe fallback: Returns true (keeps row) on evaluation failure

---

## 15. Production Query Rewriting

### Query Rewriting (`rewrite_prod.go`)

Rewrites SELECT queries sent to Prod to account for schema divergence:

- **Dropped columns**: Removes column references from SELECT, WHERE, ORDER BY, GROUP BY, HAVING
- **Renamed columns**: Reverts column names to Prod-side originals
- **Added columns**: Not present in Prod, so stripped from Prod query; NULL injected in merge

### Delta/Tombstone Filtering

- Optionally adds WHERE clauses to exclude delta and tombstoned PKs from Prod results
- Prevents duplicate data when merging

---

## 16. Utility & Materialization

### Temp Table Management (`util_table.go`)

- `utilTableName()`: Generates deterministic temp table name from SQL hash
- `buildCreateTempTableSQL()`: Creates temp table matching query result schema
- `bulkInsertToUtilTable()`: Batch inserts rows into temp table
- `dropUtilTable()`: Cleans up temporary tables after use

### Scoped Materialization

- `buildScopedMaterializationSQL()`: Pushes WHERE predicates into materialization query
- Only materializes rows relevant to the original query (optimization)

### SQL Construction Helpers

- `quoteIdent()`: Double-quote identifiers with escaping
- `quoteLiteral()`: Single-quote string values with escaping
- `buildInsertSQL()`: Constructs INSERT from rows/columns, skipping generated columns
- `capSQL()`: Applies row limit to materialization queries

---

## 17. Edge Cases & Special Handling

### Generated Columns
- Detected via `pg_attribute.attgenerated = 's'`
- Excluded from hydration INSERTs (PostgreSQL rejects explicit values)
- Graceful degradation on PostgreSQL < 12

### PK-less Tables
- Uses `ctid` (system column) as row identity for deduplication
- Falls back to insert count tracking instead of PK-level deltas

### Composite Primary Keys
- Serialized as JSON for delta map keys
- Multi-column FK validation with ordered column matching

### CockroachDB Support
- Thin adapter (`cockroach.go`) wrapping the PostgreSQL engine
- Reuses all pgwire protocol handling, classification, and proxy logic
- Single method override: `ID()` returns CockroachDB

### Read-Replica Resilience
- Automatic retry on recovery conflicts (SQLSTATE 40001)
- Exponential backoff: 200ms, 400ms, 600ms (max 5 retries)

### Docker Lifecycle
- Dynamic port allocation via OS
- Trust auth for local throwaway database
- Container cleanup on init failure
- 30-second startup timeout with 500ms polling
- Image pull only if not cached locally

### Managed Extension Filtering
- Filters cloud-provider internal extensions:
  - Google Cloud SQL: plpgsql, google_vacuum_mgmt, google_columnar_engine, google_db_advisor, google_ml_integration
  - AWS RDS: rds_tools, rdsutils
  - Aiven: aiven_extras
  - Azure: azure, citus_columnar
  - Supabase: supautils

### TRUNCATE Handling
- Marks table as `IsFullyShadowed` in schema registry
- All subsequent reads go to Shadow only (skip Prod)
- CASCADE tracking for related tables

### INSERT ... ON CONFLICT
- Detects conflict columns from ON CONFLICT clause
- Pre-hydrates potentially conflicting Prod rows
- Merges PKs from hydration + RETURNING

### Schema-Aware Protocol
- Extended protocol statements cached with shadow-only tracking
- Statements referencing schema-modified tables auto-routed to merged read
- Close('S') cleans up cache entries
