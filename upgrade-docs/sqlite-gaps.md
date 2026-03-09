# SQLite Engine — Comprehensive Gap Analysis vs. PostgreSQL Engine

This document provides an exhaustive comparison between the SQLite engine (`internal/engine/sqlite/`) and the PostgreSQL engine (the most mature implementation), identifying every missing, partial, or stubbed feature.

---

## Table of Contents

1. [Engine Overview](#1-engine-overview)
2. [Feature Parity Matrix](#2-feature-parity-matrix)
3. [Classification Gaps](#3-classification-gaps)
4. [Read Operation Gaps](#4-read-operation-gaps)
5. [Write Operation Gaps](#5-write-operation-gaps)
6. [DDL & Schema Tracking Gaps](#6-ddl--schema-tracking-gaps)
7. [Transaction Management Gaps](#7-transaction-management-gaps)
8. [Foreign Key Enforcement Gaps](#8-foreign-key-enforcement-gaps)
9. [Protocol-Level Gaps](#9-protocol-level-gaps)
10. [Prepared Statement Gaps](#10-prepared-statement-gaps)
11. [Guard Rail Gaps](#11-guard-rail-gaps)
12. [Initialization Gaps](#12-initialization-gaps)
13. [Priority Recommendations](#13-priority-recommendations)

---

## 1. Engine Overview

The SQLite engine is in an **early-but-functional** state. The core architecture is sound: it correctly implements the proxy-as-pgwire-server pattern (clients connect via PostgreSQL wire protocol), maintains dual database handles (prod read-only, shadow read-write), and integrates with the shared `core.Router`, `delta.Map`, `delta.TombstoneSet`, and `schema.Registry` abstractions.

**What works today:**
- Connection string parsing with multiple URI formats (`connstr/`)
- Full schema dump and table metadata detection via `PRAGMA table_info` (`schema/`)
- Shadow database creation via file copy (`shadow/`)
- Auto-increment offset detection and application (`schema/schema.go`)
- pgwire startup handshake, SSL rejection, simple and extended query protocol handling (`proxy/`)
- Regex-based SQL classifier with table extraction, PK extraction, feature flags (`classify/`)
- Single-table merged read with PK injection, dedup, ORDER BY re-sort, LIMIT overfetch (`proxy/conn.go`)
- Basic aggregate read for `COUNT(*)` without GROUP BY (`proxy/conn.go`)
- Basic hydration for point UPDATEs (`proxy/conn.go`)
- Delta/tombstone tracking with staging for transactions (`proxy/conn.go`, `proxy/txn.go`)
- DDL schema registry updates for CREATE TABLE, DROP TABLE, ADD COLUMN, RENAME COLUMN (`proxy/conn.go`)
- Write guard L1 (routing assertion) and L3 (pre-dispatch check) (`proxy/guard.go`, `proxy/conn.go`)
- Extended query protocol with Parse/Bind/Describe/Execute/Close/Sync handling (`proxy/ext_handler.go`)
- Transaction management with BEGIN/COMMIT/ROLLBACK and staged delta promotion (`proxy/txn.go`)

**What is absent or critically incomplete** is documented in the sections below.

---

## 2. Feature Parity Matrix

| Feature | PostgreSQL | SQLite | Status |
|---------|-----------|--------|--------|
| **Classification** | | | |
| AST-based parser (pg_query_go) | Yes | No (regex) | ⚠️ Partial |
| SELECT classification | Yes | Yes | ✅ Implemented |
| INSERT classification | Yes | Yes | ✅ Implemented |
| UPDATE classification | Yes | Yes | ✅ Implemented |
| DELETE classification | Yes | Yes | ✅ Implemented |
| TRUNCATE classification | Yes | No | ❌ Missing |
| DDL classification (CREATE/ALTER/DROP) | Yes | Yes | ✅ Implemented |
| REPLACE INTO classification | N/A | Yes | ✅ Implemented |
| Transaction classification (BEGIN/COMMIT/ROLLBACK) | Yes | Yes | ✅ Implemented |
| SAVEPOINT/RELEASE classification | Yes (SubSavepoint/SubRelease) | Partial (SubOther) | ⚠️ Partial |
| Cursor classification (DECLARE/FETCH/CLOSE) | Yes (SubCursor) | No | ❌ Missing |
| Session classification (SET/SHOW) | Yes (SubSet/SubShow) | No | ❌ Missing |
| EXPLAIN classification | Yes (SubExplain) | No (OpOther) | ⚠️ Partial |
| Prepared stmt classification (PREPARE/EXECUTE/DEALLOCATE) | Yes | No | ❌ Missing |
| LISTEN/UNLISTEN classification | Yes (SubListen) | No | ➖ N/A |
| HasReturning flag | Yes | No | ❌ Missing |
| HasOnConflict flag | Yes | No | ❌ Missing |
| HasWindowFunc flag | Yes | No | ❌ Missing |
| HasDistinct flag | Yes | No | ❌ Missing |
| HasCursor flag | Yes | No | ❌ Missing |
| IsMetadataQuery flag | Yes | No | ❌ Missing |
| HasComplexAgg flag | Yes | No | ❌ Missing |
| CTE write detection (mutating CTEs) | Yes (per-CTE AST walk) | Partial (substring match) | ⚠️ Partial |
| PK extraction from WHERE | Yes (AST walk) | Partial (regex) | ⚠️ Partial |
| TypeCast unwrapping in PK extraction | Yes | No | ❌ Missing |
| Parameterized PK extraction ($1 resolution) | Yes (ClassifyWithParams) | Yes (? resolution) | ✅ Implemented |
| **Read Operations** | | | |
| Single-table merged read | Yes | Yes | ✅ Implemented |
| PK injection for dedup | Yes | Yes | ✅ Implemented |
| CTID injection for PK-less tables | Yes | No | ❌ Missing |
| LIMIT overfetch | Yes | Yes | ✅ Implemented |
| ORDER BY re-sort | Yes | Yes | ✅ Implemented |
| Schema adaptation (added/dropped/renamed cols) | Yes | No | ❌ Missing |
| Aggregate queries (COUNT/SUM/AVG/MIN/MAX + GROUP BY) | Yes (full re-aggregation) | Partial (COUNT(*) only) | ⚠️ Partial |
| Complex aggregates (array_agg, json_agg, etc.) | Yes (materialization) | No | ❌ Missing |
| HAVING filter on re-aggregated results | Yes | No | ❌ Missing |
| JOIN patch (StrategyJoinPatch) | Yes (dedicated handler) | No (falls through to merged read) | ❌ Missing |
| Complex reads (CTEs, derived tables) | Yes (materialization + rewrite) | No | ❌ Missing |
| Set operations (UNION/INTERSECT/EXCEPT) | Yes (in-memory decomposition) | No | ❌ Missing |
| Window functions (materialization strategy) | Yes | No | ❌ Missing |
| Production query rewriting (schema diffs) | Yes (AST-level rewrite) | No | ❌ Missing |
| Temp table materialization framework | Yes (util_table.go) | No | ❌ Missing |
| WHERE clause evaluation on patched rows | Yes (where_eval.go) | No | ❌ Missing |
| Scoped materialization (predicate pushdown) | Yes | No | ❌ Missing |
| Cursor operations (DECLARE/FETCH/CLOSE) | Yes (cursor.go) | No | ❌ Missing |
| **Write Operations** | | | |
| Simple INSERT (shadow-only) | Yes | Yes | ✅ Implemented |
| INSERT with RETURNING (PK extraction) | Yes | No | ❌ Missing |
| INSERT ... ON CONFLICT (upsert with pre-hydration) | Yes | No | ❌ Missing |
| Point UPDATE (PK-based hydration) | Yes | Yes | ✅ Implemented |
| Bulk UPDATE (WHERE-based hydration) | Yes | No | ❌ Missing |
| Cross-table UPDATE (UPDATE ... FROM) | Yes | No | ❌ Missing |
| Point DELETE with tombstoning | Yes | Yes | ✅ Implemented |
| Bulk DELETE (pre-query PKs from Prod) | Yes | No | ❌ Missing |
| DELETE with RETURNING | Yes | No | ❌ Missing |
| TRUNCATE handling (IsFullyShadowed) | Yes | No | ❌ Missing |
| Generated column filtering during hydration | Yes | No | ❌ Missing |
| Schema rewriting for Prod compat on bulk queries | Yes | No | ❌ Missing |
| Correct row count for Prod-only deletes | Yes | No | ❌ Missing |
| Max rows hydration cap | Yes | No | ❌ Missing |
| **DDL & Schema Tracking** | | | |
| FK stripping from DDL before Shadow execution | Yes | No | ❌ Missing |
| FK metadata extraction from DDL | Yes | No | ❌ Missing |
| ADD COLUMN tracking | Yes | Yes | ✅ Implemented |
| DROP COLUMN tracking | Yes | No | ❌ Missing |
| RENAME COLUMN tracking | Yes | Yes | ✅ Implemented |
| ALTER TYPE tracking | Yes | No | ❌ Missing |
| CREATE TABLE tracking | Yes | Yes | ✅ Implemented |
| DROP TABLE tracking | Yes | Yes | ✅ Implemented |
| AST-based DDL parsing | Yes (pg_query) | No (string splitting) | ⚠️ Partial |
| Query rewriting for dropped columns | Yes | No | ❌ Missing |
| Query rewriting for renamed columns | Yes | No | ❌ Missing |
| NULL injection for added columns in Prod reads | Yes | No | ❌ Missing |
| **Transaction Management** | | | |
| BEGIN (dual-backend) | Yes | Yes | ✅ Implemented |
| COMMIT (dual-backend + delta promotion) | Yes | Yes | ✅ Implemented |
| ROLLBACK (dual-backend + delta discard) | Yes | Yes | ✅ Implemented |
| Prod BEGIN ISOLATION LEVEL REPEATABLE READ | Yes | No | ❌ Missing |
| Schema registry snapshot at BEGIN | Yes | No | ❌ Missing |
| Schema registry restore at ROLLBACK | Yes | No | ❌ Missing |
| SAVEPOINT (stack-based snapshots) | Yes | No (forwarded as-is) | ❌ Missing |
| RELEASE SAVEPOINT | Yes | No (forwarded as-is) | ❌ Missing |
| ROLLBACK TO SAVEPOINT | Yes | No (forwarded as-is) | ❌ Missing |
| Savepoint snapshot of delta/tombstone/insert/schema | Yes | No | ❌ Missing |
| **Foreign Key Enforcement** | | | |
| FK discovery from Prod schema | Yes | No | ❌ Missing |
| FK metadata stored in Schema Registry | Yes | No | ❌ Missing |
| Parent row validation (INSERT/UPDATE) | Yes | No | ❌ Missing |
| RESTRICT/NO ACTION enforcement (DELETE) | Yes | No | ❌ Missing |
| CASCADE enforcement (DELETE) | Yes | No | ❌ Missing |
| Multi-column (composite) FK validation | Yes | No | ❌ Missing |
| FK caching per table | Yes | No | ❌ Missing |
| FK extraction from DDL | Yes | No | ❌ Missing |
| **Protocol Handling** | | | |
| pgwire startup handshake | Yes (relay to Prod) | Yes (synthetic) | ✅ Implemented |
| SSL/TLS negotiation | Yes (relay to Prod) | Partial (rejects SSL) | ⚠️ Partial |
| Simple query protocol ('Q') | Yes | Yes | ✅ Implemented |
| Extended query protocol (Parse/Bind/Execute/Sync) | Yes | Yes | ✅ Implemented |
| Binary parameter decoding (bool, int16/32/64, UUID) | Yes | No (text only) | ⚠️ Partial |
| Statement cache with shadow-only tracking | Yes | Partial (no shadow tracking) | ⚠️ Partial |
| Describe response (RowDescription/ParameterDescription) | Yes | Partial (NoData) | ⚠️ Partial |
| Flush ('H') message handling | Yes | No | ❌ Missing |
| SASL/SCRAM auth stripping | Yes | N/A (no auth relay) | ➖ N/A |
| Connection drain on mid-stream errors | Yes | No | ❌ Missing |
| ReadyForQuery transaction state tracking (I/T/E) | Yes | No (always sends 'I') | ❌ Missing |
| **Guard Rails** | | | |
| L1: validateRouteDecision | Yes | Yes | ✅ Implemented |
| L2: SafeProdConn (inspect outgoing writes) | Yes | No | ❌ Missing |
| L3: classifyAndRoute final assertion | Yes | Yes | ✅ Implemented |
| SQLSTATE MR001 for guard errors | Yes | Yes | ✅ Implemented |
| looksLikeWrite heuristic | Yes | Yes | ✅ Implemented |
| **Initialization** | | | |
| Connection string parsing | Yes | Yes | ✅ Implemented |
| Version detection | Yes | Yes | ✅ Implemented |
| Schema dump | Yes (pg_dump) | Yes (sqlite_master) | ✅ Implemented |
| Table metadata detection | Yes | Yes | ✅ Implemented |
| Sequence/auto-increment offset calculation | Yes | Yes | ✅ Implemented |
| Shadow creation | Yes (Docker container) | Yes (file copy) | ✅ Implemented |
| Extension handling | Yes | N/A | ➖ N/A |
| Generated column detection | Yes | No | ❌ Missing |
| Foreign key detection | Yes | No | ❌ Missing |
| Read-replica conflict handling (retry) | Yes | N/A | ➖ N/A |

---

## 3. Classification Gaps

### 3.1 Regex-Based vs. AST-Based Parsing

The SQLite classifier (`classify/classifier.go`) uses regex-based parsing rather than an AST parser. While this avoids a dependency on a SQLite parser library, it creates several accuracy issues:

**Impact areas:**
- **Table extraction from complex queries** (lines 294-323): The regex `reFromClause` can misidentify tokens as table names in subqueries, CTEs, or function calls.
- **PK extraction** (lines 229-261): The regex pattern `(?i)\b%s\s*=\s*(?:'([^']*)'|(\d+)|(\?))` misses qualified column references (`t.id = 1`), expressions with casts, `IN (...)` lists, and compound WHERE clauses where the same column appears multiple times.
- **CTE write detection** (lines 212-227): Uses `strings.Contains(upper, "INSERT")` which would false-positive on CTEs that contain the word "INSERT" in string literals or column names.

### 3.2 Missing Statement Type Classifications

The following statement types classified in PostgreSQL are missing in SQLite:

| Statement | PostgreSQL SubType | SQLite Handling |
|-----------|-------------------|-----------------|
| TRUNCATE | SubTruncate | Not classified at all |
| SET | SubSet | Not classified |
| SHOW | SubShow | Not classified |
| EXPLAIN (without ANALYZE) | SubExplain | Classified as OpOther/SubOther instead of dedicated |
| PREPARE/EXECUTE/DEALLOCATE | SubPrepare/SubExecute/SubDeallocate | Not classified |
| DECLARE/FETCH/CLOSE | SubCursor | Not classified |
| LISTEN/UNLISTEN | SubListen | N/A for SQLite |

**File:** `classify/classifier.go`, lines 42-88 -- the switch statement only handles SELECT, INSERT, UPDATE, DELETE, REPLACE, CREATE, ALTER, DROP, BEGIN, COMMIT, ROLLBACK, SAVEPOINT/RELEASE, PRAGMA/EXPLAIN/ANALYZE/VACUUM/REINDEX/ATTACH/DETACH, and WITH.

### 3.3 Missing Feature Flags

The SQLite classifier sets none of the following flags that the PostgreSQL classifier extracts:

- `HasReturning` -- SQLite does support RETURNING (added in 3.35.0) but the classifier never sets this flag. **Impact:** INSERT/DELETE with RETURNING cannot leverage PK extraction from returned rows.
- `HasOnConflict` -- SQLite supports `INSERT ... ON CONFLICT` (3.24.0) but the flag is never set. **Impact:** Upsert operations are not routed to StrategyHydrateAndWrite.
- `HasWindowFunc` -- SQLite supports window functions (3.25.0) but the flag is never detected. **Impact:** Window function queries cannot be routed to the materialization strategy.
- `HasDistinct` -- Never set. **Impact:** SELECT DISTINCT handling may produce incorrect results if dedup logic doesn't account for it.
- `HasCursor` -- Not applicable (SQLite doesn't support cursors in the same way).
- `IsMetadataQuery` -- SQLite has `sqlite_master` and PRAGMA introspection tables, but these are never flagged. **Impact:** Metadata queries may be incorrectly routed through merged read.
- `HasComplexAgg` -- Never detected. In SQLite, `GROUP_CONCAT` is the equivalent of PostgreSQL's `string_agg`/`array_agg`.

### 3.4 SAVEPOINT SubType Mapping

SAVEPOINT and RELEASE are classified as `SubOther` (line 73) instead of the dedicated `SubSavepoint` and `SubRelease` subtypes. This prevents the transaction handler from differentiating them and implementing proper savepoint snapshot management.

**File:** `classify/classifier.go`, lines 70-72.

---

## 4. Read Operation Gaps

### 4.1 Schema Adaptation on Merged Reads (CRITICAL)

The PostgreSQL engine's `read_single.go` adapts Prod results when schema divergence exists:
- Dropped columns: removed from Prod result set
- Renamed columns: Prod results use old name, mapped to new name during merge
- Added columns: NULL values injected into Prod rows to match Shadow schema

**The SQLite engine has none of this.** If an `ALTER TABLE ADD COLUMN` has been executed, merged reads will fail or produce misaligned column sets because the Prod database will have fewer columns than Shadow.

**File:** `proxy/conn.go`, `executeMergedRead()` (lines 354-524) -- no schema adaptation logic.

### 4.2 JOIN Handling (CRITICAL)

The PostgreSQL engine has a dedicated `read_join.go` with the JoinPatch algorithm:
1. Identify delta tables in the JOIN
2. Execute JOIN on Prod
3. Classify each Prod row as clean/delta/dead
4. Patch delta rows with Shadow data
5. Re-evaluate WHERE on patched rows
6. Deduplicate by composite key

**The SQLite engine has no JOIN-specific handling.** In `conn.go` line 127, `StrategyJoinPatch` is handled identically to `StrategyMergedRead`, which only supports single-table reads. The `executeMergedRead` function (line 360) only looks at `cl.Tables[0]` -- the first table -- ignoring all joined tables.

**Impact:** Any JOIN query involving dirty tables will produce incorrect results because:
- Only the first table's delta/tombstone state is considered
- Multi-table PK dedup is impossible
- WHERE clause re-evaluation on patched rows doesn't happen

### 4.3 Complex Read Handling (CTE, Derived Tables)

The PostgreSQL engine's `read_complex.go` decomposes CTEs and derived tables:
- Non-recursive CTEs: materializes dirty CTEs into temp tables
- Recursive CTEs: materializes dirty base tables, rewrites query
- Derived tables: recursively rewrites subqueries
- LATERAL joins: materializes referenced dirty base tables

**The SQLite engine has no complex read handling.** CTEs are classified as `IsComplexRead = true` but the merged read path in `executeMergedRead` (line 354) does not check this flag or implement any special handling.

### 4.4 Set Operations (UNION/INTERSECT/EXCEPT)

The PostgreSQL engine's `read_setop.go` decomposes set operations into leaf SELECTs, runs each through merged read, then applies the set operation in memory.

**The SQLite engine does not handle set operations.** The `HasSetOp` flag is detected by the classifier but never consumed in the proxy. Set operation queries on dirty tables will produce incorrect results because the merged read treats them as a single query.

### 4.5 Window Functions

The PostgreSQL engine's `read_window.go` materializes the base dataset through merged read, writes it to a temp table, and re-executes the original query against the temp table.

**The SQLite engine does not detect or handle window functions.** The `HasWindowFunc` flag is never set by the classifier, and no materialization strategy exists.

### 4.6 Full Aggregate Support

The PostgreSQL engine supports three aggregate paths:
1. Simple re-aggregatable (COUNT, SUM, AVG, MIN, MAX with GROUP BY) -- full in-memory re-aggregation
2. Complex aggregates (array_agg, json_agg, string_agg) -- materialization + re-execution
3. Fallback

**The SQLite engine only handles `COUNT(*)` without GROUP BY** (`proxy/conn.go`, lines 530-593). The `executeAggregateRead` method:
- Skips GROUP BY queries entirely (line 575: `if strings.Contains(upper, "GROUP BY") { return "" }`)
- Only converts to `SELECT pk FROM table` and counts rows
- Cannot handle SUM, AVG, MIN, MAX
- Cannot handle HAVING clauses
- Cannot handle COUNT(DISTINCT col)

### 4.7 CTID/ROWID Injection for PK-less Tables

The PostgreSQL engine injects `ctid` (system column) for tables without a declared primary key, using it as a row identity for deduplication.

**The SQLite engine does not handle PK-less tables in merged reads.** In `executeMergedRead` (line 469), when `pkIdx < 0`, the filtered prod rows are used as-is without any dedup, which can produce duplicate rows.

SQLite has `rowid` which could serve the same purpose, but it is not injected.

### 4.8 Production Query Rewriting

The PostgreSQL engine's `rewrite_prod.go` rewrites SELECT queries sent to Prod to account for schema divergence (dropped columns removed, renamed columns reverted, added columns stripped).

**The SQLite engine sends the same SQL to both Prod and Shadow** with no rewriting. This will cause errors or incorrect results when schema has diverged.

### 4.9 Temp Table Materialization Framework

The PostgreSQL engine's `util_table.go` provides:
- Deterministic temp table naming via SQL hash
- `buildCreateTempTableSQL()`: Creates temp table matching query result schema
- `bulkInsertToUtilTable()`: Batch inserts rows
- `dropUtilTable()`: Cleanup
- `buildScopedMaterializationSQL()`: Predicate pushdown optimization

**The SQLite engine has none of this infrastructure**, which is a prerequisite for implementing window functions, complex aggregates, complex reads, and JOIN schema-diff handling.

### 4.10 WHERE Clause Evaluation

The PostgreSQL engine's `where_eval.go` evaluates WHERE clause AST nodes against in-memory rows for JOIN patch re-evaluation. Supports =, !=, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL, AND, OR, NOT.

**The SQLite engine has no WHERE evaluation.** This is needed for JOIN patch correctness.

---

## 5. Write Operation Gaps

### 5.1 INSERT with RETURNING

The PostgreSQL engine captures RETURNING clause results to extract inserted PKs for precise delta tracking (individual PK-level deltas instead of just insert count).

**The SQLite engine never sets `HasReturning`** and always uses `MarkInserted(table)` (insert count tracking) instead of PK-level tracking (`proxy/conn.go`, lines 991-994). This means reads must use the less precise insert-count-based heuristic for these tables.

### 5.2 INSERT ... ON CONFLICT (Upsert)

The PostgreSQL engine detects `HasOnConflict`, routes to `StrategyHydrateAndWrite`, pre-hydrates potentially conflicting Prod rows, then executes the upsert on Shadow.

**The SQLite engine never sets `HasOnConflict`** (`classify/classifier.go`), so upserts are routed as simple `StrategyShadowWrite`. This means:
- Conflicting rows in Prod are not hydrated into Shadow
- The ON CONFLICT DO UPDATE will fail to find the conflicting row
- The result is a new INSERT instead of an UPDATE, creating duplicates

Note: SQLite also has `INSERT OR REPLACE` and `REPLACE INTO` which are classified but not given upsert handling.

### 5.3 Bulk UPDATE (No PKs Extractable)

The PostgreSQL engine handles bulk UPDATEs by:
1. Querying Prod for all matching rows
2. Hydrating them into Shadow
3. Rewriting the UPDATE with concrete PK values
4. Executing on Shadow

**The SQLite engine only handles point UPDATEs** where PKs are extractable from the WHERE clause (`proxy/conn.go`, `hydrateBeforeUpdate`, lines 940-963). If `cl.PKs` is empty, the `trackWriteEffects` falls back to `MarkInserted(table)` (lines 1006-1009) which is an imprecise heuristic, and no hydration occurs.

### 5.4 Cross-Table UPDATE (UPDATE ... FROM)

The PostgreSQL engine handles `UPDATE t1 SET ... FROM t2 WHERE ...` by extracting subquery predicates and hydrating referenced tables.

**Not implemented in the SQLite engine.**

### 5.5 Bulk DELETE (No PKs Extractable)

The PostgreSQL engine pre-queries Prod for matching PKs, computes total count (Shadow actual + Prod-only matches), and tombstones all matching PKs.

**The SQLite engine only tombstones rows where PKs are extractable.** For bulk DELETEs without PKs, it falls back to `MarkInserted(table)` (`proxy/conn.go`, lines 1030-1035), which is semantically incorrect for deletes.

### 5.6 DELETE with RETURNING

The PostgreSQL engine supports RETURNING on DELETE by hydrating rows from Prod for return data synthesis.

**Not implemented in the SQLite engine.**

### 5.7 TRUNCATE Handling

The PostgreSQL engine marks tables as `IsFullyShadowed` in the schema registry after TRUNCATE, causing all subsequent reads to go to Shadow only. CASCADE tracking handles related tables.

**TRUNCATE is not classified at all** in the SQLite engine. SQLite doesn't natively support TRUNCATE (uses `DELETE FROM table` instead), but the pattern should still be recognized.

### 5.8 Generated Column Filtering During Hydration

The PostgreSQL engine detects generated columns via `pg_attribute.attgenerated` and filters them from hydration INSERTs (PostgreSQL rejects explicit values for GENERATED ALWAYS AS STORED columns).

**The SQLite engine does not detect or filter generated columns.** SQLite 3.31.0+ supports generated columns, and hydration will fail if such columns exist because the `buildHydrateInsert` function (`proxy/conn.go`, lines 966-981) includes all columns.

### 5.9 Max Rows Hydration Cap

The PostgreSQL engine has a configurable max rows cap on hydration to prevent unbounded data movement.

**Not implemented in the SQLite engine.** A bulk UPDATE or DELETE on a large table could attempt to hydrate millions of rows.

### 5.10 Correct Row Count for Prod-Only Deletes

The PostgreSQL engine reports accurate delete counts by combining Shadow actual deletes with Prod-only matches.

**The SQLite engine only reports the count from Shadow** (`proxy/conn.go`, `executeExecQuery`, line 319), which will undercount when deleting rows that only exist in Prod.

---

## 6. DDL & Schema Tracking Gaps

### 6.1 FK Stripping from DDL

The PostgreSQL engine strips FK constraints from DDL before executing on Shadow (because the proxy enforces FKs itself). It extracts FK metadata first for proxy-layer enforcement.

**The SQLite engine does not strip FKs.** However, the proxy sets `PRAGMA foreign_keys=OFF` on Shadow (`proxy/proxy.go`, line 99), which effectively disables FK enforcement at the database level. This is a partial workaround but means:
- FK metadata is never extracted from DDL for proxy-layer enforcement
- The proxy cannot validate referential integrity

### 6.2 DROP COLUMN Tracking

The PostgreSQL engine records `RecordDropColumn(table, name)` and subsequently filters dropped columns from Prod reads.

**The SQLite engine does not track DROP COLUMN.** The `trackDDLEffects` function (`proxy/conn.go`, lines 1058-1113) has no case for DROP COLUMN. Note: SQLite 3.35.0+ supports `ALTER TABLE DROP COLUMN`, so this is a real gap.

### 6.3 ALTER TYPE Tracking

The PostgreSQL engine records `RecordTypeChange(table, col, old, new)` for column type changes.

**Not tracked in the SQLite engine.** SQLite doesn't support `ALTER TABLE ALTER COLUMN TYPE` natively, but this should be noted for completeness.

### 6.4 AST-Based DDL Parsing

The PostgreSQL engine uses `pg_query_go` to parse DDL into structured `ddlChange` records (`ddl_parse.go`), handling all ALTER TABLE subcommands.

**The SQLite engine uses string splitting** (`parseAlterRenameColumn` and `parseAlterAddColumn` in `proxy/conn.go`, lines 1117-1180). This is fragile:
- Does not handle quoted identifiers with spaces in column names
- Does not handle ALTER TABLE with multiple actions
- Does not parse ADD CONSTRAINT (FK definitions)
- Does not detect DROP COLUMN

### 6.5 Query Rewriting for Schema Diffs

The PostgreSQL engine rewrites queries to Prod to handle:
- Dropped columns: removed from SELECT, WHERE, ORDER BY, GROUP BY, HAVING
- Renamed columns: reverted to Prod names
- Added columns: stripped from Prod query, NULL injected in merge

**No query rewriting exists in the SQLite engine.** This means:
- After ADD COLUMN: Prod queries will fail if the new column is referenced
- After RENAME COLUMN: Prod queries will reference the wrong column name
- After DROP COLUMN: Prod queries may reference non-existent columns

---

## 7. Transaction Management Gaps

### 7.1 Prod Isolation Level

The PostgreSQL engine sends `BEGIN ISOLATION LEVEL REPEATABLE READ` to Prod for merged read consistency within a transaction.

**The SQLite engine sends plain `BEGIN` to Prod** (`proxy/txn.go`, line 48). SQLite defaults to DEFERRED transactions. For read consistency, the Prod connection should use `BEGIN IMMEDIATE` or the connection should set the journal mode appropriately.

### 7.2 Schema Registry Snapshots

The PostgreSQL engine takes a schema registry snapshot at BEGIN and restores it at ROLLBACK.

**The SQLite engine does not snapshot the schema registry** (`proxy/txn.go`). This means:
- DDL inside a rolled-back transaction will leave schema registry in a dirty state
- The schema registry will indicate changes that were actually reverted

### 7.3 Savepoint System (CRITICAL)

The PostgreSQL engine implements a full stack-based savepoint system:
- Each SAVEPOINT captures: delta map, tombstone set, insert counts, schema registry state
- RELEASE SAVEPOINT: pops snapshot (changes persist)
- ROLLBACK TO SAVEPOINT: restores state from snapshot, keeps savepoint for re-use

**The SQLite engine's TxnHandler has no savepoint support.** SAVEPOINT/RELEASE/ROLLBACK TO are forwarded as "other" statements (`proxy/txn.go`, line 133-139, `handleOther`), which just executes on both databases without any delta/tombstone state management.

**Impact:** ROLLBACK TO SAVEPOINT will revert Shadow changes but the delta map and tombstone set will retain stale entries from the reverted operations, causing read path corruption.

### 7.4 ReadyForQuery Transaction State

The PostgreSQL engine sends proper transaction state indicators:
- 'I' for idle
- 'T' for in-transaction
- 'E' for error-in-transaction

**The SQLite engine always sends 'I' (idle)** (`proxy/pgmsg.go`, line 163: `buildReadyForQueryMsg` hardcodes `[]byte{'I'}`). This means:
- Clients cannot detect they are in a transaction
- Some client libraries (like psycopg2, pgx) rely on this state to manage connection pooling
- Error state after failed queries within a transaction is never signaled

---

## 8. Foreign Key Enforcement Gaps

The PostgreSQL engine has a complete `FKEnforcer` (`proxy/fk.go`) that:
1. Discovers FKs from Prod schema during initialization
2. Stores FK metadata in the Schema Registry
3. Validates parent rows exist before INSERT/UPDATE (checks delta map, tombstones, Shadow, then Prod)
4. Enforces RESTRICT/NO ACTION on DELETE (rejects if child rows exist)
5. Enforces CASCADE on DELETE (tombstones child rows recursively)
6. Supports multi-column (composite) FKs
7. Caches FK discovery per table
8. Extracts FK metadata from DDL (new constraints via ALTER TABLE)

**The SQLite engine has zero FK enforcement.** There is no:
- FK discovery during initialization (`init.go` does not call any FK detection)
- `FKEnforcer` struct or equivalent
- Pre-write FK validation
- Post-delete cascade enforcement
- FK metadata in the schema registry

The Shadow database has `PRAGMA foreign_keys=OFF` (`proxy/proxy.go`, line 99), and there is no compensating proxy-layer enforcement.

**Impact:** Applications relying on FK constraints will see silent referential integrity violations.

---

## 9. Protocol-Level Gaps

### 9.1 Embedded vs. Wire Protocol Architecture

The fundamental architectural difference: PostgreSQL uses a client-server model where the proxy sits on the network path, while SQLite is embedded (file-based). The SQLite engine bridges this gap by:
- Acting as a pgwire server that clients connect to
- Internally using `database/sql` to query SQLite files

This is correct but introduces several limitations:

### 9.2 Binary Parameter Decoding

The PostgreSQL engine decodes binary-format parameters: bool (1 byte), int16 (2), int32 (4), int64 (8), UUID (16 bytes).

**The SQLite engine treats all parameters as text** (`proxy/pgmsg.go`, `parseBindMsgPayload`, lines 256-299). Binary format parameters from clients using binary mode will be misinterpreted.

### 9.3 Type OID Accuracy

The SQLite engine hardcodes all column OIDs as 25 (text) in RowDescription messages (`proxy/conn.go`, lines 271-273; `proxy/pgmsg.go`, line 118). PostgreSQL clients may rely on accurate OIDs for type coercion.

### 9.4 Describe Response

The PostgreSQL engine responds to Describe messages with proper RowDescription or ParameterDescription. The SQLite engine responds with NoData (`proxy/ext_handler.go`, line 100), which is incorrect for portals that have a result set.

### 9.5 Connection Drain on Errors

The PostgreSQL engine drains the connection on mid-stream errors to prevent protocol desync.

**The SQLite engine does not implement connection draining.** If an error occurs mid-stream in the extended protocol, the connection may desync.

### 9.6 ReadyForQuery Transaction State

As noted in section 7.4, the SQLite engine always sends 'I' (idle) instead of tracking the actual transaction state ('I', 'T', 'E').

---

## 10. Prepared Statement Gaps

### 10.1 SQL-Level PREPARE/EXECUTE/DEALLOCATE

The PostgreSQL engine classifies and routes SQL-level PREPARE/EXECUTE/DEALLOCATE statements with proper parameter substitution caching.

**The SQLite engine does not classify these statements.** They fall through to `OpOther/SubOther` and are forwarded to Prod, which for SQLite is the embedded database that doesn't understand PostgreSQL's PREPARE syntax.

### 10.2 Statement Cache Shadow-Only Tracking

The PostgreSQL engine tracks which cached statements reference shadow-only or schema-modified tables, auto-routing them to merged read when executed.

**The SQLite engine's `ExtHandler.stmtCache`** (`proxy/ext_handler.go`, line 22) only maps statement names to SQL text. It does not track whether statements reference dirty tables.

### 10.3 Parameter Substitution Approach

The SQLite engine uses string-based parameter substitution (`reconstructSQL` in `proxy/pgmsg.go`, lines 326-338), replacing `$1`, `$2` etc. with quoted literal values. This approach:
- Does not handle `?` placeholders (SQLite's native parameterization)
- Uses PostgreSQL-style `$N` placeholders which SQLite doesn't understand natively
- The string replacement can break on edge cases (e.g., `$1` appearing in string literals)

---

## 11. Guard Rail Gaps

### 11.1 L2: SafeProdConn

The PostgreSQL engine wraps the Prod connection with `SafeProdConn` which inspects all outgoing messages for write SQL, providing a second layer of defense.

**The SQLite engine has no L2 guard.** The `PRAGMA query_only=ON` on the Prod database (`proxy/proxy.go`, line 87) provides some protection, but:
- It only works if the SQLite library honors it (modernc.org/sqlite does)
- It doesn't provide the logging/alerting that SafeProdConn does
- It doesn't cover CTE writes that start with WITH/SELECT

### 11.2 Write Detection Gaps

The SQLite `looksLikeWrite` function (`proxy/guard.go`, lines 50-84) includes VACUUM and REINDEX as write prefixes, which is correct for SQLite. However, it doesn't detect:
- TRUNCATE (not applicable for SQLite but consistent with PG)
- GRANT/REVOKE (not applicable for SQLite)

---

## 12. Initialization Gaps

### 12.1 Generated Column Detection

The PostgreSQL engine detects generated columns via `pg_attribute.attgenerated = 's'` and stores them in table metadata.

**The SQLite engine does not detect generated columns.** SQLite 3.31.0+ supports generated columns (`GENERATED ALWAYS AS (...) STORED/VIRTUAL`), which can be detected via `PRAGMA table_xinfo` (the extended version of `table_info` that includes generated column info). This data is needed for:
- Filtering generated columns during hydration
- Correct handling of INSERT statements

**File:** `schema/schema.go`, `detectTablePK` (lines 73-135) -- uses `PRAGMA table_info` instead of `PRAGMA table_xinfo`.

### 12.2 Foreign Key Detection

The PostgreSQL engine discovers FKs from `pg_catalog` during initialization.

**The SQLite engine does not detect FKs.** SQLite exposes FK info via `PRAGMA foreign_key_list(table)`, which returns: id, seq, table, from, to, on_update, on_delete, match. This is never called.

**File:** `init.go` -- no FK detection step exists.

### 12.3 Shadow Creation Method

The PostgreSQL engine creates a schema-only Shadow via `pg_dump --schema-only`, meaning the Shadow starts empty (no data rows).

**The SQLite engine copies the entire database file** (`shadow/shadow.go`, `CreateShadow`, lines 21-47), which means the Shadow starts with a full data copy. This is actually a design choice that has trade-offs:
- Pro: No need for hydration on first read (data already present)
- Con: Initialization is slow for large databases
- Con: The Shadow file can be very large
- Con: All existing rows in Shadow are "phantom deltas" -- they were never modified by the user but differ from Prod if Prod data changes externally

This is a fundamental architectural difference that may need re-evaluation. If the intent is copy-on-write semantics like PostgreSQL, the shadow should be schema-only, and the merged read pipeline should handle the data gap.

### 12.4 Config Persistence

The SQLite engine writes config but does not persist all metadata equivalently:
- Writes tables.json (equivalent to PostgreSQL)
- Does not write FK metadata
- Does not write generated column info

---

## 13. Priority Recommendations

Ordered by impact and dependency (items needed by later items come first):

### P0 -- Critical (Causes Incorrect Results)

1. **ReadyForQuery transaction state tracking** -- Send 'T' when in transaction, 'E' after errors. Without this, client libraries may malfunction.
   - **File to modify:** `proxy/pgmsg.go` (add state parameter to `buildReadyForQueryMsg`), `proxy/conn.go`, `proxy/txn.go`
   - **Effort:** Small

2. **Schema adaptation in merged reads** -- Handle added/dropped/renamed columns when merging Prod and Shadow results. Without this, any DDL change breaks reads.
   - **Files to modify:** `proxy/conn.go` (`executeMergedRead`, `mergedReadRows`)
   - **Effort:** Medium

3. **Production query rewriting for schema diffs** -- Rewrite SQL sent to Prod to remove added columns, revert renamed columns, strip dropped columns.
   - **New file:** `proxy/rewrite_prod.go`
   - **Effort:** Medium (can use string-based rewriting as a start since SQLite lacks pg_query)

4. **Proper savepoint support** -- Implement savepoint snapshot stack for delta map, tombstone set, and schema registry.
   - **File to modify:** `proxy/txn.go` (add `savepointSnapshot` struct and stack)
   - **Effort:** Medium

5. **ON CONFLICT / INSERT OR REPLACE handling** -- Detect upsert pattern, hydrate conflicting rows before execution.
   - **Files to modify:** `classify/classifier.go` (set `HasOnConflict`), `proxy/conn.go` (add upsert hydration path)
   - **Effort:** Medium

### P1 -- High (Significant Functionality Gaps)

6. **JOIN patch implementation** -- Implement the JOIN patch algorithm for multi-table reads.
   - **New file:** `proxy/read_join.go`
   - **Effort:** Large

7. **Full aggregate support** -- Implement re-aggregation for SUM, AVG, MIN, MAX, COUNT(DISTINCT), GROUP BY, HAVING.
   - **Files to modify:** `proxy/conn.go` (expand `executeAggregateRead`)
   - **Effort:** Large

8. **Bulk UPDATE hydration** -- Query Prod for matching rows when PKs are not extractable, hydrate, then execute.
   - **File to modify:** `proxy/conn.go` (`hydrateBeforeUpdate` needs bulk path)
   - **Effort:** Medium

9. **Bulk DELETE with tombstoning** -- Pre-query Prod for matching PKs, tombstone all, report correct count.
   - **File to modify:** `proxy/conn.go` (`trackWriteEffects` ShadowDelete case)
   - **Effort:** Medium

10. **FK detection and enforcement** -- Detect FKs via `PRAGMA foreign_key_list`, implement parent row validation on INSERT/UPDATE, RESTRICT enforcement on DELETE.
    - **New files:** `proxy/fk.go`; modify `init.go` and `schema/schema.go`
    - **Effort:** Large

11. **Generated column detection and filtering** -- Use `PRAGMA table_xinfo` to detect generated columns, filter from hydration INSERTs.
    - **Files to modify:** `schema/schema.go`, `proxy/conn.go` (`buildHydrateInsert`)
    - **Effort:** Small

### P2 -- Medium (Feature Completeness)

12. **Temp table materialization framework** -- Build utility for creating, populating, and dropping temp tables. Required for window functions, complex reads, set operations.
    - **New file:** `proxy/util_table.go`
    - **Effort:** Medium

13. **Set operation handling (UNION/INTERSECT/EXCEPT)** -- Decompose set operations into leaf SELECTs, run each through merged read, apply set ops in memory.
    - **New file:** `proxy/read_setop.go`
    - **Effort:** Medium (depends on #12)

14. **Window function handling** -- Materialize base query through merged read, re-execute original query against temp table.
    - **New file:** `proxy/read_window.go`; modify classifier to detect window functions
    - **Effort:** Medium (depends on #12)

15. **Complex read handling (CTEs, derived tables)** -- Decompose and materialize dirty sub-queries.
    - **New file:** `proxy/read_complex.go`
    - **Effort:** Large (depends on #12)

16. **RETURNING clause support** -- Detect RETURNING in classifier, capture returned rows for PK extraction on INSERT/DELETE.
    - **Files to modify:** `classify/classifier.go`, `proxy/conn.go`
    - **Effort:** Medium

17. **DROP COLUMN tracking** -- Detect and record DROP COLUMN in schema registry.
    - **File to modify:** `proxy/conn.go` (`trackDDLEffects`)
    - **Effort:** Small

18. **TRUNCATE handling** -- Classify TRUNCATE (as DELETE FROM in SQLite), mark table as fully shadowed.
    - **Files to modify:** `classify/classifier.go`, `proxy/conn.go`
    - **Effort:** Small

19. **Schema registry snapshot at BEGIN/ROLLBACK** -- Take snapshot at BEGIN, restore at ROLLBACK.
    - **File to modify:** `proxy/txn.go`
    - **Effort:** Small

### P3 -- Low (Nice to Have)

20. **Binary parameter decoding** -- Decode binary-format parameters in extended protocol Bind messages.
    - **File to modify:** `proxy/pgmsg.go` (`parseBindMsgPayload`)
    - **Effort:** Small

21. **Describe response accuracy** -- Return proper RowDescription for Describe messages instead of NoData.
    - **File to modify:** `proxy/ext_handler.go`
    - **Effort:** Medium

22. **ROWID injection for PK-less tables** -- Inject `rowid` column for dedup in merged reads.
    - **File to modify:** `proxy/conn.go`
    - **Effort:** Small

23. **WHERE clause evaluator** -- Port the PostgreSQL engine's `where_eval.go` for JOIN patch re-evaluation.
    - **New file:** `proxy/where_eval.go`
    - **Effort:** Medium (depends on #6)

24. **Connection drain on errors** -- Implement protocol-level error recovery.
    - **File to modify:** `proxy/conn.go`
    - **Effort:** Small

25. **Statement cache shadow-only tracking** -- Track which cached statements reference dirty tables.
    - **File to modify:** `proxy/ext_handler.go`
    - **Effort:** Small

26. **Max rows hydration cap** -- Add configurable limit to hydration queries.
    - **File to modify:** `proxy/conn.go`
    - **Effort:** Small

---

## Summary Statistics

| Category | Implemented | Partial | Missing | N/A | Total |
|----------|:---------:|:------:|:------:|:---:|:-----:|
| Classification | 7 | 5 | 10 | 1 | 23 |
| Read Operations | 4 | 1 | 12 | 0 | 17 |
| Write Operations | 3 | 0 | 10 | 0 | 13 |
| DDL & Schema Tracking | 4 | 1 | 6 | 0 | 11 |
| Transaction Management | 3 | 0 | 7 | 0 | 10 |
| Foreign Key Enforcement | 0 | 0 | 8 | 0 | 8 |
| Protocol Handling | 3 | 4 | 3 | 1 | 11 |
| Guard Rails | 4 | 0 | 1 | 0 | 5 |
| Initialization | 4 | 0 | 2 | 2 | 8 |
| **Total** | **32** | **11** | **59** | **4** | **106** |

The SQLite engine implements approximately **30%** of the PostgreSQL engine's feature set, with another **10%** partially implemented. The core architecture is solid, but the read/write strategy layer and FK enforcement need substantial work.
