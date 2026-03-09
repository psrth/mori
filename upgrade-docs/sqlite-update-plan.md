# SQLite Engine Update Plan

This document is the implementation plan for bringing the SQLite engine to full feature parity with the PostgreSQL engine. Agents implementing these changes should **always refer to the PostgreSQL implementation** as the reference, and secondarily the DuckDB engine (which shares the embedded-DB + pgwire-wrapper architecture). Adapt for SQLite SQL dialect and embedded database semantics.

---

## Guiding Principles

1. **Mirror the PostgreSQL patterns.** Same strategies: hydrate-then-execute, materialize-via-temp-table, dual-execute + merge. Adapt for SQLite syntax.
2. **Let SQLite do the heavy lifting.** For complex SQL (window functions, complex aggregates, CTEs), materialize merged data into a temp table and re-execute on Shadow.
3. **Prod safety is non-negotiable.** The pgwire proxy + `PRAGMA query_only=ON` on Prod is the safety layer — extend it, don't weaken it.
4. **Regex classifier limitations are accepted.** No SQLite AST parser exists in Go. Improve regex accuracy where possible, but accept inherent limitations.
5. **Shadow is a full file copy.** Unlike postgres (schema-only), the SQLite shadow starts with all prod data. Merged read dedup logic must handle this correctly (shadow wins on PK collision).

---

## P0 — Critical Correctness

### 1.1 ReadyForQuery Transaction State Tracking

**Problem:** `buildReadyForQueryMsg` hardcodes `'I'` (idle) — clients can't detect they're in a transaction. Client libraries like pgx rely on this state for connection management.

**Implementation:**
- Add `txnState byte` field to the connection handler (or a shared state struct accessible by `buildReadyForQueryMsg`)
- Track state transitions:
  - After `BEGIN` → set state to `'T'`
  - After query error inside transaction → set state to `'E'`
  - After `COMMIT` / `ROLLBACK` → set state to `'I'`
- In `buildReadyForQueryMsg()`, pass the current state instead of hardcoded `'I'`
- **File:** `proxy/pgmsg.go` (`buildReadyForQueryMsg`), `proxy/conn.go`, `proxy/txn.go`
- **Reference:** `postgres/proxy/conn.go` ReadyForQuery state tracking

### 1.2 Generated Column Detection and Filtering

**Problem:** `buildHydrateInsert` includes all columns. Tables with `GENERATED ALWAYS AS (...) STORED/VIRTUAL` columns crash during hydration because SQLite rejects explicit INSERT values for generated columns.

**Implementation:**
- In `schema/schema.go`, switch from `PRAGMA table_info(table)` to `PRAGMA table_xinfo(table)`:
  - `table_xinfo` returns an additional `hidden` column: 0=normal, 2=virtual generated, 3=stored generated
  - Filter columns where `hidden IN (2, 3)` into `TableMeta.GeneratedCols`
- In `proxy/conn.go` `buildHydrateInsert()`, skip columns in `GeneratedCols` when building INSERT
- **Reference:** `postgres/schema/dumper.go` `DetectGeneratedColumns()`

### 1.3 Schema Adaptation in Merged Reads

**Problem:** After ADD/DROP/RENAME COLUMN, Prod and Shadow have different schemas. `executeMergedRead()` merges results without adapting column sets, causing misalignment or errors.

**Implementation:**
- In `proxy/conn.go` `executeMergedRead()`, after fetching Prod rows:
  1. Check schema registry for the table: `schemaReg.GetChanges(table)`
  2. For added columns: inject NULL at the appropriate position in Prod rows
  3. For dropped columns: remove from Prod result set
  4. For renamed columns: map Prod's old column name to Shadow's new name
- Build column alignment mapping once per query, apply to each Prod row
- **Reference:** `postgres/proxy/read_single.go` `adaptRow()`

### 1.4 Production Query Rewriting for Schema Diffs

**Problem:** After DDL changes, the same SQL is sent to both Prod and Shadow. Prod queries referencing new/renamed/dropped columns will fail.

**Implementation:**
- New file: `proxy/rewrite_prod.go`
- `rewriteForProd(sql string, table string, schemaReg) string`:
  - Dropped columns: remove from SELECT, WHERE, ORDER BY, GROUP BY, HAVING
  - Renamed columns: revert to Prod names (replace new name → old name)
  - Added columns: strip from Prod query entirely
- Use regex-based rewriting (no AST available for SQLite in Go)
- Call before sending any query to Prod when schema diffs exist for affected tables
- **Reference:** `postgres/proxy/rewrite_prod.go`

### 1.5 INSERT ... ON CONFLICT / INSERT OR REPLACE Handling

**Problem:** `HasOnConflict` is never set. Upserts route as plain `StrategyShadowWrite`, missing Prod rows that should be updated.

**Implementation:**
- In `classify/classifier.go`, detect:
  - `INSERT ... ON CONFLICT` (SQLite 3.24+) → set `HasOnConflict = true`
  - `INSERT OR REPLACE INTO ...` → set `HasOnConflict = true`
  - `REPLACE INTO ...` (already classified as OpWrite/SubInsert) → set `HasOnConflict = true`
- Router maps `HasOnConflict` on INSERT → `StrategyHydrateAndWrite`
- In `proxy/conn.go`, add upsert handler:
  1. Extract target table and conflict columns (PK or UNIQUE columns)
  2. Extract values from the INSERT VALUES clause
  3. Query Prod for matching rows
  4. Hydrate matching rows into Shadow (skip delta rows, filter generated columns)
  5. Execute the original upsert on Shadow
  6. Track affected PKs in delta map
- **Reference:** `postgres/proxy/write_insert.go` upsert path, `mysql-update-plan.md` §1.4

### 1.6 Bulk UPDATE Hydration (PKs Not Extractable)

**Problem:** When PKs can't be extracted from WHERE, `hydrateBeforeUpdate` does nothing. Affected Prod-only rows are never updated.

**Implementation:**
- In `proxy/conn.go`, when UPDATE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod (with schema rewriting)
  2. Hydrate all matching rows into Shadow (skip rows already in delta map, filter generated columns)
  3. Execute the original UPDATE on Shadow
  4. Add all affected PKs to delta map
- Respect `max_rows_hydrate` cap
- **Reference:** `postgres/proxy/write_update.go` bulk update path

### 1.7 Bulk DELETE Tombstoning (PKs Not Extractable)

**Problem:** When PKs can't be extracted, DELETE falls back to `MarkInserted(table)` which is semantically wrong for deletes.

**Implementation:**
- In `proxy/conn.go`, when DELETE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod
  2. Tombstone all matching PKs
  3. Execute DELETE on Shadow
  4. Compute total count: Shadow actual deletes + Prod-only matches
  5. Return corrected row count to client via synthetic CommandComplete
- **Reference:** `postgres/proxy/write_delete.go` bulk delete path

### 1.8 INSERT PK Tracking (Row-Level)

**Problem:** `handleInsert` uses `MarkInserted(table)` (table-level) instead of row-level tracking, forcing imprecise merged reads.

**Implementation:**
- After INSERT on Shadow, capture inserted PKs:
  - For AUTOINCREMENT tables: query `last_insert_rowid()` from Shadow
  - For non-AUTOINCREMENT tables: extract PKs from the INSERT VALUES clause
- Replace `MarkInserted(table)` with `deltaMap.Add(table, pk)` for each inserted PK
- Detect RETURNING clause (see 4.2) for multi-row inserts
- **Reference:** `postgres/proxy/write_insert.go` PK capture via RETURNING

### 1.9 Max Rows Hydration Cap

**Problem:** No upper bound on hydration. Large tables can cause unbounded data movement.

**Implementation:**
- Add `MaxRowsHydrate int` from `engine.ProxyDeps`
- In all Prod queries for hydration/materialization, append `LIMIT {max_rows_hydrate}` when configured
- **Reference:** `postgres/proxy/write_update.go` `capSQL()`

---

## P1 — High Priority

### 2.1 SAVEPOINT State Management

**Problem:** SAVEPOINT/RELEASE/ROLLBACK TO are forwarded as-is via `handleOther`. Delta map and tombstone state is not snapshotted or restored.

**Implementation:**
- In `classify/classifier.go`, change SAVEPOINT/RELEASE classification:
  - `SAVEPOINT name` → `SubSavepoint` (instead of `SubOther`)
  - `RELEASE SAVEPOINT name` → `SubRelease` (instead of `SubOther`)
  - `ROLLBACK TO SAVEPOINT name` → differentiate from full ROLLBACK, set `SubSavepoint` with rollback flag
- In `proxy/txn.go`:
  - On SAVEPOINT: Push snapshot of delta map, tombstone set, insert counts, schema registry
  - On RELEASE SAVEPOINT: Pop snapshot (changes persist)
  - On ROLLBACK TO SAVEPOINT: Restore from snapshot, keep snapshot for repeat rollbacks
  - On full COMMIT: Flush all, clear stack
  - On full ROLLBACK: Discard all, clear stack
- **Reference:** `postgres/proxy/txn.go` savepoint system

### 2.2 Schema Registry Snapshot on BEGIN / Restore on ROLLBACK

**Problem:** DDL within a rolled-back transaction leaves schema registry dirty.

**Implementation:**
- In `proxy/txn.go` `handleBegin()`: `schemaReg.SnapshotAll()` and store
- In `proxy/txn.go` `handleRollback()`: `schemaReg.RestoreAll(snapshot)`
- **Reference:** `postgres/proxy/txn.go`

### 2.3 Prod Isolation Level on BEGIN

**Problem:** Plain `BEGIN` sent to Prod — reads may be non-repeatable.

**Implementation:**
- For the embedded Prod connection, send `BEGIN IMMEDIATE` instead of `BEGIN`
  - `BEGIN IMMEDIATE` acquires a RESERVED lock immediately, preventing other writers from modifying the database during the transaction
  - This is SQLite's closest equivalent to REPEATABLE READ — once the transaction starts, the snapshot is consistent
- Alternatively, if Prod is in WAL mode (recommended), plain `BEGIN` with WAL provides snapshot isolation automatically
- **Reference:** `postgres/proxy/txn.go` `BEGIN ISOLATION LEVEL REPEATABLE READ`

### 2.4 TRUNCATE Handling

**Problem:** TRUNCATE is not classified. SQLite doesn't support TRUNCATE natively — uses `DELETE FROM table` instead.

**Implementation:**
- In `classify/classifier.go`, detect `DELETE FROM table` with no WHERE clause → treat as SubTruncate
- Also detect the word `TRUNCATE` (some ORMs may send it) → classify as SubTruncate
- In `proxy/conn.go`, add StrategyTruncate handler:
  1. Forward `DELETE FROM table` to Shadow
  2. `schemaReg.MarkFullyShadowed(table)`
  3. Clear delta map and tombstone set for the table
- **Reference:** `postgres/proxy/conn.go` StrategyTruncate

### 2.5 FK Discovery and Enforcement

**Problem:** Zero FK enforcement. Shadow has `PRAGMA foreign_keys=OFF`, and no proxy-layer validation exists.

**Implementation:**

**Phase 1 — FK Discovery:**
- In `schema/schema.go`, add `DetectForeignKeys()`:
  ```sql
  PRAGMA foreign_key_list(table)
  ```
  Returns: id, seq, table (parent), from (child col), to (parent col), on_update, on_delete, match
- Call for each table during init
- Store as `schema.ForeignKey` structs in schema registry

**Phase 2 — Parent Row Validation (INSERT/UPDATE):**
- New file: `proxy/fk.go`
- On INSERT/UPDATE that sets FK columns, validate parent row exists:
  1. Delta map check → parent in delta → exists in Shadow ✓
  2. Tombstone check → parent tombstoned → REJECT
  3. Shadow query → check Shadow
  4. Prod query → fall back to Prod
- Composite FK support

**Phase 3 — Referential Action Enforcement (DELETE):**
- RESTRICT / NO ACTION: Reject if child rows exist
- CASCADE: Tombstone child rows recursively
- **Reference:** `postgres/proxy/fk.go`

### 2.6 Schema Rewriting for Prod Queries (Full)

**Problem:** Only `stripNewColumnsFromQuery` partial logic. No dropped/renamed column handling in WHERE/ORDER BY/GROUP BY.

**Implementation:**
- Expand the partial rewriting into full `rewriteForProd()` (see 1.4)
- Apply to ALL Prod queries: merged reads, hydration SELECTs, bulk UPDATE/DELETE PK discovery
- **Reference:** `postgres/proxy/rewrite_prod.go`

### 2.7 DROP COLUMN Tracking

**Problem:** `ALTER TABLE DROP COLUMN` (SQLite 3.35+) is not parsed or tracked.

**Implementation:**
- In `proxy/conn.go` `trackDDLEffects()`, add detection for `ALTER TABLE t DROP COLUMN c`:
  - Regex: `(?i)ALTER\s+TABLE\s+(\S+)\s+DROP\s+COLUMN\s+(\S+)`
  - Call `schemaReg.RecordDropColumn(table, col)`
- **Reference:** `postgres/proxy/ddl_parse.go`

---

## P2 — Advanced Read Paths

### 3.1 Temp Table Materialization Infrastructure

**Prerequisite for 3.2-3.6.**

**Implementation:**
- New file: `proxy/util_table.go`
- `utilTableName(sql string) string` → `_mori_util_{md5_prefix}`
- `buildCreateTempTableSQL(columns, types)` → `CREATE TEMP TABLE _mori_util_{hash} (col1 type1, ...)`
- `bulkInsertToUtilTable(table, rows, columns)` → batch INSERT
- `dropUtilTable(table)` → `DROP TABLE IF EXISTS _mori_util_{hash}`
- SQLite temp tables are connection-scoped (same as session-scoped)
- **Reference:** `postgres/proxy/util_table.go`

### 3.2 Full Aggregate Re-Aggregation

**Problem:** Only bare COUNT(*) handled. No SUM/AVG/MIN/MAX, no GROUP BY, no HAVING.

**Implementation:**
- Expand aggregate handler in `proxy/conn.go` (or new file `proxy/aggregate.go`):
  - Simple re-aggregatable: COUNT, SUM, AVG, MIN, MAX with GROUP BY → row-level merge + in-memory re-aggregation
  - HAVING filter post re-aggregation
  - COUNT(DISTINCT col) support
- For complex aggregates (GROUP_CONCAT): materialize into temp table, re-execute on Shadow
- **Reference:** `postgres/proxy/aggregate.go`

### 3.3 Window Function Handling

**Problem:** Not detected or handled — falls through to Prod.

**Implementation:**
- In classifier, detect window functions: `HasWindowFunc = true` for `OVER (` pattern
- New file: `proxy/read_window.go`:
  1. Build base SELECT (strip window functions)
  2. Merged read pipeline
  3. Materialize into temp table (requires 3.1)
  4. Rewrite query to reference temp table
  5. Execute on Shadow
- SQLite supports window functions since 3.25.0
- **Reference:** `postgres/proxy/read_window.go`

### 3.4 Set Operation Handling

**Implementation:**
- New file: `proxy/read_setop.go`
- Decompose UNION/INTERSECT/EXCEPT into leaf SELECTs
- Each leaf through merged read independently
- In-memory set operation application
- **Reference:** `postgres/proxy/read_setop.go`

### 3.5 Complex Read Handling (CTEs, Derived Tables)

**Implementation:**
- New file: `proxy/read_complex.go`
- Non-recursive CTEs: materialize dirty CTEs into temp tables, rewrite query
- Recursive CTEs (WITH RECURSIVE): materialize dirty base tables, rewrite
- Derived tables: recursively rewrite subqueries
- SQLite supports CTEs since 3.8.3 and recursive CTEs since 3.8.3
- **Reference:** `postgres/proxy/read_complex.go`

### 3.6 JOIN Patch Strategy

**Problem:** `StrategyJoinPatch` handled identically to `StrategyMergedRead` — only first table considered.

**Implementation:**
- New file: `proxy/read_join.go`
- Replace stub with full JOIN patch algorithm:
  1. Identify delta/tombstone tables in JOIN
  2. Execute JOIN on Prod
  3. Classify rows: clean/delta/dead
  4. Patch delta rows from Shadow
  5. Re-evaluate WHERE on patched rows (requires 3.8)
  6. Deduplicate by composite key
- For schema-diff JOINs: materialize dirty tables into temp tables (requires 3.1)
- **Reference:** `postgres/proxy/read_join.go`

### 3.7 WHERE Clause Evaluator

**Implementation:**
- New file: `proxy/where_eval.go`
- Port from postgres: =, !=, <>, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL, IS NOT NULL, AND, OR, NOT
- Type-aware comparison, NULL handling
- Safe fallback: return true on evaluation failure
- **Reference:** `postgres/proxy/where_eval.go`

### 3.8 Cursor Operations

**Problem:** SQLite doesn't have SQL-level cursors, but pgwire clients may use the extended query protocol's portal-based cursor mechanism.

**Implementation:**
- In `proxy/ext_handler.go`, handle Execute messages with row-count limits (portal-based cursors):
  - If the inner query is affected (dirty tables), materialize via merged read
  - Serve subsequent Executes from the materialized result
- **Reference:** `postgres/proxy/cursor.go` (adapted for pgwire portal cursors, not SQL DECLARE CURSOR)

---

## P3 — Polish & Edge Cases

### 4.1 Classifier Feature Flag Additions

- `HasReturning`: Detect `RETURNING` clause in INSERT/UPDATE/DELETE (SQLite 3.35+)
- `HasOnConflict`: Detect `ON CONFLICT` and `OR REPLACE` patterns (see 1.5)
- `HasWindowFunc`: Detect `OVER (` pattern
- `HasDistinct`: Detect `SELECT DISTINCT`
- `HasComplexAgg`: Detect `GROUP_CONCAT` (SQLite's complex aggregate)
- `IsMetadataQuery`: Detect queries against `sqlite_master`, `sqlite_schema`, or `PRAGMA`
- `SubTruncate`: See 2.4
- `SubSavepoint`: See 2.1
- `SubRelease`: See 2.1
- `SubExplain`: Detect `EXPLAIN` and `EXPLAIN QUERY PLAN`
- `SubNotSupported`: Block `EXPLAIN` on write queries (if applicable)
- **Reference:** `postgres/classify/classifier.go`

### 4.2 RETURNING Clause Support

- Detect `RETURNING` in classifier → set `HasReturning = true`
- For INSERT with RETURNING: capture returned rows for PK extraction
- For DELETE with RETURNING: capture returned data for client response
- SQLite supports RETURNING since 3.35.0
- **Reference:** `postgres/proxy/write_insert.go` RETURNING handling

### 4.3 PK Extraction Improvements

- Support `value = column` reversal
- Handle `CAST(value AS type)` unwrapping
- Composite PK serialization as JSON
- Handle negative numbers
- **Reference:** `postgres/classify/pk.go`

### 4.4 ROWID Injection for PK-less Tables

**Problem:** PK-less tables have no dedup in merged reads.

**Implementation:**
- SQLite has an implicit `rowid` column for most tables (unless WITHOUT ROWID)
- Inject `rowid` into SELECT for tables without declared PKs
- Use `rowid` as dedup key in merged read
- Detect `WITHOUT ROWID` tables and fall back to full-row hash for those
- **Reference:** PostgreSQL uses `ctid` — SQLite's `rowid` is the equivalent

### 4.5 L2 Guard: SafeProdConn

**Problem:** No L2 write inspection. `PRAGMA query_only=ON` provides database-level protection, but no protocol-level logging/alerting.

**Implementation:**
- Wrap the Prod database handle with a SafeProdConn equivalent
- Inspect all SQL passed to `p.prodDB.Query()` and `p.prodDB.Exec()` through `looksLikeWrite()`
- Log and reject any write SQL that reaches Prod
- This is a defense-in-depth layer on top of `PRAGMA query_only=ON`
- **Reference:** `postgres/proxy/guard.go` SafeProdConn

### 4.6 Binary Parameter Decoding

**Problem:** All parameters treated as text. Binary-format parameters from pgwire clients are misinterpreted.

**Implementation:**
- In `proxy/pgmsg.go` `parseBindMsgPayload()`, decode binary-format parameters:
  - bool (1 byte), int16 (2 bytes), int32 (4 bytes), int64 (8 bytes), UUID (16 bytes)
- **Reference:** `postgres/proxy/ext_handler.go` binary parameter decoding

### 4.7 Describe Response Accuracy

**Problem:** All Describe messages return `NoData`, which is incorrect for portals with result sets.

**Implementation:**
- In `proxy/ext_handler.go`, for Describe('P') (portal):
  - If the statement produces results, execute `PRAGMA table_info` or run the query with `LIMIT 0` on Shadow to get column metadata
  - Return proper RowDescription with column names and type OIDs
- For Describe('S') (statement): return ParameterDescription
- **Reference:** `postgres/proxy/ext_handler.go`

### 4.8 Flush ('H') Message Handling

**Problem:** Flush messages are not handled, potentially causing protocol desync.

**Implementation:**
- In `proxy/ext_handler.go`, handle 'H' message by flushing any buffered output to the client
- **Reference:** `postgres/proxy/ext_handler.go`

### 4.9 Connection Drain on Errors

**Problem:** Mid-stream errors in extended protocol can cause protocol desync.

**Implementation:**
- After an error in the extended protocol, read and discard all remaining messages until Sync ('S') is received
- Then send ErrorResponse + ReadyForQuery
- **Reference:** `postgres/proxy/conn.go` connection draining

### 4.10 Statement Cache Shadow-Only Tracking

**Problem:** `stmtCache` only maps names to SQL. Doesn't track whether statements reference dirty tables.

**Implementation:**
- Extend `stmtCache` to include classification metadata
- When Execute is called, check if cached statement's tables have deltas/tombstones
- If so, route through merged read instead of Prod
- **Reference:** `postgres/proxy/ext_handler.go` shadow-only tracking

### 4.11 Type OID Mapping

**Problem:** All column OIDs hardcoded to 25 (text). ORMs may rely on accurate OIDs.

**Implementation:**
- Map SQLite column types to appropriate PostgreSQL OIDs:
  - INTEGER → 23 (int4)
  - REAL → 701 (float8)
  - TEXT → 25 (text)
  - BLOB → 17 (bytea)
  - NULL → 25 (text, default)
- Use `sql.ColumnType()` or column type affinity from `PRAGMA table_info`
- **Reference:** This is SQLite-specific — no direct postgres equivalent

---

## Implementation Order

```
Phase 1 (P0 — correctness):
  1.1 ReadyForQuery transaction state
  1.2 Generated column detection
  1.3 Schema adaptation in merged reads
  1.4 Prod query rewriting for schema diffs
  1.5 ON CONFLICT / OR REPLACE handling
  1.6 Bulk UPDATE hydration
  1.7 Bulk DELETE tombstoning
  1.8 INSERT PK tracking
  1.9 Max rows hydration cap

Phase 2 (P1 — features):
  2.1 SAVEPOINT state management  (depends on: 4.1 SubSavepoint classification)
  2.2 Schema registry snapshot/restore
  2.3 Prod isolation level
  2.4 TRUNCATE handling
  2.5 FK enforcement
  2.6 Full Prod query rewriting
  2.7 DROP COLUMN tracking

Phase 3 (P2 — advanced reads, requires 3.1 first):
  3.1 Temp table infrastructure  (prerequisite)
  3.2 Full aggregate re-aggregation
  3.3 Window functions
  3.4 Set operations
  3.5 Complex reads (CTEs, derived tables)
  3.6 JOIN patch strategy
  3.7 WHERE clause evaluator
  3.8 Cursor operations (portal-based)

Phase 4 (P3 — polish):
  4.1-4.11 in any order
```

---

## Key SQLite vs PostgreSQL Syntax/Architecture Differences

| Feature | PostgreSQL | SQLite |
|---------|-----------|--------|
| Architecture | Client-server (pgwire relay) | Embedded (pgwire server + database/sql) |
| Shadow creation | Schema-only (pg_dump) | Full file copy (all data) |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` | `INSERT ... ON CONFLICT DO UPDATE` or `INSERT OR REPLACE` |
| RETURNING | `INSERT ... RETURNING *` | `INSERT ... RETURNING *` (3.35+) |
| Temp tables | `CREATE TEMP TABLE name` | `CREATE TEMP TABLE name` |
| TRUNCATE | `TRUNCATE TABLE name` | `DELETE FROM name` (no TRUNCATE keyword) |
| Type cast | `value::type` | `CAST(value AS type)` |
| Boolean | `true` / `false` | `1` / `0` |
| Identity | `GENERATED ALWAYS AS IDENTITY` | `AUTOINCREMENT` on `INTEGER PRIMARY KEY` |
| Generated cols | `pg_attribute.attgenerated` | `PRAGMA table_xinfo` (hidden column 2=virtual, 3=stored) |
| FK discovery | `pg_catalog` | `PRAGMA foreign_key_list(table)` |
| Quoting | `"identifier"` | `"identifier"` or `` `identifier` `` or `[identifier]` |
| PK-less row ID | `ctid` (system column) | `rowid` (implicit, unless WITHOUT ROWID) |
| Isolation on BEGIN | `BEGIN ISOLATION LEVEL REPEATABLE READ` | `BEGIN IMMEDIATE` (WAL mode gives snapshot isolation) |
| Savepoint release | `RELEASE SAVEPOINT name` | `RELEASE SAVEPOINT name` (same syntax) |
| Window functions | Supported | Supported (3.25+) |
| CTEs | Supported | Supported (3.8.3+) |
| ALTER TABLE | Full support | Limited (ADD COLUMN, RENAME COLUMN, DROP COLUMN 3.35+) |
