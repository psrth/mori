# DuckDB Engine Update Plan

This document is the implementation plan for bringing the DuckDB engine to full feature parity with the PostgreSQL engine. Agents implementing these changes should **always refer to the PostgreSQL implementation** as the reference, and secondarily the SQLite engine (which shares the embedded-DB + pgwire-wrapper architecture). Adapt for DuckDB SQL dialect. Note that DuckDB's SQL is much closer to PostgreSQL than SQLite is, so many postgres patterns can be ported more directly.

---

## Guiding Principles

1. **Mirror the PostgreSQL patterns.** Same strategies: hydrate-then-execute, materialize-via-temp-table, dual-execute + merge. DuckDB's SQL is very close to PostgreSQL — most patterns port directly.
2. **Let DuckDB do the heavy lifting.** For complex SQL (window functions, complex aggregates, CTEs), materialize merged data into a temp table and re-execute on Shadow.
3. **Prod safety is non-negotiable.** The pgwire proxy + `access_mode=READ_ONLY` on Prod is the safety layer — extend it, don't weaken it.
4. **Regex classifier limitations are accepted.** No DuckDB AST parser exists in Go. Improve regex accuracy where possible, but accept inherent limitations.
5. **Shadow is a full file copy.** Unlike postgres (schema-only), the DuckDB shadow starts with all prod data. Merged read dedup logic must handle this correctly (shadow wins on PK collision).

---

## P0 — Critical Correctness

### 1.1 ReadyForQuery Transaction State Tracking

**Problem:** `buildReadyForQueryMsg` hardcodes `'I'` (idle) — protocol violation. Client libraries rely on 'T' (in-transaction) and 'E' (error-in-transaction) states.

**Implementation:**
- Add `txnState byte` field to the connection handler
- Track state transitions:
  - After `BEGIN` → set state to `'T'`
  - After query error inside transaction → set state to `'E'`
  - After `COMMIT` / `ROLLBACK` → set state to `'I'`
- In `buildReadyForQueryMsg()`, pass the current state instead of hardcoded `'I'`
- **File:** `proxy/pgmsg.go` (`buildReadyForQueryMsg`), `proxy/conn.go`, `proxy/txn.go`
- **Reference:** `postgres/proxy/conn.go` ReadyForQuery state tracking

### 1.2 Generated Column Detection and Filtering

**Problem:** `buildHydrateInsert` includes all columns. DuckDB's generated columns will crash hydration.

**Implementation:**
- In `schema/schema.go`, add `DetectGeneratedColumns()`:
  ```sql
  SELECT column_name, table_name
  FROM information_schema.columns
  WHERE table_schema = 'main'
    AND is_generated = 'ALWAYS'
  ```
  Or alternatively: `SELECT column_name FROM duckdb_columns() WHERE is_generated`
- Store in `TableMeta.GeneratedCols`
- In `proxy/conn.go` `buildHydrateInsert()`, skip columns in `GeneratedCols`
- **Reference:** `postgres/schema/dumper.go` `DetectGeneratedColumns()`

### 1.3 Schema Adaptation in Merged Reads

**Problem:** After ADD/DROP/RENAME COLUMN, Prod and Shadow have different schemas. `executeMergedRead()` merges results without adapting column sets.

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
  - Renamed columns: revert to Prod names
  - Added columns: strip from Prod query
- Regex-based rewriting (no AST available)
- Call before sending any query to Prod when schema diffs exist
- **Reference:** `postgres/proxy/rewrite_prod.go`

### 1.5 INSERT ... ON CONFLICT / INSERT OR REPLACE Handling

**Problem:** `HasOnConflict` is never set. Upserts route as plain `StrategyShadowWrite`.

**Implementation:**
- In `classify/classifier.go`, detect:
  - `INSERT ... ON CONFLICT` → set `HasOnConflict = true`
  - `INSERT OR REPLACE INTO ...` → set `HasOnConflict = true`
- Router maps `HasOnConflict` on INSERT → `StrategyHydrateAndWrite`
- In `proxy/conn.go`, add upsert handler:
  1. Extract target table and conflict columns
  2. Extract values from INSERT VALUES clause
  3. Query Prod for matching rows
  4. Hydrate matching rows into Shadow
  5. Execute upsert on Shadow
  6. Track affected PKs in delta map
- DuckDB supports `INSERT ... ON CONFLICT` with same syntax as PostgreSQL
- **Reference:** `postgres/proxy/write_insert.go` upsert path

### 1.6 Bulk UPDATE Hydration (PKs Not Extractable)

**Problem:** When PKs can't be extracted from WHERE, UPDATE is forwarded to Shadow without hydration.

**Implementation:**
- In `proxy/conn.go`, when UPDATE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod (with schema rewriting)
  2. Hydrate all matching rows into Shadow (skip delta rows, filter generated columns)
  3. Execute the original UPDATE on Shadow
  4. Add all affected PKs to delta map
- Respect `max_rows_hydrate` cap
- **Reference:** `postgres/proxy/write_update.go` bulk update path

### 1.7 Bulk DELETE Tombstoning (PKs Not Extractable)

**Problem:** When PKs can't be extracted, DELETE only works on Shadow data.

**Implementation:**
- In `proxy/conn.go`, when DELETE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod
  2. Tombstone all matching PKs
  3. Execute DELETE on Shadow
  4. Compute total count: Shadow actual deletes + Prod-only matches
  5. Return corrected row count via synthetic CommandComplete
- **Reference:** `postgres/proxy/write_delete.go` bulk delete path

### 1.8 INSERT PK Tracking (Row-Level)

**Problem:** `MarkInserted(table)` is table-level tracking. Forces imprecise merged reads.

**Implementation:**
- After INSERT on Shadow, capture inserted PKs:
  - For SEQUENCE-backed tables: use DuckDB's `currval('seq_name')` or capture from RETURNING
  - For non-sequence tables: extract PKs from INSERT VALUES clause
- Replace `MarkInserted(table)` with `deltaMap.Add(table, pk)` for each inserted PK
- **Reference:** `postgres/proxy/write_insert.go` PK capture via RETURNING

### 1.9 TRUNCATE Classification and Handling

**Problem:** TRUNCATE not classified, not in `looksLikeWrite`, not handled. This is also a guard rail gap.

**Implementation:**
- In `classify/classifier.go`, detect `TRUNCATE TABLE` → `SubTruncate`
- Add `TRUNCATE` to `writePrefixes` in `guard.go`
- In `proxy/conn.go`, add StrategyTruncate handler:
  1. Forward to Shadow
  2. `schemaReg.MarkFullyShadowed(table)`
  3. Clear delta map and tombstone set for the table
- DuckDB supports `TRUNCATE TABLE` natively
- **Reference:** `postgres/proxy/conn.go` StrategyTruncate

### 1.10 Max Rows Hydration Cap

**Problem:** No upper bound on hydration.

**Implementation:**
- Add `MaxRowsHydrate int` from `engine.ProxyDeps`
- In all Prod queries for hydration/materialization, append `LIMIT {max_rows_hydrate}` when configured
- **Reference:** `postgres/proxy/write_update.go` `capSQL()`

---

## P1 — High Priority

### 2.1 SAVEPOINT State Management

**Problem:** SAVEPOINTs forwarded without state snapshots. ROLLBACK TO SAVEPOINT desyncs delta map.

**Implementation:**
- In `classify/classifier.go`, change SAVEPOINT/RELEASE classification:
  - `SAVEPOINT name` → `SubSavepoint`
  - `RELEASE SAVEPOINT name` → `SubRelease`
  - `ROLLBACK TO SAVEPOINT name` → differentiate from full ROLLBACK
- In `proxy/txn.go`:
  - On SAVEPOINT: Push snapshot of delta map, tombstone set, insert counts, schema registry
  - On RELEASE: Pop snapshot (changes persist)
  - On ROLLBACK TO: Restore from snapshot, keep snapshot for repeat rollbacks
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

**Problem:** Plain `BEGIN` sent to Prod. Reads may be non-repeatable.

**Implementation:**
- DuckDB supports `BEGIN TRANSACTION` and has snapshot isolation by default when using transactions
- The embedded `database/sql` DuckDB driver creates a snapshot at BEGIN time automatically
- Verify that the Prod `*sql.DB` connection is configured for snapshot isolation (default behavior)
- No syntax change needed — DuckDB's MVCC provides REPEATABLE READ equivalent by default
- **Reference:** `postgres/proxy/txn.go`

### 2.4 FK Discovery and Enforcement

**Problem:** Zero FK enforcement. No FK detection at init.

**Implementation:**

**Phase 1 — FK Discovery:**
- In `schema/schema.go`, add `DetectForeignKeys()`:
  ```sql
  SELECT * FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY'
  ```
  Or use `information_schema.referential_constraints` + `information_schema.key_column_usage`
- Store as `schema.ForeignKey` structs in schema registry
- Call during init

**Phase 2 — Parent Row Validation (INSERT/UPDATE):**
- New file: `proxy/fk.go`
- Validate parent row exists: delta check → tombstone check → Shadow query → Prod query
- Composite FK support

**Phase 3 — Referential Action Enforcement (DELETE):**
- RESTRICT / NO ACTION: Reject if child rows exist
- CASCADE: Tombstone child rows recursively
- **Reference:** `postgres/proxy/fk.go`

### 2.5 DROP COLUMN Tracking

**Problem:** `ALTER TABLE ... DROP COLUMN` not parsed or tracked.

**Implementation:**
- In `proxy/conn.go` `trackDDLEffects()`, add detection for `ALTER TABLE t DROP COLUMN c`:
  - Regex: `(?i)ALTER\s+TABLE\s+(\S+)\s+DROP\s+COLUMN\s+(\S+)`
  - Call `schemaReg.RecordDropColumn(table, col)`
- **Reference:** `postgres/proxy/ddl_parse.go`

### 2.6 Schema Rewriting for Prod Queries (Full)

**Problem:** No Prod query rewriting at all.

**Implementation:**
- Apply the `rewriteForProd()` from 1.4 to ALL Prod queries
- Merged reads, hydration SELECTs, bulk UPDATE/DELETE PK discovery
- **Reference:** `postgres/proxy/rewrite_prod.go`

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
- DuckDB temp tables are connection-scoped
- **Reference:** `postgres/proxy/util_table.go`

### 3.2 Full Aggregate Re-Aggregation

**Problem:** Only bare COUNT(*) handled.

**Implementation:**
- New file: `proxy/aggregate.go`
- Simple re-aggregatable: COUNT, SUM, AVG, MIN, MAX with GROUP BY → row-level merge + in-memory re-aggregation
- HAVING filter post re-aggregation
- COUNT(DISTINCT col) support
- For complex aggregates (list(), array_agg(), string_agg()): materialize into temp table, re-execute on Shadow
- DuckDB complex aggregates to detect: `list()`, `array_agg()`, `string_agg()`, `json_group_array()`, `json_group_object()`
- **Reference:** `postgres/proxy/aggregate.go`

### 3.3 Window Function Handling

**Problem:** Not detected or handled.

**Implementation:**
- In classifier, detect: `HasWindowFunc = true` for `OVER (` pattern
- New file: `proxy/read_window.go`:
  1. Build base SELECT (strip window functions)
  2. Merged read pipeline
  3. Materialize into temp table
  4. Rewrite query to reference temp table
  5. Execute on Shadow
- DuckDB has full window function support
- **Reference:** `postgres/proxy/read_window.go`

### 3.4 Set Operation Handling

**Implementation:**
- New file: `proxy/read_setop.go`
- Decompose UNION/INTERSECT/EXCEPT into leaf SELECTs
- Each leaf through merged read independently
- In-memory set operation application
- **Reference:** `postgres/proxy/read_setop.go`

### 3.5 Complex Read Handling (CTEs, Derived Tables, LATERAL)

**Implementation:**
- New file: `proxy/read_complex.go`
- Non-recursive CTEs: materialize dirty CTEs into temp tables, rewrite query
- Recursive CTEs: materialize dirty base tables, rewrite
- Derived tables: recursively rewrite subqueries
- LATERAL joins: DuckDB supports `LATERAL` keyword (same syntax as PostgreSQL)
- **Reference:** `postgres/proxy/read_complex.go`

### 3.6 JOIN Patch Strategy

**Problem:** `StrategyJoinPatch` handled identically to `StrategyMergedRead`.

**Implementation:**
- New file: `proxy/read_join.go`
- Full JOIN patch algorithm:
  1. Identify delta/tombstone tables in JOIN
  2. Execute JOIN on Prod
  3. Classify rows: clean/delta/dead
  4. Patch delta rows from Shadow
  5. Re-evaluate WHERE on patched rows (requires 3.8)
  6. Deduplicate by composite key
- For schema-diff JOINs: materialize dirty tables into temp tables
- **Reference:** `postgres/proxy/read_join.go`

### 3.7 WHERE Clause Evaluator

**Implementation:**
- New file: `proxy/where_eval.go`
- Port from postgres: =, !=, <>, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL, IS NOT NULL, AND, OR, NOT
- Type-aware comparison, NULL handling
- Safe fallback: return true on evaluation failure
- **Reference:** `postgres/proxy/where_eval.go`

---

## P3 — Polish & Edge Cases

### 4.1 Classifier Feature Flag Additions

- `HasReturning`: Detect `RETURNING` clause (DuckDB supports RETURNING)
- `HasOnConflict`: Detect `ON CONFLICT` and `OR REPLACE` patterns (see 1.5)
- `HasWindowFunc`: Detect `OVER (` pattern
- `HasDistinct`: Detect `SELECT DISTINCT`
- `HasComplexAgg`: Detect `list()`, `array_agg()`, `string_agg()`
- `IsMetadataQuery`: Detect queries against `information_schema.*`, `duckdb_tables()`, `duckdb_columns()`
- `SubTruncate`: See 1.9
- `SubSavepoint` / `SubRelease`: See 2.1
- `SubExplain`: Detect `EXPLAIN` and `EXPLAIN ANALYZE` → reject ANALYZE
- `SubSet`: Detect `SET` → StrategyForwardBoth
- `SubShow`: Detect `SHOW` → StrategyProdDirect
- `SubNotSupported`: Block `COPY TO`, `EXPORT DATABASE` (destructive external operations)
- **Reference:** `postgres/classify/classifier.go`

### 4.2 RETURNING Clause Support

- Detect `RETURNING` in classifier → set `HasReturning = true`
- For INSERT with RETURNING: capture returned rows for PK extraction
- For DELETE with RETURNING: capture returned data for client response
- DuckDB supports RETURNING with same syntax as PostgreSQL
- **Reference:** `postgres/proxy/write_insert.go` RETURNING handling

### 4.3 PK Extraction Improvements

- Support `value = column` reversal
- Handle `CAST(value AS type)` and `value::type` unwrapping (DuckDB supports both)
- Composite PK serialization as JSON
- Handle negative numbers, UUID literals
- **Reference:** `postgres/classify/pk.go`

### 4.4 ALTER TYPE Tracking

- Detect `ALTER TABLE t ALTER COLUMN c TYPE newtype` → `RecordTypeChange(table, col, "", newtype)`
- DuckDB uses same syntax as PostgreSQL for ALTER COLUMN TYPE
- **Reference:** `postgres/proxy/ddl_parse.go`

### 4.5 ROWID Injection for PK-less Tables

**Problem:** PK-less tables have no dedup in merged reads.

**Implementation:**
- DuckDB has an implicit `rowid` column for most tables
- Inject `rowid` into SELECT for tables without declared PKs
- Use `rowid` as dedup key in merged read
- **Reference:** PostgreSQL uses `ctid` — DuckDB's `rowid` is equivalent

### 4.6 L2 Guard: SafeProdConn

**Problem:** No L2 write inspection. `access_mode=READ_ONLY` provides protection, but no protocol-level logging.

**Implementation:**
- Wrap the Prod database handle with SafeProdConn equivalent
- Inspect all SQL through `looksLikeWrite()` before passing to Prod
- Log and reject any write SQL that reaches Prod
- **Reference:** `postgres/proxy/guard.go` SafeProdConn

### 4.7 Binary Parameter Decoding

- In `proxy/pgmsg.go` `parseBindMsgPayload()`, decode binary parameters:
  - bool (1 byte), int16 (2 bytes), int32 (4 bytes), int64 (8 bytes), UUID (16 bytes)
- **Reference:** `postgres/proxy/ext_handler.go`

### 4.8 Type OID Mapping

**Problem:** All column OIDs hardcoded to 25 (text).

**Implementation:**
- Map DuckDB column types to PostgreSQL OIDs:
  - INTEGER/BIGINT → 23/20
  - DOUBLE/FLOAT → 701/700
  - VARCHAR/TEXT → 25
  - BOOLEAN → 16
  - DATE → 1082
  - TIMESTAMP → 1114
  - UUID → 2950
  - BLOB → 17
- Use `sql.ColumnType()` or `duckdb_columns()` for type detection
- **Reference:** DuckDB-specific — no direct postgres equivalent

### 4.9 Describe Response Accuracy

**Problem:** All Describe messages return `NoData`.

**Implementation:**
- For Describe('P') (portal): run query with `LIMIT 0` on Shadow to get column metadata, return proper RowDescription
- For Describe('S') (statement): return ParameterDescription
- **Reference:** `postgres/proxy/ext_handler.go`

### 4.10 Statement Cache Shadow-Only Tracking

- Extend `stmtCache` to include classification metadata
- When Execute is called, check if cached statement's tables have deltas/tombstones
- Route through merged read if dirty
- **Reference:** `postgres/proxy/ext_handler.go`

### 4.11 Connection Drain on Errors

- After error in extended protocol, drain messages until Sync ('S')
- Send ErrorResponse + ReadyForQuery
- **Reference:** `postgres/proxy/conn.go`

### 4.12 FK Extraction from DDL

- When DDL contains `ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES`, extract FK metadata
- Store in schema registry for proxy-level enforcement
- **Reference:** `postgres/proxy/ddl.go`

### 4.13 PK-less Table Full-Row Hash Fallback

- For tables explicitly declared as `WITHOUT ROWID` (rare in DuckDB), fall back to full-row hash dedup
- Hash all column values, deduplicate by hash, Shadow wins on collision
- **Reference:** `mysql-update-plan.md` §4.9

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
  1.9 TRUNCATE classification and handling
  1.10 Max rows hydration cap

Phase 2 (P1 — features):
  2.1 SAVEPOINT state management  (depends on: 4.1 SubSavepoint classification)
  2.2 Schema registry snapshot/restore
  2.3 Prod isolation level
  2.4 FK enforcement
  2.5 DROP COLUMN tracking
  2.6 Full Prod query rewriting

Phase 3 (P2 — advanced reads, requires 3.1 first):
  3.1 Temp table infrastructure  (prerequisite)
  3.2 Full aggregate re-aggregation
  3.3 Window functions
  3.4 Set operations
  3.5 Complex reads (CTEs, derived tables, LATERAL)
  3.6 JOIN patch strategy
  3.7 WHERE clause evaluator

Phase 4 (P3 — polish):
  4.1-4.13 in any order
```

---

## Key DuckDB vs PostgreSQL Syntax/Architecture Differences

| Feature | PostgreSQL | DuckDB |
|---------|-----------|--------|
| Architecture | Client-server (pgwire relay) | Embedded (pgwire server + database/sql) |
| Shadow creation | Schema-only (pg_dump) | Full file copy (all data) |
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` | `INSERT ... ON CONFLICT DO UPDATE` (same syntax) |
| RETURNING | `INSERT ... RETURNING *` | `INSERT ... RETURNING *` (same syntax) |
| Temp tables | `CREATE TEMP TABLE name` | `CREATE TEMP TABLE name` (same syntax) |
| TRUNCATE | `TRUNCATE TABLE name` | `TRUNCATE TABLE name` (same syntax) |
| Type cast | `value::type` | `value::type` or `CAST(value AS type)` (both work) |
| Boolean | `true` / `false` | `true` / `false` (same syntax) |
| Identity | `GENERATED ALWAYS AS IDENTITY` | `GENERATED ALWAYS AS IDENTITY` (same syntax) |
| Generated cols | `pg_attribute.attgenerated` | `information_schema.columns.is_generated` or `duckdb_columns()` |
| FK discovery | `pg_catalog` | `duckdb_constraints()` or `information_schema` |
| Quoting | `"identifier"` | `"identifier"` (same syntax) |
| PK-less row ID | `ctid` (system column) | `rowid` (implicit) |
| Isolation on BEGIN | `BEGIN ISOLATION LEVEL REPEATABLE READ` | `BEGIN TRANSACTION` (snapshot isolation by default) |
| LATERAL | `LATERAL (subquery)` | `LATERAL (subquery)` (same syntax) |
| String concat | `'a' \|\| 'b'` | `'a' \|\| 'b'` (same syntax) |
| Window functions | Full support | Full support (same syntax) |
| CTEs | Full support | Full support (same syntax) |
| ALTER TABLE | Full support | Full support (same syntax as PostgreSQL) |
| Complex aggregates | `array_agg`, `string_agg`, `json_agg` | `list()`, `array_agg()`, `string_agg()` |

**Key insight:** DuckDB's SQL dialect is nearly identical to PostgreSQL. Most postgres patterns can be ported with minimal syntax changes. The main differences are architectural (embedded vs client-server) and in the system catalog queries.
