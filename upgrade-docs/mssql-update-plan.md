# MSSQL Engine Update Plan

This document is the implementation plan for bringing the MSSQL engine to full feature parity with the PostgreSQL engine. Agents implementing these changes should **always refer to the PostgreSQL implementation** as the reference, and secondarily the MySQL engine (which shares the network-proxy architecture pattern). Adapt for TDS protocol and T-SQL syntax.

---

## Guiding Principles

1. **Mirror the PostgreSQL patterns.** Same strategies: hydrate-then-execute, materialize-via-temp-table, dual-execute + merge. Adapt for T-SQL syntax.
2. **Let SQL Server do the heavy lifting.** For complex SQL (window functions, complex aggregates, CTEs), materialize merged data into a temp table and re-execute on Shadow.
3. **Prod safety is non-negotiable.** The TDS three-layer guard is already solid — extend it, don't weaken it.
4. **Regex classifier limitations are accepted.** No T-SQL AST parser exists in Go. Improve regex accuracy where possible, but accept inherent limitations.

---

## P0 — Critical Correctness

### 1.1 Generated/Computed Column Detection and Filtering

**Problem:** `hydrateRow` does `SELECT *` + `INSERT ... VALUES(all_columns)` — will crash on tables with computed columns.

**Implementation:**
- In `schema/schema.go`, add `DetectComputedColumns()`:
  ```sql
  SELECT c.name AS column_name, t.name AS table_name
  FROM sys.computed_columns c
  JOIN sys.tables t ON c.object_id = t.object_id
  WHERE SCHEMA_NAME(t.schema_id) = 'dbo'
  ```
- Store in `TableMeta.GeneratedCols`
- In `proxy/write.go` `hydrateRow()`, skip columns in `GeneratedCols` when building INSERT
- **Reference:** `postgres/schema/dumper.go` `DetectGeneratedColumns()`

### 1.2 Bulk UPDATE Hydration (PKs Not Extractable)

**Problem:** When PKs can't be extracted from WHERE, UPDATE silently misses Prod-only rows.

**Implementation:**
- In `proxy/write.go`, when UPDATE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod (via `execQueryProd`)
  2. Hydrate all matching rows into Shadow (skip delta rows, filter computed columns)
  3. Execute the original UPDATE on Shadow
  4. Add all affected PKs to delta map
- Handle IDENTITY_INSERT: wrap hydration in `SET IDENTITY_INSERT table ON` / `OFF`
- Respect max_rows_hydrate cap
- **Reference:** `postgres/proxy/write_update.go` bulk update path, `mysql-update-plan.md` §1.2

### 1.3 Bulk DELETE Tombstoning (PKs Not Extractable)

**Problem:** DELETE without extractable PKs silently misses Prod-only rows.

**Implementation:**
- In `proxy/write.go`, when DELETE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod
  2. Tombstone all matching PKs
  3. Execute DELETE on Shadow
  4. Compute total count: Shadow actual deletes + Prod-only matches
  5. Return corrected row count via synthetic DONE token
- **Reference:** `postgres/proxy/write_delete.go` bulk delete path

### 1.4 INSERT PK Tracking (Row-Level)

**Problem:** `handleInsert` uses `MarkInserted(table)` (table-level) instead of row-level tracking, forcing merged reads for all rows.

**Implementation:**
- After INSERT on Shadow, capture inserted PKs via:
  - For IDENTITY tables: query `SCOPE_IDENTITY()` or capture from OUTPUT clause
  - For non-IDENTITY tables: extract PKs from the INSERT VALUES clause
- Replace `MarkInserted(table)` with `deltaMap.Add(table, pk)` for each inserted PK
- Detect OUTPUT clause in classifier (see 4.1) for multi-row inserts
- **Reference:** `postgres/proxy/write_insert.go` PK capture via RETURNING

### 1.5 MERGE-as-Upsert Handling

**Problem:** MERGE INTO ... WHEN NOT MATCHED THEN INSERT WHEN MATCHED THEN UPDATE is MSSQL's upsert pattern. Not detected or handled.

**Implementation:**
- In `classify/classifier.go`, detect MERGE statements and set `HasOnConflict = true` when the MERGE contains both WHEN MATCHED and WHEN NOT MATCHED clauses
- Router maps `HasOnConflict` on INSERT → `StrategyHydrateAndWrite`
- In `proxy/write.go`, add MERGE handler:
  1. Extract the target table and join condition columns from the MERGE
  2. Extract values from the USING clause
  3. Query Prod for matching rows
  4. Hydrate matching rows into Shadow
  5. Execute MERGE on Shadow
  6. Track affected PKs in delta map
- **Reference:** `postgres/proxy/write_insert.go` upsert path, `mysql-update-plan.md` §1.4

### 1.6 Max Rows Hydration Cap

**Problem:** No upper bound on hydration.

**Implementation:**
- Add `MaxRowsHydrate int` from `engine.ProxyDeps`
- In all Prod queries for hydration/materialization, append `TOP {max_rows_hydrate}` when configured
- **Reference:** `postgres/proxy/write_update.go` `capSQL()`

---

## P1 — High Priority

### 2.1 TRUNCATE Handling

**Problem:** TRUNCATE classified as SubOther with no special routing.

**Implementation:**
- In `classify/classifier.go`, detect `TRUNCATE TABLE` → `SubTruncate`
- Add `TRUNCATE` to `writePrefixes` in `guard.go`
- In `proxy/conn.go`, add StrategyTruncate handler:
  1. Forward to Shadow
  2. `schemaReg.MarkFullyShadowed(table)`
  3. Clear delta map and tombstone set for the table
- **Reference:** `postgres/proxy/conn.go` StrategyTruncate

### 2.2 SAVEPOINT State Management

**Problem:** No `SAVE TRAN` / `ROLLBACK TRAN savepointname` support.

**Implementation:**
- In `classify/classifier.go`, detect:
  - `SAVE TRAN[SACTION] name` → `SubSavepoint`
  - `ROLLBACK TRAN[SACTION] name` (partial rollback) → `SubSavepoint` (differentiated by rollback flag)
- In `proxy/txn.go`:
  - On SAVE TRAN: Push snapshot of delta map, tombstone set, insert counts, schema registry
  - On ROLLBACK TRAN name: Restore from snapshot, keep snapshot for repeat rollbacks
  - On full COMMIT: Flush all, clear stack
  - On full ROLLBACK: Discard all, clear stack
- Note: MSSQL has no RELEASE SAVEPOINT — savepoints are implicitly released on COMMIT
- **Reference:** `postgres/proxy/txn.go` savepoint system

### 2.3 Schema Registry Snapshot on BEGIN / Restore on ROLLBACK

**Problem:** DDL within a rolled-back transaction leaves schema registry dirty.

**Implementation:**
- In `proxy/txn.go` `handleBegin()`: `schemaReg.SnapshotAll()` and store
- In `proxy/txn.go` `handleRollback()`: `schemaReg.RestoreAll(snapshot)`
- **Reference:** `postgres/proxy/txn.go`

### 2.4 Prod Isolation Level on BEGIN

**Problem:** Plain `BEGIN TRAN` sent to Prod — reads may be non-repeatable.

**Implementation:**
- Before forwarding BEGIN to Prod, send `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` (if available) or `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
- SNAPSHOT isolation is preferred (non-blocking reads) but requires database-level enablement
- Fall back to REPEATABLE READ if SNAPSHOT not available
- **Reference:** `postgres/proxy/txn.go` `BEGIN ISOLATION LEVEL REPEATABLE READ`

### 2.5 FK Enforcement

**Problem:** No FK enforcement at all.

**Implementation:**

**Phase 1 — FK Discovery:**
- In `schema/schema.go`, add `DetectForeignKeys()`:
  ```sql
  SELECT
    fk.name AS constraint_name,
    tp.name AS child_table,
    cp.name AS child_column,
    tr.name AS parent_table,
    cr.name AS parent_column,
    fk.delete_referential_action_desc AS delete_rule,
    fk.update_referential_action_desc AS update_rule
  FROM sys.foreign_keys fk
  JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
  JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
  JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
  JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
  JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
  ORDER BY fk.name, fkc.constraint_column_id
  ```
- Store in schema registry

**Phase 2 — Parent Row Validation (INSERT/UPDATE):**
- New file: `proxy/fk.go`
- Validate parent row exists: delta check → tombstone check → Shadow query → Prod query
- Composite FK support

**Phase 3 — Referential Action Enforcement (DELETE):**
- RESTRICT / NO ACTION: Reject if child rows exist
- CASCADE: Tombstone child rows recursively
- **Reference:** `postgres/proxy/fk.go`

### 2.6 Schema Rewriting for Prod Queries

**Problem:** Only new-column stripping exists. No dropped/renamed column handling.

**Implementation:**
- Expand `stripNewColumnsFromQuery` into full `rewriteForProd()`:
  - Dropped columns: remove from SELECT, WHERE, ORDER BY, GROUP BY, HAVING
  - Renamed columns: revert to Prod names (handle `sp_rename` tracked changes)
  - Added columns: strip from Prod query, inject NULL in merge
- **Reference:** `postgres/proxy/rewrite_prod.go`

### 2.7 RENAME Column Tracking via sp_rename

**Problem:** `EXEC sp_rename 't.old_col', 'new_col', 'COLUMN'` is classified as OpOther — never reaches DDL handler.

**Implementation:**
- In `classify/classifier.go`, detect `sp_rename` calls with `'COLUMN'` as third argument → classify as `OpDDL/SubAlter`
- In DDL handler, parse the sp_rename arguments to extract table, old name, new name
- Call `schemaReg.RecordRenameColumn(table, oldName, newName)`
- **Reference:** `postgres/proxy/ddl_parse.go` rename handling

---

## P2 — Advanced Read Paths

### 3.1 Temp Table Materialization Infrastructure

**Prerequisite for 3.2-3.6.**

**Implementation:**
- New file: `proxy/util_table.go`
- `utilTableName(sql string) string` → `#_mori_util_{md5_prefix}` (MSSQL uses `#` prefix for temp tables)
- `buildCreateTempTableSQL(columns, types)` → `CREATE TABLE #_mori_util_{hash} (col1 type1, ...)`
- `bulkInsertToUtilTable(table, rows, columns)` → batch INSERT
- `dropUtilTable(table)` → `DROP TABLE #_mori_util_{hash}`
- Note: MSSQL temp tables use `#` prefix and are session-scoped
- **Reference:** `postgres/proxy/util_table.go`

### 3.2 Full Aggregate Re-Aggregation

**Problem:** Only bare COUNT(*) handled.

**Implementation:**
- Expand aggregate handler in `proxy/read.go`:
  - Simple re-aggregatable: COUNT, SUM, AVG, MIN, MAX with GROUP BY → row-level merge + in-memory re-aggregation
  - HAVING filter post re-aggregation
  - COUNT(DISTINCT col) support
- For complex aggregates (STRING_AGG, etc.): materialize into temp table, re-execute on Shadow
- **Reference:** `postgres/proxy/aggregate.go`

### 3.3 Window Function Handling

**Problem:** Not handled — falls through to Prod.

**Implementation:**
- Detect window functions in classifier: `HasWindowFunc = true` for `OVER (` pattern
- New file: `proxy/read_window.go`:
  1. Build base SELECT (strip window functions)
  2. Merged read pipeline
  3. Materialize into temp table
  4. Rewrite query to reference temp table
  5. Execute on Shadow
- **Reference:** `postgres/proxy/read_window.go`

### 3.4 Set Operation Handling

**Implementation:**
- New file: `proxy/read_setop.go`
- Decompose UNION/INTERSECT/EXCEPT into leaf SELECTs
- Each leaf through merged read independently
- In-memory set operation application
- **Reference:** `postgres/proxy/read_setop.go`

### 3.5 Complex Read Handling (CTEs, Derived Tables, CROSS/OUTER APPLY)

**Implementation:**
- New file: `proxy/read_complex.go`
- Non-recursive CTEs: materialize dirty CTEs into temp tables, rewrite query
- Recursive CTEs: materialize dirty base tables, rewrite
- Derived tables: recursively rewrite subqueries
- CROSS APPLY / OUTER APPLY: materialize dirty base tables (MSSQL-specific syntax for LATERAL)
- **Reference:** `postgres/proxy/read_complex.go`

### 3.6 JOIN Patch Strategy

**Problem:** Falls back to Prod passthrough.

**Implementation:**
- Replace stub in `proxy/read.go` with full JOIN patch algorithm:
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

**Problem:** T-SQL cursor RPCs (sp_cursoropen, sp_cursorfetch) not handled.

**Implementation:**
- In `proxy/ext_handler.go`, handle cursor-related proc IDs (1-9):
  - sp_cursoropen: Classify inner query, if affected → materialize via merged read, open cursor on Shadow temp table
  - sp_cursorfetch: Forward to appropriate backend
  - sp_cursorclose: Forward to both, clean up temp table
- **Reference:** `postgres/proxy/cursor.go`

---

## P3 — Polish & Edge Cases

### 4.1 Classifier Feature Flag Additions

- `HasReturning`: Detect `OUTPUT` clause in INSERT/UPDATE/DELETE
- `HasOnConflict`: Detect MERGE-as-upsert (see 1.5)
- `HasWindowFunc`: Detect `OVER (` pattern
- `HasDistinct`: Detect `SELECT DISTINCT`
- `HasComplexAgg`: Detect STRING_AGG, separate from simple aggregates
- `IsMetadataQuery`: Detect queries against `sys.*`, `INFORMATION_SCHEMA.*`
- `SubTruncate`: See 2.1
- `SubSavepoint`: See 2.2
- `SubExplain`: Detect `SET SHOWPLAN_ALL ON` or `SET STATISTICS PROFILE ON` patterns
- `SubNotSupported`: Block EXPLAIN ANALYZE equivalent (`SET STATISTICS IO ON` + actual execution)
- **Reference:** `postgres/classify/classifier.go`

### 4.2 PK Extraction Improvements

- Support `value = column` reversal
- Handle `CAST(value AS type)` / `CONVERT(type, value)` unwrapping
- Composite PK serialization as JSON
- Handle negative numbers and GUID literals `{...}`
- **Reference:** `postgres/classify/pk.go`

### 4.3 ALTER TYPE Tracking

- Detect `ALTER TABLE t ALTER COLUMN c newtype` → `RecordTypeChange(table, col, "", newtype)`
- **Reference:** `postgres/proxy/ddl_parse.go`

### 4.4 FK Extraction from DDL

- When DDL contains `ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES`, extract FK metadata before stripping
- Store in schema registry for proxy-level enforcement
- **Reference:** `postgres/proxy/ddl.go` FK extraction before stripping

### 4.5 StrategyNotSupported Handler

- Detect and block with clear error messages:
  - BULK INSERT (analogous to COPY)
  - OPENROWSET / OPENDATASOURCE (external data access)
  - xp_cmdshell (system command execution)
  - sp_send_dbmail (email — destructive)
- **Reference:** `postgres/classify/classifier.go` SubNotSupported

### 4.6 Synthetic Response via Raw TDS

**Problem:** Merged read uses `buildSyntheticSelect` (VALUES query through Prod), causing type loss and double-encoding.

**Implementation:**
- Switch to `buildTDSSelectResponse` (already exists at `tdsquery.go:1106`) for merged read results
- Build raw TDS COLMETADATA + ROW tokens directly
- Preserves original column types, avoids Prod round-trip
- **Reference:** This is MSSQL-specific — no postgres equivalent

### 4.7 RPC Write Guard (L2)

- Extend SafeProdConn to also inspect RPC (type 0x03) packets
- Extract SQL from sp_executesql RPC payloads and run through `looksLikeWrite()`
- **Reference:** `postgres/proxy/guard.go` extended protocol inspection

### 4.8 Parameter Substitution for sp_execute

- When sp_execute is called, extract parameter values from RPC payload
- Substitute into cached SQL for PK extraction via `ClassifyWithParams`
- **Reference:** `postgres/proxy/ext_handler.go` parameter substitution

### 4.9 PK-less Table Handling

- MSSQL has no ctid equivalent
- Use full-row hash comparison for dedup (same approach as MySQL plan)
- Hash all column values, deduplicate by hash, Shadow wins on collision
- **Reference:** `mysql-update-plan.md` §4.9

### 4.10 IDENTITY_INSERT Concurrency

- SQL Server only allows one table with IDENTITY_INSERT ON per session
- When hydrating multiple tables sequentially, properly toggle ON/OFF between tables
- Add error handling for "IDENTITY_INSERT is already ON for table X"

### 4.11 OUTPUT Clause Capture

- For INSERT/UPDATE/DELETE with OUTPUT clause, capture returned data for:
  - PK extraction (row-level delta tracking)
  - Return to client (DELETE with OUTPUT is MSSQL's RETURNING equivalent)
- Parse OUTPUT token from Shadow response
- **Reference:** `postgres/proxy/write_insert.go` RETURNING handling

---

## Implementation Order

```
Phase 1 (P0 — correctness):
  1.1 Computed column detection
  1.2 Bulk UPDATE hydration
  1.3 Bulk DELETE tombstoning
  1.4 INSERT PK tracking
  1.5 MERGE-as-upsert handling
  1.6 Max rows hydration cap

Phase 2 (P1 — features):
  2.1 TRUNCATE handling
  2.2 SAVEPOINT state management  (depends on: 4.1 SubSavepoint classification)
  2.3 Schema registry snapshot/restore
  2.4 Prod isolation level
  2.5 FK enforcement
  2.6 Full Prod query rewriting
  2.7 sp_rename column tracking

Phase 3 (P2 — advanced reads, requires 3.1 first):
  3.1 Temp table infrastructure  (prerequisite)
  3.2 Full aggregate re-aggregation
  3.3 Window functions
  3.4 Set operations
  3.5 Complex reads (CTEs, APPLY, derived tables)
  3.6 JOIN patch strategy
  3.7 WHERE clause evaluator
  3.8 Cursor operations

Phase 4 (P3 — polish):
  4.1-4.11 in any order
```

---

## Key MSSQL vs PostgreSQL Syntax Differences

| Feature | PostgreSQL | MSSQL |
|---------|-----------|-------|
| Upsert | `INSERT ... ON CONFLICT DO UPDATE` | `MERGE INTO ... WHEN MATCHED/NOT MATCHED` |
| RETURNING | `INSERT ... RETURNING *` | `INSERT ... OUTPUT inserted.*` |
| Temp tables | `CREATE TEMP TABLE name` | `CREATE TABLE #name` (session-scoped via `#` prefix) |
| LIMIT | `LIMIT N` | `TOP N` or `OFFSET/FETCH` |
| Type cast | `value::type` | `CAST(value AS type)` or `CONVERT(type, value)` |
| Boolean | `true` / `false` | `1` / `0` |
| Identity | `GENERATED ALWAYS AS IDENTITY` | `IDENTITY(1,1)` |
| Computed cols | `pg_attribute.attgenerated` | `sys.computed_columns` |
| FK discovery | `pg_catalog` | `sys.foreign_keys` + `sys.foreign_key_columns` |
| Quoting | `"identifier"` | `[identifier]` |
| String concat | `'a' \|\| 'b'` | `'a' + 'b'` or `CONCAT('a','b')` |
| Savepoint | `SAVEPOINT name` / `RELEASE name` | `SAVE TRAN name` (no RELEASE) |
| Rename column | `ALTER TABLE t RENAME COLUMN old TO new` | `EXEC sp_rename 't.old', 'new', 'COLUMN'` |
| LATERAL | `LATERAL (subquery)` | `CROSS APPLY` / `OUTER APPLY` |
| Isolation BEGIN | `BEGIN ISOLATION LEVEL REPEATABLE READ` | `SET TRANSACTION ISOLATION LEVEL SNAPSHOT; BEGIN TRAN` |
