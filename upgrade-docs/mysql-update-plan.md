# MySQL Engine Update Plan

This document is the implementation plan for bringing the MySQL engine to full feature parity with the PostgreSQL engine. Agents implementing these changes should **always refer to the PostgreSQL implementation** as the reference when considering how to handle something.

---

## Guiding Principles

1. **Mirror the PostgreSQL implementation patterns.** The postgres engine is the reference. Reuse the same strategies (hydrate-then-execute, materialize-via-temp-table, dual-execute + merge) adapted for MySQL syntax.
2. **Let MySQL do the heavy lifting.** For complex SQL semantics (window functions, complex aggregates, recursive CTEs), materialize merged data into a temp table and re-execute the original query on Shadow. Don't reimplement SQL semantics in Go.
3. **Prod safety is non-negotiable.** Never mutate Prod. Block by default, allow only when proven safe.
4. **"Not supported" must be loud.** Every unsupported feature must be caught by the classifier and return a clear error to the client.

---

## Table of Contents

1. [P0 — Critical Correctness](#p0--critical-correctness)
2. [P1 — High Priority](#p1--high-priority)
3. [P2 — Advanced Read Paths](#p2--advanced-read-paths)
4. [P3 — Polish & Edge Cases](#p3--polish--edge-cases)

---

## P0 — Critical Correctness

These cause **wrong data** or **crashes** in the current implementation. Fix first.

### 1.1 Generated Column Detection and Filtering

**Problem:** Tables with `GENERATED ALWAYS AS ... STORED` columns crash during hydration because MySQL rejects explicit INSERT values for generated columns.

**Resolution:** Detect generated columns during initialization and filter them from hydration INSERTs.

**Implementation:**
- In `schema/schema.go`, add `DetectGeneratedColumns()` that queries:
  ```sql
  SELECT TABLE_NAME, COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = ? AND EXTRA LIKE '%GENERATED%'
  ```
- Store in `TableMeta.GeneratedCols` (same field as postgres)
- In `proxy/write.go` `buildMySQLInsertSQL()`, skip columns that appear in `GeneratedCols`
- **Reference:** `postgres/schema/dumper.go` `DetectGeneratedColumns()`, `postgres/proxy/write_insert.go` generated column filtering

### 1.2 Bulk UPDATE Hydration (PKs Not Extractable)

**Problem:** When PKs can't be extracted from WHERE, UPDATE is forwarded to Shadow without hydration. Affected Prod-only rows are never updated (0 rows affected).

**Resolution:** Query Prod for matching PKs, hydrate all matching rows, then execute UPDATE on Shadow.

**Implementation:**
- In `proxy/write.go`, when UPDATE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod (with schema rewriting for Prod compatibility)
  2. Hydrate all matching rows into Shadow (skip rows already in delta map)
  3. Execute the original UPDATE on Shadow
  4. Add all affected PKs to delta map
- Respect `max_rows_hydrate` cap (see 1.7)
- **Reference:** `postgres/proxy/write_update.go` bulk update path (search for "bulk" or "no PKs")

### 1.3 Bulk DELETE Tombstoning (PKs Not Extractable)

**Problem:** When PKs can't be extracted from WHERE, DELETE is forwarded to Shadow without tombstoning. Deleted Prod rows reappear in subsequent merged reads.

**Resolution:** Pre-query Prod for matching PKs, tombstone them, then execute DELETE on Shadow.

**Implementation:**
- In `proxy/write.go`, when DELETE has no extractable PKs:
  1. Run `SELECT pk_columns FROM table WHERE <same conditions>` on Prod
  2. Add all matching PKs to tombstone set
  3. Execute DELETE on Shadow
  4. Compute total count: Shadow actual deletes + Prod-only matches
  5. Return corrected row count to client
- **Reference:** `postgres/proxy/write_delete.go` bulk delete path

### 1.4 INSERT ... ON DUPLICATE KEY UPDATE Handling

**Problem:** MySQL's upsert syntax is not detected or handled. Upserts against Prod rows fail on Shadow (row not present for the UPDATE path).

**Resolution:** Mirror postgres ON CONFLICT handling exactly — detect conflict, pre-hydrate, execute on Shadow.

**Implementation:**
- In `classify/classifier.go` `classifyInsert()`, check `ins.OnDup != nil` from the Vitess AST. Set `HasOnConflict = true` on the Classification.
- This causes the Router to return `StrategyHydrateAndWrite` instead of `StrategyShadowWrite`
- In `proxy/write.go`, add an upsert handler:
  1. Extract the key columns from the INSERT's column list + table's unique keys (PK + any UNIQUE indexes)
  2. Extract the values being inserted
  3. Check Prod for rows matching those key values (`SELECT pk_cols FROM table WHERE key = value`)
  4. Hydrate matching Prod rows into Shadow (skip rows already in delta)
  5. Execute the original INSERT ... ON DUPLICATE KEY UPDATE on Shadow
  6. Track all affected PKs in delta map (from both hydrated rows and newly inserted rows)
- For non-PK unique key conflicts (e.g., `UNIQUE(email)`), discover unique indexes during init via `INFORMATION_SCHEMA.STATISTICS WHERE NON_UNIQUE = 0`
- **Reference:** `postgres/proxy/write_insert.go` upsert path

### 1.5 Hydration Fix: Use INSERT ... ON DUPLICATE KEY UPDATE

**Problem:** Current hydration uses `INSERT IGNORE` which silently drops rows that violate constraints, masking real issues.

**Resolution:** Switch to `INSERT ... ON DUPLICATE KEY UPDATE` for hydration. This ensures the row exists in Shadow without error and updates if already present.

**Implementation:**
- In `proxy/write.go` `buildMySQLInsertSQL()`, change from `INSERT IGNORE INTO ...` to `INSERT INTO ... ON DUPLICATE KEY UPDATE col1=VALUES(col1), col2=VALUES(col2), ...` for all non-generated, non-PK columns.
- This is idempotent — safe to call multiple times for the same row.
- **Reference:** `postgres/proxy/write_insert.go` `buildInsertSQL()` (postgres uses plain INSERT since it hydrates only non-delta rows)

### 1.6 Composite PK Handling in Hydration

**Problem:** `hydrateRow` uses only `meta.PKColumns[0]`, ignoring composite primary keys.

**Resolution:** Support multi-column PK lookup in hydration.

**Implementation:**
- In `proxy/write.go` `hydrateRow()`, build WHERE clause using ALL PK columns: `WHERE pk1 = ? AND pk2 = ? AND ...`
- Serialize composite PK as JSON for delta map keys (same as postgres)
- **Reference:** `postgres/proxy/write_update.go` composite PK hydration

### 1.7 Max Rows Hydration Cap

**Problem:** No upper bound on hydration. Large tables can cause unbounded data movement.

**Resolution:** Add `max_rows_hydrate` config option, apply across all hydration and materialization paths.

**Implementation:**
- Add `MaxRowsHydrate int` to `engine.ProxyDeps` (already exists in core)
- In all Prod queries for hydration/materialization, append `LIMIT {max_rows_hydrate}` when configured
- Apply to: bulk UPDATE hydration, bulk DELETE PK discovery, upsert hydration, temp table materialization
- **Reference:** `postgres/proxy/write_update.go` `capSQL()`, ProxyDeps.MaxRowsHydrate

---

## P1 — High Priority

Significant feature gaps affecting transactional correctness and common operations.

### 2.1 TRUNCATE Handling

**Problem:** TRUNCATE is classified as SubOther with no special routing. Table state is not tracked.

**Resolution:** Classify TRUNCATE, route via StrategyTruncate, mark table as fully shadowed.

**Implementation:**
- In `classify/classifier.go`, detect `*sqlparser.TruncateTable` and return `SubTruncate`
- Router already maps SubTruncate → StrategyTruncate
- In `proxy/conn.go`, add StrategyTruncate handler:
  1. Forward TRUNCATE to Shadow
  2. Call `schemaReg.MarkFullyShadowed(table)`
  3. Clear delta map and tombstone set for the table
  4. Handle CASCADE: if MySQL TRUNCATE cascades (it doesn't by default — MySQL requires FK checks disabled), track cascaded tables
- **Reference:** `postgres/proxy/conn.go` StrategyTruncate handling

### 2.2 SAVEPOINT State Management

**Problem:** SAVEPOINTs are forwarded without state snapshots. ROLLBACK TO SAVEPOINT desyncs delta map from Shadow.

**Resolution:** Stack-based snapshots matching postgres exactly.

**Implementation:**
- In `proxy/txn.go`:
  - On SAVEPOINT: Push snapshot of delta map (`SnapshotAll()`), tombstone set (`SnapshotAll()`), insert counts (`SnapshotInsertedTables()`), and schema registry (`SnapshotAll()`) onto a stack
  - On ROLLBACK TO SAVEPOINT: Restore from stack snapshot, keep snapshot for repeat rollbacks
  - On RELEASE SAVEPOINT: Pop snapshot from stack (changes persist)
  - On full COMMIT: Flush all state, clear stack
  - On full ROLLBACK: Discard all, clear stack
- Requires adding SubSavepoint and SubRelease to classifier (see 4.1)
- **Reference:** `postgres/proxy/txn.go` savepoint system — copy the pattern directly

### 2.3 Schema Registry Snapshot on BEGIN / Restore on ROLLBACK

**Problem:** DDL within a transaction followed by ROLLBACK leaves schema registry dirty.

**Resolution:** Snapshot schema registry on BEGIN, restore on ROLLBACK.

**Implementation:**
- In `proxy/txn.go` `handleBegin()`: Call `schemaReg.SnapshotAll()` and store
- In `proxy/txn.go` `handleRollback()`: Call `schemaReg.RestoreAll(snapshot)`
- **Reference:** `postgres/proxy/txn.go` `handleBegin()` and `handleRollback()`

### 2.4 FK Enforcement

**Problem:** No FK enforcement at all. INSERTs with invalid parent references succeed, CASCADE doesn't fire, RESTRICT doesn't enforce.

**Resolution:** Implement full FK enforcement at the proxy layer, matching postgres.

**Implementation:**

**Phase 1 — FK Discovery:**
- In `schema/schema.go`, add `DetectForeignKeys()`:
  ```sql
  SELECT
    kcu.CONSTRAINT_NAME,
    kcu.TABLE_NAME AS child_table,
    kcu.COLUMN_NAME AS child_column,
    kcu.REFERENCED_TABLE_NAME AS parent_table,
    kcu.REFERENCED_COLUMN_NAME AS parent_column,
    rc.DELETE_RULE,
    rc.UPDATE_RULE
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
    AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
  WHERE kcu.TABLE_SCHEMA = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
  ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
  ```
- Store as `schema.ForeignKey` structs in schema registry
- Call during init and on ALTER TABLE ADD CONSTRAINT

**Phase 2 — Parent Row Validation (INSERT/UPDATE):**
- New file: `proxy/fk.go`
- On INSERT/UPDATE that sets FK columns, validate parent row exists:
  1. Delta map check: parent PK in delta → exists in Shadow ✓
  2. Tombstone check: parent PK tombstoned → REJECT
  3. Shadow query: check Shadow for parent row
  4. Prod query: fall back to Prod
- Support composite FKs (multi-column)

**Phase 3 — Referential Action Enforcement (DELETE):**
- RESTRICT / NO ACTION: Reject parent delete if child rows exist (check Shadow + Prod)
- CASCADE: Tombstone child rows when parent deleted, recursively cascade
- **Reference:** `postgres/proxy/fk.go` — follow the same structure exactly

### 2.5 Schema Rewriting for Prod During Hydration

**Problem:** Hydration queries Prod with `SELECT *` which fails if columns have been renamed or dropped via DDL.

**Resolution:** Rewrite Prod hydration queries to account for schema diffs.

**Implementation:**
- Before querying Prod for hydration, check schema registry for the table
- If dropped columns exist: remove them from SELECT list
- If renamed columns exist: use old (Prod) names in SELECT, map back to new names for Shadow INSERT
- **Reference:** `postgres/proxy/rewrite_prod.go`

---

## P2 — Advanced Read Paths

These require the temp table materialization infrastructure as a prerequisite.

### 3.1 Temp Table Materialization Infrastructure

**Problem:** No temp table management. This is a prerequisite for window functions, set ops, complex reads, and complex aggregates.

**Resolution:** Build MySQL-equivalent utility functions.

**Implementation:**
- New file: `proxy/util_table.go`
- `utilTableName(sql string) string` — deterministic temp table name from SQL hash: `_mori_util_{md5_prefix}`
- `buildCreateTempTableSQL(columns []string, types []string) string` — `CREATE TEMPORARY TABLE _mori_util_{hash} (col1 type1, col2 type2, ...)`
- `bulkInsertToUtilTable(table string, rows [][]string, columns []string) error` — batch INSERT into temp table
- `dropUtilTable(table string) error` — `DROP TEMPORARY TABLE IF EXISTS _mori_util_{hash}`
- `buildScopedMaterializationSQL(baseQuery string, predicates string) string` — pushdown WHERE into materialization
- MySQL temp tables use `CREATE TEMPORARY TABLE` (session-scoped, auto-dropped on disconnect)
- **Reference:** `postgres/proxy/util_table.go` — adapt syntax for MySQL

### 3.2 Full Aggregate Re-Aggregation

**Problem:** Only bare COUNT(*) is handled. No SUM/AVG/MIN/MAX, no GROUP BY, no HAVING.

**Resolution:** Implement row-level merge + in-memory re-aggregation matching postgres.

**Implementation:**
- New file: `proxy/aggregate.go`
- For simple re-aggregatable aggregates (COUNT, SUM, AVG, MIN, MAX with GROUP BY):
  1. Build row-level base query (strip aggregates, keep GROUP BY columns + referenced columns)
  2. Run through merged read pipeline
  3. Re-aggregate in Go (in-memory grouping + function application)
  4. Apply HAVING filter
  5. Support COUNT(DISTINCT col)
- For complex aggregates (GROUP_CONCAT, JSON_ARRAYAGG, JSON_OBJECTAGG):
  1. Materialize base data into temp table (requires 3.1)
  2. Re-execute original query against temp table on Shadow
- MySQL-specific complex aggregates to detect: `GROUP_CONCAT`, `JSON_ARRAYAGG`, `JSON_OBJECTAGG`
- **Reference:** `postgres/proxy/aggregate.go` — same algorithm, different aggregate function names

### 3.3 Window Function Handling

**Problem:** Window functions not detected or handled. Queries produce incorrect results for affected tables.

**Resolution:** Materialize + Shadow re-execution pattern.

**Implementation:**
- In classifier, detect window functions: functions with `OVER (...)` clause. Set `HasWindowFunc = true`.
- New handler in `proxy/read.go` (or new file `proxy/read_window.go`):
  1. Build base SELECT (same FROM/WHERE, no window functions)
  2. Run base through merged read pipeline
  3. Materialize into temp table on Shadow (requires 3.1)
  4. Rewrite original query to read from temp table
  5. Execute on Shadow, return results
  6. Drop temp table
- **Reference:** `postgres/proxy/read_window.go`

### 3.4 Set Operation Handling (UNION/INTERSECT/EXCEPT)

**Problem:** Set operations routed through standard merged read with no decomposition.

**Resolution:** Decompose into leaf SELECTs, merge each independently, apply set operation in memory.

**Implementation:**
- New file: `proxy/read_setop.go`
- Recursively decompose UNION/INTERSECT/EXCEPT into leaf SELECT nodes using Vitess AST
- Execute each leaf through merged read pipeline independently
- Apply set operation in memory:
  - UNION: combine + deduplicate by full-row equality
  - UNION ALL: concatenate
  - INTERSECT: keep rows in both sets
  - EXCEPT: keep rows only in first set
- **Reference:** `postgres/proxy/read_setop.go`

### 3.5 Complex Read Handling (CTEs, Derived Tables)

**Problem:** CTEs and derived tables fall through to default merged read, producing incorrect results when referenced tables have deltas.

**Resolution:** Selective materialization matching postgres pattern.

**Implementation:**
- New file: `proxy/read_complex.go`
- **Non-recursive CTEs:** Check if CTE references dirty tables. If clean, leave in-place. If dirty, execute CTE body as merged read, materialize into temp table, rewrite query to reference temp table.
- **Recursive CTEs (WITH RECURSIVE):** Materialize dirty base tables, rewrite query with temp table references, let MySQL handle recursion.
- **Derived tables (subqueries in FROM):** Recursively rewrite dirty subqueries with temp table references.
- Note: MySQL 8.0+ supports CTEs. MySQL 5.7 does not — for 5.7, CTEs should return "not supported" error.
- **Reference:** `postgres/proxy/read_complex.go`

### 3.6 JOIN Schema Diff Handling

**Problem:** JOIN queries on tables with schema diffs fail or fall back to Shadow-only.

**Resolution:** Materialize dirty tables into temp tables, rewrite JOIN to reference them.

**Implementation:**
- In `proxy/read.go` JOIN patch path, when any joined table has schema diffs:
  1. For each dirty table: run merged read, materialize into temp table
  2. Use scoped predicates for pushdown optimization (only materialize relevant rows)
  3. Rewrite JOIN query to reference temp tables instead of dirty tables
  4. Execute rewritten JOIN on Shadow
- **Reference:** `postgres/proxy/read_join.go` schema diff handling section

### 3.7 WHERE Clause Evaluator

**Problem:** JOIN patch algorithm doesn't re-evaluate WHERE on patched rows. Patched rows may not satisfy original query conditions.

**Resolution:** Implement expression evaluator for post-patch filtering.

**Implementation:**
- New file: `proxy/where_eval.go`
- Support operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`
- Type-aware comparison (numeric vs string vs boolean)
- NULL handling: NULL comparisons → false (SQL three-valued logic)
- Safe fallback: return true (keep row) on evaluation failure
- Used in JOIN patch to re-evaluate original WHERE on patched rows
- **Reference:** `postgres/proxy/where_eval.go` — port directly

### 3.8 Prod Query Rewriting (Full)

**Problem:** Current schema diff rewriting is partial — only handles added/renamed columns in top-level SELECT. Missing: dropped columns in WHERE/ORDER BY/GROUP BY/HAVING, nested subqueries.

**Resolution:** Full query rewriting for Prod compatibility.

**Implementation:**
- Expand `rewriteForProd()` in `proxy/read.go`:
  - Dropped columns: remove from SELECT, WHERE, ORDER BY, GROUP BY, HAVING
  - Renamed columns: revert to Prod names everywhere (not just SELECT)
  - Added columns: strip from Prod query, inject NULL in merge phase
  - Handle SELECT * correctly when columns have been both added and dropped
- Use Vitess AST for rewriting where possible (more reliable than string manipulation)
- **Reference:** `postgres/proxy/rewrite_prod.go`

---

## P3 — Polish & Edge Cases

### 4.1 Classifier SubType Refinements

**Problem:** Many statements lumped under SubOther, preventing proper routing.

**Implementation:**
- In `classify/classifier.go`, add detection for:
  - `SET` → SubSet (enables StrategyForwardBoth)
  - `SHOW` → SubShow (enables StrategyProdDirect)
  - `EXPLAIN` → SubExplain (with EXPLAIN ANALYZE rejection: detect `ANALYZE` keyword, return SubNotSupported)
  - `TRUNCATE` → SubTruncate (see 2.1)
  - `SAVEPOINT` → SubSavepoint
  - `RELEASE SAVEPOINT` → SubRelease
- **Reference:** `postgres/classify/classifier.go` statement classification

### 4.2 Missing Feature Flags

**Implementation:**
- In `classify/classifier.go`:
  - `HasWindowFunc`: Walk select expressions for `OVER (...)` pattern in Vitess AST (check for `*sqlparser.FuncExpr` with `Over` field set)
  - `HasDistinct`: Check `sel.Distinct` in Vitess AST
  - `HasComplexAgg`: Detect MySQL-specific complex aggregates: GROUP_CONCAT, JSON_ARRAYAGG, JSON_OBJECTAGG
  - `HasOnConflict`: Check `ins.OnDup` (see 1.4)
  - `IsMetadataQuery`: Detect queries against `information_schema.*` or `mysql.*` system schemas
  - `HasCursor`: N/A for MySQL (cursors are only in stored procedures)
- **Reference:** `postgres/classify/classifier.go` feature flag detection

### 4.3 PK Extraction Improvements

**Implementation:**
- Support `value = column` (reversed comparison) in `extractPKsFromExpr()`
- Handle TypeCast unwrapping: `CAST(? AS UNSIGNED)` → extract inner value
- Support composite PK serialization as JSON (matching postgres pattern)
- **Reference:** `postgres/classify/pk.go`

### 4.4 ALTER TYPE Tracking

**Problem:** MySQL `MODIFY COLUMN` and `CHANGE COLUMN` syntax not parsed for type changes.

**Implementation:**
- In `proxy/ddl.go` `parseMySQLDDLChanges()`, add regex patterns for:
  - `MODIFY COLUMN col_name new_type` → `RecordTypeChange(table, col, "", new_type)`
  - `CHANGE COLUMN old_name new_name new_type` → handle as rename + type change
- **Reference:** `postgres/proxy/ddl_parse.go`

### 4.5 Multi-Statement ALTER TABLE Parsing

**Problem:** `ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b` only parses the first change.

**Implementation:**
- In `proxy/ddl.go` `parseMySQLDDLChanges()`, loop over comma-separated clauses instead of returning after first match
- Split on top-level commas (not inside parentheses) and parse each clause independently
- Return a slice of changes instead of a single change
- **Reference:** `postgres/proxy/ddl_parse.go` (postgres AST handles this naturally)

### 4.6 RENAME TABLE Handling

**Problem:** MySQL's `RENAME TABLE old TO new` is not parsed.

**Implementation:**
- In classifier, detect `RENAME TABLE` as DDL
- In DDL handler, update schema registry: remove old table, record new table
- Update delta map and tombstone set references

### 4.7 Read-Replica Conflict Handling

**Problem:** No retry logic for read-replica scenarios.

**Implementation:**
- Detect MySQL error codes that indicate replica lag/conflict (e.g., ER_LOCK_WAIT_TIMEOUT 1205, ER_LOCK_DEADLOCK 1213)
- Retry with exponential backoff (200ms increments, max 5 attempts)
- Apply to: schema detection, table metadata queries, FK discovery
- **Reference:** `postgres/schema/dumper.go` `retryOnConflict()`

### 4.8 Shadow-Only Statement Tracking (Prepared Statements)

**Problem:** Prepared statements that reference shadow-only or schema-modified tables are not tracked.

**Implementation:**
- In `proxy/ext_handler.go`, track statements that reference:
  - Shadow-only tables (created via DDL in Shadow)
  - Schema-modified tables (columns added/dropped/renamed)
- When re-used, route to merged read if tables still have diffs
- **Reference:** `postgres/proxy/ext_handler.go` shadow-only tracking

### 4.9 PK-less Table Deduplication

**Problem:** MySQL has no `ctid` equivalent. Dedup is silently skipped for PK-less tables.

**Resolution:** Use full-row hash comparison for deduplication.

**Implementation:**
- In merged read dedup, when table has no PK (`PKType: "none"`):
  - Hash all column values per row (e.g., SHA256 of concatenated column values)
  - Deduplicate by hash (Shadow rows win on collision)
  - This is not perfect (hash collisions, NULL handling) but works for most cases
- **Reference:** New approach — postgres uses `ctid` which MySQL lacks

### 4.10 Protocol Gaps

**Implementation:**
- `COM_FIELD_LIST`: Forward to Prod (deprecated, low priority)
- `COM_STMT_RESET`: Evict cached prepared statement state
- `COM_STMT_FETCH`: Server-side cursor fetch — implement or return "not supported"
- `COM_STMT_SEND_LONG_DATA`: Buffer large parameter chunks before EXECUTE
- Multi-result sets: Handle `SERVER_MORE_RESULTS_EXIST` flag in OK packets for stored procedure results

### 4.11 L2 Guard COM_STMT_EXECUTE Inspection

**Problem:** SafeProdConn only inspects COM_QUERY packets, not COM_STMT_EXECUTE.

**Implementation:**
- In `proxy/guard.go`, extend `Write()` to also inspect COM_STMT_EXECUTE packets
- Look up the statement SQL from cache and run through `looksLikeWrite()`
- **Reference:** `postgres/proxy/guard.go`

---

## Implementation Order

The recommended implementation order follows dependency chains:

```
Phase 1 (P0 — correctness):
  1.1 Generated column detection
  1.2 Bulk UPDATE hydration
  1.3 Bulk DELETE tombstoning
  1.4 INSERT ... ON DUPLICATE KEY UPDATE
  1.5 Hydration INSERT fix
  1.6 Composite PK in hydration
  1.7 Max rows hydration cap

Phase 2 (P1 — features):
  2.1 TRUNCATE handling
  2.2 SAVEPOINT state management  (depends on: 4.1 classifier SubType for SubSavepoint)
  2.3 Schema registry snapshot/restore
  2.4 FK enforcement  (depends on: 1.6 composite PK)
  2.5 Schema rewriting for Prod hydration

Phase 3 (P2 — advanced reads, requires 3.1 first):
  3.1 Temp table infrastructure  (prerequisite for 3.2-3.6)
  3.2 Full aggregate re-aggregation
  3.3 Window functions
  3.4 Set operations
  3.5 Complex reads (CTEs, derived tables)
  3.6 JOIN schema diff handling
  3.7 WHERE clause evaluator
  3.8 Full Prod query rewriting

Phase 4 (P3 — polish):
  4.1-4.11 in any order
```

---

## Key MySQL vs PostgreSQL Syntax Differences

Agents should be aware of these when porting postgres patterns:

| Feature | PostgreSQL | MySQL |
|---------|-----------|-------|
| Upsert | `INSERT ... ON CONFLICT (col) DO UPDATE SET ...` | `INSERT ... ON DUPLICATE KEY UPDATE ...` |
| RETURNING | `INSERT ... RETURNING *` | Not available (use `LAST_INSERT_ID()` for auto-inc) |
| Temp tables | `CREATE TEMP TABLE ...` | `CREATE TEMPORARY TABLE ...` |
| Type cast | `value::type` | `CAST(value AS type)` |
| String concat | `'a' \|\| 'b'` | `CONCAT('a', 'b')` |
| Boolean | `true` / `false` | `1` / `0` |
| CTID | `ctid` (system column) | No equivalent — use full-row hash |
| Identity | `GENERATED ALWAYS AS IDENTITY` | `AUTO_INCREMENT` |
| Generated cols | `pg_attribute.attgenerated` | `INFORMATION_SCHEMA.COLUMNS.EXTRA LIKE '%GENERATED%'` |
| FK discovery | `pg_catalog` | `INFORMATION_SCHEMA.KEY_COLUMN_USAGE + REFERENTIAL_CONSTRAINTS` |
| Quoting | `"identifier"` | `` `identifier` `` |
| Regex | `~ 'pattern'` | `REGEXP 'pattern'` |
| EXPLAIN ANALYZE | `EXPLAIN ANALYZE` | `EXPLAIN ANALYZE` (MySQL 8.0.18+) |
| Isolation on BEGIN | `BEGIN ISOLATION LEVEL REPEATABLE READ` | Plain `BEGIN` (InnoDB defaults to REPEATABLE READ) |
