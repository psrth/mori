# MSSQL Engine — Comprehensive Gap Analysis

This document identifies every gap between the MSSQL engine and the PostgreSQL engine (the reference implementation). It is based on an exhaustive file-by-file comparison of:
- `/internal/engine/mssql/` (23 files, ~4,500 lines)
- `/internal/engine/postgres/` (reference: ~40+ files, ~15,000+ lines in proxy alone)
- `/engine-gaps/postgres-docs.md` (feature reference)

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

The MSSQL engine provides a functional but incomplete implementation of the Mori copy-on-write proxy pattern adapted for SQL Server. The architecture mirrors the PostgreSQL engine:

- **Production**: Read-only MSSQL server (prod), never mutated
- **Shadow**: Docker-based MSSQL Express clone (schema-only), receives writes
- **Proxy**: TDS protocol proxy that classifies T-SQL, routes to appropriate backend

**Current state**: The MSSQL engine has the foundational infrastructure in place (TDS protocol handling, connection management, SQL classification, basic reads/writes, transactions, guard rails, prepared statements). However, it is missing the majority of the advanced features that make the PostgreSQL engine production-grade: JOIN patching, complex read strategies, aggregate decomposition, window function materialization, set operations, foreign key enforcement, cursor support, WHERE clause evaluation, savepoints, generated column handling, bulk hydration, and many classification feature flags.

**Estimated completeness**: ~35-40% of PostgreSQL engine feature coverage.

---

## 2. Feature Parity Matrix

| Feature | PostgreSQL | MSSQL | Status | Notes |
|---------|-----------|-------|--------|-------|
| **Classification** | | | | |
| AST-based parsing | `pg_query_go` AST | Regex-based | ⚠️ Partial | Regex is fragile, no AST for T-SQL |
| SELECT classification | Full | Basic | ⚠️ Partial | Missing many feature flags |
| INSERT classification | Full | Basic | ⚠️ Partial | No OUTPUT/MERGE-as-upsert detection |
| UPDATE classification | Full | Basic | ⚠️ Partial | No cross-table UPDATE FROM detection |
| DELETE classification | Full | Basic | ✅ Implemented | |
| DDL classification | Full | Basic | ✅ Implemented | CREATE/ALTER/DROP |
| Transaction classification | Full | Partial | ⚠️ Partial | No SAVEPOINT/RELEASE |
| TRUNCATE classification | Full | Basic | ⚠️ Partial | SubType is SubOther, not SubTruncate |
| MERGE classification | N/A | Basic | ✅ Implemented | MSSQL-specific |
| Cursor classification | Full | N/A | ❌ Missing | No DECLARE/FETCH/CLOSE |
| EXPLAIN classification | Full | N/A | ❌ Missing | No EXPLAIN support |
| PREPARE/EXECUTE/DEALLOCATE | Full | N/A | ❌ Missing | Handled via RPC, not SQL |
| LISTEN/UNLISTEN | Full | N/A | ➖ N/A | Not applicable to MSSQL |
| CTE detection | Full | Basic | ⚠️ Partial | Detects but doesn't decompose |
| Feature flag: `HasReturning` | Yes | No | ❌ Missing | No OUTPUT clause detection |
| Feature flag: `HasOnConflict` | Yes | No | ❌ Missing | No MERGE-as-upsert detection |
| Feature flag: `HasWindowFunc` | Yes | No | ❌ Missing | Sets `IsComplexRead` instead |
| Feature flag: `HasComplexAgg` | Yes | No | ❌ Missing | STRING_AGG detected but not flagged separately |
| Feature flag: `HasDistinct` | Yes | No | ❌ Missing | Not detected at all |
| Feature flag: `HasCursor` | Yes | No | ❌ Missing | No cursor support |
| Feature flag: `IsMetadataQuery` | Yes | No | ❌ Missing | No sys.* / INFORMATION_SCHEMA detection |
| PK extraction from WHERE | Full (AST) | Basic (regex) | ⚠️ Partial | No `value = column` reversal, no composite PK |
| Parameter resolution | Full ($1-based) | Basic (@p-based) | ⚠️ Partial | Basic but functional |
| **Routing** | | | | |
| StrategyProdDirect | Yes | Yes | ✅ Implemented | |
| StrategyMergedRead | Yes | Yes | ✅ Implemented | |
| StrategyJoinPatch | Yes | Stubbed | ❌ Missing | Falls back to Prod passthrough |
| StrategyShadowWrite | Yes | Yes | ✅ Implemented | |
| StrategyHydrateAndWrite | Yes | Yes | ⚠️ Partial | Point update only, no bulk |
| StrategyShadowDelete | Yes | Yes | ⚠️ Partial | Point delete only, no bulk |
| StrategyShadowDDL | Yes | Yes | ⚠️ Partial | No FK extraction from DDL |
| StrategyTransaction | Yes | Yes | ⚠️ Partial | No savepoints |
| StrategyNotSupported | Yes | No | ❌ Missing | No explicit unsupported handling |
| StrategyForwardBoth | Yes | Yes | ✅ Implemented | SET/session commands |
| StrategyTruncate | Yes | No | ❌ Missing | No IsFullyShadowed tracking |
| StrategyListenOnly | Yes | N/A | ➖ N/A | Not applicable |
| **Read Operations** | | | | |
| Single-table merged read | Full | Basic | ⚠️ Partial | Works but no schema adaptation |
| PK injection for dedup | Yes | Yes | ✅ Implemented | |
| CTID fallback (PK-less) | Yes | No | ❌ Missing | No equivalent for PK-less tables |
| LIMIT over-fetch | Yes | Yes | ✅ Implemented | TOP rewriting |
| ORDER BY re-sort | Yes | Yes | ✅ Implemented | |
| Schema adaptation (drop/rename/add cols) | Full | Partial | ⚠️ Partial | New-column stripping only |
| JOIN patching (StrategyJoinPatch) | Full (848 lines) | Stubbed | ❌ Missing | Falls back to Prod |
| Aggregate decomposition | Full (959 lines) | Minimal | ⚠️ Partial | COUNT(*) only, no SUM/AVG/MIN/MAX/GROUP BY |
| Complex aggregates (array_agg, json_agg) | Full | None | ❌ Missing | |
| CTE materialization | Full (596 lines) | None | ❌ Missing | No temp table materialization |
| Derived table (subquery FROM) | Full | None | ❌ Missing | |
| LATERAL join handling | Full | None | ❌ Missing | No CROSS/OUTER APPLY handling |
| Set operations (UNION/INTERSECT/EXCEPT) | Full (303 lines) | None | ❌ Missing | No leaf decomposition |
| Window function materialization | Full (442 lines) | None | ❌ Missing | No temp table + re-execute |
| WHERE clause evaluation | Full (457 lines) | None | ❌ Missing | No in-memory WHERE re-eval |
| Cursor operations | Full (140 lines) | None | ❌ Missing | No DECLARE/FETCH/CLOSE |
| Prod query rewriting | Full (381 lines) | Minimal | ⚠️ Partial | Only new-column stripping |
| Temp table management | Full (213 lines) | None | ❌ Missing | No util_table equivalent |
| Scoped materialization | Yes | No | ❌ Missing | |
| **Write Operations** | | | | |
| Simple INSERT | Full | Basic | ⚠️ Partial | No PK extraction from result, no RETURNING |
| INSERT with OUTPUT (RETURNING) | Full | None | ❌ Missing | No OUTPUT clause capture |
| Upsert (INSERT ON CONFLICT / MERGE) | Full | None | ❌ Missing | No MERGE-as-upsert hydration |
| Point UPDATE with hydration | Full | Basic | ⚠️ Partial | Works for single-PK point updates |
| Bulk UPDATE (no PKs) | Full | None | ❌ Missing | No Prod pre-query + PK rewrite |
| Cross-table UPDATE (UPDATE FROM) | Full | None | ❌ Missing | |
| Point DELETE with tombstone | Full | Basic | ⚠️ Partial | Works for extracted PKs only |
| Bulk DELETE (no PKs) | Full | None | ❌ Missing | No Prod pre-query |
| DELETE with OUTPUT (RETURNING) | Full | None | ❌ Missing | |
| FK enforcement on INSERT | Full | None | ❌ Missing | |
| FK enforcement on UPDATE | Full | None | ❌ Missing | |
| FK enforcement on DELETE (RESTRICT/CASCADE) | Full | None | ❌ Missing | |
| Generated column filtering | Full | None | ❌ Missing | No computed column detection |
| Schema rewriting for hydration | Full | None | ❌ Missing | |
| Max rows hydration cap | Yes | No | ❌ Missing | |
| **DDL & Schema Tracking** | | | | |
| ADD COLUMN tracking | Yes | Yes | ✅ Implemented | |
| DROP COLUMN tracking | Yes | Yes | ✅ Implemented | |
| RENAME COLUMN tracking | Yes | No | ❌ Missing | MSSQL uses sp_rename |
| ALTER TYPE tracking | Yes | No | ❌ Missing | |
| CREATE TABLE tracking | Yes | Yes | ✅ Implemented | |
| DROP TABLE tracking | Yes | Yes | ✅ Implemented | |
| FK extraction from DDL | Yes | No | ❌ Missing | Only stripped, not stored |
| FK metadata storage | Yes | No | ❌ Missing | |
| Schema registry persistence | Yes | Yes | ✅ Implemented | |
| **Transaction Management** | | | | |
| BEGIN/COMMIT/ROLLBACK | Full | Full | ✅ Implemented | |
| Staged delta system | Full | Full | ✅ Implemented | Stage/Commit/Rollback |
| SAVEPOINT support | Full | None | ❌ Missing | No SAVE TRAN / ROLLBACK TO |
| Schema registry snapshot (txn) | Yes | No | ❌ Missing | |
| Prod REPEATABLE READ on BEGIN | Yes | No | ❌ Missing | No isolation level upgrade |
| **Foreign Key Enforcement** | | | | |
| FK discovery from Prod | Full (660 lines) | None | ❌ Missing | |
| Parent row validation | Full | None | ❌ Missing | |
| RESTRICT/NO ACTION enforcement | Full | None | ❌ Missing | |
| CASCADE enforcement | Full | None | ❌ Missing | |
| Multi-column FK support | Full | None | ❌ Missing | |
| FK caching | Full | None | ❌ Missing | |
| **Protocol Handling** | | | | |
| TDS packet read/write | Full | Full | ✅ Implemented | |
| TDS handshake (PRELOGIN+LOGIN7) | N/A | Full | ✅ Implemented | |
| TLS/encryption negotiation | N/A | Full | ✅ Implemented | |
| MARS disabling | N/A | Full | ✅ Implemented | |
| SQL_BATCH message parsing | Full | Full | ✅ Implemented | UTF-16LE extraction |
| RPC message parsing | N/A | Full | ✅ Implemented | sp_executesql/prepare/execute |
| Transaction Manager Request | N/A | Full | ✅ Implemented | Forward to both |
| Attention signal handling | N/A | Yes | ✅ Implemented | Cancel support |
| TDS token parsing (COLMETADATA, ROW, etc.) | N/A | Full | ✅ Implemented | Comprehensive type support |
| Synthetic response construction | pgwire format | TDS VALUES-based | ✅ Implemented | |
| Error response construction | pgwire ErrorResponse | TDS ERROR+DONE tokens | ✅ Implemented | |
| **Prepared Statements** | | | | |
| sp_executesql routing | N/A | Full | ✅ Implemented | Classify + route |
| sp_prepare handle caching | N/A | Full | ✅ Implemented | |
| sp_execute dispatch | N/A | Full | ✅ Implemented | |
| sp_unprepare cache eviction | N/A | Full | ✅ Implemented | |
| Extended protocol batch processing | Full | N/A | ➖ N/A | MSSQL uses RPC, not Parse/Bind/Execute |
| Parameter binding/substitution | Full | Basic | ⚠️ Partial | RPC params parsed but not substituted |
| **Guard Rails** | | | | |
| L1: validateRouteDecision | Yes | Yes | ✅ Implemented | |
| L2: SafeProdConn | Yes | Yes | ✅ Implemented | |
| L3: classifyAndRoute final check | Yes | Yes | ✅ Implemented | |
| looksLikeWrite detection | Yes | Yes | ✅ Implemented | T-SQL adapted |
| SQLSTATE error codes | MR001 | Error 50000 | ✅ Implemented | MSSQL equivalent |
| **Initialization** | | | | |
| Connection string parsing | Full | Full | ✅ Implemented | URI + key-value |
| Version detection | Full | Full | ✅ Implemented | @@VERSION |
| Docker image resolution | Full | Full | ✅ Implemented | 2017/2019/2022 |
| Schema dump (DDL reconstruction) | pg_dump | INFORMATION_SCHEMA | ✅ Implemented | |
| Table metadata detection | Full | Full | ✅ Implemented | PK columns + types |
| Identity offset calculation | Full | Full | ✅ Implemented | DBCC CHECKIDENT |
| FK discovery at init | Full | None | ❌ Missing | |
| Generated column detection | Full | None | ❌ Missing | No computed column detection |
| Extension handling | Full | N/A | ➖ N/A | Not applicable to MSSQL |
| Read-replica conflict handling | Full | None | ❌ Missing | |
| Schema application | Full | Full | ✅ Implemented | |

---

## 3. Classification Gaps

**File**: `/internal/engine/mssql/classify/classifier.go`

The MSSQL classifier is regex-based (the PostgreSQL classifier uses `pg_query_go` for AST-based parsing). While regex is the practical choice for T-SQL (no equivalent Go library), the current implementation has significant feature flag gaps:

### 3.1 Missing Feature Flags

The following `core.Classification` fields are never set by the MSSQL classifier:

| Field | PostgreSQL Sets It? | MSSQL Sets It? | Impact |
|-------|-------------------|---------------|--------|
| `HasReturning` | Yes | **No** | Cannot detect OUTPUT clauses on INSERT/UPDATE/DELETE |
| `HasOnConflict` | Yes | **No** | Cannot detect MERGE-as-upsert patterns |
| `HasWindowFunc` | Yes | **No** | Window functions set `IsComplexRead` instead — router cannot distinguish |
| `HasComplexAgg` | Yes | **No** | STRING_AGG is in the aggregate regex but not flagged separately |
| `HasDistinct` | Yes | **No** | SELECT DISTINCT not detected at all (line 124-180, no DISTINCT check) |
| `HasCursor` | Yes | **No** | No cursor operations classified |
| `IsMetadataQuery` | Yes | **No** | Queries on `sys.*`, `INFORMATION_SCHEMA.*` not detected |
| `IntrospectedTable` | Yes | **No** | No introspection table tracking |

### 3.2 Missing Statement Classifications

| Statement | PostgreSQL | MSSQL | Gap |
|-----------|-----------|-------|-----|
| SAVEPOINT / SAVE TRAN | `SubSavepoint` | Not classified | Lines 69-98: no SAVE TRAN case |
| RELEASE SAVEPOINT | `SubRelease` | N/A | MSSQL doesn't have RELEASE |
| EXPLAIN | `SubExplain` | Not classified | No detection |
| PREPARE/EXECUTE/DEALLOCATE | `SubPrepare`, `SubExecute`, `SubDeallocate` | Not classified | Handled via RPC only |
| CURSOR operations | `SubCursor` | Not classified | No DECLARE CURSOR support |
| TRUNCATE | `SubTruncate` | `SubOther` | Line 63: uses SubOther, should be SubTruncate |

### 3.3 PK Extraction Limitations

**File**: `/internal/engine/mssql/classify/classifier.go`, lines 302-336

- **No `value = column` reversal**: Only matches `column = value`, not `value = column` (PostgreSQL handles both)
- **No composite PK serialization**: Only extracts first matching PK column, doesn't handle multi-column PKs as JSON keys
- **No TypeCast unwrapping**: No equivalent of `CAST(value AS type)` handling (e.g., `CAST('...' AS UNIQUEIDENTIFIER)`)
- **No IN clause handling**: Cannot extract PKs from `WHERE id IN (1, 2, 3)`
- **Regex fragility**: Pattern at line 320 (`\b\[?%s\]?\s*=\s*(?:'([^']*)'|(\d+)|(@p\d+))`) doesn't handle:
  - Negative numbers
  - Decimal/float values
  - GUID literals (e.g., `'550e8400-...'` works but `{550e8400-...}` doesn't)
  - CONVERT/CAST-wrapped parameters

### 3.4 CTE Classification Weakness

**File**: `/internal/engine/mssql/classify/classifier.go`, lines 264-281

The CTE classifier at line 264 (`classifyCTE`) uses `strings.Contains(upper, "INSERT")` to detect mutating CTEs. This is overly broad — it would classify `WITH cte AS (SELECT 'INSERT' AS word) SELECT * FROM cte` as a write. The PostgreSQL engine uses AST-level CTE detection to avoid this.

---

## 4. Read Operation Gaps

**File**: `/internal/engine/mssql/proxy/read.go` (~817 lines)

### 4.1 JOIN Patch Strategy — Not Implemented

**PostgreSQL file**: `read_join.go` (848 lines)
**MSSQL implementation**: Line 44-46 in `read.go`:
```go
case core.StrategyJoinPatch:
    // For JOINs, fall back to prod for now (same as Postgres v1).
    return forwardAndRelay(rawMsg, rh.prodConn, clientConn)
```

**Missing functionality**:
- No identification of which joined tables have deltas/tombstones
- No execution of JOIN on Prod followed by row classification (clean/delta/dead)
- No Shadow data fetching for delta rows
- No row patching with Shadow values
- No WHERE clause re-evaluation on patched rows
- No composite key deduplication
- No schema diff handling for JOINs (temp table materialization)
- No scoped predicate pushdown optimization

**Impact**: Any SELECT with JOINs on affected tables returns stale Prod data.

### 4.2 Aggregate Decomposition — Minimal

**PostgreSQL file**: `aggregate.go` (959 lines)
**MSSQL implementation**: Lines 214-283 in `read.go`

The MSSQL aggregate handler only supports bare `COUNT(*)` without GROUP BY (line 266: `if strings.Contains(upper, "GROUP BY") { return "" }`). Everything else falls back to Prod.

**Missing**:
- **No GROUP BY support**: Any aggregate with GROUP BY goes to Prod (line 266)
- **No SUM/AVG/MIN/MAX re-aggregation**: Only COUNT(*) is decomposed
- **No COUNT(DISTINCT col) handling**
- **No HAVING filter on re-aggregated results**
- **No complex aggregate materialization** (STRING_AGG, etc.) — no temp table + re-execute
- **Result type loss**: The aggregate result is always returned as `_mori_count` with INTN type (line 256), losing the original column name and type

### 4.3 Complex Read Strategies — Not Implemented

**PostgreSQL file**: `read_complex.go` (596 lines)
**MSSQL**: No equivalent file exists.

**Missing**:
- **CTE materialization**: No temp table creation for dirty CTEs
- **Recursive CTE handling**: No base table materialization + query rewriting
- **Derived table (subquery in FROM) handling**: No subquery result materialization
- **LATERAL join handling**: No CROSS APPLY / OUTER APPLY base table materialization

### 4.4 Set Operations — Not Implemented

**PostgreSQL file**: `read_setop.go` (303 lines)
**MSSQL**: No equivalent file exists.

**Missing**:
- No decomposition of UNION/INTERSECT/EXCEPT into leaf SELECT nodes
- No independent merged read for each leaf
- No in-memory set operation application (dedup for UNION, intersection, difference)
- No count tracking for ALL variants

### 4.5 Window Functions — Not Implemented

**PostgreSQL file**: `read_window.go` (442 lines)
**MSSQL**: No equivalent file exists.

**Missing**:
- No base SELECT construction (stripping window functions)
- No merged read of base data
- No temp table materialization of merged result
- No query rewriting to reference temp table
- No re-execution on Shadow

### 4.6 Schema Adaptation — Partial

**PostgreSQL file**: `rewrite_prod.go` (381 lines)
**MSSQL**: `stripNewColumnsFromQuery` in `read.go` lines 289-362

The MSSQL engine can strip newly-added columns from the SELECT list when querying Prod (to avoid errors). However, it is missing:

- **Dropped column removal from Prod queries**: No stripping of dropped columns from SELECT/WHERE/ORDER BY/GROUP BY
- **Renamed column reversion**: No rewriting of new column names to old names for Prod queries. MSSQL uses `sp_rename` which is not even classified
- **Added column NULL injection**: The merge step uses column index alignment (line 475-490) but doesn't explicitly inject NULLs for added columns
- **Delta/tombstone filtering via WHERE**: No WHERE clause injection to exclude delta/tombstoned PKs from Prod queries (optimization)

### 4.7 PK-less Table Handling — Not Implemented

**PostgreSQL**: Uses `ctid` system column as row identity fallback
**MSSQL**: No equivalent. Tables without PKs cannot be deduplicated during merged reads.

### 4.8 Temp Table Management — Not Implemented

**PostgreSQL file**: `util_table.go` (213 lines)
**MSSQL**: No equivalent file exists.

**Missing**:
- No `utilTableName()` for deterministic temp table naming
- No `buildCreateTempTableSQL()` for creating temp tables matching query schema
- No `bulkInsertToUtilTable()` for batch inserting rows
- No `dropUtilTable()` for cleanup
- No `buildScopedMaterializationSQL()` for pushdown optimization

### 4.9 WHERE Clause Evaluation — Not Implemented

**PostgreSQL file**: `where_eval.go` (457 lines)
**MSSQL**: No equivalent file exists.

**Missing**:
- No expression evaluator for `=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`
- No boolean `AND`/`OR`/`NOT` evaluation
- No type handling (boolean normalization, numeric comparison)
- No NULL handling (three-valued logic)

This is required for JOIN WHERE re-evaluation on patched rows.

---

## 5. Write Operation Gaps

**File**: `/internal/engine/mssql/proxy/write.go` (~194 lines)

### 5.1 INSERT Gaps

**PostgreSQL file**: `write_insert.go` (588 lines)
**MSSQL**: `handleInsert` at lines 48-61 in `write.go`

**Missing**:
- **No FK enforcement before INSERT**: PostgreSQL validates parent rows exist before INSERT; MSSQL does not
- **No OUTPUT clause capture**: PostgreSQL captures RETURNING to extract inserted PKs; MSSQL has no OUTPUT handling, only calls `MarkInserted(table)` without specific PKs (line 53)
- **No precise PK delta tracking**: Uses `MarkInserted(table)` (table-level) instead of `Add(table, pk)` (row-level). This is imprecise and forces merged reads for all rows in the table
- **No MERGE-as-upsert support**: PostgreSQL handles `INSERT ... ON CONFLICT` with pre-hydration; MSSQL has no equivalent for `MERGE INTO ... WHEN NOT MATCHED THEN INSERT WHEN MATCHED THEN UPDATE`

### 5.2 UPDATE Gaps

**PostgreSQL file**: `write_update.go` (970 lines)
**MSSQL**: `handleUpdate` at lines 64-100 in `write.go`

**Missing**:
- **No bulk UPDATE (PKs not extractable)**: When WHERE clause doesn't yield PKs, PostgreSQL queries Prod for all matching rows, hydrates them, then rewrites the UPDATE with concrete PKs. MSSQL just executes on Shadow with whatever PKs were extracted (line 70-81), missing rows that exist only in Prod
- **No cross-table UPDATE (UPDATE FROM)**: PostgreSQL detects correlated subqueries in FROM clause and hydrates referenced tables. MSSQL has no FROM clause hydration
- **No FK enforcement on UPDATE**: No parent row validation for FK columns being updated
- **No WHERE clause re-evaluation**: After hydrating and updating, PostgreSQL re-evaluates WHERE on the patched rows to filter invalid results. MSSQL does not
- **No generated/computed column filtering during hydration**: Line 134-194 in `write.go` — `hydrateRow` does `SELECT * FROM table WHERE pk = value` and inserts all columns. If the table has computed columns, the INSERT will fail
- **No schema rewriting for Prod compatibility**: Hydration queries to Prod are not rewritten for schema diffs (dropped/renamed columns)
- **No max rows hydration cap**: Unbounded hydration from Prod

### 5.3 DELETE Gaps

**PostgreSQL file**: `write_delete.go` (487 lines)
**MSSQL**: `handleDelete` at lines 103-131 in `write.go`

**Missing**:
- **No bulk DELETE (PKs not extractable)**: When WHERE clause doesn't yield PKs, PostgreSQL pre-queries Prod for matching PKs. MSSQL only tombstones rows where PKs were extracted from the WHERE clause — Prod-only rows matching the WHERE are not tombstoned
- **No OUTPUT (RETURNING) support**: PostgreSQL hydrates from Prod for return data synthesis on DELETE with RETURNING. MSSQL has no OUTPUT handling
- **No FK RESTRICT/NO ACTION enforcement**: PostgreSQL rejects parent DELETE if child rows exist. MSSQL does not check
- **No FK CASCADE enforcement**: PostgreSQL automatically tombstones child rows when parent is deleted with CASCADE. MSSQL does not
- **No correct row count computation**: PostgreSQL computes total count as Shadow actual deletes + Prod-only matches. MSSQL only counts Shadow deletes

### 5.4 Hydration System Gaps

**File**: `/internal/engine/mssql/proxy/write.go`, lines 134-194 (`hydrateRow`)

The hydration system is minimal compared to PostgreSQL:

- **Single-row only**: `hydrateRow` fetches and inserts one row at a time. No bulk hydration
- **No generated/computed column filtering**: `SELECT *` includes computed columns that cannot be explicitly inserted
- **No IDENTITY_INSERT safety**: The IDENTITY_INSERT toggle (lines 169-181) wraps the INSERT but doesn't handle concurrent access or multi-table IDENTITY_INSERT conflicts (SQL Server only allows one table at a time)
- **No schema rewriting**: Hydration queries don't account for schema diffs between Prod and Shadow
- **No NULL handling refinement**: All values are quoted as string literals via `quoteLiteralMSSQL` (line 163), which may cause type mismatches for binary, datetime, or numeric columns
- **No max rows cap**: No limit on how many rows can be hydrated from Prod

---

## 6. DDL & Schema Tracking Gaps

**File**: `/internal/engine/mssql/proxy/conn.go`, lines 539-627 (`trackDDLEffects`, `parseMSSQLAlterAddColumn`, `parseMSSQLAlterDropColumn`)

### 6.1 Missing DDL Change Tracking

| Change Type | PostgreSQL | MSSQL | Gap |
|-------------|-----------|-------|-----|
| ADD COLUMN | `RecordAddColumn` | `RecordAddColumn` | ✅ Implemented |
| DROP COLUMN | `RecordDropColumn` | `RecordDropColumn` | ✅ Implemented |
| RENAME COLUMN | `RecordRenameColumn` | Not implemented | ❌ Missing — MSSQL uses `EXEC sp_rename` which is classified as OpOther |
| ALTER TYPE | `RecordTypeChange` | Not implemented | ❌ Missing — `ALTER TABLE t ALTER COLUMN c newtype` not parsed |
| CREATE TABLE | `RecordNewTable` | `RecordNewTable` | ✅ Implemented |
| DROP TABLE | `RemoveTable` | `RemoveTable` | ✅ Implemented |

### 6.2 Missing DDL Parsing

**PostgreSQL file**: `ddl_parse.go` (345 lines) — dedicated DDL parser
**MSSQL**: Inline regex in `conn.go` (lines 597-627) — only handles ADD and DROP COLUMN

**Missing DDL patterns**:
- `ALTER TABLE t ALTER COLUMN c newtype` (type change)
- `EXEC sp_rename 't.old_col', 'new_col', 'COLUMN'` (rename column — classified as OpOther/SubOther, never reaches DDL handler)
- `ALTER TABLE t ADD CONSTRAINT ... FOREIGN KEY ...` (FK extraction)
- `ALTER TABLE t DROP CONSTRAINT ...` (constraint drop)
- `CREATE INDEX ... ON t(...)` (index creation tracked at classification level but not in schema registry)

### 6.3 FK Extraction from DDL — Not Implemented

**PostgreSQL**: Extracts FK metadata from DDL statements before stripping, stores in schema registry for proxy-level FK enforcement.
**MSSQL**: `StripForeignKeys` in `schema/schema.go` (lines 174-186) removes FK lines but does not extract or store the FK metadata.

### 6.4 TRUNCATE Handling — Incomplete

**PostgreSQL**: Marks table as `IsFullyShadowed` so all subsequent reads go to Shadow only.
**MSSQL**: TRUNCATE is classified as `SubOther` (line 63 of `classifier.go`) and not routed to `StrategyTruncate`. No `IsFullyShadowed` tracking.

---

## 7. Transaction Management Gaps

**File**: `/internal/engine/mssql/proxy/txn.go` (~216 lines)

### 7.1 Savepoint System — Not Implemented

**PostgreSQL**: Full stack-based savepoint management (SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT). Each savepoint captures delta map, tombstone set, insert counts, and schema registry state.

**MSSQL**: No savepoint support at all. `SAVE TRAN savepointname` is not classified (the classifier at line 69 only checks `BEGIN TRAN` / `BEGIN TRANSACTION`). `ROLLBACK TRAN savepointname` (partial rollback) is not handled.

**Missing**:
- No `SAVE TRAN` / `SAVE TRANSACTION` classification
- No savepoint snapshot stack
- No `ROLLBACK TRAN savepointname` handling (partial rollback to savepoint)
- No schema registry snapshot at savepoint creation

### 7.2 Schema Registry Snapshot — Not Implemented

**PostgreSQL**: Takes a schema registry snapshot at BEGIN, restores on ROLLBACK.
**MSSQL**: No schema registry snapshot. If DDL executes inside a transaction and then ROLLBACK occurs, the schema registry will be inconsistent with the actual Shadow state.

### 7.3 Prod Isolation Level — Not Implemented

**PostgreSQL**: Sends `BEGIN ISOLATION LEVEL REPEATABLE READ` to Prod for merged read consistency.
**MSSQL**: Forwards the original BEGIN TRAN to Prod without upgrading the isolation level. This means Prod reads within a transaction may see non-repeatable data.

---

## 8. Foreign Key Enforcement Gaps

**PostgreSQL file**: `fk.go` (660 lines)
**MSSQL**: No FK enforcement at all. The entire `fk.go` equivalent is missing.

### 8.1 FK Discovery — Not Implemented

- No FK detection from `sys.foreign_keys` / `sys.foreign_key_columns` at initialization
- No FK metadata storage in schema registry
- No FK discovery from DDL (ALTER TABLE ADD CONSTRAINT)
- No FK caching per table

### 8.2 Parent Row Validation (INSERT/UPDATE) — Not Implemented

The PostgreSQL engine validates that parent rows exist before allowing INSERT/UPDATE on child tables:
1. Delta map check (parent PK in delta → exists)
2. Tombstone check (parent PK tombstoned → REJECT)
3. Shadow query check
4. Prod query fallback

None of this exists in MSSQL. A child INSERT referencing a tombstoned parent will succeed in Shadow (FK constraints stripped) but would fail in production.

### 8.3 Referential Action Enforcement (DELETE) — Not Implemented

- No RESTRICT / NO ACTION enforcement (reject parent delete if child rows exist)
- No CASCADE enforcement (tombstone child rows when parent deleted)
- No SET NULL / SET DEFAULT handling

---

## 9. Protocol-Level Gaps

**File**: `/internal/engine/mssql/proxy/tdsmsg.go`, `tdsquery.go`, `startup.go`

### 9.1 TDS Protocol — Well Implemented

The TDS protocol handling is one of the strongest areas of the MSSQL engine. It includes:
- Complete PRELOGIN/LOGIN7 handshake with encryption negotiation
- SQL_BATCH message construction and SQL extraction (UTF-16LE)
- RPC payload parsing for sp_executesql/sp_prepare/sp_execute/sp_unprepare
- Comprehensive COLMETADATA + ROW/NBC_ROW token parsing covering all major MSSQL data types
- TLS upgrade support for Shadow connections
- MARS disabling for proxy compatibility
- Attention signal (cancel) handling
- Multi-packet message assembly
- Error token construction

### 9.2 Synthetic Response Limitations

**File**: `/internal/engine/mssql/proxy/read.go`, lines 772-809 (`buildSyntheticSelect`)

The merged read returns data by constructing a synthetic `SELECT ... FROM (VALUES ...) AS _mori_merged(...)` query and executing it through Prod. This approach:

- **Loses original column types**: All columns are returned as NVARCHAR(4000) because the VALUES constructor casts everything to string. The PostgreSQL engine's pgwire approach preserves original column OIDs
- **Has size limits**: Very large result sets may exceed TDS packet size or SQL Server's query length limit
- **Double-encodes data**: Values are SQL-escaped, sent to Prod, then returned — adding latency and potential encoding issues (especially for binary data, NULLs, and special characters)

An alternative approach (building a raw TDS COLMETADATA + ROW response) exists as `buildTDSSelectResponse` (line 1106 of `tdsquery.go`) but is not used by the merged read path.

### 9.3 Missing Transaction Descriptor Header Handling

The `buildSQLBatchMessage` function (line 245 of `tdsmsg.go`) uses a zero transaction descriptor. When the proxy sends internal queries (hydration, merged reads) during an active transaction, the zero descriptor may cause issues with MSSQL's transaction tracking. The PostgreSQL engine doesn't have this issue because pgwire doesn't require transaction descriptors in messages.

---

## 10. Prepared Statement Gaps

**File**: `/internal/engine/mssql/proxy/ext_handler.go` (~565 lines)

### 10.1 Implemented Features

The ExtHandler provides solid coverage for the TDS RPC-based prepared statement pattern:
- `sp_executesql`: SQL extraction, classification, and routing
- `sp_prepare`: Handle capture from RETURNVALUE token, SQL caching
- `sp_execute`: Handle lookup, classification from cache, routing
- `sp_unprepare`: Cache eviction
- Well-known proc ID mapping (proc IDs 1-15)

### 10.2 Missing Features

- **No parameter substitution for classification**: When `sp_execute` is called with parameters, the cached SQL contains `@p1`, `@p2` placeholders but the actual parameter values from the RPC payload are not extracted and substituted for PK extraction. PostgreSQL's `ClassifyWithParams` resolves `$1`, `$2` to actual values
- **No sp_prepexec handling**: Proc ID 13 (`sp_prepexec`) combines prepare + execute in one RPC. Currently falls through to Prod passthrough
- **No shadow-only statement tracking**: PostgreSQL tracks which prepared statements reference shadow-only or schema-modified tables and auto-routes them. MSSQL does not
- **No sp_cursoropen/sp_cursorfetch routing**: Cursor-related RPCs (proc IDs 1-9) are not handled, falling through to Prod

---

## 11. Guard Rail Gaps

**File**: `/internal/engine/mssql/proxy/guard.go` (~161 lines)

### 11.1 Well Implemented

The three-layer write protection is fully implemented and tested:
- **L1** (`validateRouteDecision`): Asserts write/DDL never gets StrategyProdDirect — overrides to StrategyShadowWrite
- **L2** (`SafeProdConn`): Wraps Prod connection, inspects outgoing SQL_BATCH for write SQL
- **L3** (`classifyAndRoute` in `conn.go`): Final check before Prod dispatch — logs CRITICAL and returns error

### 11.2 Missing Guard Rails

- **No RPC write guard**: The SafeProdConn only inspects SQL_BATCH packets (type 0x01). RPC packets (type 0x03) containing write SQL via sp_executesql are not inspected at L2. The classification-level routing (L1) and the dispatch logic handle this, but there's no defense-in-depth for RPC writes that might bypass classification
- **No DENY statement detection**: `looksLikeWrite` (line 114) classifies DENY as a safe prefix (not in the write list), but it should be included as it modifies permissions. Actually, looking again, DENY is not in `safePrefixes` either — it falls through to `false` (no match). This means DENY would pass the guard check but it's also not blocked. However, GRANT and REVOKE *are* in writePrefixes (line 134)

---

## 12. Initialization Gaps

**File**: `/internal/engine/mssql/init.go` (~236 lines), `/internal/engine/mssql/schema/schema.go`

### 12.1 Implemented

- Connection string parsing (URI + key-value formats)
- Version detection via `@@VERSION`
- Docker image selection (2017/2019/2022)
- Schema dump via INFORMATION_SCHEMA queries (no external dump tool needed)
- Table metadata detection (PK columns, types, IDENTITY)
- Identity offset calculation (same formula as PostgreSQL: `max(prod_max * 10, prod_max + 10_000_000)`)
- FK stripping from DDL
- Schema application to Shadow (with GO batch separator support)
- Container lifecycle management

### 12.2 Missing

| Feature | PostgreSQL | MSSQL | Gap |
|---------|-----------|-------|-----|
| FK discovery at init | Queries `pg_catalog` for FK metadata | Not implemented | ❌ No `sys.foreign_keys` query |
| Generated/computed column detection | Queries `pg_attribute.attgenerated` | Not implemented | ❌ No `sys.computed_columns` query |
| Read-replica conflict handling | Retries on SQLSTATE 40001 with backoff | Not implemented | ❌ No retry logic |
| Extension handling | Detects, filters, installs extensions | N/A | ➖ Not applicable |
| Unique constraint detection | Detects UNIQUEs for ON CONFLICT | Not implemented | ❌ No unique constraint metadata |
| Index detection | Detects indexes for schema replication | Partial | ⚠️ CREATE INDEX is in DDL but not detected at init |
| Check constraint detection | Detects CHECK constraints | Not implemented | ❌ Not detected |
| Default value handling | Full | Partial | ⚠️ COLUMN_DEFAULT captured but complex defaults (functions, sequences) may not reconstruct correctly |
| View detection | N/A | Not implemented | ❌ Views in INFORMATION_SCHEMA not filtered — could cause issues if Shadow doesn't have the underlying tables |

### 12.3 Schema Dump Limitations

**File**: `/internal/engine/mssql/schema/schema.go`, lines 19-52 (`DumpSchema`)

The schema dump only queries `dbo` schema tables (`WHERE TABLE_SCHEMA = 'dbo'`). This misses:
- Tables in non-dbo schemas (e.g., `sales.Orders`, `hr.Employees`)
- Triggers (which could affect write behavior)
- User-defined types
- User-defined functions
- Stored procedures (relevant for sp_executesql routing)
- Indexes (only PKs are captured)
- Unique constraints
- Check constraints
- Computed column definitions

---

## 13. Priority Recommendations

Ordered by impact on production correctness and user experience:

### P0 — Critical (Data correctness at risk)

1. **Bulk UPDATE hydration** (`write.go`)
   - When PKs are not extractable from WHERE, UPDATE silently misses Prod-only rows
   - Must pre-query Prod for matching PKs, hydrate, then execute
   - Reference: `postgres/proxy/write_update.go` lines 100-200

2. **Bulk DELETE tombstoning** (`write.go`)
   - Same issue: DELETE without extractable PKs silently misses Prod-only rows
   - Must pre-query Prod for matching PKs
   - Reference: `postgres/proxy/write_delete.go` lines 80-150

3. **Generated/computed column filtering** (`write.go`)
   - `hydrateRow` does `SELECT *` + `INSERT ... VALUES(all_columns)` — will fail on tables with computed columns
   - Must detect computed columns at init (query `sys.computed_columns`) and filter them from hydration INSERTs
   - Reference: `postgres/proxy/write.go` generated column filtering

4. **INSERT PK tracking** (`write.go`)
   - `handleInsert` uses `MarkInserted(table)` (table-level) instead of row-level delta tracking
   - This forces merged reads for all rows, even untouched ones
   - Must capture inserted PKs via OUTPUT clause or SCOPE_IDENTITY()

### P1 — High (Feature correctness)

5. **JOIN patch strategy** (`read.go`)
   - JOINs on affected tables return stale data
   - Requires: Prod JOIN execution, row classification, Shadow patching, WHERE re-eval
   - Reference: `postgres/proxy/read_join.go` (848 lines)

6. **Foreign key enforcement** (new file needed: `fk.go`)
   - Without FK enforcement, child INSERTs referencing tombstoned parents succeed in Shadow but would fail in production
   - Must: discover FKs at init, validate parent rows on INSERT/UPDATE, enforce RESTRICT/CASCADE on DELETE
   - Reference: `postgres/proxy/fk.go` (660 lines)

7. **Savepoint system** (`txn.go`)
   - ORMs (Entity Framework, Sequelize) and applications commonly use `SAVE TRAN` / `ROLLBACK TRAN name`
   - Must: classify SAVE TRAN/ROLLBACK TRAN savepointname, implement snapshot stack
   - Reference: `postgres/proxy/txn.go` lines 150-300

8. **Schema registry snapshot on BEGIN** (`txn.go`)
   - DDL inside a rolled-back transaction leaves schema registry in inconsistent state
   - Must snapshot schema registry at BEGIN, restore on ROLLBACK

### P2 — Medium (Advanced read correctness)

9. **Aggregate decomposition** (`read.go`)
   - Extend beyond bare COUNT(*) to support SUM/AVG/MIN/MAX with GROUP BY
   - Support HAVING filters
   - Reference: `postgres/proxy/aggregate.go` (959 lines)

10. **Set operations (UNION/INTERSECT/EXCEPT)** (new file needed)
    - Decompose into leaf SELECTs, merge independently, apply set operation
    - Reference: `postgres/proxy/read_setop.go` (303 lines)

11. **Window function materialization** (new file needed)
    - Build base SELECT, merge, materialize into temp table, re-execute with windows
    - Reference: `postgres/proxy/read_window.go` (442 lines)

12. **CTE/derived table materialization** (new file needed)
    - Materialize dirty CTEs/subqueries into temp tables, rewrite query
    - Reference: `postgres/proxy/read_complex.go` (596 lines)

13. **Temp table management utilities** (new file needed)
    - Deterministic temp table naming, creation, bulk insert, cleanup
    - Reference: `postgres/proxy/util_table.go` (213 lines)

### P3 — Low (Polish and edge cases)

14. **WHERE clause evaluator** (new file needed)
    - Required for JOIN patch WHERE re-evaluation
    - Reference: `postgres/proxy/where_eval.go` (457 lines)

15. **Prod query rewriting for schema diffs** (new file or extend `read.go`)
    - Drop/rename/add column rewriting for Prod queries
    - Reference: `postgres/proxy/rewrite_prod.go` (381 lines)

16. **Missing classifier feature flags**
    - `HasDistinct`, `HasReturning` (OUTPUT), `HasWindowFunc`, `HasComplexAgg`, `IsMetadataQuery`
    - Enables better routing decisions

17. **TRUNCATE handling with IsFullyShadowed**
    - Mark table as fully shadowed after TRUNCATE, skip Prod for subsequent reads

18. **Rename column tracking via sp_rename**
    - Detect `EXEC sp_rename 't.old', 'new', 'COLUMN'` and record in schema registry

19. **IDENTITY_INSERT concurrency handling**
    - SQL Server only allows one table with IDENTITY_INSERT ON per session
    - Hydrating multiple tables in sequence needs proper ON/OFF toggling

20. **Synthetic response via raw TDS (not VALUES query)**
    - Use `buildTDSSelectResponse` instead of `buildSyntheticSelect` to avoid type loss and double-encoding
    - The infrastructure already exists in `tdsquery.go` line 1106

---

## Appendix: File-by-File Comparison

### PostgreSQL proxy files with no MSSQL equivalent

| PostgreSQL File | Lines | Purpose | MSSQL Status |
|----------------|-------|---------|--------------|
| `fk.go` | 660 | FK discovery + enforcement | ❌ Not started |
| `read_join.go` | 848 | JOIN patch strategy | ❌ Not started (stubbed) |
| `read_complex.go` | 596 | CTE/derived table handling | ❌ Not started |
| `read_setop.go` | 303 | UNION/INTERSECT/EXCEPT | ❌ Not started |
| `read_window.go` | 442 | Window function materialization | ❌ Not started |
| `aggregate.go` | 959 | Full aggregate decomposition | ⚠️ Minimal (COUNT only) |
| `where_eval.go` | 457 | WHERE clause evaluator | ❌ Not started |
| `util_table.go` | 213 | Temp table utilities | ❌ Not started |
| `rewrite_prod.go` | 381 | Prod query rewriting for schema diffs | ⚠️ Minimal (new-col strip only) |
| `ddl_parse.go` | 345 | DDL change parser | ⚠️ Minimal (inline regex, ADD/DROP only) |
| `write_insert.go` | 588 | INSERT with FK + RETURNING | ⚠️ Basic (no FK, no OUTPUT) |
| `write_update.go` | 970 | UPDATE with bulk hydration | ⚠️ Basic (point update only) |
| `write_delete.go` | 487 | DELETE with FK + RETURNING | ⚠️ Basic (point delete only) |
| `cursor.go` | 140 | Cursor operations | ❌ Not started |
| `read_single.go` | ~400 | Single-table merged read | ⚠️ Merged into `read.go` |

**Total missing/incomplete code**: ~7,389 lines of PostgreSQL proxy logic have no equivalent in MSSQL.

### MSSQL-specific files with no PostgreSQL equivalent

| MSSQL File | Lines | Purpose | Notes |
|-----------|-------|---------|-------|
| `proxy/tdsmsg.go` | 240 | TDS packet read/write/build | Protocol-specific |
| `proxy/tdsquery.go` | 1174 | TDS token parsing + query execution | Protocol-specific, well implemented |
| `proxy/startup.go` | 454 | TDS handshake (PRELOGIN/LOGIN7) | Protocol-specific |
| `proxy/ext_handler.go` | 565 | RPC (sp_executesql/prepare) | Protocol-specific |
| `connstr/connstr.go` | 230 | MSSQL connection string parsing | Fully implemented |
| `shadow/container.go` | 297 | Docker container management | Fully implemented |
