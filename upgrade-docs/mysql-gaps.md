# MySQL Engine — Gap Analysis vs. PostgreSQL Engine

This document provides an exhaustive gap analysis of the MySQL engine implementation compared to the PostgreSQL engine (the most mature implementation in Mori). Each section identifies what is implemented, what is partially implemented, and what is entirely missing.

---

## 1. Engine Overview

The MySQL engine provides a functional but incomplete implementation of the Mori copy-on-write proxy pattern. The core architecture mirrors PostgreSQL: a proxy intercepts MySQL wire protocol traffic, classifies SQL, routes to Prod or Shadow, and merges results. The following subsystems are present:

- **Classifier** (`classify/classifier.go`): AST-based via Vitess sqlparser with regex fallback
- **Connection string parsing** (`connstr/connstr.go`): Go DSN and URI formats
- **Wire protocol** (`proxy/mysqlmsg.go`, `proxy/mysqlwire.go`): MySQL packet read/write, message construction
- **Handshake & TLS** (`proxy/startup.go`): Full MySQL handshake relay with SSL/TLS negotiation
- **Read handler** (`proxy/read.go`): Merged read and JOIN patch with dedup, sort, limit
- **Write handler** (`proxy/write.go`): INSERT, UPDATE (with hydration), DELETE (with tombstones)
- **DDL handler** (`proxy/ddl.go`): Schema tracking for ADD/DROP/RENAME COLUMN, CREATE/DROP TABLE
- **Transaction handler** (`proxy/txn.go`): BEGIN/COMMIT/ROLLBACK with staged deltas
- **Extended protocol** (`proxy/ext_handler.go`): COM_STMT_PREPARE/EXECUTE/CLOSE routing
- **Guard rails** (`proxy/guard.go`): Three-layer write protection (L1, L2, L3)
- **Shadow container** (`shadow/container.go`): Docker lifecycle management for MySQL
- **Schema dump & init** (`schema/schema.go`, `init.go`): mysqldump, table metadata, auto_increment offsets

**Overall maturity**: The MySQL engine covers roughly 50-60% of PostgreSQL engine features. Core read/write paths work for simple cases, but many advanced features (complex reads, aggregates, FK enforcement, window functions, set operations, cursors, generated columns) are missing or severely limited.

---

## 2. Feature Parity Matrix

| Feature | PostgreSQL | MySQL | Status |
|---------|-----------|-------|--------|
| **Classification** | | | |
| AST-based SQL parsing | pg_query_go | Vitess sqlparser | ✅ Implemented |
| Regex fallback classifier | Yes | Yes | ✅ Implemented |
| SELECT classification | Yes | Yes | ✅ Implemented |
| INSERT/UPDATE/DELETE | Yes | Yes | ✅ Implemented |
| DDL (CREATE/ALTER/DROP) | Yes | Yes | ✅ Implemented |
| Transaction (BEGIN/COMMIT/ROLLBACK) | Yes | Yes | ✅ Implemented |
| SAVEPOINT/RELEASE classification | SubSavepoint/SubRelease | Not classified (falls to SubOther) | ❌ Missing |
| CURSOR (DECLARE/FETCH/CLOSE) | SubCursor | Not classified | ❌ Missing |
| PREPARE/EXECUTE/DEALLOCATE classification | SubPrepare/SubExecute/SubDeallocate | Not classified (handled at protocol level only) | ➖ N/A (MySQL uses binary protocol) |
| LISTEN/UNLISTEN | SubListen | Not classified | ➖ N/A (MySQL has no LISTEN) |
| EXPLAIN classification | SubExplain (rejects ANALYZE) | SubOther (no ANALYZE rejection) | ⚠️ Partial |
| SET/SHOW classification | SubSet/SubShow | SubOther (both lumped together) | ⚠️ Partial |
| TRUNCATE classification | SubTruncate | SubOther | ⚠️ Partial |
| HasWindowFunc flag | Yes | No | ❌ Missing |
| HasDistinct flag | Yes | No | ❌ Missing |
| HasComplexAgg flag | Yes | No | ❌ Missing |
| HasCursor flag | Yes | No | ❌ Missing |
| IsMetadataQuery flag | Yes | No | ❌ Missing |
| HasReturning flag | Yes | No | ➖ N/A (MySQL has no RETURNING in standard syntax) |
| HasOnConflict flag | Yes | No | ❌ Missing (MySQL uses ON DUPLICATE KEY UPDATE) |
| IntrospectedTable | Yes | No | ❌ Missing |
| Mutating CTE detection | Yes (walks WITH clause) | Regex-only (checks for INSERT/UPDATE/DELETE keywords) | ⚠️ Partial |
| **Routing** | | | |
| StrategyProdDirect | Yes | Yes | ✅ Implemented |
| StrategyMergedRead | Yes | Yes | ✅ Implemented |
| StrategyJoinPatch | Yes | Yes | ✅ Implemented |
| StrategyShadowWrite | Yes | Yes | ✅ Implemented |
| StrategyHydrateAndWrite | Yes | Yes | ✅ Implemented |
| StrategyShadowDelete | Yes | Yes | ✅ Implemented |
| StrategyShadowDDL | Yes | Yes | ✅ Implemented |
| StrategyTransaction | Yes | Yes | ✅ Implemented |
| StrategyNotSupported | Yes | Not explicitly used | ⚠️ Partial |
| StrategyForwardBoth (SET) | Yes | Yes (targetBoth in conn.go) | ✅ Implemented |
| StrategyTruncate | Yes (marks IsFullyShadowed) | Not implemented (TRUNCATE classified as SubOther, no special routing) | ❌ Missing |
| StrategyListenOnly | Yes | N/A | ➖ N/A |
| **Read Operations** | | | |
| Single-table merged read | Yes | Yes | ✅ Implemented |
| PK injection for dedup | Yes (with CTID fallback) | Yes (no CTID equivalent) | ⚠️ Partial |
| LIMIT over-fetching | Yes | Yes | ✅ Implemented |
| Schema adaptation (added/dropped/renamed cols) | Yes | Yes | ✅ Implemented |
| ORDER BY re-sorting | Yes | Yes | ✅ Implemented |
| Aggregate queries (simple: COUNT/SUM/AVG/MIN/MAX) | Full re-aggregation pipeline | COUNT(*) only fallback | ⚠️ Partial |
| Aggregate queries (GROUP BY) | Full re-aggregation with HAVING | Falls back to Prod-only | ❌ Missing |
| Complex aggregates (json_agg, array_agg, etc.) | Materialization + Shadow re-execution | Not implemented | ❌ Missing |
| JOIN patch algorithm | Full (classify/patch/dedup/WHERE re-eval) | Partial (classify/patch/dedup, no WHERE re-eval) | ⚠️ Partial |
| JOIN schema diff handling | Materializes dirty tables into temp tables | Not implemented | ❌ Missing |
| CTE handling (non-recursive) | Materializes dirty CTEs into temp tables | Not implemented (falls through to Prod or Shadow) | ❌ Missing |
| CTE handling (recursive) | Materializes dirty base tables, rewrites query | Not implemented | ❌ Missing |
| Derived tables (subqueries in FROM) | Recursive rewrite with temp tables | Not implemented | ❌ Missing |
| LATERAL joins | Materializes dirty base tables | Not implemented | ❌ Missing |
| Set operations (UNION/INTERSECT/EXCEPT) | Full leaf decomposition + in-memory set ops | Not implemented (routed but no special handling) | ❌ Missing |
| Window functions | Materialization + Shadow re-execution | Not implemented | ❌ Missing |
| Prod query rewriting (dropped/renamed/added cols) | Full AST-based rewriting | String-based rewriting (partial) | ⚠️ Partial |
| WHERE clause evaluation (for JOIN patch) | Full expression evaluator | Not implemented | ❌ Missing |
| **Write Operations** | | | |
| Simple INSERT | Yes (with FK enforcement + RETURNING PK capture) | Yes (no FK enforcement, no PK capture) | ⚠️ Partial |
| INSERT ... ON CONFLICT / ON DUPLICATE KEY | Full (detects conflict cols, pre-hydrates) | Not implemented (no HasOnConflict detection) | ❌ Missing |
| Point UPDATE (PKs extractable) | Yes (with FK enforcement) | Yes (no FK enforcement) | ⚠️ Partial |
| Bulk UPDATE (PKs not extractable) | Full (queries Prod, hydrates, rewrites WHERE) | Forward to Shadow without hydration | ❌ Missing |
| Cross-table UPDATE (UPDATE ... FROM / JOIN) | Yes (extracts subquery predicates) | Not implemented | ❌ Missing |
| Point DELETE (PKs extractable) | Yes (with FK enforcement, RETURNING support) | Yes (no FK enforcement, no RETURNING) | ⚠️ Partial |
| Bulk DELETE (PKs not extractable) | Full (pre-queries Prod for matching PKs) | Forward to Shadow without tombstoning | ❌ Missing |
| DELETE RETURNING | Yes | Not applicable (MySQL has no RETURNING) | ➖ N/A |
| TRUNCATE handling | Marks table IsFullyShadowed, CASCADE tracking | Not implemented | ❌ Missing |
| Hydration system | Full (generated col filtering, schema rewriting, max rows cap) | Basic (single row by PK, no generated col filtering) | ⚠️ Partial |
| Generated column filtering during hydration | Yes (pg_attribute.attgenerated) | Not implemented | ❌ Missing |
| Schema rewriting for Prod during hydration | Yes | Not implemented | ❌ Missing |
| Max rows hydration cap | Yes | Not implemented | ❌ Missing |
| **DDL & Schema Tracking** | | | |
| ADD COLUMN tracking | Yes | Yes | ✅ Implemented |
| DROP COLUMN tracking | Yes | Yes | ✅ Implemented |
| RENAME COLUMN tracking | Yes | Yes | ✅ Implemented |
| ALTER TYPE tracking | Yes (RecordTypeChange) | Not implemented | ❌ Missing |
| CREATE TABLE tracking | Yes (RecordNewTable) | Yes | ✅ Implemented |
| DROP TABLE tracking | Yes (RemoveTable) | Yes | ✅ Implemented |
| FK stripping from DDL | Yes | Yes (line-based removal) | ✅ Implemented |
| FK metadata extraction from DDL | Yes (stored before stripping) | Not implemented | ❌ Missing |
| Query rewriting for schema diffs | Full (dropped/renamed/added in SELECT/WHERE/ORDER BY/GROUP BY/HAVING) | Partial (added cols stripped from SELECT, renamed cols replaced) | ⚠️ Partial |
| **Transaction Management** | | | |
| BEGIN (dual-backend) | Yes (Shadow=BEGIN, Prod=REPEATABLE READ) | Yes (both backends BEGIN, no isolation upgrade) | ⚠️ Partial |
| COMMIT (staged delta promotion) | Yes | Yes | ✅ Implemented |
| ROLLBACK (staged delta discard) | Yes | Yes | ✅ Implemented |
| Schema registry snapshot on BEGIN | Yes | Not implemented | ❌ Missing |
| Schema registry restore on ROLLBACK | Yes | Not implemented | ❌ Missing |
| SAVEPOINT (stack-based snapshots) | Full (delta map, tombstone set, insert counts, schema state) | Forward to both backends only, no state snapshot | ❌ Missing |
| RELEASE SAVEPOINT | Full (pop snapshot) | Forward to both backends only | ❌ Missing |
| ROLLBACK TO SAVEPOINT | Full (restore snapshot, keep savepoint) | Forward to both backends only | ❌ Missing |
| Staged delta system (Stage/Commit/Rollback) | Yes | Yes | ✅ Implemented |
| **Foreign Key Enforcement** | | | |
| FK discovery from Prod | Yes (pg_catalog query) | Not implemented | ❌ Missing |
| FK metadata storage in Schema Registry | Yes | Not implemented | ❌ Missing |
| FK discovery from DDL | Yes | Not implemented | ❌ Missing |
| Parent row validation (INSERT/UPDATE) | Full (delta check, tombstone check, Shadow, Prod) | Not implemented | ❌ Missing |
| RESTRICT/NO ACTION enforcement (DELETE) | Yes | Not implemented | ❌ Missing |
| CASCADE enforcement (DELETE) | Yes (recursive tombstoning) | Not implemented | ❌ Missing |
| Multi-column (composite) FK validation | Yes | Not implemented | ❌ Missing |
| FK caching | Yes | Not implemented | ❌ Missing |
| **Protocol Handling** | | | |
| Wire protocol read/write | pgwire (5-byte header) | MySQL protocol (4-byte header) | ✅ Implemented |
| Handshake relay | Yes (with SSL/TLS) | Yes (with SSL/TLS) | ✅ Implemented |
| SSL/TLS negotiation | SASL PLUS stripping | CLIENT_SSL stripping + TLS upgrade | ✅ Implemented |
| Result set materialization | execQuery() with drain | execMySQLQuery() with full parse | ✅ Implemented |
| Response building (from in-memory data) | RowDescription + DataRow construction | MySQL result set construction | ✅ Implemented |
| COM_QUIT handling | N/A | Yes | ✅ Implemented |
| COM_PING handling | N/A | Yes | ✅ Implemented |
| COM_INIT_DB handling | N/A | Yes (forward to both) | ✅ Implemented |
| **Extended Protocol / Prepared Statements** | | | |
| Statement preparation | Parse ('P') message | COM_STMT_PREPARE | ✅ Implemented |
| Parameter binding | Bind ('B') with binary decoding | COM_STMT_EXECUTE param parsing | ✅ Implemented |
| Statement execution with routing | Full pipeline (classify, route, dispatch) | Full pipeline (classify, route, dispatch) | ✅ Implemented |
| Statement close | Close ('C') with cache eviction | COM_STMT_CLOSE with cache eviction | ✅ Implemented |
| Describe (statement/portal) | Yes ('D' message) | Not applicable (MySQL returns metadata in PREPARE response) | ➖ N/A |
| Sync/batch processing | Yes ('S' message, batch accumulation) | Not applicable (MySQL is request-response) | ➖ N/A |
| Binary param decoding (bool, int, UUID) | Yes (bool, int16, int32, int64, UUID) | Yes (tiny, short, long, longlong, float, double, string) | ✅ Implemented |
| Shadow-only statement tracking | Yes | Not implemented | ❌ Missing |
| Prepared statement write routing (INSERT/UPDATE/DELETE) | Full (InsertExt, UpdateExt, DeleteExt) | Present but via SQL reconstruction (loses binary protocol fidelity) | ⚠️ Partial |
| Prepared statement read routing (merged read/join patch) | Full (MergedReadExt, JoinPatchExt) | Present (substituteParams + text protocol) | ⚠️ Partial |
| **Guard Rails** | | | |
| L1: validateRouteDecision | Yes | Yes | ✅ Implemented |
| L2: SafeProdConn | Yes | Yes | ✅ Implemented |
| L3: classifyAndRoute final assertion | Yes | Yes | ✅ Implemented |
| Write detection (looksLikeWrite) | Yes (INSERT, UPDATE, DELETE, TRUNCATE, CREATE, ALTER, DROP, GRANT, REVOKE, CTE writes) | Yes (same + REPLACE, LOAD DATA, RENAME) | ✅ Implemented |
| SQLSTATE error codes | MR001 | MR001 (code 1105) | ✅ Implemented |
| **Initialization** | | | |
| Connection string parsing | libpq format | Go DSN + URI format | ✅ Implemented |
| Version detection | SHOW server_version | SELECT VERSION() | ✅ Implemented |
| Docker image matching | postgres:X.Y | mysql:X.Y / mariadb:X.Y | ✅ Implemented |
| Schema dump | pg_dump --schema-only (inside Docker) | mysqldump --no-data (inside Docker) | ✅ Implemented |
| FK stripping from dump | Yes | Yes | ✅ Implemented |
| Table metadata detection | Yes (PK columns, types) | Yes (PK columns, types, auto_increment) | ✅ Implemented |
| Generated column detection | Yes (pg_attribute.attgenerated) | Not implemented | ❌ Missing |
| Sequence/auto_increment offset calculation | Yes (MAX(pk) with offset formula) | Yes (MAX(pk) with same offset formula) | ✅ Implemented |
| FK discovery during init | Yes (pg_catalog query) | Not implemented | ❌ Missing |
| Extension handling | Yes (detect, filter managed, auto-install) | Not applicable | ➖ N/A |
| Read-replica conflict handling | Yes (SQLSTATE 40001 retry) | Not implemented | ❌ Missing |
| **Cursor Operations** | | | |
| DECLARE CURSOR | Classifies inner query, materializes if affected | Not implemented | ❌ Missing |
| FETCH | Reads from appropriate backend | Not implemented | ❌ Missing |
| CLOSE cursor | Closes on both backends | Not implemented | ❌ Missing |
| **Utility & Materialization** | | | |
| Temp table management | utilTableName, buildCreateTempTableSQL, bulkInsert, drop | Not implemented | ❌ Missing |
| Scoped materialization | buildScopedMaterializationSQL | Not implemented | ❌ Missing |
| SQL construction helpers | quoteIdent, quoteLiteral, buildInsertSQL, capSQL | Minimal (buildMySQLInsertSQL only) | ⚠️ Partial |
| **MariaDB Support** | | | |
| Thin adapter (like CockroachDB for PG) | CockroachDB: ID() override only | MariaDB: ID() override + Init override for image detection | ✅ Implemented |

---

## 3. Classification Gaps

### Missing SubType Distinctions

The MySQL classifier (`classify/classifier.go`) lumps many distinct statement types under `core.SubOther`:

| Statement | PostgreSQL SubType | MySQL SubType | Gap |
|-----------|-------------------|---------------|-----|
| SET | SubSet | SubOther | Prevents SET-specific routing (StrategyForwardBoth) |
| SHOW | SubShow | SubOther | Prevents SHOW-specific routing (StrategyProdDirect) |
| EXPLAIN | SubExplain | SubOther | No EXPLAIN ANALYZE rejection |
| TRUNCATE | SubTruncate | SubOther | Prevents TRUNCATE-specific routing (StrategyTruncate) |
| SAVEPOINT | SubSavepoint | Not classified (falls to default in TxnHandler) | No savepoint state management |
| RELEASE SAVEPOINT | SubRelease | Not classified | No savepoint release handling |

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/classify/classifier.go`, lines 141-159 -- the `default` case in `classifyStmt()` catches SET, SHOW, ExplainStmt, ExplainTab, Use all as `SubOther`.

### Missing Feature Flags

The MySQL classifier does not set the following feature flags on `core.Classification`:

1. **HasWindowFunc** -- Window function detection (`FUNC() OVER (...)`) is absent. The classifier does not walk select expressions for window function patterns. This means window function queries will never be routed through the window function materialization path.
   - **File**: `classify/classifier.go`, `classifySelect()` lines 163-218 -- no window function detection logic.

2. **HasDistinct** -- `SELECT DISTINCT` is not detected. The `HasDistinct` flag is never set.
   - **File**: `classify/classifier.go`, `classifySelect()` -- no check for `sel.Distinct`.

3. **HasComplexAgg** -- Complex aggregate detection (GROUP_CONCAT, JSON_ARRAYAGG, JSON_OBJECTAGG in MySQL's case) is not distinguished from simple aggregates. `HasComplexAgg` is never set.
   - **File**: `classify/classifier.go`, `containsAggregate()` lines 397-416 -- detects aggregate names but does not set a separate `HasComplexAgg` flag for MySQL-specific complex aggregates.

4. **HasOnConflict** -- MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` is not detected. The classifier does not check for `ins.OnDup` in the Vitess AST.
   - **File**: `classify/classifier.go`, `classifyInsert()` lines 245-261 -- no `OnDup` check.

5. **IsMetadataQuery** -- Queries against `information_schema` or `mysql` system schemas are not detected.
   - **File**: `classify/classifier.go` -- no metadata query detection logic.

6. **HasCursor** -- MySQL cursors (DECLARE/FETCH/CLOSE within stored procedures) are not classified. However, MySQL's server-side cursors via COM_STMT_FETCH are also not handled.

### PK Extraction Limitations

- **Only supports `column = value` and `column = ?`**: The MySQL PK extractor in `extractPKsFromExpr()` (line 429) only handles `ComparisonExpr` with `EqualOp` and `AndExpr`. It does not handle:
  - `value = column` (reversed comparison) -- PostgreSQL handles this
  - TypeCast unwrapping (e.g., `CAST(? AS UNSIGNED)`) -- PostgreSQL unwraps `::uuid`
  - `IN` clauses with single values
- **No composite PK serialization**: While composite PKs are detected in metadata, the classifier only extracts individual PK columns, not composite key tuples serialized as JSON (as PostgreSQL does).

---

## 4. Read Operation Gaps

### 4.1 Aggregate Queries

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/read.go`, lines 210-283

The aggregate read implementation (`aggregateReadCore`) is severely limited:

- **Only handles bare `COUNT(*)`**: The `buildAggregateBaseQuery` method (line 263) explicitly returns `""` (fallback to Prod) when `GROUP BY` is present. It only converts `COUNT(*)` to a row-level query.
- **No SUM/AVG/MIN/MAX re-aggregation**: PostgreSQL re-aggregates all standard functions in Go. MySQL falls back to Prod for everything except bare COUNT(*).
- **No HAVING support**: GROUP BY queries with HAVING are not handled.
- **No COUNT(DISTINCT col)**: Not specially handled.
- **Complex aggregates (GROUP_CONCAT, JSON_ARRAYAGG, JSON_OBJECTAGG)**: Not handled at all. PostgreSQL materializes base data into a temp table and re-executes on Shadow.
- **Fallback behavior**: Returns only the count from the merged base query. Does not properly return column metadata from the original query (line 256: returns hardcoded `count(*)` column name).

### 4.2 Complex Queries (CTEs, Derived Tables, LATERAL)

**Entirely missing**. PostgreSQL has a dedicated `read_complex.go` file with:
- Non-recursive CTE materialization
- Recursive CTE rewriting
- Derived table (subquery in FROM) rewriting
- LATERAL join materialization

MySQL classifies these as `IsComplexRead = true` but has no corresponding handler. Complex reads fall through to the default merged read path, which will produce incorrect results when CTE/subquery tables have deltas.

### 4.3 Set Operations (UNION/INTERSECT/EXCEPT)

**Entirely missing**. PostgreSQL has `read_setop.go` with leaf decomposition and in-memory set operations. MySQL classifies `HasSetOp = true` but has no set operation handler. These queries are routed through the standard merged read, which does not decompose the UNION into independent leaf queries.

### 4.4 Window Functions

**Entirely missing**. PostgreSQL has `read_window.go` with materialization into temp table + Shadow re-execution. MySQL does not detect window functions (no `HasWindowFunc` flag) and has no window function handler. Window function queries will be routed as regular reads, producing incorrect results for affected tables.

### 4.5 WHERE Clause Evaluation

**Entirely missing**. PostgreSQL has `where_eval.go` with a full expression evaluator supporting `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, LIKE, IN, BETWEEN, IS NULL, IS NOT NULL, AND, OR, NOT. MySQL's JOIN patch algorithm (`patchDeltaRow`, line 1331) patches delta values but never re-evaluates the original WHERE clause on patched rows. This means patched JOIN rows may not satisfy the original query's WHERE conditions.

### 4.6 Temp Table / Materialization Infrastructure

**Entirely missing**. PostgreSQL has `util_table.go` with:
- `utilTableName()` for deterministic temp table naming
- `buildCreateTempTableSQL()` for temp table creation from query results
- `bulkInsertToUtilTable()` for batch inserts
- `dropUtilTable()` for cleanup
- `buildScopedMaterializationSQL()` for pushdown optimization

This infrastructure is a prerequisite for implementing complex reads, window functions, complex aggregates, and set operations in MySQL.

### 4.7 PK-less Table Handling

**Missing**. PostgreSQL uses `ctid` (system column) as row identity for tables without primary keys. MySQL has no equivalent system column. The MySQL dedup function (line 1093) silently skips deduplication when no PK is found, which means duplicate rows may appear in merged read results for PK-less tables.

---

## 5. Write Operation Gaps

### 5.1 INSERT Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/write.go`, lines 48-56

- **No FK constraint enforcement**: PostgreSQL validates parent row existence before INSERT. MySQL does not.
- **No PK capture from result**: PostgreSQL captures RETURNING clause to extract inserted PKs for precise delta tracking. MySQL only calls `MarkInserted(table)` which tracks at the table level, not the row level.
- **No INSERT ... ON DUPLICATE KEY UPDATE handling**: This is MySQL's equivalent of PostgreSQL's `INSERT ... ON CONFLICT`. PostgreSQL detects conflict columns, pre-hydrates matching Prod rows, and merges PKs. MySQL has no detection or handling. An ON DUPLICATE KEY UPDATE that conflicts with a Prod row will fail on Shadow (row not present).

### 5.2 UPDATE Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/write.go`, lines 58-97

- **No FK constraint enforcement on SET values**: PostgreSQL validates FK columns in the SET clause.
- **Bulk UPDATE (no PKs) is incomplete**: When PKs cannot be extracted from WHERE, MySQL just forwards to Shadow without hydration (line 62-66). PostgreSQL queries Prod for matching rows, hydrates them all, and rewrites the WHERE clause with concrete PK values.
- **No cross-table UPDATE handling**: MySQL UPDATE ... JOIN syntax is not specially handled. PostgreSQL extracts subquery predicates and hydrates referenced tables.
- **No WHERE clause evaluation on patched delta rows**: PostgreSQL evaluates the WHERE on delta rows to ensure they actually match.
- **No schema rewriting for Prod during bulk hydration**: PostgreSQL rewrites Prod queries to account for schema diffs.

### 5.3 DELETE Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/write.go`, lines 99-132

- **No FK RESTRICT/NO ACTION enforcement**: PostgreSQL rejects parent DELETE if child rows exist. MySQL does not check.
- **No FK CASCADE enforcement**: PostgreSQL recursively tombstones child rows on CASCADE. MySQL does not.
- **Bulk DELETE (no PKs) is incomplete**: When PKs cannot be extracted, MySQL forwards to Shadow without tombstoning (lines 101-107). Prod rows matching the WHERE are not tracked, leading to ghost rows in subsequent reads.
- **No correct row count computation**: PostgreSQL computes total count including Prod-only rows. MySQL just forwards the Shadow response.

### 5.4 Hydration Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/write.go`, lines 134-189

- **No generated column filtering**: PostgreSQL detects `GENERATED ALWAYS AS STORED` columns via `pg_attribute.attgenerated` and excludes them from hydration INSERTs. MySQL does not detect generated columns at all. Hydrating a row with a generated column will cause a MySQL error (`The value specified for generated column ... is not allowed`).
- **No schema rewriting**: Hydration queries Prod with `SELECT *` which will fail if columns have been renamed or dropped via DDL.
- **No max rows cap**: PostgreSQL has a cap on hydration rows to prevent unbounded hydration. MySQL does not.
- **No composite PK handling in hydration**: `hydrateRow` uses only `meta.PKColumns[0]` (line 141), ignoring composite primary keys.
- **Uses INSERT IGNORE**: The `buildMySQLInsertSQL` function (line 173) uses `INSERT IGNORE` which silently drops rows that violate constraints. This could mask hydration errors.

---

## 6. DDL & Schema Tracking Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/ddl.go`

### Implemented
- ADD COLUMN detection and tracking
- DROP COLUMN detection and tracking
- RENAME COLUMN detection and tracking
- CREATE TABLE tracking (RecordNewTable)
- DROP TABLE tracking (RemoveTable + ClearTable on delta/tombstones)

### Missing

1. **ALTER TYPE tracking**: PostgreSQL calls `RecordTypeChange(table, col, oldType, newType)`. MySQL DDL parsing does not detect type changes (`ALTER TABLE t MODIFY COLUMN c BIGINT`). MySQL-specific ALTER syntax (`MODIFY COLUMN`, `CHANGE COLUMN`) is not parsed.
   - **File**: `proxy/ddl.go`, `parseMySQLDDLChanges()` -- no regex for MODIFY COLUMN or CHANGE COLUMN.

2. **FK metadata extraction before stripping**: PostgreSQL extracts and stores FK metadata from DDL BEFORE stripping FKs. MySQL strips FKs during init (`schema.StripForeignKeys`) but does not extract them.
   - **File**: `/Users/psrth/Development/mori/internal/engine/mysql/schema/schema.go`, `StripForeignKeys()` line 50 -- just removes lines, does not parse FK relationships.

3. **Multi-statement ALTER TABLE**: MySQL allows `ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b`. The regex-based parser only extracts the first change from a multi-operation ALTER.
   - **File**: `proxy/ddl.go`, `parseMySQLDDLChanges()` -- returns after first match.

4. **RENAME TABLE**: Not handled. MySQL's `RENAME TABLE old TO new` is not parsed.

5. **Query rewriting for schema diffs**: The `rewriteForProd()` method (read.go line 832) handles added columns (strips from Prod SELECT) and renamed columns (replaces names). However:
   - Dropped columns are not removed from WHERE, ORDER BY, GROUP BY, or HAVING clauses
   - Only operates on the top-level SELECT list, not nested subqueries
   - Does not handle SELECT * correctly when columns have been both added and dropped

---

## 7. Transaction Management Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/txn.go`

### Implemented
- BEGIN on both backends
- COMMIT with staged delta promotion
- ROLLBACK with staged delta discard
- Staged delta system (Stage/Commit/Rollback)
- In-transaction write routing with staging

### Missing

1. **No REPEATABLE READ isolation for Prod**: PostgreSQL sends `BEGIN ISOLATION LEVEL REPEATABLE READ` to Prod for consistent merged reads. MySQL sends plain BEGIN to both (line 69-72). The comment says "standard BEGIN is sufficient" but this does not guarantee snapshot isolation on Prod during the transaction.

2. **No schema registry snapshot**: PostgreSQL takes a schema registry snapshot on BEGIN and restores it on ROLLBACK. MySQL does not snapshot or restore the schema registry, meaning a ROLLBACK after DDL within a transaction will leave the schema registry in a dirty state.
   - **File**: `proxy/txn.go`, `handleBegin()` -- no snapshot logic.
   - **File**: `proxy/txn.go`, `handleRollback()` -- no restore logic.

3. **No SAVEPOINT state management**: SAVEPOINTs are forwarded to both backends but no state snapshots are taken. PostgreSQL captures delta map, tombstone set, insert counts, and schema registry state for each SAVEPOINT. On ROLLBACK TO SAVEPOINT, it restores state. MySQL just forwards the commands, so:
   - `ROLLBACK TO SAVEPOINT sp1` will roll back Shadow data but the delta map and tombstone set will retain changes made after the savepoint.
   - This causes read divergence: the proxy thinks rows are delta/tombstoned but Shadow has rolled them back.

4. **No RELEASE SAVEPOINT handling**: Just forwarded, no stack pop.

---

## 8. Foreign Key Enforcement Gaps

**Entirely missing**. PostgreSQL has a dedicated `fk.go` file with:

- FK discovery from Prod schema (`pg_catalog` query) during initialization
- FK metadata storage in Schema Registry
- Additional FK discovery from DDL
- Parent row validation on INSERT/UPDATE (4-step check: delta map, tombstone, Shadow query, Prod query)
- RESTRICT/NO ACTION enforcement on DELETE
- CASCADE enforcement on DELETE (recursive tombstoning)
- Multi-column (composite) FK validation
- FK caching per table

MySQL has **none** of these. The only FK-related code is:
- `StripForeignKeys()` in `schema/schema.go` -- removes FK lines from schema dump
- No FK discovery during init
- No FK validation on writes
- No referential action enforcement on deletes

This means:
- INSERTs with invalid foreign key references will succeed on Shadow but fail on Prod
- DELETEs of parent rows with CASCADE dependencies will not tombstone child rows
- The proxy cannot enforce referential integrity at all

---

## 9. Protocol-Level Gaps

### Implemented
- MySQL wire protocol packet read/write (`mysqlmsg.go`)
- Handshake relay with SSL/TLS (`startup.go`)
- COM_QUERY routing
- COM_QUIT, COM_PING, COM_INIT_DB handling
- COM_STMT_PREPARE/EXECUTE/CLOSE routing (`ext_handler.go`)
- Result set parsing and building (`read.go`)
- Error packet construction

### Missing

1. **COM_FIELD_LIST (0x04)**: Not handled -- forwards to Prod as unknown command. This is deprecated but still used by some MySQL clients.

2. **COM_STMT_RESET (0x1a)**: Not handled. Should evict cached prepared statement state.

3. **COM_STMT_FETCH (0x1c)**: Server-side cursor fetch for prepared statements. Not handled at all. MySQL server-side cursors will not work through the proxy.

4. **COM_CHANGE_USER (0x11)**: Not handled. If a client changes user mid-connection, the proxy will not re-authenticate with Shadow.

5. **Multi-result sets**: MySQL supports `CLIENT_MULTI_RESULTS` and stored procedures can return multiple result sets. The `relayResponse()` function only handles a single result set (one column-defs-EOF-rows-EOF cycle).
   - **File**: `proxy/mysqlwire.go`, `relayResponse()` lines 110-155.

6. **LOCAL INFILE**: `LOAD DATA LOCAL INFILE` requires special protocol handling (the server requests the file from the client). Not handled.

7. **Deprecation notices / OK packet flags**: MySQL OK packets contain server status flags (e.g., `SERVER_MORE_RESULTS_EXIST`, `SERVER_STATUS_IN_TRANS`). These are not inspected or modified by the proxy.

---

## 10. Extended Protocol / Prepared Statement Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/proxy/ext_handler.go`

### Implemented
- COM_STMT_PREPARE with SQL extraction and caching
- COM_STMT_EXECUTE with param parsing, classification, routing
- COM_STMT_CLOSE with cache eviction
- Binary parameter decoding (integer types, strings)
- Write routing (INSERT, UPDATE, DELETE) via SQL reconstruction
- Read routing (merged read, JOIN patch) via parameter substitution

### Missing

1. **Shadow-only statement tracking**: PostgreSQL tracks statements that reference shadow-only or schema-modified tables and auto-routes them to merged read. MySQL's ext_handler does not track this.
   - **File**: `proxy/ext_handler.go` -- no `shadowOnly` tracking in `stmtCacheEntry`.

2. **Dual-prepare**: Prepared statements are only prepared on Prod (line 69: `eh.prodConn.Write(pkt.Raw)`). Shadow receives COM_QUERY with reconstructed SQL at execute time. This means:
   - Shadow cannot use the binary protocol for prepared statements
   - Parameter types may be lost in text conversion
   - Type fidelity is degraded (e.g., binary blobs converted to string representation)

3. **SQL reconstruction fidelity**: `reconstructSQL()` (line 601) simply returns `cl.RawSQL`, which is the original prepared statement SQL with `?` placeholders. For write operations, `substituteParams()` (line 606) replaces `?` with quoted string values. This is lossy:
   - Binary data is converted to `fmt.Sprintf("%v")` representation
   - NULL handling works but type-specific formatting may be incorrect
   - String escaping is basic (only single-quote doubling, no backslash handling)

4. **No `COM_STMT_SEND_LONG_DATA` (0x18)** handling: Large parameter values sent in chunks before EXECUTE are not handled.

---

## 11. Guard Rail Gaps

### Implemented (fully matching PostgreSQL)
- L1: `validateRouteDecision` -- asserts write/DDL never gets StrategyProdDirect
- L2: `SafeProdConn` -- wraps Prod conn, inspects COM_QUERY for write SQL
- L3: `classifyAndRoute` final assertion in conn.go routing loop

### Missing

1. **L2 does not inspect COM_STMT_EXECUTE**: The `SafeProdConn.Write()` only checks for COM_QUERY packets (payload[0] == 0x03). COM_STMT_EXECUTE packets (payload[0] == 0x17) that contain write operations are not inspected. However, since prepared statements are currently only forwarded to Prod for PREPARE (reads), this is a lower-risk gap.
   - **File**: `proxy/guard.go`, line 76 -- only checks `comQuery`.

2. **No EXPLAIN ANALYZE rejection**: PostgreSQL rejects EXPLAIN ANALYZE as unsupported (it modifies data). MySQL classifies EXPLAIN as SubOther and forwards to Prod, including potential `EXPLAIN ANALYZE` in MySQL 8.0.18+.

---

## 12. Initialization Gaps

**File**: `/Users/psrth/Development/mori/internal/engine/mysql/init.go`

### Implemented
- Connection string parsing
- Version detection (`SELECT VERSION()`)
- Docker image matching (MySQL and MariaDB)
- Schema dump via mysqldump/mariadb-dump inside Docker
- FK stripping from dump
- Table metadata detection (PK columns, types, auto_increment)
- Auto_increment offset calculation and application
- Shadow container creation and readiness polling
- Config persistence

### Missing

1. **Generated column detection**: PostgreSQL detects generated columns via `pg_attribute.attgenerated = 's'` and stores them for hydration filtering. MySQL has no equivalent detection. MySQL's generated columns (`GENERATED ALWAYS AS (expr) STORED/VIRTUAL`) can be detected via `INFORMATION_SCHEMA.COLUMNS.GENERATION_EXPRESSION IS NOT NULL` or the `EXTRA` column containing `VIRTUAL GENERATED` / `STORED GENERATED`.
   - **Impact**: Hydration will fail on tables with `GENERATED ALWAYS AS ... STORED` columns because MySQL rejects explicit INSERT values for stored generated columns.

2. **FK discovery during init**: PostgreSQL queries `pg_catalog` for FK relationships and stores them in the schema registry. MySQL does not query `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` / `REFERENTIAL_CONSTRAINTS` for FK metadata.

3. **Read-replica conflict handling**: PostgreSQL retries on SQLSTATE 40001 (recovery conflict) with exponential backoff. MySQL has no equivalent retry logic for read-replica scenarios (e.g., InnoDB cluster, Group Replication read conflicts).

4. **No `COLUMN_STATISTICS` handling**: MySQL 8.0's mysqldump queries the `mysql.column_statistics` table, which may not exist on all targets. The MariaDB adapter works around this by using mariadb-dump. However, no `--column-statistics=0` flag is passed when using MySQL images against non-standard targets.

---

## 13. Priority Recommendations

Ordered by impact on correctness and breadth of affected queries:

### P0 -- Critical (data correctness issues)

1. **Generated column detection and filtering during hydration**
   - Without this, any table with `GENERATED ALWAYS AS ... STORED` columns will fail on UPDATE hydration, breaking the core write path.
   - Implementation: Query `INFORMATION_SCHEMA.COLUMNS` for `GENERATION_EXPRESSION IS NOT NULL` during init, filter generated columns in `buildMySQLInsertSQL()`.
   - Reference: PostgreSQL `init.go` DetectGeneratedColumns, `write_insert.go` filtering.

2. **Bulk UPDATE hydration (PKs not extractable)**
   - Currently silently forwards to Shadow without hydration, causing the UPDATE to affect 0 rows if the target rows only exist in Prod.
   - Implementation: Query Prod for matching PKs using the WHERE clause, hydrate them, then execute UPDATE on Shadow.
   - Reference: PostgreSQL `write_update.go` bulk update path.

3. **Bulk DELETE tombstoning (PKs not extractable)**
   - Currently silently forwards to Shadow without adding tombstones, causing deleted Prod rows to reappear in subsequent reads.
   - Implementation: Pre-query Prod for matching PKs, tombstone them.
   - Reference: PostgreSQL `write_delete.go` bulk delete path.

4. **INSERT ... ON DUPLICATE KEY UPDATE handling**
   - Without detection and pre-hydration, upserts against Prod rows will fail on Shadow (row not present for the UPDATE path).
   - Implementation: Detect `OnDup` in classifier, pre-hydrate conflicting rows from Prod.
   - Reference: PostgreSQL `write_insert.go` upsert path.

### P1 -- High Priority (significant feature gaps)

5. **TRUNCATE handling with StrategyTruncate**
   - TRUNCATE should mark the table as `IsFullyShadowed` so all subsequent reads go to Shadow only. Currently, TRUNCATE is just forwarded without state tracking.
   - Reference: PostgreSQL router TRUNCATE handling + schema registry `IsFullyShadowed`.

6. **SAVEPOINT state management**
   - Without savepoint snapshots, any `ROLLBACK TO SAVEPOINT` will desync the delta map from Shadow state. This breaks transactional correctness.
   - Implementation: Stack-based snapshot of delta map, tombstone set, and schema registry on SAVEPOINT. Restore on ROLLBACK TO.
   - Reference: PostgreSQL `txn.go` savepoint system.

7. **Schema registry snapshot on BEGIN/restore on ROLLBACK**
   - DDL within a transaction followed by ROLLBACK leaves the schema registry dirty.
   - Reference: PostgreSQL `txn.go` schema snapshot logic.

8. **FK enforcement (at least parent row validation)**
   - FK discovery during init + parent row validation on INSERT/UPDATE is the highest-value FK feature. CASCADE and RESTRICT enforcement can come later.
   - Reference: PostgreSQL `fk.go`.

### P2 -- Medium Priority (advanced read paths)

9. **Temp table materialization infrastructure**
   - This is a prerequisite for items 10-13. Build `utilTableName()`, `buildCreateTempTableSQL()`, `bulkInsertToUtilTable()`, `dropUtilTable()` for MySQL.
   - Reference: PostgreSQL `util_table.go`.

10. **Window function handling**
    - Requires materialization infrastructure. Materialize base data into temp table, rewrite query to reference temp table, execute on Shadow.
    - Reference: PostgreSQL `read_window.go`.

11. **Set operation handling (UNION/INTERSECT/EXCEPT)**
    - Decompose into leaf SELECTs, run each through merged read, apply set operation in memory.
    - Reference: PostgreSQL `read_setop.go`.

12. **Complex read handling (CTEs, derived tables)**
    - Materialize dirty CTEs/subqueries into temp tables, rewrite query.
    - Reference: PostgreSQL `read_complex.go`.

13. **Full aggregate re-aggregation**
    - Implement row-level merge + in-memory re-aggregation for SUM/AVG/MIN/MAX/COUNT with GROUP BY and HAVING.
    - Reference: PostgreSQL `aggregate.go`.

### P3 -- Lower Priority (polish and edge cases)

14. **WHERE clause evaluator for JOIN patch**
    - Re-evaluate original WHERE on patched rows to filter rows that no longer match.
    - Reference: PostgreSQL `where_eval.go`.

15. **Classifier SubType refinement** (SET -> SubSet, SHOW -> SubShow, EXPLAIN -> SubExplain, TRUNCATE -> SubTruncate, SAVEPOINT -> SubSavepoint)
    - Enables proper per-statement-type routing.

16. **Missing feature flags** (HasWindowFunc, HasDistinct, HasComplexAgg, HasOnConflict, IsMetadataQuery)
    - Required for proper routing decisions.

17. **ALTER TYPE tracking and MODIFY COLUMN / CHANGE COLUMN parsing**

18. **Hydration improvements**: composite PK support, max rows cap, schema rewriting for Prod queries

19. **Read-replica conflict handling**

20. **Multi-result set support in protocol relay**
