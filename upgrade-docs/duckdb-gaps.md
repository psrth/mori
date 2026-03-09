# DuckDB Engine Gap Analysis

Comprehensive comparison of the DuckDB engine implementation against the PostgreSQL engine (baseline reference).

---

## 1. Engine Overview

The DuckDB engine is in an **early-functional** state. It implements the core copy-on-write architecture (prod + shadow + proxy), basic SQL classification via regex, a simplified merged read path, basic hydration for UPDATEs, and basic transaction coordination. However, it lacks the depth and robustness of the PostgreSQL engine across nearly every subsystem.

**Key architectural differences:**

- **Embedded database**: DuckDB runs in-process via `database/sql` + `go-duckdb` driver, not as a Docker container. There is no pgwire connection to the backend -- the proxy translates pgwire from clients into `database/sql` calls internally.
- **Shadow creation**: Full file copy of prod database (not schema-only dump). This means the shadow starts with prod data, unlike PostgreSQL where the shadow starts empty.
- **Classifier**: Regex-based parsing instead of AST-based (`pg_query_go`). This is fundamentally less accurate and misses many edge cases.
- **No FK enforcement**: Zero foreign key validation or discovery.
- **No generated column handling**: No detection or filtering of generated columns.
- **No complex read strategies**: No dedicated JOIN patch, set operation, window function, or complex CTE handling.

**Files analyzed:**
- `/Users/psrth/Development/mori/internal/engine/duckdb/engine.go` (18 files total across 5 subdirectories)
- Total DuckDB implementation: ~2,300 lines across 17 `.go` files
- PostgreSQL proxy alone: ~38 files, estimated 8,000+ lines

---

## 2. Feature Parity Matrix

| Feature | PostgreSQL | DuckDB | Notes |
|---------|-----------|--------|-------|
| **Classification** | | | |
| AST-based SQL parsing | Yes | No (regex) | DuckDB uses regex; no `pg_query_go` equivalent |
| SELECT classification | Yes | Yes | Basic regex matching works |
| INSERT classification | Yes | Yes | |
| UPDATE classification | Yes | Yes | |
| DELETE classification | Yes | Yes | |
| TRUNCATE classification | Yes | No | Not classified at all -- no `SubTruncate` |
| DDL (CREATE/ALTER/DROP) | Yes | Yes | |
| Transaction (BEGIN/COMMIT/ROLLBACK) | Yes | Yes | |
| SAVEPOINT classification | Yes | Partial | Classified as `SubOther` instead of `SubSavepoint` |
| RELEASE SAVEPOINT classification | Yes | Partial | Classified as `SubOther` instead of `SubRelease` |
| EXPLAIN classification | Yes | Partial | Grouped with SHOW/PRAGMA as `SubOther` instead of `SubExplain` |
| EXPLAIN ANALYZE rejection | Yes | No | No detection or blocking |
| SET/SHOW classification | Yes | Partial | Classified as `SubOther` instead of `SubSet`/`SubShow` |
| PREPARE/EXECUTE/DEALLOCATE | Yes | No | Classified as `SubOther` |
| LISTEN/UNLISTEN | Yes | No (N/A) | DuckDB has no LISTEN/NOTIFY |
| CURSOR (DECLARE/FETCH/CLOSE) | Yes | No | Not classified |
| CTE (WITH) classification | Yes | Yes | |
| Mutating CTE detection | Yes | Yes | Checks for INSERT/UPDATE/DELETE in WITH |
| HasReturning flag | Yes | No | Never set |
| HasOnConflict flag | Yes | No | Never set; INSERT OR REPLACE not detected |
| HasWindowFunc flag | Yes | No | Never set |
| HasComplexAgg flag | Yes | No | Never set |
| HasDistinct flag | Yes | No | Never set |
| HasCursor flag | Yes | No | Never set |
| IsMetadataQuery flag | Yes | No | Never set |
| IntrospectedTable | Yes | No | Never set |
| NotSupportedMsg | Yes | No | Never set |
| TypeCast unwrapping (`::uuid`) | Yes | No | PK extraction misses casted values |
| Composite PK extraction | Yes | No | Only extracts single-column PKs |
| **Routing** | | | |
| StrategyProdDirect | Yes | Yes | |
| StrategyMergedRead | Yes | Yes | |
| StrategyJoinPatch | Yes | Partial | Routed but falls through to basic merged read |
| StrategyShadowWrite | Yes | Yes | |
| StrategyHydrateAndWrite | Yes | Partial | Only for point UPDATEs with extractable PKs |
| StrategyShadowDelete | Yes | Partial | Basic tombstoning, no bulk delete support |
| StrategyShadowDDL | Yes | Yes | |
| StrategyTransaction | Yes | Yes | |
| StrategyNotSupported | Yes | No | Never used |
| StrategyForwardBoth | Yes | Partial | SET goes to `SubOther`, handled implicitly |
| StrategyTruncate | Yes | No | TRUNCATE not handled |
| StrategyListenOnly | Yes | No (N/A) | |
| **Read Operations** | | | |
| Single-table merged read | Yes | Yes | Core dedup/filter/merge works |
| PK injection for dedup | Yes | Yes | |
| CTID fallback for PK-less tables | Yes | No | No ctid equivalent in DuckDB |
| LIMIT over-fetch | Yes | Yes | |
| ORDER BY re-sorting | Yes | Yes | Insertion sort implementation |
| Schema adaptation (add/drop/rename cols) | Yes | No | No Prod query rewriting for schema diffs |
| JOIN patch strategy | Yes | No | Classified but no dedicated handler |
| Aggregate re-aggregation | Yes | Minimal | Only COUNT(*) without GROUP BY |
| Complex aggregate (array_agg, json_agg) | Yes | No | |
| GROUP BY aggregate handling | Yes | No | `buildAggregateBaseQuery` returns "" for GROUP BY |
| Set operations (UNION/INTERSECT/EXCEPT) | Yes | No | No in-memory set operation logic |
| Window function materialization | Yes | No | No temp table + rewrite strategy |
| CTE materialization | Yes | No | No per-CTE dirty table handling |
| Derived table rewriting | Yes | No | |
| LATERAL join handling | Yes | No | |
| DISTINCT handling | Yes | No | |
| **Write Operations** | | | |
| Simple INSERT | Yes | Yes | Routed to shadow |
| INSERT with RETURNING PK capture | Yes | No | No PK extraction from RETURNING |
| INSERT ... ON CONFLICT (upsert) | Yes | No | HasOnConflict never set; no pre-hydration |
| Point UPDATE (hydrate + write) | Yes | Partial | Basic hydration; no generated column filtering |
| Bulk UPDATE (PK not extractable) | Yes | No | No bulk hydration pipeline |
| Cross-table UPDATE (FROM clause) | Yes | No | |
| WHERE clause evaluation on patched rows | Yes | No | No `where_eval.go` equivalent |
| Point DELETE | Yes | Partial | Basic tombstoning for extractable PKs |
| Bulk DELETE (PK not extractable) | Yes | No | No pre-query for matching PKs |
| DELETE with RETURNING | Yes | No | No return data synthesis |
| TRUNCATE handling | Yes | No | Not classified or handled |
| Generated column filtering in hydration | Yes | No | No generated column detection |
| Max rows hydration cap | Yes | No | No safety cap on hydration volume |
| Schema rewriting for Prod compatibility | Yes | No | No `rewrite_prod.go` equivalent |
| **DDL & Schema Tracking** | | | |
| CREATE TABLE tracking | Yes | Yes | Records in schema registry |
| DROP TABLE tracking | Yes | Yes | Clears deltas/tombstones |
| ADD COLUMN tracking | Yes | Yes | |
| DROP COLUMN tracking | Yes | No | Not parsed or tracked |
| RENAME COLUMN tracking | Yes | Yes | |
| ALTER TYPE tracking | Yes | No | Not parsed or tracked |
| FK stripping from DDL | Yes | No | |
| FK metadata extraction from DDL | Yes | No | |
| **Transaction Management** | | | |
| BEGIN/COMMIT/ROLLBACK | Yes | Yes | |
| Staged delta system | Yes | Yes | Uses `Stage()`/`Commit()`/`Rollback()` |
| Prod REPEATABLE READ isolation | Yes | No | Prod BEGIN sent as plain BEGIN |
| Schema registry snapshot on BEGIN | Yes | No | No snapshot/restore mechanism |
| SAVEPOINT stack | Yes | No | SAVEPOINTs forwarded but no state snapshots |
| RELEASE SAVEPOINT | Yes | No | Forwarded without state management |
| ROLLBACK TO SAVEPOINT | Yes | No | No state restoration |
| **Foreign Key Enforcement** | | | |
| FK discovery from prod schema | Yes | No | No FK detection at init |
| FK metadata storage | Yes | No | |
| Parent row validation (INSERT/UPDATE) | Yes | No | |
| RESTRICT/NO ACTION enforcement (DELETE) | Yes | No | |
| CASCADE enforcement (DELETE) | Yes | No | |
| Multi-column FK support | Yes | No | |
| FK caching | Yes | No | |
| **Protocol Handling** | | | |
| pgwire simple query protocol | Yes | Yes | |
| pgwire extended query protocol | Yes | Yes | Parse/Bind/Describe/Execute/Close/Sync |
| SSL/TLS negotiation | Prod relay | Reject ('N') | DuckDB always rejects SSL |
| SASL auth stripping | Yes | No (N/A) | DuckDB proxy does AuthOk directly |
| Binary parameter decoding | Yes | No | All params treated as text strings |
| Statement cache (named statements) | Yes | Yes | |
| Shadow-only statement tracking | Yes | No | |
| Parameter substitution | Yes | Yes | `reconstructSQL` handles $N placeholders |
| Describe ('D') message handling | Yes | Stub | Returns NoData for all Describe |
| **Guard Rails** | | | |
| L1: validateRouteDecision | Yes | Yes | |
| L2: SafeProdConn wrapper | Yes | No | No connection-level write inspection |
| L3: classifyAndRoute final assertion | Yes | Yes | In `routeLoop` before prod dispatch |
| looksLikeWrite heuristic | Yes | Yes | DuckDB-adapted version |
| SQLSTATE MR001 for guard errors | Yes | Yes | |
| **Initialization** | | | |
| Connection string parsing | Yes | Yes | DuckDB file path + URI format |
| Version detection | Yes | Yes | `SELECT version()` |
| Shadow creation | Docker pg_dump | File copy | DuckDB copies entire file (with data) |
| Table metadata detection | Yes | Yes | via INFORMATION_SCHEMA |
| PK detection | Yes | Yes | |
| Generated column detection | Yes | No | |
| Sequence offset calculation | Yes | Yes | Same `max*10 vs max+10M` formula |
| Foreign key detection | Yes | No | |
| Extension handling | Yes | No (N/A) | DuckDB extensions handled differently |
| Read-replica conflict handling | Yes | No (N/A) | DuckDB has no replicas |

---

## 3. Classification Gaps

### 3.1 Regex vs AST Parsing

The DuckDB classifier at `/Users/psrth/Development/mori/internal/engine/duckdb/classify/classifier.go` uses regex-based parsing. The PostgreSQL engine uses `pg_query_go` which provides a full AST from the actual PostgreSQL parser internals. This is the single biggest architectural gap in the classifier.

**Consequences:**
- Regex can be confused by SQL in string literals, comments, or subqueries
- Table extraction from complex FROM clauses (subqueries, LATERAL, multiple JOINs with aliases) is unreliable
- No recursive AST walking for nested CTEs, derived tables, or correlated subqueries

### 3.2 Missing SubTypes

The DuckDB classifier never produces these core SubTypes (line 71-94 of `classifier.go`):

| SubType | PostgreSQL | DuckDB | Impact |
|---------|-----------|--------|--------|
| `SubSavepoint` | Yes | Maps to `SubOther` (line 72) | Savepoint state management cannot trigger |
| `SubRelease` | Yes | Maps to `SubOther` (line 72) | Release state management cannot trigger |
| `SubTruncate` | Yes | Not classified at all | TRUNCATE not handled; no `IsFullyShadowed` marking |
| `SubSet` | Yes | Maps to `SubOther` (line 79) | No `StrategyForwardBoth` routing |
| `SubShow` | Yes | Maps to `SubOther` (line 75) | No `StrategyProdDirect` routing |
| `SubExplain` | Yes | Maps to `SubOther` (line 74) | No clean/affected table detection for routing |
| `SubPrepare` | Yes | Not classified | SQL-level PREPARE not handled |
| `SubExecute` | Yes | Not classified | SQL-level EXECUTE not handled |
| `SubDeallocate` | Yes | Not classified | SQL-level DEALLOCATE not handled |
| `SubCursor` | Yes | Not classified | DECLARE/FETCH/CLOSE not handled |
| `SubListen` | N/A for DuckDB | N/A | Not applicable |
| `SubNotSupported` | Yes | Not classified | No blocking of unsafe operations |

### 3.3 Missing Feature Flags

These `Classification` fields are never populated by the DuckDB classifier (verified via grep -- zero references in the `duckdb/` tree):

- **`HasReturning`**: INSERT/UPDATE/DELETE ... RETURNING never detected. This means RETURNING-based PK capture is impossible.
- **`HasOnConflict`**: INSERT ... ON CONFLICT / INSERT OR REPLACE never detected. Upserts route as plain INSERTs.
- **`HasWindowFunc`**: Window functions (e.g., `ROW_NUMBER() OVER(...)`) never detected. Cannot trigger materialization strategy.
- **`HasComplexAgg`**: Complex aggregates (`list()`, `array_agg()`, `string_agg()`) never flagged distinctly from simple aggregates.
- **`HasDistinct`**: SELECT DISTINCT never flagged.
- **`HasCursor`**: Cursor operations never flagged.
- **`IsMetadataQuery`**: Queries against `information_schema` or `duckdb_tables()` never detected.
- **`IntrospectedTable`**: Never extracted.
- **`NotSupportedMsg`**: Never set, so unsupported operations are never explicitly rejected.

### 3.4 PK Extraction Limitations

The regex-based PK extractor at `classifier.go:227-263` has these gaps vs the PostgreSQL AST walker:

1. **No TypeCast handling**: PostgreSQL unwraps `'value'::uuid` nodes. DuckDB regex does not handle `CAST(val AS uuid)` or `val::type`.
2. **No `column = value` AND `value = column` reversal**: Only matches `column = value` pattern.
3. **Single-column only**: Does not serialize composite PKs as JSON.
4. **No float constant support**: Only matches integers and quoted strings.
5. **Ambiguous column resolution**: Does not resolve unqualified columns against PK metadata when multiple tables share column names.

---

## 4. Read Operation Gaps

### 4.1 Merged Read (Implemented, Partial)

**What works** (`conn.go:350-510`):
- Queries both prod and shadow
- Filters tombstoned and delta rows from prod
- PK injection and stripping
- LIMIT over-fetch
- ORDER BY re-sort
- PK-based deduplication

**What's missing:**
- **No schema adaptation**: Dropped/renamed/added columns in prod results are not handled. The PostgreSQL engine has `adaptRow()` that removes dropped columns, reverts renamed columns, and injects NULL for added columns. DuckDB has none of this.
- **No Prod query rewriting**: No equivalent of `rewrite_prod.go`. When schema diffs exist, prod queries are not rewritten to strip dropped columns or revert renamed columns.
- **No CTID fallback**: PK-less tables have no deduplication strategy. The PostgreSQL engine injects `ctid` as a surrogate key. DuckDB has `rowid` but it's not used.
- **Only first PK column used**: `pkIdx` search at `conn.go:418` only looks for `meta.PKColumns[0]`. Composite PKs are not supported for dedup.

### 4.2 JOIN Patch (Missing)

**Status**: The router may produce `StrategyJoinPatch`, but `conn.go:127` handles it identically to `StrategyMergedRead`:

```go
if decision.strategy == core.StrategyMergedRead || decision.strategy == core.StrategyJoinPatch {
    resp := p.executeMergedRead(sqlStr, decision.classification, connID)
```

The PostgreSQL engine has a dedicated `read_join.go` (~400 lines) that:
1. Identifies which joined tables have deltas/tombstones
2. Executes JOIN on Prod
3. Classifies each row as Clean/Delta/Dead
4. Fetches Shadow data for delta rows
5. Patches Prod rows with Shadow values
6. Re-evaluates WHERE on patched rows
7. Deduplicates by composite key

**None of this exists in DuckDB.** JOINs involving dirty tables will produce incorrect results.

### 4.3 Aggregate Queries (Minimal)

**What works** (`conn.go:513-564`):
- Detects `HasAggregate` and calls `executeAggregateRead`
- For simple `COUNT(*)` without GROUP BY, builds a base query selecting PKs, runs through merged read, and returns the count

**What's missing:**
- **GROUP BY handling**: `buildAggregateBaseQuery` at `conn.go:547` explicitly returns "" for GROUP BY queries, falling through to prod-only execution.
- **SUM/AVG/MIN/MAX re-aggregation**: Only COUNT is handled. No in-memory re-aggregation for other functions.
- **Complex aggregates**: No materialization + re-execution strategy for `array_agg`, `json_agg`, `string_agg`, `list()`.
- **HAVING support**: No post-aggregation filtering.
- **COUNT(DISTINCT)**: Not handled.

### 4.4 Set Operations (Missing)

No implementation. The PostgreSQL engine has `read_setop.go` that:
- Recursively decomposes UNION/INTERSECT/EXCEPT into leaf SELECT nodes
- Runs each leaf through merged read
- Applies in-memory set operations (UNION dedup, INTERSECT, EXCEPT)
- Handles ALL variants with count tracking

DuckDB falls through to prod-only execution for all set operations.

### 4.5 Window Functions (Missing)

No implementation. The PostgreSQL engine has `read_window.go` that:
- Builds a base SELECT without window functions
- Runs base through merged read
- Materializes into temp table on Shadow
- Rewrites query to reference temp table
- Executes rewritten query on Shadow

DuckDB falls through to prod-only execution for all window functions.

### 4.6 Complex Reads (Missing)

No implementation. The PostgreSQL engine has `read_complex.go` that handles:
- Non-recursive CTE materialization
- Recursive CTE rewriting
- Derived table (subquery in FROM) rewriting
- LATERAL join base table materialization

DuckDB's `IsComplexRead` flag is set by the classifier but never consumed by any specialized handler.

### 4.7 Cursor Operations (Missing)

No implementation. The PostgreSQL engine has `cursor.go` that handles DECLARE CURSOR (classifies inner query, materializes if affected), FETCH, and CLOSE.

---

## 5. Write Operation Gaps

### 5.1 INSERT Gaps

**What works:**
- Simple INSERT routed to shadow via `StrategyShadowWrite`
- Table marked as inserted in delta map (`MarkInserted`)

**What's missing:**
- **No RETURNING clause handling**: PostgreSQL captures inserted PKs from RETURNING for precise delta tracking. DuckDB only calls `MarkInserted(table)` which is imprecise.
- **No INSERT ... ON CONFLICT / INSERT OR REPLACE detection**: `HasOnConflict` is never set. Upserts are routed as plain INSERTs, which means conflicting Prod rows are never pre-hydrated. **This will cause incorrect results** when an upsert modifies an existing Prod row.
- **No FK constraint enforcement**: PostgreSQL validates parent row existence before INSERT. DuckDB skips this entirely.
- **No insert count tracking**: PostgreSQL tracks insert counts per table for delta visibility. DuckDB only calls `MarkInserted`.

### 5.2 UPDATE Gaps

**What works** (`conn.go:892-915`):
- Point UPDATEs with extractable PKs trigger `hydrateBeforeUpdate`
- Hydration copies matching rows from prod to shadow via `INSERT OR REPLACE`

**What's missing:**
- **No bulk UPDATE handling**: When PKs are not extractable from WHERE, PostgreSQL queries Prod for all matching rows, hydrates them, and rewrites the UPDATE with concrete PK values. DuckDB just executes the UPDATE on shadow without any Prod data, producing wrong results.
- **No cross-table UPDATE (UPDATE ... FROM)**: Not handled.
- **No generated column filtering**: `buildHydrateInsert` at `conn.go:919-934` includes all columns. If any are GENERATED ALWAYS, the hydration INSERT will fail.
- **No WHERE clause evaluation**: PostgreSQL re-evaluates WHERE on patched delta rows. DuckDB doesn't.
- **No FK enforcement on UPDATE**: No parent row validation.
- **No max rows hydration cap**: No safety limit on how many rows get hydrated.
- **No schema rewriting for Prod queries**: Hydration SELECT queries Prod without adapting for renamed/dropped columns.

### 5.3 DELETE Gaps

**What works:**
- Point DELETEs with extractable PKs add tombstones and execute on shadow

**What's missing:**
- **No bulk DELETE handling**: When PKs are not extractable, PostgreSQL pre-queries Prod for matching PKs, computes total count, and tombstones all. DuckDB just executes on shadow without Prod awareness, producing incorrect row counts.
- **No RETURNING support**: PostgreSQL hydrates from Prod for return data synthesis. DuckDB ignores RETURNING on DELETE.
- **No FK RESTRICT/NO ACTION enforcement**: PostgreSQL checks for child rows before deletion. DuckDB doesn't.
- **No FK CASCADE enforcement**: PostgreSQL automatically tombstones child rows. DuckDB doesn't.

### 5.4 TRUNCATE (Missing)

TRUNCATE is not classified, not routed, and not handled. The PostgreSQL engine:
- Classifies as `SubTruncate`
- Routes to `StrategyTruncate`
- Executes on shadow
- Marks table as `IsFullyShadowed` in schema registry
- All subsequent reads go to shadow only
- Tracks CASCADE for related tables

---

## 6. DDL & Schema Tracking Gaps

### 6.1 What Works (`conn.go:999-1103`)

- CREATE TABLE: Recorded via `RecordNewTable`
- DROP TABLE: Recorded via `RemoveTable`, clears deltas/tombstones
- ADD COLUMN: Parsed and recorded via `RecordAddColumn`
- RENAME COLUMN: Parsed and recorded via `RecordRenameColumn`

### 6.2 What's Missing

| DDL Operation | PostgreSQL | DuckDB | Location |
|--------------|-----------|--------|----------|
| DROP COLUMN | `RecordDropColumn(table, name)` | Not parsed or tracked | `conn.go:1000-1054` -- no case for DROP COLUMN |
| ALTER TYPE | `RecordTypeChange(table, col, old, new)` | Not parsed or tracked | Same location |
| FK stripping from DDL | Strips FK constraints before Shadow execution | Not done | |
| FK metadata extraction | Extracts FK info from DDL for proxy enforcement | Not done | |
| Schema-aware query rewriting | Rewrites Prod queries for dropped/renamed cols | Not done | |
| Schema adaptation in merge | Adapts Prod rows (NULL for added, strip dropped) | Not done | |

### 6.3 DDL Parsing Fragility

The DDL parsers `parseAlterRenameColumn` and `parseAlterAddColumn` at `conn.go:1056-1103` use naive `strings.Fields` splitting. This will fail for:
- Quoted identifiers with spaces: `ALTER TABLE "my table" ADD COLUMN "my col" INTEGER`
- Multiple ALTER operations: `ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b TEXT`
- Schema-qualified names: `ALTER TABLE myschema.mytable ...`

---

## 7. Transaction Management Gaps

### 7.1 What Works (`txn.go`)

- BEGIN: Executes on both prod and shadow
- COMMIT: Promotes staged deltas, persists state
- ROLLBACK: Discards staged deltas
- Staged delta system: Uses `Stage()`/`Commit()`/`Rollback()` correctly

### 7.2 What's Missing

| Feature | PostgreSQL | DuckDB | Impact |
|---------|-----------|--------|--------|
| Prod REPEATABLE READ | `BEGIN ISOLATION LEVEL REPEATABLE READ` on Prod | Plain `BEGIN` (line 47) | Prod reads within a transaction may see inconsistent snapshots |
| Schema registry snapshot | Snapshot taken at BEGIN for rollback | Not done | ROLLBACK cannot restore schema registry to pre-BEGIN state |
| Savepoint stack | Stack of (delta, tombstone, insert count, schema) snapshots | Not implemented | SAVEPOINT/RELEASE/ROLLBACK TO are forwarded but state is not tracked |
| ROLLBACK TO SAVEPOINT | Restores state from savepoint snapshot | Not implemented | Rolling back to a savepoint discards nothing from delta/tombstone maps |
| ReadyForQuery transaction state | Sends 'T' during txn, 'E' on error | Always sends 'I' (idle) | Clients may not know they're in a transaction |

### 7.3 ReadyForQuery State Bug

`buildReadyForQueryMsg()` at `pgmsg.go:158` always returns `'I'` (idle). During a transaction it should return `'T'`, and during a failed transaction it should return `'E'`. This is a protocol correctness issue.

---

## 8. Foreign Key Enforcement Gaps

**Status: Entirely missing.**

The PostgreSQL engine has comprehensive FK enforcement in `fk.go`:

| FK Feature | PostgreSQL | DuckDB |
|-----------|-----------|--------|
| FK discovery from prod (pg_catalog) | Yes | No |
| FK metadata in schema registry | Yes | No |
| FK discovery from DDL (ALTER TABLE ADD CONSTRAINT) | Yes | No |
| Parent row validation on INSERT | Yes | No |
| Parent row validation on UPDATE | Yes | No |
| RESTRICT/NO ACTION on DELETE | Yes | No |
| CASCADE on DELETE | Yes | No |
| Multi-column FK support | Yes | No |
| Per-table FK caching | Yes | No |

**Impact**: Since the shadow database is a full copy (not schema-only), DuckDB's native FK constraints are present and may catch some violations. However, the proxy cannot validate FKs across the prod/shadow boundary (e.g., inserting a child row in shadow that references a parent row only in prod).

---

## 9. Protocol-Level Gaps

### 9.1 Embedded vs Wire Protocol

DuckDB is embedded, so the proxy architecture is fundamentally different:

| Aspect | PostgreSQL | DuckDB |
|--------|-----------|--------|
| Backend communication | pgwire TCP sockets | `database/sql` in-process calls |
| Response construction | Relay raw pgwire bytes from backend | Build pgwire messages from `sql.Rows`/`sql.Result` |
| Type OIDs | Preserved from backend RowDescription | Hardcoded to 25 (text) for all columns |
| Error propagation | Relay backend ErrorResponse | Construct ErrorResponse from `error.Error()` |
| Connection draining | Drain mid-stream on error to prevent desync | N/A (no persistent connection) |
| Transaction state tracking | Reads 'Z' message from backend | Must be tracked manually (currently broken -- always 'I') |

### 9.2 Type OID Hardcoding

At `conn.go:269-271` and `pgmsg.go:117`, all column OIDs are hardcoded to 25 (text):

```go
colOIDs[i] = 25 // text OID
```

This means clients that depend on OID metadata for type inference (e.g., ORMs like GORM, SQLAlchemy) will see all columns as text type. The PostgreSQL engine preserves actual OIDs from the backend RowDescription.

### 9.3 SSL/TLS

The DuckDB proxy always rejects SSL (`startup.go:22`: responds with `'N'`). This is acceptable since it only binds to localhost, but it differs from PostgreSQL which relays SSL negotiation to the prod backend.

### 9.4 Binary Parameter Decoding

The PostgreSQL extended protocol handler decodes binary parameters (bool, int16, int32, int64, UUID as byte arrays). The DuckDB `parseBindMsgPayload` at `pgmsg.go:252-296` reads all parameters as raw strings. This will silently corrupt binary-format parameters.

---

## 10. Prepared Statement Gaps

### 10.1 SQL-Level PREPARE/EXECUTE/DEALLOCATE

Not classified (`SubPrepare`, `SubExecute`, `SubDeallocate` never produced). In PostgreSQL, these are classified and routed:
- PREPARE: Routed to shadow (StrategyShadowWrite)
- EXECUTE: Full classify -> route -> dispatch
- DEALLOCATE: Forward to both backends

### 10.2 Extended Protocol Statement Cache

**What works:**
- Statement names cached in `stmtCache` map
- Close('S') evicts from cache
- Bind resolves SQL from cache when Parse is not in same batch

**What's missing:**
- **No shadow-only tracking**: PostgreSQL tracks which statements reference shadow-only or schema-modified tables and auto-routes them. DuckDB doesn't.
- **No Describe response**: DuckDB returns `NoData` for all Describe messages (`ext_handler.go:98`). PostgreSQL forwards Describe to the appropriate backend and returns proper RowDescription/ParameterDescription.

---

## 11. Guard Rail Gaps

### 11.1 What Works

- **L1 (validateRouteDecision)**: Implemented at `guard.go:18-43`. Asserts writes/DDL never get `StrategyProdDirect`.
- **L3 (final assertion in routeLoop)**: At `conn.go:136-141`, checks OpType before prod dispatch.
- **looksLikeWrite**: Implemented at `guard.go:50-85` with DuckDB-adapted prefixes.

### 11.2 What's Missing

- **L2 (SafeProdConn)**: PostgreSQL wraps the prod connection in a `SafeProdConn` that inspects every outgoing Query message for write SQL. DuckDB has no equivalent -- write SQL could reach `p.prodDB.Exec()` if routing logic has a bug, and `database/sql` would execute it. This is mitigated by opening prod as `READ_ONLY` (`proxy.go:78`), but the `access_mode=READ_ONLY` is a DuckDB-level guard, not a protocol-level one.
- **CTE write detection in L2**: PostgreSQL's `looksLikeWrite` detects `WITH ... INSERT/UPDATE/DELETE`. DuckDB's version does too (`guard.go:76-82`), so this specific case is covered.
- **TRUNCATE detection**: `looksLikeWrite` at `guard.go:65-69` lists `INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, COPY, EXPORT, IMPORT` but **not TRUNCATE**. A TRUNCATE statement would not be caught by the write heuristic.

---

## 12. Initialization Gaps

### 12.1 What Works (`init.go`)

- Connection string parsing (file path, URI, in-memory)
- Database validation and version detection
- Shadow creation (full file copy)
- Table metadata detection (INFORMATION_SCHEMA)
- PK detection and type classification
- Sequence offset calculation
- Configuration persistence

### 12.2 What's Missing

| Init Feature | PostgreSQL | DuckDB |
|-------------|-----------|--------|
| Schema-only shadow | pg_dump --schema-only | Full file copy | DuckDB shadow starts with all prod data |
| Extension detection | `pg_extension` query | Not done | DuckDB extensions not detected |
| Extension installation | apt-get inside Docker | N/A | |
| Generated column detection | `pg_attribute.attgenerated` | Not done | No generated column metadata |
| Foreign key detection | `pg_catalog` query | Not done | No FK metadata at init |
| Managed extension filtering | Cloud provider extension filtering | N/A | |
| Read-replica conflict handling | Retry with exponential backoff | N/A | DuckDB has no replicas |
| Docker lifecycle management | Container create/start/stop | N/A | DuckDB is embedded |

### 12.3 Shadow Contains Prod Data

The most significant initialization difference: DuckDB's `shadow.CreateShadow` (`shadow/shadow.go:21-47`) performs a byte-level file copy. This means the shadow starts with **all production data**, unlike PostgreSQL where the shadow starts empty. Implications:

1. **Larger disk footprint**: Shadow is a full copy of prod.
2. **Different merged read semantics**: All rows exist in both databases initially. The merged read dedup logic must handle this correctly (shadow wins on conflict, which it does).
3. **Sequence offsets less critical**: Since data already exists in shadow, sequence offsets only matter for new inserts.

---

## 13. Priority Recommendations

Ordered by impact on correctness and usability:

### P0 -- Critical Correctness Issues

1. **Fix ReadyForQuery transaction state** (`pgmsg.go:158`): Return `'T'` during transactions, `'E'` during failed transactions. This is a protocol violation that causes client libraries to misbehave. Requires `TxnHandler` to expose transaction state to the pgwire builder.

2. **Add TRUNCATE classification and handling**: TRUNCATE is not classified, not in `looksLikeWrite`, and not handled. Add `SubTruncate`, route to shadow, mark `IsFullyShadowed`. Also add `TRUNCATE` to the `writePrefixes` list in `guard.go`.

3. **Detect INSERT OR REPLACE / ON CONFLICT**: Set `HasOnConflict` flag. Route to `StrategyHydrateAndWrite` so conflicting Prod rows get hydrated before the upsert executes on shadow.

4. **Add bulk UPDATE hydration**: When PKs are not extractable from WHERE, query Prod for matching rows, hydrate, and rewrite the UPDATE. Without this, any UPDATE without a literal PK in WHERE silently produces wrong results.

5. **Add bulk DELETE handling**: Pre-query Prod for matching PKs when not extractable. Without this, DELETE row counts are wrong and tombstones are not set for Prod-only rows.

### P1 -- Important Feature Gaps

6. **Add DROP COLUMN tracking**: Parse `ALTER TABLE ... DROP COLUMN` in `trackDDLEffects` and call `RecordDropColumn`. Without this, queries referencing dropped columns will fail against Prod.

7. **Add proper SAVEPOINT/RELEASE classification**: Use `SubSavepoint` and `SubRelease` instead of `SubOther`. Implement savepoint state snapshots.

8. **Implement JOIN patch strategy**: Dedicated handler that patches delta rows in JOIN results. Current behavior of treating JOINs as merged reads produces incorrect results when joined tables have different delta status.

9. **Add generated column detection**: At init, detect generated columns. Filter them from hydration INSERTs to prevent "cannot insert into generated column" errors.

10. **Add FK discovery and enforcement**: Detect FKs at init, validate parent rows on INSERT/UPDATE, enforce RESTRICT/CASCADE on DELETE.

### P2 -- Robustness & Completeness

11. **Add Prod query rewriting for schema diffs**: Implement equivalent of `rewrite_prod.go` to strip dropped columns, revert renamed columns, and handle added columns in Prod queries.

12. **Add set operation handling**: Implement UNION/INTERSECT/EXCEPT decomposition and in-memory merge.

13. **Add window function materialization**: Temp table + rewrite strategy for window functions on dirty tables.

14. **Add HasReturning detection and PK capture**: Parse RETURNING clause, extract inserted PKs for precise delta tracking instead of `MarkInserted`.

15. **Add WHERE clause evaluator**: Port `where_eval.go` for re-evaluating WHERE conditions on patched rows.

16. **Fix type OID hardcoding**: At minimum, detect integer/float/bool/timestamp types from `sql.ColumnType()` and map to appropriate PG OIDs.

### P3 -- Nice to Have

17. **Add binary parameter decoding**: Decode int16/int32/int64/bool/UUID from binary format in Bind messages.

18. **Add SET/SHOW proper classification**: Use `SubSet`/`SubShow` for correct routing.

19. **Add EXPLAIN classification**: Use `SubExplain`, detect EXPLAIN ANALYZE and reject.

20. **Add complex aggregate handling**: Materialization + re-execution for `array_agg`, `json_agg`, `list()`, `string_agg`.

21. **Add CTE/derived table materialization**: Per-CTE dirty table handling for complex reads.

22. **Add PREPARE/EXECUTE/DEALLOCATE classification**: Proper routing for SQL-level prepared statements.

23. **Add Describe message handling**: Return proper RowDescription from appropriate backend instead of always NoData.

24. **Add composite PK support in classifier**: Serialize composite PKs as JSON for delta map keys, matching PostgreSQL behavior.

---

## Appendix: File-by-File Summary

| File | Lines | Status |
|------|-------|--------|
| `engine.go` | 114 | Complete -- interface adapter |
| `init.go` | 139 | Functional -- missing FK/generated col detection |
| `classify/classifier.go` | 447 | Partial -- regex, missing 10+ SubTypes and 8+ flags |
| `classify/classifier_test.go` | 352 | Good coverage for implemented features |
| `connstr/connstr.go` | 127 | Complete |
| `connstr/connstr_test.go` | 130 | Complete |
| `proxy/proxy.go` | 190 | Complete -- server lifecycle |
| `proxy/conn.go` | 1200 | Core routing + merged read + basic write tracking |
| `proxy/ext_handler.go` | 399 | Functional extended protocol handler |
| `proxy/guard.go` | 93 | L1 implemented, L2 missing |
| `proxy/pgmsg.go` | 335 | pgwire message builders (ReadyForQuery state bug) |
| `proxy/startup.go` | 110 | Complete -- pgwire startup handshake |
| `proxy/txn.go` | 140 | Basic BEGIN/COMMIT/ROLLBACK (no savepoints) |
| `proxy/txn_test.go` | 302 | Good test coverage |
| `schema/persist.go` | 41 | Complete |
| `schema/schema.go` | 167 | Functional -- missing FK/generated col detection |
| `shadow/shadow.go` | 61 | Complete -- file copy approach |
