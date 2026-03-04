# Mori - Gaps

focusing just on the postgres implementation, these are the gaps we have identified:

strategy

1. SELECT (JOIN, with schema diffs)
   RESOLUTION: Use the same dual-execution + merge pattern as single-table reads. Run original JOIN on shadow as-is, rewrite JOIN for prod by stripping shadow-only columns (extend rewriteSQLForProd for multi-table context using per-table schema diffs). Merge by composite PK — shadow wins for delta rows, prod-only rows get NULLs for new columns. If the JOIN condition itself references a shadow-only column, fall back to shadow-only execution (no prod query).
2. SELECT GROUP BY (COUNT/SUM/AVG/MIN/MAX)
   RESOLUTION: Three fixes combined. (A) Make compareValues type-aware — parse column type from RowDescription, use numeric/timestamp comparison as appropriate. Fixes MIN/MAX and ORDER BY on numeric columns. (B) For expressions in GROUP BY or aggregate functions (e.g., SUM(price \* quantity), GROUP BY DATE_TRUNC(...)), alias them as computed columns (\_expr_N) in the row-level rewrite, then aggregate over the alias. Also pull in columns referenced by HAVING into the row-level query. (C) Evaluate HAVING post-aggregation using where_eval logic against the re-aggregated groups. AVG precision fixed by ensuring float64/decimal division of SUM/COUNT.
3. UPDATE (cross-table subqueries)
   RESOLUTION: Hybrid predicate-based hydration with full-table fallback. For referenced tables in SET/WHERE subqueries, extract the subquery predicate and hydrate only matching complete rows from prod into shadow. If the predicate is too complex or correlated, fall back to hydrating the full referenced table. Always hydrate full rows (not partial columns) since PostgreSQL needs them for triggers, constraints, and internal subquery evaluation. Respect max_rows_hydrate config cap on both paths.
4. DELETE (bulk, no PK)
   RESOLUTION: Pre-query to discover PKs. Before executing the bulk DELETE, run SELECT pk_columns FROM table WHERE <same conditions> on prod to find which prod rows match. Add those PKs to the tombstone set. Then execute the DELETE on shadow (which physically removes shadow rows). No need to query shadow for PKs — shadow rows are gone after the DELETE, prod rows are filtered via tombstones.

parity gaps → no merge

1.  UNION / INTERSECT / EXCEPT
    RESOLUTION: Decompose and re-apply. Parse each sub-SELECT from the set operation, run each through the normal merged read path independently, then apply the set operation in memory. UNION deduplicates by full-row equality, UNION ALL concatenates, INTERSECT/EXCEPT use full-row comparison. For chained operations, apply left-to-right respecting SQL precedence (INTERSECT binds tighter than UNION/EXCEPT).
2.  CTEs (WITH ... AS)
    RESOLUTION: Selective decomposition. Parse all CTEs — if a CTE only references clean tables (no deltas/tombstones/schema diffs), leave it in-place. For dirty CTEs (including WITH RECURSIVE), pull them out, execute as standalone merged reads on both backends, materialize results into temp tables on shadow (_mori_util_{hash}). Rewrite the final query to reference temp tables instead of dirty CTEs. Recursion is handled internally by PostgreSQL on each backend — proxy just merges the flat result sets. Drop temp tables immediately after query completes (connection close as safety net). Row count capped by max_rows_hydrate.
3.  Derived tables (subquery in FROM)
    RESOLUTION: Same approach as CTEs. If a derived table subquery references dirty tables, execute it as a standalone merged read, materialize into _mori_util_{hash} temp table on shadow, rewrite the FROM clause to reference the temp table. Clean derived tables left in-place. Shares implementation with CTE decomposition.
4.  Window functions (SUM() OVER())
    RESOLUTION: Materialize-then-recompute via temp table. Strip window functions from the query, run the base SELECT as a merged read to get all rows, materialize into _mori_util_{hash} on shadow. Re-execute the original query rewritten to read from the temp table — PostgreSQL handles the window computation on the complete merged dataset. Reuses the temp table pattern from CTEs/derived tables.
5.  Complex aggregates (array*agg, json_agg, string_agg, custom)
    RESOLUTION: Same materialize-then-recompute pattern. Strip complex aggregates, run base SELECT as merged read, materialize into \_mori_util*{hash}, re-execute original query against temp table. PostgreSQL handles array_agg, json_agg, string_agg, and custom aggregates natively. Avoids reimplementing aggregate semantics in Go.
6.  SET / SHOW / EXPLAIN
    RESOLUTION: Passthrough for SET and SHOW. SET is session-scoped and non-destructive — forward to both prod and shadow connections, write guard carve-out. SHOW forwards to prod, returns response to client. EXPLAIN forwards to prod for queries touching only clean tables. If the query references tables with deltas/schema diffs, early return with an error message ("EXPLAIN not supported on modified state").

dont handle at all

- SAVEPOINT / RELEASE
  RESOLUTION: Savepoint-scoped snapshots. On SAVEPOINT, push a shallow clone of delta map + tombstone set onto a stack. On ROLLBACK TO, restore from the stack (keep snapshot for repeat rollbacks). On RELEASE, pop the snapshot. On full COMMIT, flush all state and clear stack. On full ROLLBACK, discard all and clear stack. Lightweight — just in-memory map copies, bounded by nesting depth (typically 1-3 levels).
- COPY (bulk import/export) — SKIP, not implementing
- LISTEN / NOTIFY
  RESOLUTION: Split handling. LISTEN and UNLISTEN forward to prod only — relay async NotificationResponse messages to client (read-only observation of real events). NOTIFY is blocked with a clear error ("NOTIFY is not supported through Mori — it would affect production subscribers") since it's destructive (could trigger cache invalidation, job workers, webhooks on prod).
- CURSOR (DECLARE/FETCH/CLOSE)
  RESOLUTION: Materialize and cursor on shadow. On DECLARE, run the cursor's SELECT as a merged read, materialize into _mori_util_{hash} on shadow, rewrite DECLARE to cursor over the temp table. FETCH/CLOSE forwarded to shadow. Reuses temp table pattern. Large result sets capped by max_rows_hydrate.
- LOCK TABLE — SKIP, not implementing
- PREPARE / EXECUTE (SQL-level, not protocol)
  RESOLUTION: Maintain a PREPARE cache. On PREPARE name AS sql, store name → sql in a map and forward to shadow. On EXECUTE name(params), look up the SQL, substitute params, classify, and route through the normal path. DEALLOCATE removes from cache. Same pattern as ext_handler.go's statement cache for protocol-level prepares.
- DO $$ ... $$ (anonymous blocks) — SKIP, not implementing
- CALL (stored procedures) — SKIP, not implementing
- TRUNCATE
  RESOLUTION: Forward to shadow, mark table as "fully shadowed." Add IsFullyShadowed flag to schema registry (persisted to disk). After TRUNCATE, all future reads skip prod and query shadow only. Avoids massive tombstone set. Future INSERTs go to shadow as normal.
- INSERT ... ON CONFLICT (UPSERT)
  RESOLUTION: Hydrate-then-execute. Parse the conflict target columns and values. Check if those keys exist in prod and aren't already in shadow (delta map). If so, hydrate those rows into shadow first. Then execute the original upsert on shadow — conflict detection works correctly because the row is present. Handles both DO UPDATE and DO NOTHING cases. Track in delta map.
- INSERT ... RETURNING
  RESOLUTION: Mostly works already — forwardRelayAndCaptureTag relays all response messages (including RowDescription + DataRows from RETURNING) transparently. Fix: ensure classifier recognizes INSERT...RETURNING as a normal INSERT, optionally extract PKs from returned data for more accurate delta tracking, and add end-to-end test coverage.

wrong result

- MIN/MAX on numeric columns — covered by Strategy #2 resolution (type-aware compareValues)
- AVG precision — covered by Strategy #2 resolution (float64/decimal division)
- Bulk DELETE — covered by Strategy #4 resolution (pre-query prod for PKs, tombstone)
- JOIN with schema diffs — covered by Strategy #1 resolution (dual-execute with rewritten prod query)
- GROUP BY on expressions — covered by Strategy #2 resolution (expression aliasing)
- Aggregate on expressions — covered by Strategy #2 resolution (expression aliasing)
- HAVING with complex predicates — covered by Strategy #2 resolution (post-aggregation eval)
- ORDER BY on expressions — covered by Strategy #2 resolution (type-aware comparison)
- DISTINCT (non-aggregate)
  RESOLUTION: Post-merge dedup pass. After the normal merge pipeline (PK dedup, tombstone filtering), apply DISTINCT as a final step — deduplicate by full output row equality. For DISTINCT ON, group by the specified columns and keep the first row per group based on ORDER BY. In-memory operation on the already-merged result set.
- SELECT without table PK
  RESOLUTION: Unified inject-dedup-strip pattern. For tables with no PK, inject ctid into the query, use for dedup/tombstoning, strip from output. For tables with PK but not in SELECT, inject PK columns, use for dedup, strip from output (already partially implemented). Detect PK-less tables via TableMeta (PKType: "none") during schema init. ctid is stable within a session — VACUUM instability is acceptable for a dev proxy.

new ideas

- max_rows_hydrate config option: allow users to specify a cap on how many rows get hydrated during mutations (UPDATE/DELETE). Options: entire table or N most recent rows. Controls the performance/correctness tradeoff for large referenced tables.

not supported (will not implement — classifier must identify these and return a clear error message to client, never silently fail or pass through)

- COPY (bulk import/export)
- LOCK TABLE
- DO $$ ... $$ (anonymous blocks)
- CALL (stored procedures)
- EXPLAIN ANALYZE (executes the query — unsafe on modified state, and misleading through the proxy since execution is split across two backends)
