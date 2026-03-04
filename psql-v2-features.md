# Mori PostgreSQL v2 Features

Focusing on the PostgreSQL proxy implementation, these are the identified gaps and their agreed resolutions.

---

# 1. New Features

## 1.1 Foreign Key Enforcement

### Context

Currently, FK constraints are stripped from shadow DDL because shadow can't reference prod rows for validation. This means invalid parent references are accepted, cascade actions don't fire, and restrict rules don't enforce.

### Resolution

Parse and remember FK definitions during DDL processing (store in schema registry alongside column diffs — referenced table, columns, ON DELETE/UPDATE action). Enforce at the proxy layer:

- **Core FK check** (INSERT/UPDATE that sets a FK value, including self-referential FKs): Check if parent row exists in local shadow → allow. In tombstone → reject. Not in local state → check prod, if present allow, if absent reject.
- **ON DELETE/UPDATE CASCADE**: Merged read on FK to find all children (local + prod). Apply cascade action locally — tombstone children (DELETE CASCADE) or update FK values (UPDATE CASCADE, SET NULL, SET DEFAULT). Must be recursive: each cascaded action can trigger further cascades down the FK dependency graph.
- **ON DELETE/UPDATE RESTRICT / NO ACTION**: Merged read to find children. If any exist, reject the parent DELETE/UPDATE.
- **DEFERRABLE INITIALLY DEFERRED**: Queue FK checks during transaction, validate all at COMMIT time using the same core check logic. If any fail, reject the COMMIT.

Implementation notes:

- FK metadata must be parsed from original DDL before stripping and stored in schema registry (persisted to disk)
- Composite FKs (multi-column) use the same logic with multi-column lookups
- Performance: batch FK checks where possible (`SELECT ... WHERE id IN (...)`) to reduce prod round-trips for bulk inserts
- Cascade processing must walk the full FK dependency graph recursively

## 1.2 SAVEPOINT / RELEASE

### Context

Savepoints are forwarded to both backends but without any delta/tombstone state management. If you ROLLBACK TO a savepoint, shadow undoes the changes but the proxy's delta map and tombstone set still have entries from the rolled-back statements. ORMs like Rails and Django wrap nested transaction blocks in savepoints, making this common.

### Resolution

Savepoint-scoped snapshots. On SAVEPOINT, push a shallow clone of delta map + tombstone set onto a stack. On ROLLBACK TO, restore from the stack (keep snapshot for repeat rollbacks). On RELEASE, pop the snapshot. On full COMMIT, flush all state and clear stack. On full ROLLBACK, discard all and clear stack. Lightweight — just in-memory map copies, bounded by nesting depth (typically 1-3 levels).

## 1.3 LISTEN / NOTIFY

### Context

Not handled. LISTEN requires the proxy to relay asynchronous NotificationResponse messages from the backend to the client. NOTIFY would send messages to production subscribers, which is destructive.

### Resolution

Split handling. LISTEN and UNLISTEN forward to prod only — relay async NotificationResponse messages to client (read-only observation of real events). NOTIFY is blocked with a clear error ("NOTIFY is not supported through Mori — it would affect production subscribers") since it's destructive (could trigger cache invalidation, job workers, webhooks on prod).

## 1.4 CURSOR (DECLARE / FETCH / CLOSE)

### Context

Not handled. DECLARE creates a named cursor for a query, then FETCH retrieves rows incrementally. The cursor's query should reflect merged state, but the cursor lives on a single backend.

### Resolution

Materialize and cursor on shadow. On DECLARE, run the cursor's SELECT as a merged read, materialize into `_mori_util_{hash}` on shadow, rewrite DECLARE to cursor over the temp table. FETCH/CLOSE forwarded to shadow. Reuses temp table pattern. Large result sets capped by `max_rows_hydrate`.

## 1.5 SQL-Level PREPARE / EXECUTE

### Context

The extended query protocol (Parse/Bind/Execute at the wire level) is handled by ext_handler.go. But SQL-level `PREPARE` and `EXECUTE` come in as simple query ('Q') messages. `EXECUTE get_user(42)` doesn't contain the actual SQL, so it can't be classified for routing.

### Resolution

Maintain a PREPARE cache. On `PREPARE name AS sql`, store `name → sql` in a map and forward to shadow. On `EXECUTE name(params)`, look up the SQL, substitute params, classify, and route through the normal path. DEALLOCATE removes from cache. Same pattern as ext_handler.go's statement cache for protocol-level prepares.

## 1.6 TRUNCATE

### Context

Not handled. TRUNCATE removes all rows from a table. The proxy would need to tombstone every prod row, which could mean millions of entries.

### Resolution

Forward to shadow, mark table as "fully shadowed." Add `IsFullyShadowed` flag to schema registry (persisted to disk). After TRUNCATE, all future reads skip prod and query shadow only. Avoids massive tombstone set. Future INSERTs go to shadow as normal.

## 1.7 INSERT ... ON CONFLICT (UPSERT)

### Context

Not handled. The ON CONFLICT behavior depends on whether the row already exists. If the row is in prod but not shadow, the INSERT succeeds on shadow (no conflict), but the correct behavior should be an UPDATE since the row exists in the merged view.

### Resolution

Hydrate-then-execute. Parse the conflict target columns and values. Check if those keys exist in prod and aren't already in shadow (delta map). If so, hydrate those rows into shadow first. Then execute the original upsert on shadow — conflict detection works correctly because the row is present. Handles both DO UPDATE and DO NOTHING cases. Track in delta map.

## 1.8 INSERT ... RETURNING

### Context

Not explicitly handled, but mostly works already — `forwardRelayAndCaptureTag` relays all response messages (including RowDescription + DataRows from RETURNING) transparently.

### Resolution

Ensure classifier recognizes INSERT...RETURNING as a normal INSERT, optionally extract PKs from returned data for more accurate delta tracking, and add end-to-end test coverage.

## 1.9 UNION / INTERSECT / EXCEPT

### Context

The classifier sets `HasSetOp = true` but there's no merge strategy. The query falls through to shadow-only or prod-direct, giving wrong results if any sub-SELECT touches dirty tables.

### Resolution

Decompose and re-apply. Parse each sub-SELECT from the set operation, run each through the normal merged read path independently, then apply the set operation in memory. UNION deduplicates by full-row equality, UNION ALL concatenates, INTERSECT/EXCEPT use full-row comparison. For chained operations, apply left-to-right respecting SQL precedence (INTERSECT binds tighter than UNION/EXCEPT).

## 1.10 CTEs (WITH ... AS)

### Context

The classifier sets `IsComplexRead = true` but there's no merge strategy. CTEs are heavily used for readability and breaking complex queries into logical steps. Recursive CTEs are used for tree traversal and graph walking.

### Resolution

Selective decomposition. Parse all CTEs — if a CTE only references clean tables (no deltas/tombstones/schema diffs), leave it in-place. For dirty CTEs (including WITH RECURSIVE), pull them out, execute as standalone merged reads on both backends, materialize results into temp tables on shadow (`_mori_util_{hash}`). Rewrite the final query to reference temp tables instead of dirty CTEs. Recursion is handled internally by PostgreSQL on each backend — proxy just merges the flat result sets. Drop temp tables immediately after query completes (connection close as safety net). Row count capped by `max_rows_hydrate`.

## 1.11 Derived Tables (Subquery in FROM)

### Context

Subqueries in the FROM clause are classified as `IsComplexRead = true` with no merge strategy. Very common in reporting and dashboard queries.

### Resolution

Same approach as CTEs. If a derived table subquery references dirty tables, execute it as a standalone merged read, materialize into `_mori_util_{hash}` temp table on shadow, rewrite the FROM clause to reference the temp table. Clean derived tables left in-place. Shares implementation with CTE decomposition.

## 1.12 Window Functions (SUM() OVER())

### Context

No handling. Window functions depend on the ordering and completeness of the entire result set — a missing row changes the RANK() or SUM() OVER() for every other row.

### Resolution

Materialize-then-recompute via temp table. Strip window functions from the query, run the base SELECT as a merged read to get all rows, materialize into `_mori_util_{hash}` on shadow. Re-execute the original query rewritten to read from the temp table — PostgreSQL handles the window computation on the complete merged dataset. Reuses the temp table pattern from CTEs/derived tables.

## 1.13 Complex Aggregates (array_agg, json_agg, string_agg, custom)

### Context

The current re-aggregation pipeline in aggregate.go only handles COUNT, SUM, AVG, MIN, MAX. Complex aggregates like array_agg, json_agg, string_agg, and custom aggregates aren't recognized.

### Resolution

Same materialize-then-recompute pattern. Strip complex aggregates, run base SELECT as merged read, materialize into `_mori_util_{hash}`, re-execute original query against temp table. PostgreSQL handles array_agg, json_agg, string_agg, and custom aggregates natively. Avoids reimplementing aggregate semantics in Go.

## 1.14 SET / SHOW / EXPLAIN

### Context

These session/utility commands aren't well-defined in the proxy. SET should apply consistently, SHOW should return prod values, EXPLAIN needs special handling for modified state.

### Resolution

Passthrough for SET and SHOW. SET is session-scoped and non-destructive — forward to both prod and shadow connections, write guard carve-out. SHOW forwards to prod, returns response to client. EXPLAIN forwards to prod for queries touching only clean tables. If the query references tables with deltas/schema diffs, early return with an error message ("EXPLAIN not supported on modified state").

## 1.15 max_rows_hydrate Config Option

### Context

Several features (UPDATE hydration, CTE materialization, cursor materialization, FK cascade checks) involve copying rows from prod into shadow. For large tables, this can be expensive with no upper bound.

### Resolution

Add a `max_rows_hydrate` config option allowing users to specify a cap on how many rows get hydrated during mutations and materializations. Options: entire table or N most recent rows. Controls the performance/correctness tradeoff for large referenced tables. Applied across all hydration and materialization paths.

---

# 2. Strategy Improvements

## 2.1 SELECT JOIN with Schema Diffs

### Context

When the developer has made DDL changes in shadow (e.g., added a column) and runs a JOIN query referencing that modified table, the JOIN query sent to prod will reference columns that don't exist. The code in rewrite_prod.go handles single-table rewrites but the JOIN path doesn't fully integrate this — it either fails or falls back to shadow-only execution, missing prod-only rows.

### Resolution

Use the same dual-execution + merge pattern as single-table reads. Run original JOIN on shadow as-is, rewrite JOIN for prod by stripping shadow-only columns (extend `rewriteSQLForProd` for multi-table context using per-table schema diffs). Merge by composite PK — shadow wins for delta rows, prod-only rows get NULLs for new columns. If the JOIN condition itself references a shadow-only column, fall back to shadow-only execution (no prod query).

## 2.2 SELECT GROUP BY / Aggregates

### Context

The re-aggregation pipeline in aggregate.go has several edge cases that produce wrong results: `compareValues` does string comparison ("9" > "10" lexicographically), expressions in GROUP BY or aggregate functions aren't handled, HAVING predicates aren't evaluated after re-aggregation, and AVG uses integer division.

### Resolution

Three fixes combined:

- **(A) Type-aware comparison**: Make `compareValues` type-aware — parse column type from RowDescription, use numeric/timestamp comparison as appropriate. Fixes MIN/MAX and ORDER BY on numeric columns.
- **(B) Expression aliasing**: For expressions in GROUP BY or aggregate functions (e.g., `SUM(price * quantity)`, `GROUP BY DATE_TRUNC(...)`), alias them as computed columns (`_expr_N`) in the row-level rewrite, then aggregate over the alias. Also pull in columns referenced by HAVING into the row-level query.
- **(C) HAVING evaluation**: Evaluate HAVING post-aggregation using where_eval logic against the re-aggregated groups.
- AVG precision fixed by ensuring float64/decimal division of SUM/COUNT.

This also fixes: MIN/MAX on numeric columns, AVG precision, GROUP BY on expressions, aggregate on expressions, HAVING with complex predicates, ORDER BY on expressions.

## 2.3 UPDATE with Cross-Table Subqueries

### Context

The current hydration logic handles the target table but hydration of referenced tables in SET/WHERE subqueries may be incomplete. If the subquery is complex (correlated, nested, or uses expressions), identifying exactly which rows to hydrate becomes difficult.

### Resolution

Hybrid predicate-based hydration with full-table fallback. For referenced tables in SET/WHERE subqueries, extract the subquery predicate and hydrate only matching complete rows from prod into shadow. If the predicate is too complex or correlated, fall back to hydrating the full referenced table. Always hydrate full rows (not partial columns) since PostgreSQL needs them for triggers, constraints, and internal subquery evaluation. Respect `max_rows_hydrate` config cap on both paths.

## 2.4 Bulk DELETE (No PK)

### Context

For bulk deletes where PKs can't be extracted from the WHERE clause, the current code forwards to shadow without tombstoning. Deleted rows that only exist in prod keep showing up in merged reads.

### Resolution

Pre-query to discover PKs. Before executing the bulk DELETE, run `SELECT pk_columns FROM table WHERE <same conditions>` on prod to find which prod rows match. Add those PKs to the tombstone set. Then execute the DELETE on shadow (which physically removes shadow rows). No need to query shadow for PKs — shadow rows are gone after the DELETE, prod rows are filtered via tombstones.

---

# 3. Minor Fixes

## 3.1 DISTINCT (Non-Aggregate)

### Context

The merged read pipeline deduplicates by PK (shadow wins), but DISTINCT operates on output columns, not PKs. After merging, duplicate output rows can appear if the same column values come from different PKs across prod and shadow.

### Resolution

Post-merge dedup pass. After the normal merge pipeline (PK dedup, tombstone filtering), apply DISTINCT as a final step — deduplicate by full output row equality. For DISTINCT ON, group by the specified columns and keep the first row per group based on ORDER BY. In-memory operation on the already-merged result set.

## 3.2 SELECT without Table PK

### Context

Tables without a primary key (logs, audit trails, event streams) or queries that don't include the PK in their SELECT list break the merge pipeline, which relies on PKs for dedup, tombstoning, and delta tracking.

### Resolution

Unified inject-dedup-strip pattern. For tables with no PK, inject `ctid` into the query, use for dedup/tombstoning, strip from output. For tables with PK but not in SELECT, inject PK columns, use for dedup, strip from output (already partially implemented). Detect PK-less tables via TableMeta (`PKType: "none"`) during schema init. `ctid` is stable within a session — VACUUM instability is acceptable for a dev proxy.

---

# 4. Not Supported

1. async notification response not relayed on notify/listen -> need to figure this out

All items below must be identified by the classifier and return a clear error message to the client. They must never silently fail or pass through.

## 4.1 COPY (Bulk Import/Export)

### Context

COPY uses a special sub-protocol in pgwire (CopyData/CopyDone/CopyFail streaming mode). Significant protocol work to implement with limited use in a dev proxy context.

## 4.2 LOCK TABLE

### Context

Explicit table-level locking. Semantically broken through a proxy that splits reads/writes across two backends, and forwarding to prod would be destructive (could stall production).

## 4.3 DO $$ ... $$ (Anonymous Blocks)

### Context

PL/pgSQL anonymous code blocks are opaque to the proxy. The actual queries are embedded inside procedural code and can't be classified or routed — would require a full PL/pgSQL interpreter.

## 4.4 CALL (Stored Procedures)

### Context

Same problem as DO blocks. The procedure body lives in pg_proc on the server and is opaque to the proxy. It could contain any mix of reads, writes, DDL, and transaction control.

## 4.5 EXPLAIN ANALYZE

### Context

EXPLAIN ANALYZE actually executes the query, which is unsafe on modified state and misleading through the proxy since execution is split across two backends. Regular EXPLAIN (without ANALYZE) is supported for clean tables — see 1.14.

---

# Archive: Discussion Context

## Core Design Philosophy

Mori is a **safe postgres proxy** — the overriding constraint behind every decision is that production must never be mutated. The proxy intercepts queries, routes reads to both prod and a local shadow database, routes writes to shadow only, and merges results to present a unified view. Every feature decision is evaluated through this lens: **correctness of the merged view** vs. **implementation complexity** vs. **production safety**.

## Decision Framework Used

When evaluating each gap, we followed a consistent pattern:

1. **Can we reuse an existing pattern?** The codebase already has well-established patterns (dual-execute + merge, hydrate-then-write, tombstone tracking, delta maps). New features should extend these rather than invent new mechanisms. This is why the `_mori_util_{hash}` temp table materialization pattern became the universal solution for CTEs, derived tables, window functions, complex aggregates, and cursors — one mechanism, five features.

2. **Let PostgreSQL do the heavy lifting.** Whenever possible, avoid reimplementing SQL semantics in Go. The materialize-then-recompute pattern exists specifically for this reason — merge the raw rows (which the proxy is good at), then let PostgreSQL handle the complex computation (window functions, custom aggregates, recursive CTEs) by re-executing against a temp table. This was explicitly chosen over implementing window function semantics or aggregate functions in Go.

3. **Prod safety is non-negotiable.** NOTIFY was blocked (not forwarded) because it would affect production subscribers. LOCK TABLE was skipped because it could stall production. SET was allowed through only after confirming it's session-scoped. The instinct was always "block by default, allow only when proven safe."

4. **"Not supported" must be loud, not silent.** Every skipped feature (COPY, LOCK, DO blocks, CALL, EXPLAIN ANALYZE) must be actively caught by the classifier and return a clear error. Silent failures are worse than explicit errors — a developer needs to know when they've hit a proxy limitation.

## Key Architectural Patterns

**The Temp Table Materialization Pattern (`_mori_util_{hash}`):** The single most impactful decision. When a query is too complex to merge at the result level (window functions, complex aggregates, CTEs, cursors), decompose it: run the data-gathering part through the normal merged read pipeline, materialize the merged rows into a temp table on shadow, then re-execute the original complex query against the temp table. This lets the proxy handle what it's good at (merging rows from two backends) and delegates what it's bad at (SQL computation semantics) back to PostgreSQL.

**The Hydrate-Then-Execute Pattern:** For writes that depend on existing data (UPDATE, DELETE, UPSERT), copy the relevant rows from prod into shadow before executing. This ensures the write operates against the correct merged state. The key insight that emerged: always hydrate **full rows**, never partial columns, because PostgreSQL needs complete rows for triggers, constraints, and internal evaluation.

**The "Fully Shadowed" Flag:** For TRUNCATE, instead of creating millions of tombstone entries, mark the entire table as fully shadowed. This is a state-level optimization — a single boolean replaces an unbounded tombstone set. This pattern could potentially extend to other bulk operations.

## Motivations Behind Specific Decisions

**FK enforcement (best-effort, not skip):** FKs were the only item originally marked "document/warn only." The decision to implement best-effort enforcement was motivated by the realization that silent FK violations create subtle bugs — a developer thinks their cascade fired, builds logic on that assumption, then gets surprised at migration time. The proxy-layer enforcement isn't perfect (it can't handle concurrent access guarantees), but it catches the common cases and fails loudly on the rest.

**LISTEN allowed, NOTIFY blocked:** This asymmetry reflects the read-safe/write-dangerous split that runs through the whole proxy. Observing production events is safe and useful during development. Sending events to production is destructive. The same principle applies to SET (safe, session-scoped) vs. LOCK TABLE (dangerous, could stall prod).

**ctid for PK-less tables:** The alternative was to refuse merging for PK-less tables (shadow-only fallback). Using ctid was preferred because it preserves the merged-view guarantee — developers see a consistent view of prod + shadow regardless of table structure. The VACUUM instability caveat was accepted because the dev proxy context makes it a negligible risk.

**max_rows_hydrate as a config option:** Multiple features (UPDATE hydration, CTE materialization, FK cascade checks, cursor materialization) share the same performance concern: copying too many rows from prod. Rather than solving this per-feature, a single config knob was introduced that caps all hydration paths. This keeps the API surface small and gives developers a single lever for the performance/correctness tradeoff.

## How Solutions Were Brainstormed

The general approach was to start with the simplest correct solution, then simplify further:

1. **Start with correctness.** What would make this feature produce the right result in all cases?
2. **Find the existing pattern.** Does the codebase already solve a similar problem? Can we extend it?
3. **Identify the expensive part.** What makes this hard — is it protocol work, semantic complexity, or data volume?
4. **Push complexity to PostgreSQL.** If the hard part is SQL semantics, materialize and re-execute. If it's protocol work (COPY), consider skipping.
5. **Add a safety cap.** If the solution involves unbounded data movement, cap it with `max_rows_hydrate`.

Features were skipped (COPY, LOCK, DO blocks, CALL) only when the implementation cost was high AND the dev proxy use case was weak. The bar for "not supported" was both criteria, not either.
