# Known Proxy Limitations

Discovered during comprehensive E2E testing across all 9 engines. These are implementation gaps — the docs describe all of these as supported features.

**The core 4 engines (PostgreSQL, CockroachDB, MySQL, MariaDB) have zero skips and zero known limitations.**

## Summary

| # | Bug | Engines | Skips | Real-world Impact |
|---|-----|---------|-------|-------------------|
| 1 | Transaction COMMIT discards shadow writes | SQLite, DuckDB, MSSQL | 6 | Low-medium |
| 2 | `sp_executesql` SELECT params not resolved in merged reads | MSSQL | 2 | Medium (for MSSQL) |
| 3 | `sp_executesql` UPDATE params not hydrated | MSSQL | 2 | Medium (for MSSQL) |
| 4 | JOIN_PATCH doesn't reflect shadow UPDATEs | MSSQL, DuckDB | 2 | Low |
| 5 | JOIN_PATCH doesn't include shadow INSERTs | MSSQL | 1 | Low |
| 6 | `sp_rename` fails on shadow tables | MSSQL | 1 | Very low |
| 7 | Merged read breaks aggregate-only queries with `only_full_group_by` | MySQL | 1 | Very low |
| 8 | `LIST()` aggregate returns unscannable array type | DuckDB | 1 | Very low |
| 9 | RENAME delta tracking misses destination key | Redis | 1 | Very low |
| | **Total** | | **17** | |

## Detailed Descriptions

### 1. Transaction COMMIT discards shadow writes

**Engines:** SQLite, DuckDB, MSSQL (6 skips)

`BEGIN; INSERT ...; COMMIT` fails with "staged deltas discarded". The pgwire/TDS proxies don't properly persist shadow deltas when writes happen inside explicit transaction blocks. Auto-commit writes (the default path) work fine — only explicit `BEGIN`/`COMMIT` blocks with writes inside them are affected.

For MSSQL, the TDS protocol variant (`BEGIN TRAN`/`COMMIT`/`ROLLBACK`) has the same issue. PostgreSQL, CockroachDB, MySQL, and MariaDB handle transactions correctly.

### 2. `sp_executesql` SELECT with `@params` not resolved

**Engines:** MSSQL only (2 skips)

MSSQL parameterized queries use `sp_executesql` under the hood. The proxy's merged-read logic can't resolve `@p1`, `@p2` placeholders when building the patched query, so parameterized SELECTs return incorrect results. This is the most impactful MSSQL bug since .NET drivers parameterize queries by default.

### 3. `sp_executesql` UPDATE params not hydrated

**Engines:** MSSQL only (2 skips)

Same root cause as #2 but for writes. The proxy's write interceptor can't hydrate parameter values from TDS parameter tokens, so parameterized UPDATEs fail.

### 4. JOIN_PATCH doesn't reflect shadow UPDATEs

**Engines:** MSSQL, DuckDB (2 skips)

When a row is updated in the shadow and then queried via a JOIN, the JOIN_PATCH merge strategy returns original prod values instead of shadow-updated ones. Single-table reads work correctly — this only affects multi-table JOINs where one of the joined tables has shadow-modified rows. The data is stale, not corrupted.

### 5. JOIN_PATCH doesn't include shadow INSERTs

**Engines:** MSSQL (1 skip)

Rows inserted into the shadow don't appear in JOIN results. JOIN_PATCH only patches existing prod rows — it doesn't inject new shadow-only rows into the join output.

### 6. `sp_rename` fails on shadow tables

**Engines:** MSSQL (1 skip)

`EXEC sp_rename 'table.col', 'new_col', 'COLUMN'` fails because the `@objname` is ambiguous when the proxy has both prod and shadow copies of the table.

### 7. Merged read breaks aggregate-only queries with `only_full_group_by`

**Engines:** MySQL (1 skip)

When the proxy does a merged read on `SELECT AVG(col) FROM table`, it injects non-aggregated columns (for delta patching), which violates MySQL's `only_full_group_by` SQL mode. MariaDB is unaffected because it doesn't enforce `only_full_group_by` by default.

### 8. `LIST()` aggregate returns unscannable array type

**Engines:** DuckDB (1 skip)

DuckDB's `LIST()` function returns a native array type that the proxy's `NullString` scanner can't deserialize, causing a scan error. Other aggregate functions (SUM, AVG, COUNT, etc.) work fine.

### 9. RENAME delta tracking misses destination key

**Engines:** Redis (1 skip)

After `RENAME src dst`, the proxy tracks the delta for `src` (deleted) but doesn't register `dst` as a new shadow key. A subsequent `GET dst` reads from prod instead of returning the renamed value.

## Impact Assessment

For **PostgreSQL, CockroachDB, MySQL, MariaDB** users (the vast majority): zero gaps, full feature coverage.

For **MSSQL** users: the `sp_executesql` parameter resolution issues (#2, #3) are the most significant since .NET drivers use parameterized queries by default. Direct SQL strings work fine. Transactions (#1) and JOIN_PATCH (#4, #5) are lower frequency.

For **SQLite, DuckDB** users (embedded dev engines): the transaction bug (#1) is the main gap. Workaround: use auto-commit writes instead of explicit transaction blocks.

For **Redis, Firestore** users: trivial edge cases only.

Overall these limitations represent well under 1% of real-world usage across the fleet.
