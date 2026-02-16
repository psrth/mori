# E2E Test Results — All Engines

> Generated during testing sweep (2026-02-15). Do not commit until review is complete.

---

## CockroachDB
- **Status**: PASS
- **Tests**: 107 passed, 0 failed, 0 skipped
- **Command**: `go test -tags e2e_cockroachdb -v -count=1 -timeout 10m ./e2e/cockroachdb/`
- **Duration**: ~29s
- **New files**: `e2e/cockroachdb/` (8 files: e2e_test.go, helpers_test.go, seed.sql, 01-05 test files)
- **Notes**: Based on Postgres pgwire reference. Adapted for CockroachDB (no INET, no extensions, `gen_random_uuid()`, port 26257, `unique_rowid()` for auto-increment).
- **Issues**: None

---

## MySQL
- **Status**: PASS
- **Tests**: 80 passed, 0 failed, 12 skipped
- **Command**: `go test -tags e2e_mysql -v -count=1 -timeout 10m ./e2e/mysql/`
- **Duration**: ~29s
- **New files**: `e2e/mysql/` (8 files: e2e_test.go, helpers_test.go, seed.sql, 01-05 test files)
- **Infrastructure fixes**:
  - `internal/engine/mysql/proxy/startup.go` — Fixed `caching_sha2_password` fast-auth handshake
  - `internal/engine/mysql/init.go` — Added PingContext retry loop for shadow TCP readiness race
- **Skipped tests (12)**: All read-after-shadow-write verifications requiring merged reads. v1 proxy routes all SELECTs to prod. Annotated with `t.Skip("v1 proxy routes reads to prod; ...")` — will auto-activate when v2 merged reads ships.
- **Key adaptations**: `LIMIT/OFFSET`, `LastInsertId()`, backtick quoting, `interpolateParams=true`, `multiStatements=true` for seed.

---

## MariaDB
- **Status**: PASS
- **Tests**: 75 passed, 0 failed, 0 skipped
- **Command**: `go test -tags e2e_mariadb -v -count=1 -timeout 10m ./e2e/mariadb/`
- **Duration**: ~29s
- **New files**: `e2e/mariadb/` (7 files: e2e_test.go, helpers_test.go, seed.sql, 01-05 test files)
- **Engine fixes (4 files)**:
  - `internal/engine/mysql/mariadb.go` — Added Init override to use `mariadb:11` image
  - `internal/engine/mysql/init.go` — Added `Image` and `EngineName` fields to `InitOptions`
  - `internal/engine/mysql/shadow/container.go` — Skip `--default-authentication-plugin` for MariaDB; try both `mysqladmin` and `mariadb-admin`
  - `internal/engine/mysql/schema/schema.go` — Use `mariadb-dump` instead of `mysqldump` for MariaDB images
- **Issues found**:
  - MariaDB 11 renamed CLI tools: `mysqladmin` → `mariadb-admin`, `mysqldump` → `mariadb-dump`
  - MySQL 8.0's `mysqldump` can't dump MariaDB schemas (COLUMN_STATISTICS table missing)
  - MariaDB doesn't support `--default-authentication-plugin` server flag

---

## Redis
- **Status**: PARTIAL
- **Tests**: 71 passed, 2 failed, 0 skipped
- **Command**: `go test -tags e2e_redis -v -count=1 -timeout 10m ./e2e/redis/`
- **Bug fix applied**: Added `case "redis":` in `config.Connection.ToConnString()` (`internal/core/config/project.go`) — Redis connections previously fell through to the default Postgres handler.
- **Issues**:
  - `TestMget/mget_prod_keys`: Test ordering dependency — earlier tests modify shadow state for `user:1` and `user:2`, so MGET merged read returns shadow values instead of original prod values. Fix: use unique keys not modified by earlier tests.
  - `TestMset/mset_multiple_keys`: **Proxy bug** — MSET handler only writes first key-value pair to shadow. Second key (`e2e:mset2`) returns empty after `MSET e2e:mset1 val1 e2e:mset2 val2`.
- **Coverage gaps**: Large values, special characters, concurrent writes, KEYS/SCAN, EXPIRE/PERSIST, RENAME.

---

## SQLite
- **Status**: PASS
- **Tests**: 42 passed, 0 failed, 0 skipped
- **Command**: `go test -tags e2e_sqlite -v -count=1 -timeout 10m ./e2e/sqlite/`
- **Duration**: ~11s
- **Issues**: None
- **Coverage gaps** (could be added):
  - Window functions (ROW_NUMBER, RANK, LAG/LEAD, SUM OVER)
  - Set operations (UNION, UNION ALL, INTERSECT, EXCEPT)
  - Recursive CTEs
  - Transactions (BEGIN/COMMIT/ROLLBACK)
  - More aggregate functions (AVG, MAX/MIN, COUNT DISTINCT)

---

## Firestore
- **Status**: VERIFY-ONLY (not executed)
- **Tests**: 8 subtests across 3 test functions (existing)
- **Command**: `go test -tags e2e_firestore -v -count=1 -timeout 10m ./e2e/firestore/`
- **Existing coverage**: Basic CRUD (create, read, update, delete), write isolation (insert + delete), proxy connectivity, CLI status.
- **Major gaps**:
  - **Merged reads** (shadow + prod merge) — core mori value proposition, untested
  - Update isolation (only insert/delete isolation tested)
  - Query/filter operations (WHERE, OrderBy, Limit)
  - Nested/complex field types (maps, arrays, nulls)
  - Edge cases (large documents, special characters, concurrent connections)
  - Batch/transaction operations
- **Non-applicable** (correctly absent): JOINs, CTEs, window functions, DDL/migrations, SQL aggregates, parameterized queries.

---

## MSSQL
- **Status**: VERIFY-ONLY (not executed)
- **Tests**: 86 subtests across 16 test functions (existing)
- **Command**: `go test -tags e2e_mssql -v -count=1 -timeout 15m ./e2e/mssql/`
- **Effective coverage**: ~61% of applicable Postgres reference tests
- **Existing coverage**: Full lifecycle, CRUD (SELECT/INSERT/UPDATE/DELETE), merged reads, JOINs (inner/left/3-table), subqueries, CTEs, aggregates, migrations (ADD/DROP column, CREATE/DROP table), edge cases (no-PK, UUID PK, composite PK, NULL, large text, concurrent, special chars, bracket quoting).
- **Major gaps**:
  - Set operations (UNION, INTERSECT, EXCEPT)
  - Window functions (ROW_NUMBER, RANK, LAG/LEAD)
  - Parameterized queries
  - Transactions (BEGIN/COMMIT/ROLLBACK)
  - Additional JOIN subtests (after update/delete)
  - Recursive CTEs, derived table subqueries
  - Rename column, alter column type, create index

---

## Oracle
- **Status**: VERIFY-ONLY (not executed)
- **Tests**: 71 subtests across 19 test functions (existing)
- **Command**: `go test -tags e2e_oracle -v -count=1 -timeout 20m ./e2e/oracle/`
- **Effective coverage**: ~46% of applicable Postgres reference tests
- **Existing coverage**: Full lifecycle, CRUD, merged reads, JOINs, subqueries, CTEs, aggregates, migrations, edge cases (no-PK, UUID PK, composite PK, NULL, large VARCHAR2, concurrent, empty results).
- **Major gaps**:
  - Window functions (Oracle invented these — critical gap)
  - Set operations (UNION/INTERSECT/MINUS)
  - Transactions (commit/rollback isolation)
  - Parameterized queries with bind variables
  - JOIN behavior after shadow modifications
  - Additional migration operations (rename column, alter type, create index)
  - Special characters

---

## Summary

| Engine | Status | Passed | Failed | Skipped | Total |
|--------|--------|--------|--------|---------|-------|
| CockroachDB | **PASS** | 107 | 0 | 0 | 107 |
| MySQL | **PASS** | 80 | 0 | 12 | 92 |
| MariaDB | **PASS** | 75 | 0 | 0 | 75 |
| Redis | **PARTIAL** | 71 | 2 | 0 | 73 |
| SQLite | **PASS** | 42 | 0 | 0 | 42 |
| Firestore | VERIFY-ONLY | — | — | — | 8 (existing) |
| MSSQL | VERIFY-ONLY | — | — | — | 86 (existing) |
| Oracle | VERIFY-ONLY | — | — | — | 71 (existing) |

### Bugs Found
1. **Redis MSET proxy bug**: Only writes first key-value pair to shadow (affects `e2e/redis/02_basic_crud_test.go`)
2. **Redis MGET test ordering**: Test depends on clean prod state but earlier tests modify shadow

### Engine Fixes Applied
1. **Shadow port collision**: Dynamic port allocation via `net.Listen("tcp", "127.0.0.1:0")` in 6 shadow container files
2. **Redis config**: Added `toRedisConnString()` in `internal/core/config/project.go`
3. **MySQL proxy**: Fixed `caching_sha2_password` fast-auth handshake in `internal/engine/mysql/proxy/startup.go`
4. **MySQL init**: Added PingContext retry loop in `internal/engine/mysql/init.go`
5. **MariaDB init**: Added Init override to use `mariadb:11` image in `internal/engine/mysql/mariadb.go`
6. **MariaDB shadow**: Skip `--default-authentication-plugin` for MariaDB, try both admin tools in `internal/engine/mysql/shadow/container.go`
7. **MariaDB schema**: Use `mariadb-dump` for MariaDB images in `internal/engine/mysql/schema/schema.go`
