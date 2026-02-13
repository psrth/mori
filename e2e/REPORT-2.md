# E2E Test Report — New Engines (MSSQL, Oracle, SQLite)

**Date:** 2026-02-13
**Branch:** `multi-db/pg-wire`
**Platform:** macOS (Apple Silicon / ARM64)

---

## Summary

| Engine | Unit Tests | E2E Tests | Status |
|--------|-----------|-----------|--------|
| MSSQL  | 100% pass (4 packages) | Shadow init passes, proxy TDS login to shadow fails (EOF) | Code bug |
| Oracle | 100% pass (4 packages) | Shadow init fails — sequence MAX_VALUE overflows int64 | Code bug |
| SQLite | 100% pass (5 packages) | 18 of ~50 subtests fail — proxy doesn't merge shadow reads | Code bug |

**Infra bugs fixed during testing:**
- `docker run` container ID extraction on ARM (CombinedOutput includes platform WARNING)
- Shadow WaitReady timeouts too short for ARM emulation
- MSSQL + Oracle shadow port collision (both defaulted to 9001)

---

## 1. Unit Tests — All Pass

All three engines pass every unit test. No issues found.

### MSSQL (4 packages)
- `classify` — 11 test functions, all pass
- `connstr` — 10 test functions, all pass
- `proxy` — 20 test functions (write guard, TDS encoding, SQL extraction), all pass
- `schema` — 6 test functions, all pass

### Oracle (4 packages)
- `classify` — 12 test functions, all pass
- `connstr` — 10 test functions, all pass
- `proxy` — 12 test functions (write guard, TNS packets), all pass
- `schema` — 6 test functions, all pass

### SQLite (5 packages)
- `classify` — 12 test functions, all pass
- `connstr` — 8 test functions, all pass
- `proxy` — 9 test functions (write guard), all pass
- `schema` — 5 test functions, all pass
- `shadow` — 5 test functions (file-based shadow create/remove), all pass

---

## 2. MSSQL E2E — TDS Proxy Shadow Login Failure

**Outcome:** Shadow container init succeeds, but proxy can't connect to shadow. All tests skipped (proxy never becomes ready).

**Root cause:** The MSSQL TDS proxy's shadow connection handshake fails. Every connection attempt to the shadow produces:

```
WRITE GUARD: shadow connection failed, refusing: reading shadow login response: EOF
```

The proxy connects to `127.0.0.1:9001` (shadow MSSQL), sends a TDS login packet, but receives EOF instead of a login response. This is likely a TLS negotiation issue — MSSQL 2022 defaults to encrypted connections, and the proxy's raw TDS implementation in `internal/engine/mssql/proxy/startup.go` may not be performing the pre-login TLS handshake that the shadow expects.

**Evidence:**
- Shadow container starts successfully and accepts connections via `sqlcmd` (verified manually)
- Schema dump and apply to shadow succeed during Init (uses `database/sql` + `go-mssqldb` driver, which handles TLS)
- The proxy's own TDS wire protocol implementation fails on the login response

**File:** `internal/engine/mssql/proxy/startup.go` (shadow TDS login handshake)

---

## 3. Oracle E2E — Sequence MAX_VALUE Overflow

**Outcome:** Shadow container starts, but schema dump fails. All tests skipped.

**Root cause:** Oracle's default sequence `MAX_VALUE` is `9999999999999999999999999999` (28 digits), which overflows Go's `int64` (max 19 digits). The `generateSequenceDDL` function in `internal/engine/oracle/schema/schema.go:174` scans `MAX_VALUE` into `int64`:

```go
var minVal, maxVal, incBy, lastNum int64
rows.Scan(&seqName, &minVal, &maxVal, &incBy, &lastNum)
```

**Error:**
```
sql: Scan error on column index 2, name "MAX_VALUE":
converting driver.Value type string ("9999999999999999999999999999") to a int64: value out of range
```

**Fix:** Scan `MAX_VALUE` (and `MIN_VALUE`) as `string` instead of `int64`, and emit them directly into the DDL string. Oracle sequence limits are arbitrary-precision numbers that cannot be represented as int64.

**File:** `internal/engine/oracle/schema/schema.go:174`

---

## 4. SQLite E2E — 18 Test Failures (Proxy Merge Bug)

**Outcome:** 18 of ~50 subtests fail. All failures share the same root cause.

**Root cause:** The SQLite proxy routes all SELECT queries to `PROD_DIRECT` and does not merge shadow data into read results. After a write (INSERT/UPDATE/DELETE) is routed to `SHADOW_WRITE`, subsequent reads return only production data — shadow changes are invisible.

Looking at the proxy logs, every read shows:
```
READ/SELECT tables=[...] → PROD_DIRECT
```

The proxy correctly intercepts writes and sends them to the shadow, but the read path never consults the shadow database or applies delta/tombstone overlays.

### Failing Tests (18 subtests across 12 parent tests)

#### Inserts not visible in reads (5 failures)
| Test | Error |
|------|-------|
| `TestBasicInsert/insert_autoincrement_pk` | `expected 1 row for e2e_new_user, got 0` |
| `TestBasicInsert/insert_text_pk` | `unexpected result: []` |
| `TestNoPKTable/select_after_insert` | `expected to find inserted setting, got 0 rows` |
| `TestTextPK/select_after_insert` | `expected 1 row, got 0` |
| `TestSpecialCharacters/single_quotes_escaped` | `unexpected: []` |

#### Updates not visible in reads (3 failures)
| Test | Error |
|------|-------|
| `TestBasicUpdate/update_visible_in_select` | `expected 'Updated User 10', got User Number 10` |
| `TestNullHandling/update_set_to_null` | `expected NULL, got User Number 3` |
| `TestNullHandling/update_from_null_to_value` | `expected 'Restored Name', got User Number 3` |

#### Deletes not visible in reads (1 failure)
| Test | Error |
|------|-------|
| `TestBasicDelete/deleted_row_invisible` | `expected 0 rows (deleted), got 1` |

#### Merged reads broken (3 failures)
| Test | Error |
|------|-------|
| `TestMergedReads/count_after_insert_gt_prod` | `expected more than 100 users after inserts, got 100` |
| `TestMergedReads/inserted_row_visible_by_value` | `expected 1 row for e2e_new_user, got 0` |
| `TestCRUDCycle/full_insert_select_update_select_delete_select` | `READ after INSERT: []` |

#### DDL/Migration visibility broken (3 failures)
| Test | Error |
|------|-------|
| `TestMigrations/select_after_add_column` | `SQL logic error: no such column: phone` |
| `TestMigrations/select_new_column_has_value_for_shadow_row` | `SQL logic error: no such column: phone` |
| `TestMigrations/select_from_new_table` | `SQL logic error: no such table: e2e_new_table` |

#### Cross-connection and large data (3 failures)
| Test | Error |
|------|-------|
| `TestLargeText/read_10kb_text_roundtrip` | `expected 1 row, got 0` |
| `TestConcurrentConnections/two_connections_see_each_others_inserts` | `conn2 should see conn1's insert, got 0 rows` |
| `TestNullHandling/select_where_null` | `expected 1 null-display-name user, got 0` |

### Passing Tests (confirms proxy routing works for read-only paths)

All read-only tests pass — the proxy correctly forwards pure reads to prod:
- `TestLifecycle` (7/7) — connections, simple queries, write isolation
- `TestBasicSelect` (14/14) — SELECT with WHERE, LIMIT, ORDER BY, IN, LIKE, BETWEEN, IS NULL
- `TestJoins` (4/4) — INNER JOIN, LEFT JOIN, 3-table joins
- `TestSubqueries` (3/3) — WHERE IN, EXISTS, scalar subqueries
- `TestCTEs` (2/2) — simple and multiple CTEs
- `TestAggregates` (3/3) — COUNT, SUM, HAVING
- `TestEmptyResults` (2/2) — impossible WHERE, nonexistent ID
- `TestCompositePK` (2/2) — composite PK reads and inserts

---

## 5. Infrastructure Bugs (Fixed During Testing)

These three bugs were discovered and fixed during testing:

### 5a. Container ID Extraction on ARM (`CombinedOutput` bug)

**Affected:** All Docker-based engines (MSSQL, Oracle, MySQL)

**Bug:** `shadow/container.go` uses `cmd.CombinedOutput()` to capture the container ID from `docker run -d`. On ARM hosts, Docker prepends a platform mismatch WARNING on stderr. Since `CombinedOutput` captures both stdout and stderr, `strings.TrimSpace()` preserves the WARNING line, producing an invalid multi-line `containerID`. All subsequent `docker exec <containerID>` commands fail silently, and `WaitReady` polls until timeout.

**Fix applied:** Added `extractContainerID()` helper that extracts only the last non-empty line from the output.

**Files fixed:**
- `internal/engine/mssql/shadow/container.go`
- `internal/engine/oracle/shadow/container.go`
- `internal/engine/mysql/shadow/container.go`

### 5b. Shadow Port Collision

**Bug:** Both MSSQL and Oracle shadow containers default to host port 9001.

**Fix applied:** Oracle shadow default port changed to 9002.

**File fixed:** `internal/engine/oracle/shadow/container.go`

### 5c. WaitReady Timeouts

**Bug:** Shadow container readiness timeouts were too short for ARM emulation.

**Fix applied:**
- MSSQL: 180s → 300s (`internal/engine/mssql/shadow/container.go`)
- Oracle: 300s → 480s (`internal/engine/oracle/shadow/container.go`)
- E2E proxy wait for MSSQL: 180s → 360s (`e2e/mssql/e2e_test.go`)
- E2E proxy wait for Oracle: 300s → 540s (`e2e/oracle/e2e_test.go`)

---

## Collated Action Items — Logical Code Bugs

| # | Priority | Engine | Issue | File | Fix |
|---|----------|--------|-------|------|-----|
| 1 | **P0** | MSSQL | TDS proxy shadow login fails (EOF on login response) | `internal/engine/mssql/proxy/startup.go` | Fix TDS login handshake — likely needs pre-login TLS negotiation for shadow connections |
| 2 | **P0** | Oracle | Sequence MAX_VALUE overflows int64 during schema dump | `internal/engine/oracle/schema/schema.go:174` | Scan `MAX_VALUE`/`MIN_VALUE` as `string` instead of `int64` |
| 3 | **P0** | SQLite | Proxy does not merge shadow data into reads | `internal/engine/sqlite/proxy/conn.go` | Implement read-merge logic (delta overlay + tombstone filtering) in the pgwire proxy read path |
