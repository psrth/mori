# Mori Development Log

## Phase 8 & 9: Merged Reads — Integration Testing (2026-02-12)

### Setup

- **Prod**: Docker container `mori-test-prod` running PostgreSQL 16.11 on port 5433 with `trust` auth
- **Shadow**: Created by `mori init`, running on a random port with `trust` auth
- **Proxy**: `mori start --port 5488 --verbose`
- **Seed data**: 3 users (alice, bob, charlie) + 4 orders (2 for alice, 1 for bob, 1 for charlie)

### Test Results

| # | Test | Strategy | Result |
|---|------|----------|--------|
| 1 | Baseline SELECT (no deltas) | ProdDirect | PASS — 3 Prod users returned |
| 2 | INSERT new user `dave` | ShadowWrite | PASS — `INSERT 0 1`, dave gets id=10000004 (Shadow sequence offset) |
| 3 | SELECT after INSERT | MergedRead | PASS — 4 rows (3 Prod + 1 Shadow) |
| 4 | UPDATE `alice` -> `alice_v2` | HydrateAndWrite | PASS — row hydrated from Prod, updated in Shadow, delta map entry added |
| 5 | SELECT after UPDATE | MergedRead | PASS — `alice_v2` from Shadow, bob from Prod, dave from Shadow |
| 6 | DELETE charlie | ShadowDelete | PASS — tombstone added |
| 7 | SELECT after DELETE | MergedRead | PASS — 3 rows, charlie absent |
| 8 | SELECT ORDER BY name DESC | MergedRead | PASS — dave, bob, alice_v2 (correct DESC order) |
| 9 | SELECT ORDER BY id DESC LIMIT 2 | MergedRead | PASS — dave (10000004), bob (6) |
| 10 | JOIN users+orders | JoinPatch | PASS — 3 rows: alice_v2 + 2 orders, bob + 1 order. Charlie's order excluded. |
| 11 | JOIN with WHERE (no PK in SELECT) | JoinPatch | PASS with known v1 limitation — patching skipped when PK column not in result set |
| 12 | Prod verification | N/A | PASS — all 3 original users + 4 orders unchanged in Prod |

### Bugs Found & Fixed

#### 1. Shadow SCRAM-SHA-256 Auth Failure

**Symptom**: Every connection logged `Shadow handshake failed, degrading to relay: shadow requires SASL auth (not supported)`. All queries went directly to Prod — no merged reads were actually tested.

**Root cause**: PostgreSQL 16+ defaults to `scram-sha-256` authentication. The proxy's `connectShadow()` in `conn.go` only handles cleartext (type 3) and MD5 (type 5) auth, returning an error for SASL (type 10).

**Fix**: Added `POSTGRES_HOST_AUTH_METHOD=trust` to the Shadow container environment in `shadow/container.go`. Shadow is a local throwaway database bound to `127.0.0.1`, so trust auth is acceptable.

**File**: `internal/engine/postgres/shadow/container.go` (line 82)

---

#### 2. INSERT Routing Gap — SELECTs Not Triggering MergedRead After INSERT

**Symptom**: After `INSERT INTO users`, subsequent `SELECT * FROM users` was still routed as `PROD_DIRECT` instead of `MERGED_READ`. The inserted row was invisible.

**Root cause**: The `handleInsert` function in `write_insert.go` didn't update any state that the Router checks. The Router's `anyTableAffected()` only checks `deltaMap.AnyTableDelta()` and `tombstones.AnyTableTombstone()`. Since INSERTs don't add per-PK delta entries (by design — locally inserted rows live only in Shadow), the Router never knew to trigger merged reads.

**Fix**: Added `insertedTables` tracking to `delta.Map`:
- New field: `insertedTables map[string]bool` with its own `sync.RWMutex`
- New methods: `MarkInserted(table)`, `HasInserts(table)`, `InsertedTablesList()`, `LoadInsertedTables([]string)`
- Updated `AnyTableDelta()` to also check `insertedTables`
- Updated `handleInsert()` to call `deltaMap.MarkInserted(table)` after successful INSERT
- Updated persistence format: `delta.json` now uses `{"deltas": {...}, "inserted_tables": [...]}` with backward-compatible reading

**Files**:
- `internal/core/delta/map.go` — added insertedTables tracking
- `internal/core/delta/store.go` — new `deltaFile` struct, backward-compatible persistence
- `internal/engine/postgres/proxy/write_insert.go` — `handleInsert` now accepts `*core.Classification`, calls `MarkInserted`
- `internal/engine/postgres/proxy/write.go` — passes `cl` to `handleInsert`

---

#### 3. ORDER BY DESC Not Working — Trailing Semicolon

**Symptom**: `SELECT * FROM users ORDER BY name DESC` returned rows in ascending order instead of descending.

**Root cause**: `extractOrderByFromRaw()` in the classifier extracts everything after `ORDER BY` to the end of the SQL, including the trailing `;` from the query. This produced `OrderBy = "name DESC;"`. In `parseSimpleOrderBy`, `strings.EqualFold("DESC;", "DESC")` returned `false`, so the sort defaulted to ascending.

**Fix**: Added `strings.TrimRight(orderBy, "; \t\n")` at the start of `parseSimpleOrderBy()`.

**File**: `internal/engine/postgres/proxy/read_single.go` (line 407)

---

#### 4. JOIN Dedup False Positives — One-to-Many Rows Lost

**Symptom**: `SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id` returned only 2 rows instead of 3. Alice had 2 orders but only one appeared.

**Root cause**: `dedupJoin()` built its composite key using only PK columns from delta tables. When multiple tables share the same PK column name (e.g., both `users.id` and `orders.id` are named "id"), `findPKIndicesForTables` mapped both tables to the same column index (column 0). The composite key was `row[0] + "\x00" + row[0]` = `"5\x005"` for both of alice's rows, causing the second to be discarded.

**Fix**: Detect ambiguous PK column mapping (two tables mapping to the same column index) and fall back to full-row dedup (using all column values as the composite key). This correctly preserves one-to-many rows while still deduplicating true duplicates.

Also changed `handleJoinPatch` to build PK indices for ALL joined tables (`cl.Tables`) rather than just delta tables, so the composite key includes all available PKs.

**Files**:
- `internal/engine/postgres/proxy/read_join.go` — rewrote `dedupJoin()` with ambiguity detection, updated `handleJoinPatch` dedup call

---

### Known v1 Limitations Confirmed

1. **Multi-column ORDER BY**: Only single-column sort is re-applied in Go. Multi-column ORDER BY is not re-sorted after merge (rows arrive in whatever order the backends return them).

2. **JOIN PK patching requires PK in SELECT list**: If the query doesn't SELECT the PK column (e.g., `SELECT u.name, o.total FROM ...`), JoinPatch can't identify which rows to patch from Shadow. The Prod values are returned as-is.

3. **WHERE not re-evaluated after patching**: After replacing delta columns in a JOIN row with Shadow values, the row might no longer match the original WHERE clause. v1 keeps it anyway.

4. **Parameterized LIMIT**: `LIMIT $1` in the simple query protocol is not rewritten for over-fetching. This is uncommon in practice (most ORMs use literal LIMIT values in simple queries).

5. **Aggregate queries (COUNT, SUM, etc.)**: `SELECT COUNT(*) FROM users` runs on both Shadow and Prod independently, returning two rows instead of a merged count. Aggregate merging is not implemented in v1.
