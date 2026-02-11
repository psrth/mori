# Mori Protocol

**Database Virtualization via Copy-on-Write Proxying**

---

## 1. What Mori Is

Mori is a transparent proxy that sits between an application and a production database. The application connects to Mori as if it were a normal database — same wire protocol, same driver, no code changes beyond swapping a connection string. Under the hood, Mori splits the world in two:

- **Reads** are served from the real production database.
- **Writes** are captured in a local database instance (the Shadow) that starts as a schema-only clone of production.

The application sees a single, unified database. It cannot tell that some data comes from production and some from Shadow. From its perspective, it's just talking to a database.

The result: developers can test against real production data, mutate freely, and never risk the production database. Break something, reset, start over. The production database is never written to.

---

## 2. Core Invariants

These hold at all times, without exception.

1. **Production is read-only.** Mori never issues a write (INSERT, UPDATE, DELETE, DDL) to the upstream production database. The Prod connection is opened in read-only mode. There is no code path that writes to Prod.

2. **Shadow is the mutation layer.** All writes go to the Shadow database. Shadow starts as a schema-only clone of Prod with zero rows. Over time, it accumulates the developer's mutations — inserted rows, updated rows (hydrated from Prod then modified), and metadata about deleted rows.

3. **The application sees one unified database.** When the app issues a SELECT, Mori transparently merges results from Prod and Shadow. Locally-modified rows come from Shadow. Untouched rows come from Prod. The app never knows the difference.

4. **Reset is instant.** Wiping the Shadow (truncate all tables, clear metadata) restores a clean view of production with zero local state. There is no migration, no rollback procedure — just a clean slate.

---

## 3. Terminology

| Term | Definition |
|------|-----------|
| **Prod** | The upstream production database. Read-only from Mori's perspective. This is the source of truth for all unmodified data. |
| **Shadow** | A local database instance (same engine and version as Prod) that holds all local mutations. Starts schema-only with zero rows. Think of it as a transparent overlay on top of Prod. |
| **Proxy** | The Mori process itself. It intercepts wire protocol traffic between the application and the databases. It classifies queries, routes them, merges results, and manages state. |
| **Taint Map** | A set of `(table, primary_key)` pairs identifying rows that have been locally modified (inserted into Shadow via hydration + update). When a row is tainted, Shadow holds the authoritative version and Prod's version is ignored for that row. |
| **Tombstone Set** | A set of `(table, primary_key)` pairs identifying rows that have been locally deleted. Tombstoned rows are filtered out of Prod results so the app sees them as deleted. |
| **Schema Registry** | A record of structural differences between Shadow and Prod schemas, introduced by local DDL. Tracks added columns, dropped columns, renames, and type changes per table. Used to adapt Prod rows to match Shadow's schema during reads. |
| **Hydration** | The act of copying a row from Prod into Shadow so it can be mutated locally. This happens transparently when the app tries to UPDATE or DELETE a row that only exists in Prod. The row is fetched from Prod, adapted for any schema differences, inserted into Shadow, and then the mutation is applied. |
| **Classification** | The process of parsing an incoming SQL statement to determine its operation type (READ, WRITE, DDL, TRANSACTION), affected tables, and extractable primary key values. |
| **Routing** | The decision of where to execute a classified statement — Prod, Shadow, or both — based on the operation type and current taint/tombstone state. |
| **Merged Read** | A read operation where results from both Prod and Shadow are combined, with tainted/tombstoned rows filtered from Prod results. This occurs when a SELECT touches a table that has local mutations. |

---

## 4. Architecture

### 4.1 Component Overview

```
┌─────────────┐
│ Application │
└──────┬──────┘
       │ DB wire protocol (e.g., PG v3, MySQL protocol, Redis RESP)
       ▼
┌──────────────────────────────────────────────────────────┐
│                       MORI PROXY                         │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌───────────────────┐  │
│  │  Protocol   │  │ Classifier │  │      Router       │  │
│  │  Handler    │──│            │──│                   │  │
│  │            │  │ op_type    │  │ Prod Direct       │  │
│  │ accept     │  │ tables     │  │ Merged Read       │  │
│  │ parse      │  │ pks        │  │ Shadow Write      │  │
│  │ serialize  │  │ join?      │  │ Hydrate+Write     │  │
│  └────────────┘  └────────────┘  │ Shadow Delete     │  │
│                                   │ Shadow DDL        │  │
│  ┌────────────┐  ┌────────────┐  │ Transaction Ctrl  │  │
│  │   Taint    │  │  Schema    │  └───────────────────┘  │
│  │  Manager   │  │ Registry   │                          │
│  │            │  │            │  ┌───────────────────┐  │
│  │ taint_map  │  │ added_cols │  │   Merge Engine    │  │
│  │ tombstones │  │ dropped    │  │                   │  │
│  │ staging    │  │ renamed    │  │ single_table      │  │
│  └────────────┘  │ type_casts │  │ join_patch        │  │
│                   └────────────┘  │ over_fetch        │  │
│  ┌────────────┐                   │ adapt + filter    │  │
│  │Transaction │                   └───────────────────┘  │
│  │  Manager   │                                          │
│  │            │  ┌────────────┐  ┌───────────────────┐  │
│  │ per-conn   │  │  Engine    │  │   Write Engine    │  │
│  │ staging    │  │  Manager   │  │                   │  │
│  │ commit     │  │            │  │ insert            │  │
│  │ rollback   │  │ prod_pool  │  │ update+hydrate    │  │
│  └────────────┘  │ shadow_pool│  │ delete+tombstone  │  │
│                   └────────────┘  │ ddl+registry      │  │
│                                   └───────────────────┘  │
└──────────────┬───────────────────────────┬───────────────┘
               │ read-only                 │ read-write
               ▼                           ▼
        ┌────────────┐             ┌────────────┐
        │    Prod    │             │   Shadow   │
        │  Database  │             │  Database  │
        │ (upstream) │             │  (local)   │
        └────────────┘             └────────────┘
```

### 4.2 Component Responsibilities

**Protocol Handler**
Speaks the target database's native wire protocol. Accepts connections from the application, performs handshake and authentication, parses incoming messages into structured representations, and serializes outgoing results. The application's database driver connects here — it's indistinguishable from a real database server.

**Classifier**
Takes a parsed query and determines:
- **Operation type**: `READ`, `WRITE` (INSERT/UPDATE/DELETE), `DDL` (ALTER/CREATE/DROP), `TRANSACTION` (BEGIN/COMMIT/ROLLBACK), `OTHER`
- **Affected tables**: every table name referenced in the statement
- **Affected primary keys**: PK values extractable from WHERE clauses (for point lookups)
- **Join flag**: whether the statement involves multiple tables

The classifier does lightweight syntactic analysis — it parses the query structure but does not plan or optimize. It needs to be fast, as it sits in the hot path of every query.

**Router**
Takes the classifier output + current taint/tombstone state and selects an execution strategy. This is the core decision point — it determines whether a query goes to Prod, Shadow, or triggers a merge operation. The routing table is defined in §5.3.

**Taint Manager**
Maintains two data structures:
- **Taint Map**: `(table, pk) → tainted` — rows with local modifications in Shadow
- **Tombstone Set**: `(table, pk) → deleted` — rows locally deleted

Both support **transaction staging**: within a transaction, additions are staged. On COMMIT, staged entries promote to persistent state. On ROLLBACK, they're discarded. This prevents phantom taints from failed transactions.

**Schema Registry**
Tracks per-table schema divergence between Shadow and Prod. When the app runs DDL (ALTER TABLE, CREATE TABLE, etc.) on Shadow, the registry records what changed. This metadata is used to adapt Prod rows during reads (inject NULLs for new columns, strip dropped columns, rename, cast types) and during hydration.

**Transaction Manager**
Manages per-connection transaction state. Coordinates transactions across Prod (read-only) and Shadow (read-write). Ensures taint/tombstone staging semantics (promote on commit, discard on rollback). Enforces isolation on Prod reads within a transaction.

**Engine Manager**
Manages connection pools to both Prod and Shadow. Each application connection to Mori gets one dedicated Prod connection (read-only) and one dedicated Shadow connection (read-write). Handles connection lifecycle, health checks, and reconnection.

**Merge Engine**
Implements the logic for combining Prod and Shadow results during reads. Handles single-table merged reads, JOIN patching, over-fetching for LIMIT queries, row filtering (taint/tombstone), and schema adaptation. This is where the core complexity lives.

**Write Engine**
Implements mutation paths: INSERT (Shadow-only), UPDATE (hydrate from Prod if needed, then Shadow), DELETE (Shadow + tombstone). Coordinates with Taint Manager to update state on every mutation.

---

## 5. Query Lifecycle

### 5.1 The Happy Path (No Local Mutations)

When Shadow has zero rows and no taint/tombstone entries, Mori is a pure pass-through proxy:

```
App → Query → Proxy → Prod → Result → App
```

Every query goes to Prod, results come back unmodified. The overhead is the Proxy hop — parsing the wire protocol message, classifying the query (fast, no taint state to check), and forwarding. This baseline overhead should be minimal.

### 5.2 Classification

Every incoming statement is classified before routing. The classifier extracts:

```
Input:  "UPDATE users SET email = 'new@example.com' WHERE id = 42"
Output: {
  op_type: WRITE,
  sub_type: UPDATE,
  tables: ["users"],
  pks: [("users", "42")],
  is_join: false
}
```

```
Input:  "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
Output: {
  op_type: READ,
  sub_type: SELECT,
  tables: ["orders", "users"],
  pks: [],
  is_join: true
}
```

For parameterized queries (where real values are bound separately from the SQL template), classification happens in two phases:
1. **Template parse**: extract operation type, table names, and identify which parameters correspond to PK columns
2. **Parameter bind**: resolve actual PK values from bound parameters

### 5.3 Routing Table

| Operation | Condition | Strategy |
|-----------|-----------|----------|
| `SELECT` (single table) | Table has no taints/tombstones | **Prod Direct** — forward to Prod, return result |
| `SELECT` (single table) | Table has taints or tombstones | **Merged Read** — query both, filter, adapt, merge |
| `SELECT` (JOIN) | No table has taints/tombstones | **Prod Direct** — forward to Prod, return result |
| `SELECT` (JOIN) | Any table has taints/tombstones | **Join Patch** — execute on Prod, patch from Shadow |
| `INSERT` | — | **Shadow Write** — execute on Shadow only |
| `UPDATE` | — | **Hydrate + Shadow Write** — hydrate if needed, update Shadow |
| `DELETE` | — | **Shadow Delete** — delete from Shadow, add tombstone |
| `DDL` | — | **Shadow DDL** — execute on Shadow, update Schema Registry |
| `BEGIN` | — | Open transactions on both Prod and Shadow |
| `COMMIT` | — | Commit Shadow, close Prod, promote staged taints/tombstones |
| `ROLLBACK` | — | Rollback Shadow, close Prod, discard staged taints/tombstones |

---

## 6. Read Path

### 6.1 Prod Direct

The simplest case. The query touches only tables with no local mutations.

```
App → SELECT * FROM orders WHERE total > 100 → Proxy
Proxy: classify → READ, tables=[orders], orders not tainted
Proxy → forward query to Prod
Prod → result set → Proxy → App
```

Zero overhead beyond the proxy hop. No merging, no filtering.

### 6.2 Merged Read (Single Table)

When a SELECT touches a table that has entries in the Taint Map or Tombstone Set, the Proxy must merge results from both databases.

**Example:** `SELECT * FROM users WHERE active = true ORDER BY created_at LIMIT 20`
**Context:** `users` has 3 tainted rows and 1 tombstoned row.

**Step 1: Shadow Query.**
Execute the original query against Shadow verbatim. Shadow contains only locally-created or hydrated rows, so this returns the local subset that matches the query.

```sql
-- Against Shadow:
SELECT * FROM users WHERE active = true ORDER BY created_at LIMIT 20
-- Returns: rows that were inserted locally or hydrated+modified
```

**Step 2: Prod Query (with over-fetching).**
Execute the query against Prod, but adjust the LIMIT to account for rows that will be filtered out. Over-fetch by `LIMIT + |taints_for_table| + |tombstones_for_table|`.

```sql
-- Against Prod (over-fetched):
SELECT * FROM users WHERE active = true ORDER BY created_at LIMIT 24
-- (20 + 3 taints + 1 tombstone = 24)
```

If the table has schema divergence (tracked in Schema Registry), sanitize the query first: remove WHERE predicates referencing Shadow-only columns, strip references to dropped columns.

**Step 3: Filter Prod Results.**
For each row returned from Prod:
- If `(users, row.pk)` exists in Taint Map → **discard**. Shadow has the authoritative version.
- If `(users, row.pk)` exists in Tombstone Set → **discard**. Row is locally deleted.

**Step 4: Adapt Prod Results.**
If Schema Registry indicates divergence for this table:
- Inject `NULL` (or column default) for columns added in Shadow but absent in Prod.
- Drop columns that were removed in Shadow but present in Prod.
- Apply column renames and type casts as registered.

**Step 5: Merge.**
Combine Shadow results + filtered/adapted Prod results into a single result set.

**Step 6: Re-apply Ordering and Limits.**
Sort the merged set by the original `ORDER BY`. Apply `LIMIT`/`OFFSET` to the merged set. Return to app.

#### Over-Fetching Detail

Because Prod results are post-filtered (tainted and tombstoned rows removed), a `LIMIT N` on the Prod query may yield fewer than N usable rows. The over-fetching strategy:

1. First attempt: `LIMIT N + |taints| + |tombstones|`
2. If after filtering the result set is still short, issue a follow-up query with OFFSET
3. Cap at **3 iterations** to bound latency

For queries with no LIMIT, all rows are fetched and filtered — no over-fetching needed.

### 6.3 Merged Read (JOINs) — Execute on Prod, Patch from Shadow

When a SELECT joins multiple tables and at least one table has local mutations, neither database can execute the query correctly alone. Prod doesn't have the mutated rows. Shadow doesn't have the untouched rows from clean tables.

**Strategy: Execute on Prod, Patch from Shadow.**

**Example:**
```sql
SELECT o.id, o.total, u.name
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE u.active = true
ORDER BY o.created_at DESC
LIMIT 50
```
Where `users` has local mutations, `orders` is clean.

**Step 1: Execute on Prod.**
Run the query as-is against Prod. This produces result set `R_prod` — correct for all non-tainted rows.

**Step 2: Identify affected rows.**
Scan every row in `R_prod`. For each row, check if any primary key from a tainted table appears in the Taint Map or Tombstone Set. Partition into:
- **Clean rows**: no referenced PKs are tainted or tombstoned → keep as-is
- **Tainted rows**: at least one referenced PK is in the Taint Map → needs patching
- **Dead rows**: at least one referenced PK is in the Tombstone Set → discard

**Step 3: Discard tombstoned rows.**
Remove any row from `R_prod` where a referenced PK is tombstoned.

**Step 4: Patch tainted rows.**
For each row referencing a tainted PK:
1. Fetch the Shadow version of the tainted row(s)
2. Replace the tainted columns in the joined result with Shadow's values
3. Re-evaluate whether the row still matches the original WHERE clause (e.g., if `u.active` was changed to `false` in Shadow, this row should be dropped)

**Step 5: Execute on Shadow.**
Run the same query against Shadow. This catches:
- Locally-inserted rows that should appear in the result (e.g., a new user whose orders exist in Prod)
- Rows where the local mutation changes whether they match the WHERE clause (e.g., a user changed from `active=false` to `active=true`)

Shadow will only return results for rows that exist in Shadow, so this naturally captures the local overlay.

**Step 6: Merge and deduplicate.**
Combine patched `R_prod` + `R_shadow`. Deduplicate by the composite key of the result set (all PK columns from all joined tables). Re-apply `ORDER BY` and `LIMIT`.

#### Edge Cases

- **All tables tainted**: Both Prod and Shadow execution contribute. The merge handles deduplication.
- **All tables clean**: Router detects this and uses Prod Direct. No merge needed.
- **Self-joins**: Treated as a single-table merged read if the table is tainted.
- **Subqueries**: Classified by their outermost operation. If the subquery references a tainted table, the entire query goes through the JOIN patch path.

---

## 7. Write Path

### 7.1 INSERT

```sql
INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')
```

1. Execute the INSERT on Shadow only.
2. Shadow's offset sequence assigns a PK in the local range (e.g., `10,000,001`). The sequence offset guarantees locally-created PKs never collide with Prod PKs.
3. Return the result (including generated PK) to the application.

No Taint Map entry is needed for pure inserts — the row exists only in Shadow. The Router can identify locally-created rows by PK range: **any PK above the offset threshold was created locally and lives exclusively in Shadow.**

For non-integer PKs (UUIDs), Shadow generates the value locally. UUID v4 values are statistically guaranteed to never collide with Prod values. The Router cannot use range-based detection for UUIDs, so it checks the Taint Map for all lookups on UUID-keyed tables. (Since pure inserts don't add to the Taint Map, lookups for locally-inserted UUID rows check Shadow directly when the UUID is not found in Prod.)

### 7.2 UPDATE

```sql
UPDATE users SET email = 'new@example.com' WHERE id = 42
```

**Point Update (single row, PK in WHERE):**

1. **Check Shadow.** Does `(users, 42)` exist in Shadow?
   - **Yes** → Row was previously hydrated or locally created. Proceed to step 2.
   - **No** → Row exists only in Prod. **Hydrate first:**
     a. Fetch the full row from Prod: `SELECT * FROM users WHERE id = 42`
     b. Adapt the row for schema differences (inject NULLs for new columns, strip dropped columns, apply renames/casts per Schema Registry)
     c. Insert the adapted row into Shadow
2. Apply the UPDATE to the row in Shadow.
3. Add `(users, 42)` to the Taint Map (now Prod's version of this row is stale; Shadow is authoritative).
4. Return result to the application.

**Bulk Update (no PK in WHERE, potentially many rows):**

```sql
UPDATE users SET active = false WHERE last_login < '2020-01-01'
```

1. **Identify affected rows.** Run `SELECT pk_column FROM users WHERE last_login < '2020-01-01'` against **both** Prod and Shadow. Union the results (some rows may exist in both if previously hydrated).
2. **Hydrate missing rows.** For each affected Prod row not already in Shadow: fetch, adapt, insert into Shadow.
3. **Apply the UPDATE** on Shadow: `UPDATE users SET active = false WHERE last_login < '2020-01-01'`
4. **Add all affected PKs** to the Taint Map.

Bulk updates are expensive for large result sets — every affected Prod row must be hydrated. This is an acceptable cost for a development tool. Prod is never mutated regardless.

### 7.3 DELETE

```sql
DELETE FROM users WHERE id = 42
```

1. If `(users, 42)` exists in Shadow → delete it from Shadow.
2. Add `(users, 42)` to the Tombstone Set (regardless of whether the row was in Shadow — this keeps the delete path uniform and ensures the row is filtered from Prod reads).
3. Return result to the application.

For locally-created rows (PK in the offset range), the tombstone is technically unnecessary (the row doesn't exist in Prod and won't appear in Prod reads), but adding it keeps the logic branchless and simple.

**Bulk Delete** follows the same pattern as bulk update: identify affected PKs via SELECT on both databases, delete from Shadow, add all PKs to Tombstone Set.

---

## 8. DDL Path

```sql
ALTER TABLE users ADD COLUMN phone TEXT
```

1. Execute the DDL on Shadow only. Prod schema is untouched.
2. Update the Schema Registry:
   ```
   users:
     added: [{name: "phone", type: "TEXT", default: NULL}]
   ```
3. Return success to the application.

The Schema Registry is consulted in two places:
- **During reads**: Prod rows are adapted to match Shadow's schema (inject NULLs for added columns, strip dropped columns, apply renames/casts).
- **During hydration**: rows fetched from Prod are adapted before insertion into Shadow.

### 8.1 Supported DDL Operations

| DDL | Registry Records | During Hydration (Prod → Shadow) | During Reads (Prod → App) |
|-----|-----------------|----------------------------------|--------------------------|
| `ADD COLUMN` | Column name, type, default | Hydrated rows get `DEFAULT`/`NULL` for new column | Prod rows get `NULL`/default injected |
| `DROP COLUMN` | Dropped column name | Dropped column excluded from hydrated row | Dropped column stripped from Prod result |
| `RENAME COLUMN` | `old_name → new_name` | Column renamed on insert into Shadow | Column renamed in result set |
| `ALTER TYPE` | `column: old_type → new_type` | Value cast on insert into Shadow | Value cast in result set |
| `CREATE TABLE` | Table marked as Shadow-only | N/A (table doesn't exist in Prod) | N/A |
| `DROP TABLE` | Table marked as dropped | N/A | Queries referencing this table return "table not found" |
| `CREATE INDEX` | Applied to Shadow only, silent | N/A | N/A |

### 8.2 Schema Adaptation Example

Prod schema: `users(id, name, email, fax)`
After local DDL: `ALTER TABLE users ADD COLUMN phone TEXT; ALTER TABLE users DROP COLUMN fax;`
Shadow schema: `users(id, name, email, phone)`

Schema Registry for `users`:
```
added: [{name: "phone", type: "TEXT", default: NULL}]
dropped: ["fax"]
```

When a Prod row `(42, "Alice", "alice@test.com", "+1-555-0100")` is read:
1. Strip `fax` column → `(42, "Alice", "alice@test.com")`
2. Inject `phone = NULL` → `(42, "Alice", "alice@test.com", NULL)`

The app sees the row as if it came from Shadow's schema.

---

## 9. Transaction Model

### 9.1 Basic Semantics

Mori maintains per-connection transaction state. When the app opens a transaction, Mori coordinates across both Prod and Shadow:

- **`BEGIN`**: Open a read-only transaction on Prod (with REPEATABLE READ isolation or equivalent) and a read-write transaction on Shadow. This ensures all Prod reads within the transaction see a consistent snapshot.
- **`COMMIT`**: Commit the Shadow transaction. Close/release the Prod transaction (it was read-only, nothing to commit). Promote all staged Taint Map and Tombstone Set entries to persistent state.
- **`ROLLBACK`**: Rollback the Shadow transaction (undoing all local mutations within this transaction). Close/release the Prod transaction. **Discard** all staged Taint Map and Tombstone Set entries — the mutations never happened, so the taints should not persist.

### 9.2 Staged Taint Tracking

Within a transaction, Taint Map and Tombstone Set modifications are **staged** — held in a per-transaction buffer rather than applied to the persistent state immediately. This is critical for correctness:

```
BEGIN;
UPDATE users SET name = 'Bob' WHERE id = 42;   -- stages taint for (users, 42)
-- application encounters an error --
ROLLBACK;
-- staged taint for (users, 42) is discarded
-- persistent taint map is unchanged
-- Shadow row for users.42 is rolled back to its pre-transaction state
```

Without staging, a ROLLBACK would leave phantom taints — the Taint Map would reference `(users, 42)` but Shadow would not have the modified row (because the transaction was rolled back). This would cause the row to be invisible: filtered from Prod reads (because it's tainted) but absent from Shadow reads (because the mutation was rolled back).

### 9.3 Isolation

The Prod connection within a transaction uses **REPEATABLE READ** isolation at minimum. This ensures that all reads within a transaction see a consistent snapshot of Prod, even if Prod is being mutated concurrently by other sources. Without this, a transaction that reads a row from Prod, modifies it locally, and then reads related data could see inconsistent state across the reads.

### 9.4 Implicit Transactions

Many applications and ORMs operate in autocommit mode, where each statement is an implicit single-statement transaction. In this mode:
- Each statement executes atomically
- Taint/tombstone updates are applied immediately (no staging needed — a single-statement transaction either succeeds fully or fails fully)
- No explicit BEGIN/COMMIT/ROLLBACK needed

---

## 10. Initialization and Lifecycle

### 10.1 Init

```
mori init --from <prod_connection_string> [--image <docker_image>]
```

1. **Connect to Prod.** Validate the connection, determine engine type and version.
2. **Schema dump.** Extract the full schema from Prod (tables, columns, types, constraints, indexes, sequences). Engine-specific mechanism (e.g., `pg_dump --schema-only` for Postgres).
3. **Detect extensions.** Query Prod for installed extensions (e.g., `SELECT extname, extversion FROM pg_extension` in Postgres). Record the list — these must be available on Shadow.
4. **Spin up Shadow.** Launch a local database instance of the same engine and version (via Docker container or equivalent). By default, use the engine's standard image (e.g., `postgres:16.2`). If the user provides `--image`, use that instead — this is necessary when Prod uses extensions not available in the base image (e.g., PostGIS, pgvector, TimescaleDB).
5. **Install extensions on Shadow.** For each extension detected in step 3, attempt to install it on Shadow. If any installation fails, abort with a clear message: "Prod uses extension `<name>` but it's not available in the Shadow image. Re-run with `--image <image_that_has_it>`."
6. **Apply schema to Shadow.** With modifications:
   - **Strip foreign key constraints.** Shadow doesn't have Prod's data, so FK references would fail on INSERT/hydration. FKs are unnecessary — the app's own code enforces referential integrity at the application level.
   - **Preserve primary keys and unique constraints.** Needed for taint map lookups and conflict detection.
   - **Preserve indexes.** Local query performance matters.
7. **Offset sequences.** For each table with an auto-increment/serial PK:
   - Query Prod for the current max value of the PK column
   - Set Shadow's sequence to start at `max(prod_max * 10, prod_max + 10_000_000)`
   - This guarantees locally-created PKs never collide with Prod PKs for the duration of a session
8. **Initialize state.** Create empty Taint Map, Tombstone Set, and Schema Registry.
9. **Write config.** Create `.mori/` directory with connection metadata, sequence offsets, and state files.

### 10.2 Start

```
mori start [--port <port>]
```

1. Start the Shadow instance (if not already running).
2. Start the Proxy, listening on `localhost:<port>`.
3. Establish a read-only connection pool to Prod.
4. Establish a read-write connection pool to Shadow.
5. Load Taint Map, Tombstone Set, and Schema Registry from disk into memory.
6. Begin accepting application connections.

### 10.3 Stop

```
mori stop
```

1. Drain active connections (wait for in-flight queries to complete).
2. Persist Taint Map, Tombstone Set, and Schema Registry to disk.
3. Stop the Proxy.
4. Optionally stop the Shadow instance.

### 10.4 Reset

```
mori reset
```

1. Truncate all tables in Shadow (delete all local data).
2. Re-apply schema from Prod (in case Prod schema has changed since init).
3. Re-offset sequences.
4. Clear Taint Map, Tombstone Set, and Schema Registry.

Result: a clean-slate view of production, as if starting fresh.

---

## 11. Primary Key Model

Mori's taint and routing system depends on being able to identify rows by primary key. Different PK types require different handling:

### 11.1 Integer Sequences (serial, bigserial, auto-increment)

The most common case. Sequence offsets guarantee no PK collision between Prod and Shadow. The Router can use **range-based fast-path routing**: any PK value above the offset threshold was created locally and lives exclusively in Shadow. No Taint Map lookup needed for locally-created rows.

### 11.2 UUIDs

UUID v4 values are statistically guaranteed unique. Shadow generates UUIDs locally with no collision risk. However, the Router cannot use range-based routing — there's no "local range" for UUIDs. All PK lookups on UUID-keyed tables check the Taint Map. For locally-inserted UUID rows (not in Taint Map because INSERTs don't taint), the Router checks Shadow when the row is not found in Prod.

### 11.3 Composite Primary Keys

Tables with multi-column primary keys (e.g., `(user_id, role_id)`). The Taint Map serializes composite keys as tuples. All operations work the same — just with a compound key instead of a scalar.

### 11.4 Tables Without Primary Keys

These cannot be reliably tracked in the Taint Map (no unique row identifier). For v1, tables without PKs are treated as **Prod-only / read-only**. Writes to PK-less tables emit a warning. This is an acceptable limitation — PK-less tables are uncommon in well-designed schemas.

---

## 12. Decisions and Rationale

### 12.1 Foreign Key Constraints: Strip from Shadow

**Decision:** Remove all FK constraints from Shadow during init.

**Rationale:** When the app INSERTs a row that references a Prod row via FK (e.g., `INSERT INTO orders (user_id, ...) VALUES (42, ...)`), user 42 doesn't exist in Shadow (Shadow starts empty). The FK constraint would reject the INSERT. The alternative — hydrating referenced rows transitively — is a rabbit hole (one FK leads to another, and you'd end up pulling half the database). Shadow is a mutation scratchpad; the app's code already ensures referential integrity against what it believes is a real database.

**Tradeoff:** Developers won't see FK violation errors during testing. If they introduce a bug that violates referential integrity, Mori won't catch it. This is acceptable — FK validation is the database's concern, and the developer can always test against a real (non-Mori) database for constraint validation.

### 12.2 Production Data Drift: Accept It

**Decision:** Prod reads are live. No snapshot pinning.

**Rationale:** Pinning to a snapshot (via long-lived REPEATABLE READ transactions or exported snapshots) holds resources on Prod that can interfere with vacuuming, bloat WAL, and cause replication lag. For a dev tool with typically short sessions, live reads are pragmatic. The mental model: Shadow overrides are yours; Prod reads are live.

**Tradeoff:** If Prod is actively written to during a session, the developer sees a mix of their local mutations (consistent) and live Prod data (evolving). This can cause subtle inconsistencies — e.g., a row read from Prod at time T0 and hydrated into Shadow may have been updated in Prod at T1. The developer's local modifications are applied on top of the T0 version. For most development workflows, this is irrelevant. For integration testing requiring strict consistency, a future version could offer snapshot pinning via a flag.

### 12.3 Connection Model: 1:1:1

**Decision:** Each app connection to Mori gets one dedicated Prod connection and one dedicated Shadow connection.

**Rationale:** Simplicity. No multiplexing complexity, no need to manage shared connection state across app connections. Per-connection transaction isolation comes for free.

**Tradeoff:** More connections to Prod than strictly necessary. For a dev tool with typically 1-5 app connections, this is fine. If connection limits become an issue (e.g., Prod has a very low connection limit), a future version could add multiplexing.

### 12.4 Sequence Offset: Configurable, Generous Default

**Decision:** `max(prod_max * 10, prod_max + 10_000_000)` per-table, stored in config, overridable.

**Rationale:** Needs to be large enough that Prod's ongoing inserts don't catch up to the offset during a session, but not so large that it exceeds the integer type's range. 10x the current max or +10M (whichever is larger) provides comfortable headroom for most workloads.

**Tradeoff:** For extremely high-volume tables (millions of inserts per day), the offset may need manual adjustment. The config allows this. For bigserial columns, the range is effectively unlimited.

### 12.5 Parameterized Queries: Two-Phase Classification

**Decision:** Classify at template parse time (tables, operation type); resolve PK values at parameter bind time.

**Rationale:** Database wire protocols separate the SQL template from parameter values. Most routing decisions depend only on tables and operation type (known at parse time). PK-specific routing (taint map lookups for point queries) needs the actual values, available at bind time.

**Tradeoff:** Increases the complexity of the classification pipeline — state must be carried across protocol messages. But this is necessary for correctness with real-world applications (virtually all of which use parameterized queries).

### 12.6 Mutating CTEs: Execute on Shadow, v1.1 Split

**Decision:** For v1, classify entire statement as WRITE if any part mutates. Execute on Shadow. For v1.1, split mutating CTEs into separate write and read operations internally.

**Rationale:** Mutating CTEs (`WITH updated AS (UPDATE ... RETURNING *) SELECT ...`) are uncommon in application code. The simple approach (Shadow-only) works for the mutation but the read portion only sees Shadow data. Splitting is the correct solution but adds complexity.

**Tradeoff:** In v1, a mutating CTE that reads from clean tables will only see Shadow data for those tables (which may be empty). Developers hitting this can hydrate the needed tables or restructure the query. v1.1 fixes this properly.

### 12.7 Extensions: User-Provided Docker Image

**Decision:** Default to the engine's vanilla Docker image. User provides `--image` flag when Prod uses extensions not in the base image.

**Rationale:** The base PostgreSQL Docker image includes ~50 common extensions (uuid-ossp, hstore, pg_trgm, pgcrypto, citext, etc.) which cover most use cases. Third-party extensions like PostGIS, pgvector, or TimescaleDB require custom images. Rather than maintaining a fat image or auto-installing extensions (fragile), we let the user specify exactly what image to use. During init, Mori detects Prod's extensions and validates they're available on Shadow — failing fast with a clear message if not.

**Tradeoff:** Extra setup step for users with exotic extensions. They need to have (or build) a Docker image with the right extensions. In practice, most teams already have such images for their CI/CD pipelines.

---

## 13. Known Tradeoffs and Limitations

### Things That Will Be Imperfect in v1

| Limitation | Impact | Mitigation |
|-----------|--------|-----------|
| **Mutating CTEs only see Shadow data** | CTE reads from clean tables return empty | v1.1: split into write + read |
| **Stored procedures with side effects** | `SELECT my_function()` routes as READ to Prod, but function may write | v2: function introspection or config allowlist |
| **No FK validation on Shadow** | Referential integrity bugs won't surface | Acceptable — test against real DB for constraint validation |
| **Prod data drift** | Long sessions see evolving Prod data | Acceptable for dev tool — document clearly |
| **Tables without PKs** | Read-only, writes produce warning | Rare in practice — document |
| **Bulk operations on large tables** | Hydrating thousands of rows for a bulk UPDATE is slow | Acceptable for dev tool — Prod is never at risk |
| **Complex analytical queries across many tainted tables** | JOIN patching may be slow or produce subtle ordering differences | Primary use case is CRUD apps, not analytics |

### Things That Could Go Wrong

1. **PK collision due to insufficient offset.** If Prod's insertion rate exceeds the offset gap during a session, locally-created PKs could collide with new Prod PKs. The sequence offset formula (`prod_max * 10` or `+ 10M`) makes this unlikely, but extremely high-volume tables in long sessions could hit it. Mitigation: monitor and warn if Prod max approaches the offset.

2. **Large taint maps degrading read performance.** If a session accumulates thousands of taints, every merged read must filter thousands of PKs. Mitigation: use efficient data structures (hash sets). The performance hit is per-merged-read, not per-query (clean tables still go through Prod Direct).

3. **Schema drift between Prod and Shadow becoming complex.** Multiple rounds of ALTER TABLE could create deep divergence that the Schema Registry struggles to adapt correctly (e.g., a column renamed twice, or a type changed multiple ways). Mitigation: `mori reset --hard` re-syncs from Prod, clearing all drift.

4. **Wire protocol edge cases.** Database wire protocols have many features — COPY, LISTEN/NOTIFY, advisory locks, large objects, etc. Not all of these will work correctly through a proxy in v1. Mitigation: document unsupported protocol features, handle gracefully (pass through or error clearly).
