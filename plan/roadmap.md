# Mori Roadmap

---

## v1 — PostgreSQL Core

The first release. A fully functional copy-on-write proxy for PostgreSQL. A developer can point their app at Mori, mutate freely against production data, and reset to a clean state.

### CLI

| Command | Description |
|---------|------------|
| `mori init --from <conn_string> [--image <docker_image>]` | Connect to Prod, dump schema, spin up Shadow container (custom image if extensions needed), offset sequences, create `.mori/` |
| `mori start [--port <port>] [--verbose]` | Start Proxy + Shadow, listen for app connections. `--verbose` logs all queries and routing |
| `mori stop [--keep-shadow]` | Graceful shutdown, persist state. `--keep-shadow` leaves Shadow container running |
| `mori reset [--hard]` | Wipe all local state. `--hard` re-syncs schema from Prod |
| `mori status` | Display delta rows, tombstones, schema diffs, sequence offsets |
| `mori log [--tail <n>]` | Stream or dump proxy activity — queries, routing decisions, hydrations |
| `mori inspect <table>` | Detailed state for a table: deltas, tombstones, schema diffs, sequence offset |

### Proxy

- **Wire protocol**: Full PostgreSQL v3 wire protocol support (simple query + extended query / prepared statements)
- **Authentication**: Pass-through to Prod credentials, or local auth for the Mori → app connection
- **Classification**: Parse all incoming SQL to determine operation type, affected tables, extractable PKs
- **Routing**: Route queries to Prod, Shadow, or both based on operation type and delta state

### Read Path

- **Prod Direct**: Non-delta single-table SELECTs and JOINs pass through to Prod
- **Merged Read (single table)**: Query both Prod and Shadow, filter delta/tombstoned rows from Prod, adapt for schema diffs, merge, re-apply ORDER BY/LIMIT
- **Merged Read (JOIN)**: Execute on Prod, patch delta rows from Shadow, execute on Shadow to catch local inserts, merge + dedup
- **Over-fetching**: For LIMIT queries, over-fetch from Prod to account for filtered rows (cap at 3 iterations)

### Write Path

- **INSERT**: Shadow only. Offset sequences prevent PK collision with Prod
- **UPDATE**: Hydrate from Prod if row not in Shadow, apply update on Shadow, add to Delta Map
- **DELETE**: Delete from Shadow if present, add to Tombstone Set
- **Bulk operations**: Identify affected rows via SELECT on both DBs, hydrate all, apply mutation on Shadow

### DDL

- **Shadow-only execution**: All DDL runs on Shadow only
- **Schema Registry**: Tracks added/dropped/renamed columns, type changes, new/dropped tables
- **Schema adaptation**: Prod rows adapted on read and hydration to match Shadow schema

### Transactions

- **Coordinated transactions**: BEGIN opens on both Prod (REPEATABLE READ) and Shadow
- **Staged deltas**: Delta/tombstone additions staged within transaction, promoted on COMMIT, discarded on ROLLBACK
- **Autocommit support**: Implicit single-statement transactions work without explicit BEGIN/COMMIT

### State Management

- **Delta Map**: `(table, pk)` pairs for locally-modified rows, persistent across sessions
- **Tombstone Set**: `(table, pk)` pairs for locally-deleted rows, persistent across sessions
- **Schema Registry**: Per-table schema diffs, persistent across sessions
- **Config**: Connection metadata, sequence offsets, engine version
- **Persistence format**: JSON files in `.mori/` directory (human-readable for debugging)

### Primary Key Support

- **Serial / bigserial**: Sequence offset + range-based fast-path routing
- **UUID**: Local generation, no collision by nature, delta map lookup for routing
- **Composite PKs**: Serialized as tuple for delta map
- **No PK**: Table treated as read-only with warning

### Decisions Baked Into v1

- FK constraints stripped from Shadow (avoids transitive hydration)
- Prod data drift accepted (live reads, no snapshot pinning)
- 1:1:1 connection model (one Prod + one Shadow per app connection)
- Sequence offset: `max(prod_max * 10, prod_max + 10_000_000)`
- Mutating CTEs execute entirely on Shadow (read portion only sees Shadow data)
- Stored procedures/functions: out of scope (routed as plain SELECTs)
- Extensions: vanilla Docker image by default, `--image` flag for Prod with third-party extensions (PostGIS, pgvector, etc.)

---

## v1.1 — Fast Follows

Targeted improvements to known v1 limitations. No new major features — just correctness and quality-of-life fixes that didn't make the v1 cut.

### Mutating CTE Splitting

**Problem:** In v1, `WITH updated AS (UPDATE ... RETURNING *) SELECT ... FROM updated JOIN clean_table` executes entirely on Shadow. The `clean_table` portion returns empty because Shadow has no clean-table data.

**Fix:** The Proxy splits the CTE internally:
1. Extract the mutating CTE(s) and execute on Shadow (the write path)
2. Capture the RETURNING result
3. Rewrite the outer SELECT to reference the captured result instead of the CTE
4. Execute the rewritten SELECT through the normal read path (which handles merged reads / JOIN patching)

This gives correct results for mutating CTEs that reference clean tables.

### Hydrate Command

```
mori hydrate <table> [--where <condition>]
```

Explicitly pull rows from Prod into Shadow. Useful when a developer knows they'll be working with a specific table and wants JOIN correctness without waiting for on-demand hydration.

- `mori hydrate users` — pull all users into Shadow
- `mori hydrate users --where "active = true"` — pull a subset
- Adds hydrated rows to the Delta Map so they're served from Shadow going forward

### Improved Logging and Diagnostics

- **Query timing**: Log how long each query took, broken down by Prod/Shadow/merge time
- **Delta map diff**: `mori diff` shows what's changed since last reset
- **Warning aggregation**: Collect and summarize warnings (e.g., "17 JOIN queries hit delta tables in this session") rather than warning on each query

### Better Error Messages

- Clear error when app tries to write to a PK-less table
- Guidance when a query fails due to schema divergence
- Helpful output when Shadow container fails to start

### Config Validation

- `mori doctor` command: validate `.mori/` config, test Prod connectivity, verify Shadow health, check sequence offsets against current Prod max

---

## v2 — Platform

Major feature additions that expand Mori from a single-developer CLI tool into a broader platform.

### Snapshots

Save and restore complete Shadow state (data + delta map + tombstone set + schema registry).

```
mori snapshot save <name>        # persist current state as named snapshot
mori snapshot restore <name>     # restore Shadow and all metadata from snapshot
mori snapshot list               # list snapshots with timestamps and dirty-table summaries
mori snapshot delete <name>      # remove a snapshot
```

**Implementation:** Snapshot = `pg_dump` of Shadow data + JSON state files, stored in `.mori/snapshots/<name>/`. Restore = truncate Shadow, `pg_restore`, load state files.

**Use cases:**
- Save a known-good test state, restore after experiments
- Pre-configure specific edge cases for testing ("here's a state where the checkout flow breaks")

### Team Sharing

Push/pull snapshots to a shared store (S3, GCS, or a team server).

```
mori snapshot push <name> [--remote <url>]
mori snapshot pull <name> [--remote <url>]
```

Teams can share pre-configured data states: "Pull the `broken-checkout` snapshot to reproduce the bug I found."

### Stored Procedure Support

Two approaches, configurable per-function:

1. **Config allowlist**: Developer marks functions as mutating in `.mori/config`. These route to Shadow.
2. **Function introspection**: Parse function bodies (from `pg_proc`) to detect side effects. Automatically route mutating functions to Shadow.

The allowlist ships first. Introspection is a later enhancement.

### Snapshot Pinning (Consistent Reads)

```
mori start --snapshot
```

Pin all Prod reads to a point-in-time snapshot. Uses engine-specific mechanisms (e.g., `pg_export_snapshot()` in PostgreSQL). All reads within the session see a frozen view of Prod.

**Tradeoff:** Holds resources on Prod (prevents vacuum from cleaning up old row versions). Documented with clear warnings. Recommended only for short integration test runs, not long dev sessions.

### AI Agent Integration

Expose a Mori control API (HTTP or Unix socket) so AI coding agents can programmatically manage Mori instances:

```
POST /api/init     { "connection_string": "..." }
POST /api/start    { "port": 5432 }
POST /api/reset
POST /api/stop
GET  /api/status
POST /api/snapshot/save   { "name": "..." }
POST /api/snapshot/restore { "name": "..." }
```

Use case: an AI agent runs code, tests against Mori, resets between attempts, saves snapshots of interesting states. The agent can treat the database as a fully resettable sandbox.

### Partial Schema Sync

```
mori init --from <conn_string> --tables users,orders,products
```

Only shadow a subset of tables. Non-shadowed tables are always pass-through to Prod (reads go to Prod, writes go to Prod). Useful for large databases where the developer only needs to work with a few tables.

**Implication:** Writes to non-shadowed tables would hit Prod, which violates the "Prod is read-only" invariant. Two options:
- Block writes to non-shadowed tables (safe, restrictive)
- Allow writes to non-shadowed tables with an explicit warning (flexible, slightly dangerous)

Decision deferred to v2 design phase.

### Read Replica Support

```
mori init --from <prod_conn_string> --read-replica <replica_conn_string>
```

Point Mori's read connections at a read replica instead of primary Prod. Reduces load on the primary and eliminates any risk of interfering with production workload.

### Multi-Engine Support

The architecture (Protocol Handler → Classifier → Router → Merge/Write Engine) is designed to be engine-pluggable. Each new engine requires:

1. **Protocol Handler**: Implement the engine's wire protocol (e.g., MySQL protocol, Redis RESP, MongoDB wire protocol)
2. **Classifier**: Parse the engine's query language (SQL for MySQL, commands for Redis, MQL for MongoDB)
3. **Shadow Management**: Engine-specific container setup, schema dump/restore, sequence handling
4. **Adapter**: Engine-specific schema adaptation, row serialization, PK extraction

The core routing logic (delta map, tombstone set, merge strategy) is engine-agnostic and shared.

**Planned engine order:**
1. **Redis** — RESP protocol, key-based delta tracking, simpler than SQL (no JOINs, no schema)
2. **MySQL** — Very similar to Postgres. MySQL wire protocol, SQL classifier reuse, different schema dump mechanism
3. **MongoDB** — BSON wire protocol, document-based delta tracking, no schema (schemaless complicates the Schema Registry — may not need one)

Each engine is a self-contained adapter package. The core Mori protocol (delta, tombstone, routing) applies to all.
