---
name: mori
description: >
  Database virtualization proxy with copy-on-write semantics. Use when setting up,
  running, debugging, or integrating Mori — a transparent proxy that routes reads
  to production and captures writes in a local Shadow database.
---

# Mori

Mori is a transparent proxy that sits between your application and a production database. Your application connects to Mori using a standard database driver — same wire protocol, no code changes beyond swapping a connection string. Reads are served from the real production database. Writes are captured in a local database instance (the **Shadow**) that starts as a schema-only clone of production. The result: developers test against real production data, mutate freely, and never risk the production database. Break something, reset, start over.

## Core Invariants

These hold at all times, without exception.

- **Production is read-only.** Mori never issues a write to the upstream database. There is no code path that writes to Prod.
- **Shadow is the mutation layer.** All writes go to Shadow — a local instance (same engine as Prod) holding all local mutations. Starts schema-only with zero rows.
- **The application sees one unified database.** SELECTs transparently merge results from Prod and Shadow. Locally-modified rows come from Shadow, untouched rows from Prod.
- **Reset is instant.** Wiping Shadow and its metadata restores a clean view of production. No migration, no rollback.

**Prerequisites:** Go 1.21+, Docker (for Shadow containers — all engines except SQLite and DuckDB).

## Key Concepts

| Term | Definition |
|------|-----------|
| **Prod** | Upstream production database. Read-only from Mori's perspective. Source of truth for unmodified data. |
| **Shadow** | Local database instance (same engine/version as Prod). Holds all local mutations. Starts schema-only with zero rows. |
| **Proxy** | The Mori process. Intercepts wire protocol traffic, classifies queries, routes them, merges results. |
| **Delta Map** | Set of `(table, primary_key)` pairs identifying locally modified rows. When a row is delta, Shadow holds the authoritative version. |
| **Tombstone Set** | Set of `(table, primary_key)` pairs identifying locally deleted rows. Tombstoned rows are filtered from Prod results. |
| **Schema Registry** | Tracks structural differences between Shadow and Prod schemas introduced by local DDL (added/dropped/renamed columns, type changes). |
| **Hydration** | Copying a row from Prod into Shadow so it can be mutated locally. Happens transparently on UPDATE/DELETE of Prod-only rows. |
| **Merged Read** | A read where results from both Prod and Shadow are combined, with delta/tombstoned rows filtered from Prod results. |

## Supported Engines

| Engine | Protocol | Shadow |
|--------|----------|--------|
| PostgreSQL | pgwire | Docker container |
| CockroachDB | pgwire | Docker container |
| MySQL | MySQL wire | Docker container |
| MariaDB | MySQL wire | Docker container |
| MS SQL Server | TDS | Docker container |
| SQLite | pgwire (embedded) | Local file (no Docker) |
| DuckDB | pgwire (embedded) | Local file (no Docker) |
| Redis | RESP | Docker container |
| Firestore | gRPC | Emulator container |

## Setup & Lifecycle

### Build

```bash
go build -o mori ./cmd/mori
```

### Initialize a Connection

Interactive mode walks through engine, provider, and credentials:

```bash
./mori init
```

Non-interactive with a connection string:

```bash
./mori init --from "postgres://user:pass@host:5432/mydb?sslmode=require" --name my-db
```

This saves the connection to `mori.yaml`. No containers or connections are created yet.

### Start the Proxy

```bash
./mori start [connection-name] --port 9002 --verbose
```

On first start, Mori creates the Shadow database (schema-only clone of Prod), computes sequence offsets, and begins accepting connections. If only one connection exists, the name is optional.

### Point Your App at Mori

Swap your application's connection string to `localhost:<proxy-port>`. Same credentials, same database name. The app won't know the difference.

### Lifecycle

```
mori init         Save connection config to mori.yaml
     |
mori start        Create Shadow (first run), start proxy, accept connections
     |
  [app runs]      CRUD operations routed transparently
     |
mori stop         Persist state, shut down proxy
     |
mori reset        Wipe Shadow data + metadata, clean slate
```

## CLI Reference

### `mori init`

Add a database connection to `mori.yaml`.

| Flag | Default | Description |
|------|---------|-------------|
| `--from, -f` | — | Connection string (non-interactive mode) |
| `--name` | — | Connection name (used with `--from`) |
| `--image` | — | Custom Docker image for the Shadow container (persisted in `mori.yaml`) |

### `mori start [name]`

Start the proxy for a named connection. If only one connection exists, name is optional.

| Flag | Default | Description |
|------|---------|-------------|
| `--port, -p` | 9002 | Port for the proxy to listen on |
| `--verbose` | false | Log all intercepted queries and routing decisions |
| `--mcp` | false | Enable MCP server for AI tool integration |
| `--mcp-port` | 9000 | Port for the MCP server |

### `mori stop`

Gracefully shut down the proxy. State is persisted automatically.

### `mori reset`

Wipe all local mutations and restore a clean view of production.

| Flag | Default | Description |
|------|---------|-------------|
| `--hard` | false | Also re-sync schema from production (use when Prod schema changed since init) |

Soft reset: truncates Shadow tables, clears Delta Map, Tombstone Set, Schema Registry, resets sequence offsets.
Hard reset: drops and re-creates Shadow tables from current Prod schema.

### `mori status`

Display current state: engine info, connection details, delta row counts, tombstone counts, schema diffs, sequence offsets.

### `mori inspect <table>`

Deep dive on a single table's state — delta rows, tombstoned rows, schema differences.

### `mori log`

Stream proxy activity (queries, routing decisions, timing).

| Flag | Default | Description |
|------|---------|-------------|
| `--tail` | — | Show last N log entries |

### `mori ls`

List all configured connections from `mori.yaml`.

### `mori rm <name>`

Remove a connection from `mori.yaml`.

### `mori config`

View or edit the current configuration.

### `mori dash`

Launch the interactive TUI dashboard.

## How Routing Works

Every incoming query follows this path:

```
App -> Proxy (wire protocol) -> Classifier -> Router -> Execute (strategy) -> Merge -> Return
```

The **Classifier** parses each query to determine its operation type, affected tables, and extractable primary keys. The **Router** uses this classification plus current delta/tombstone state to pick a routing strategy.

| Strategy | When Used | What Happens |
|----------|-----------|--------------|
| **ProdDirect** | SELECT on tables with no deltas/tombstones | Forward to Prod, return result directly. Zero overhead. |
| **MergedRead** | SELECT on tables with deltas or tombstones | Query both Prod and Shadow, filter delta/tombstoned rows from Prod, merge results. |
| **JoinPatch** | JOIN involving tables with deltas | Execute JOIN on Prod, patch delta columns from Shadow. |
| **ShadowWrite** | INSERT | Execute on Shadow only. Uses offset-based sequences to avoid PK collisions with Prod. |
| **HydrateAndWrite** | UPDATE on a Prod-only row | Copy the row from Prod to Shadow (hydration), then apply the UPDATE on Shadow. |
| **ShadowDelete** | DELETE | Delete from Shadow (if present), add to Tombstone Set so the row is filtered from Prod reads. |
| **ShadowDDL** | ALTER, CREATE, DROP | Execute DDL on Shadow only. Update Schema Registry to track divergence. |
| **Transaction** | BEGIN, COMMIT, ROLLBACK | Open read-only Prod transaction + read-write Shadow transaction. Commit promotes staged deltas. Rollback discards them. |

Understanding routing helps explain query behavior. For example, a SELECT on a table you've never modified goes straight to Prod (ProdDirect). Once you UPDATE a row in that table, subsequent SELECTs use MergedRead to combine Prod and Shadow results.

## Configuration & State

Mori stores all state in a `.mori/` directory at the project root.

```
.mori/
  mori.yaml                     # Connection configs (engine, host, port, user, password, database, ssl_mode)
  config.json                   # Runtime config (prod connection, shadow port, active connection)
  shadow/                       # Shadow container metadata (Docker IDs, port mappings)
  state/
    delta.json                  # Delta Map: { "table_name": ["pk1", "pk2", ...] }
    tombstones.json             # Tombstone Set: { "table_name": ["pk1", "pk2", ...] }
    schema_registry.json        # Schema divergence tracking (added/dropped/renamed columns per table)
    tables.json                 # Table metadata (PK columns, PK types per table)
    sequences.json              # Sequence offsets per table
  log/                          # Query logs, routing decisions
```

### Reading State for Debugging

- **Delta Map** (`state/delta.json`): shows which tables have locally modified rows and which PKs. If a table appears here, reads on it use MergedRead.
- **Tombstone Set** (`state/tombstones.json`): shows locally deleted rows. These PKs are filtered from Prod results.
- **Schema Registry** (`state/schema_registry.json`): shows DDL differences. Non-empty means Shadow schema diverges from Prod.
- **Tables** (`state/tables.json`): PK columns and types per table. Used by the classifier and merge engine.
- **Sequences** (`state/sequences.json`): offset values for auto-increment columns. Prevents INSERT PK collisions.

## MCP Server Integration

Mori includes an MCP (Model Context Protocol) server for AI agent integration. Start it alongside the proxy:

```bash
./mori start my-db --mcp --mcp-port 9000
```

The MCP server registers engine-specific tools based on your database type:

**SQL engines** (PostgreSQL, CockroachDB, MySQL, MariaDB, MSSQL, SQLite, DuckDB):
- **`db_query`** — Execute a SQL query. Parameter: `query` (string, required).

**Redis**:
- **`redis_command`** — Execute any Redis command (free-form).
- **`redis_get`** — Get the value of a key.
- **`redis_hgetall`** — Get all fields and values of a hash.
- **`redis_keys`** — Find keys matching a pattern.

**Firestore**:
- **`firestore_get`** — Retrieve a document by collection and document ID.
- **`firestore_list`** — List documents in a collection (default 25, max 100).
- **`firestore_query`** — Query with field filters (==, !=, <, <=, >, >=, in, array-contains).

- **Endpoint:** `http://127.0.0.1:9000/mcp`
- **Behavior:** All tools go through the proxy's full routing/merge logic, same as application connections. Reads from Prod, writes to Shadow.

### MCP Client Configuration

To connect an MCP client (e.g., Claude Code), add to your MCP settings:

```json
{
  "mcpServers": {
    "mori": {
      "url": "http://127.0.0.1:9000/mcp"
    }
  }
}
```

## Debugging & Troubleshooting

### Proxy won't start

1. **Docker not running:** Shadow containers require Docker. Check with `docker ps`.
2. **Port conflict:** Default proxy port is 9002. Use `--port` to pick another.
3. **No `.mori/` directory:** Run `mori init` first to configure a connection.
4. **Another connection active:** Run `mori stop` before starting a different connection.

### Unexpected query results

1. **Run `mori status`** to see which tables have deltas/tombstones. Tables with deltas use MergedRead instead of ProdDirect.
2. **Run `mori inspect <table>`** to see exactly which rows are modified/deleted locally.
3. **Use `--verbose`** when starting the proxy to log every query's classification and routing decision.
4. **Check `mori log`** for recent query activity and any errors.

### Missing rows after INSERT

- Check sequence offsets in `state/sequences.json`. Shadow sequences start at `max(prod_max * 10, prod_max + 10_000_000)` to avoid PK collisions. If Prod has grown significantly since init, offsets may need recalculating via `mori reset --hard`.

### Stale data after reset

- Soft reset keeps the Shadow schema from init time. If Prod schema has changed, use `mori reset --hard` to re-sync.

### Shadow container issues

```bash
docker ps -a | grep mori-shadow    # List Mori Shadow containers
docker logs <container-id>          # Check container logs
docker restart <container-id>       # Restart a stuck container
```

### When to use `--hard` reset

Use `mori reset --hard` when:
- Production schema changed since `mori init` (new tables, altered columns)
- Sequence offsets are exhausted or colliding
- Shadow is in an inconsistent state

Use soft `mori reset` (no flag) for routine testing cycles — it's faster and preserves the schema.

## Known Limitations

- **Multi-column ORDER BY:** Only single-column re-sorting after merge is supported.
- **Parameterized LIMIT:** `LIMIT $1` is not rewritten for over-fetching during merged reads.
- **Aggregate merging:** COUNT/SUM on delta tables are merged as rows, not re-aggregated. Results may differ from a direct Prod query.
- **JOIN PK patching:** Requires the primary key column to be in the SELECT list for JoinPatch to work.
- **Tables without PKs:** Treated as read-only (rare in practice).
- **Bulk operations:** Hydrating thousands of rows (e.g., `UPDATE ... WHERE id IN (...)` with many IDs) is slower than single-row operations.

## Further Reading

- **Architecture deep dive:** `docs/mori.md` — covers routing logic, merge engine internals, design decisions, and the 4-layer write guard in detail.
- **Testing status:** `docs/testing.md` — tracks E2E test coverage per engine.
- **Full documentation site:** `docs/mintlify/` — MDX-based docs covering concepts, engine guides, CLI reference, and advanced topics.
