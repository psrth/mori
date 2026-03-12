# mori

A transparent database proxy with copy-on-write semantics. Reads from production, captures writes locally, resets instantly.

![Mori](./assets/hero.png)

## What is mori?

Mori sits between your application and a production database. It intercepts every query, classifies it, and routes it: writes go to a local Shadow database, while reads pull data from production and merge it with your local changes. Your application sees one unified database. Production is write-protected at all times.

Break something, reset, start over. Production never knows.

![mori-arch](./docs/mintlify/logo/mori-arch.png)

## Supported Engines

| Engine        | Protocol   | Shadow Backend     |
| ------------- | ---------- | ------------------ |
| PostgreSQL    | pgwire     | Docker (postgres)  |
| CockroachDB   | pgwire     | Docker (postgres)  |
| MySQL         | MySQL wire | Docker (mysql)     |
| MariaDB       | MySQL wire | Docker (mariadb)   |
| MS SQL Server | TDS        | Docker (mssql)     |
| SQLite        | pgwire     | Local file copy    |
| DuckDB        | pgwire     | Local file copy    |
| Redis         | RESP       | Docker (redis)     |
| Firestore     | gRPC       | Firestore emulator |

## Auth Providers

Direct, GCP Cloud SQL, AWS RDS, Neon, Supabase, Azure, PlanetScale, Vercel Postgres, MongoDB Atlas, DigitalOcean, Railway, Upstash, Cloudflare, Firebase.

Mori resolves credentials at connection time. IAM-based providers (GCP, AWS, Azure) handle token refresh automatically.

## Quick Start

```bash
# Install
curl -fsSL https://moridb.sh/install.sh | sh

# Initialize a connection (interactive)
mori init

# Or non-interactive
mori init --from "postgres://user:pass@host:5432/mydb?sslmode=require" --name my-db

# Start the proxy (first run sets up shadow)
mori start

# Point your app at the proxy
DATABASE_URL=postgres://user:pass@127.0.0.1:5432/mydb ./your-app

# See what changed
mori inspect

# Reset to clean state
mori reset
```

## For AI Agents

Download the [skill.md](https://moridb.sh/skill.md) and point your agent at it:

```bash
curl -fsSL https://moridb.sh/skill.md -o skill.md
```

## CLI Reference

| Command        | Description                                               |
| -------------- | --------------------------------------------------------- |
| `mori init`    | Add a database connection. Saves config to `mori.yaml`.   |
| `mori start`   | Start the proxy. On first run, sets up shadow.            |
| `mori stop`    | Stop the proxy. Persists state.                           |
| `mori reset`   | Reset all local state (deltas, tombstones, schema diffs). |
| `mori reinit`  | Drop all state, delete shadow container, re-initialize.   |
| `mori status`  | Display current Mori state.                               |
| `mori inspect` | Show detailed state for a table.                          |
| `mori ls`      | List all configured connections.                          |
| `mori rm`      | Remove a connection from mori.yaml.                       |
| `mori dash`    | Launch the interactive dashboard.                         |
| `mori log`     | Show proxy activity log.                                  |
| `mori config`  | View project configuration.                               |

## How It Works

![How It Works](./assets/how-it-works.png)

**Classification.** Every query is parsed into a `Classification`: operation type (READ, WRITE, DDL, TRANSACTION, OTHER), referenced tables, extractable primary keys, and structural properties (JOINs, aggregates, LIMIT/ORDER BY, window functions, CTEs, set operations). Each engine has its own classifier -- PostgreSQL uses `pg_query_go` (actual Postgres parser internals), MySQL uses Vitess, MSSQL parses TDS RPC commands, Redis classifies by command arity.

**Routing.** The router checks the classification against current delta state (which tables have local modifications, tombstones, or schema diffs) and picks a strategy:

- **PROD_DIRECT** -- No local changes to referenced tables. Pass through to production.
- **MERGED_READ** -- Tables have deltas. Query both backends, filter tombstoned/delta rows from production, adapt schema differences, merge results. Also handles aggregates, set operations, CTEs, and cursors on affected tables.
- **JOIN_PATCH** -- Multi-table read on affected tables. Execute JOIN on production, identify delta rows by PK, patch from Shadow, deduplicate.
- **SHADOW_WRITE** -- INSERTs go directly to Shadow.
- **HYDRATE_AND_WRITE** -- UPDATEs (and INSERT ... ON CONFLICT) copy the production row to Shadow first, then mutate.
- **SHADOW_DELETE** -- DELETEs execute on Shadow. Affected PKs are added to the Tombstone Set and filtered from future production reads.
- **TRUNCATE** -- Forwards to Shadow and marks the table as fully shadowed. All subsequent reads skip production entirely.
- **SHADOW_DDL** -- DDL executes on Shadow only. Schema Registry tracks the divergence.
- **TRANSACTION** -- Coordinates BEGIN/COMMIT/ROLLBACK across both backends. Production gets REPEATABLE READ; Shadow gets read-write. Deltas are staged per-transaction.
- **FORWARD_BOTH** -- SET commands forwarded to both backends. DEALLOCATE also uses this path.
- **LISTEN_ONLY** -- PostgreSQL LISTEN forwarded to production only.

**State.** Four structures track local modifications:

- **Delta Map** -- `(table, pk)` pairs that have been modified locally.
- **Tombstone Set** -- `(table, pk)` pairs that have been deleted locally.
- **Schema Registry** -- Column additions, drops, renames, type changes applied to Shadow but not production. Also tracks new tables and fully-shadowed tables.
- **Sequence Offsets** -- Shifted auto-increment ranges to prevent PK collisions.

**Merging.** For merged reads, Mori queries both backends, strips delta/tombstoned rows from the production result, injects NULLs for added columns (or strips dropped columns), and concatenates. ORDER BY and LIMIT are re-applied after merge. Over-fetching handles the case where filtering production rows pushes the result count below LIMIT -- capped at 3 iterations.

**Transactions.** Deltas are staged per-connection, per-transaction. Promoted on COMMIT, discarded on ROLLBACK. Production gets a REPEATABLE READ transaction for consistency; Shadow gets read-write.

## MCP Server

Mori includes a built-in MCP (Model Context Protocol) server for AI agent integration. Agents can read production data, write to Shadow, verify results, and reset -- without risk to production.

```bash
mori start --mcp
```

## TUI Dashboard

`mori dash` opens a real-time terminal dashboard (built with BubbleTea) showing:

- Active connections and query routing
- Delta and tombstone counts per table
- Schema divergence
- Query logs with classification and timing
- Latency metrics (P50, P95, P99) and QPS

## Network Tunneling

Mori supports SSH tunnels, GCP Cloud SQL Proxy, and AWS SSM for reaching databases behind private networks. Tunnels are configured per-connection and managed automatically.

## Contributing

Please read the [contributing guide](./CONTRIBUTING.md).

## License

MIT
