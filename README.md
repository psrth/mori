# mori

A transparent database proxy with copy-on-write semantics. Reads from production, captures writes locally, resets instantly.

![Mori](./assets/hero.png)

## What is mori?

Mori sits between your application and a production database. It intercepts every query, classifies it, and routes it: reads go to production, writes go to a local Shadow database (a schema-only clone). Your application sees one unified database. Production is write-protected at all times.

Break something, reset, start over. Production never knows.

```
                         ┌──────────────────────────────────────────┐
                         │                  mori                    │
                         │                                          │
                         │   ┌────────────┐    ┌─────────────────┐  │
               SQL       │   │            │    │     Router      │  │
 Application ──────────> │   │ Classifier │───>│                 │  │
                         │   │            │    │  PROD_DIRECT    │  │
                         │   └────────────┘    │  MERGED_READ    │  │
                         │                     │  JOIN_PATCH     │  │
                         │                     │  SHADOW_WRITE   │  │
                         │                     │  HYDRATE+WRITE  │  │
                         │                     │  SHADOW_DDL     │  │
                         │                     └────────┬────────┘  │
                         │                 ┌────────────┼────────┐  │
                         │                 │            │        │  │
                         │          ┌──────▼──────┐ ┌───▼──────┐ │  │
                         │          │ Production  │ │  Shadow  │ │  │
                         │          │ (read-only) │ │ (writes) │ │  │
                         │          └──────┬──────┘ └───┬──────┘ │  │
                         │                 │            │        │  │
                         │                 └─────┬──────┘        │  │
                         │               ┌───────▼────────┐      │  │
                         │               │  Merge Engine  │      │  │
              Result     │               └───────┬────────┘      │  │
 Application <───────────│───────────────────────┘               │  │
                         │                                       │  │
                         └───────────────────────────────────────┘
```

## Supported Engines

| Engine        | Protocol   | Shadow Backend     |
| ------------- | ---------- | ------------------ |
| PostgreSQL    | pgwire     | Docker (postgres)  |
| CockroachDB   | pgwire     | Docker (cockroach) |
| MySQL         | MySQL wire | Docker (mysql)     |
| MariaDB       | MySQL wire | Docker (mariadb)   |
| MS SQL Server | TDS        | Docker (mssql)     |
| SQLite        | pgwire     | Local file         |
| DuckDB        | pgwire     | Local file         |
| Redis         | RESP       | Docker (redis)     |
| Firestore     | gRPC       | Firestore emulator |

## Auth Providers

Direct, GCP Cloud SQL, AWS RDS, Neon, Supabase, Azure, PlanetScale, Vercel Postgres, MongoDB Atlas, DigitalOcean, Railway, Upstash, Cloudflare, Firebase.

Mori resolves credentials at connection time. IAM-based providers (GCP, AWS, Azure) handle token refresh automatically.

## Quick Start

```bash
# Build
go build -o mori ./cmd/mori

# Initialize a connection (interactive)
mori init

# Or non-interactive
mori init --engine postgres --conn "postgres://user:pass@host:5432/mydb"

# Start the proxy
mori start

# Point your app at the proxy (default: localhost:5433)
DATABASE_URL=postgres://localhost:5433/mydb ./your-app

# See what changed
mori inspect

# Reset to clean state
mori reset
```

## CLI Reference

| Command        | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| `mori init`    | Initialize a new connection. Clones schema to Shadow.        |
| `mori start`   | Start the proxy. Listens for connections and routes queries. |
| `mori stop`    | Stop the proxy and background processes.                     |
| `mori reset`   | Wipe Shadow state. Deltas, tombstones, schema diffs -- gone. |
| `mori status`  | Show proxy status, connection info, and delta summary.       |
| `mori inspect` | Show current deltas, tombstones, and schema modifications.   |
| `mori ls`      | List all configured connections.                             |
| `mori rm`      | Remove a connection and its Shadow database.                 |
| `mori dash`    | Open the real-time TUI dashboard.                            |
| `mori log`     | Tail proxy logs.                                             |
| `mori config`  | View or edit connection configuration.                       |

## How It Works

![How It Works](./assets/how-it-works.png)

**Classification.** Every query is parsed into a `Classification`: operation type (READ, WRITE, DDL, TRANSACTION), referenced tables, extractable primary keys, and structural properties (JOINs, aggregates, LIMIT/ORDER BY). Each engine has its own classifier -- PostgreSQL uses `pg_query_go` (actual Postgres parser internals), MySQL uses Vitess, MSSQL parses TDS RPC commands, Redis classifies by command arity.

**Routing.** The router checks the classification against current delta state (which tables have local modifications) and picks a strategy:

- **PROD_DIRECT** -- No local changes to referenced tables. Pass through to production.
- **MERGED_READ** -- Tables have deltas. Query both backends, filter tombstoned/delta rows from production, adapt schema differences, merge results.
- **JOIN_PATCH** -- Multi-table read. Execute JOIN on production, identify delta rows by PK, patch from Shadow, deduplicate.
- **SHADOW_WRITE** -- INSERTs go directly to Shadow.
- **HYDRATE_AND_WRITE** -- UPDATEs copy the production row to Shadow first, then mutate.
- **SHADOW_DDL** -- DDL executes on Shadow only. Schema Registry tracks the divergence.

**State.** Four structures track local modifications:

- **Delta Map** -- `(table, pk)` pairs that have been modified locally.
- **Tombstone Set** -- `(table, pk)` pairs that have been deleted locally.
- **Schema Registry** -- Column additions, drops, renames, type changes applied to Shadow but not production.
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

## Network Tunneling

Mori supports SSH tunnels, GCP Cloud SQL Proxy, and AWS SSM for reaching databases behind private networks. Tunnels are configured per-connection and managed automatically.

## Contributing

Contributions welcome. Mori uses standard Go tooling:

```bash
go test ./...
go build ./cmd/mori
```

File issues and pull requests on GitHub.

## License

MIT
