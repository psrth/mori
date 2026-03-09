# Mori — Setup Guide for AI Agents

Mori is a transparent database proxy that lets you safely read and write against a production database. Reads hit the real database; writes are captured in a local shadow copy. Production is never modified.

## Prerequisites

- Go 1.21+
- Docker (for shadow containers; not needed for SQLite/DuckDB)

## Build

```bash
git clone https://github.com/psrth/mori.git
cd mori
go build -o mori ./cmd/mori
```

## Initialize

Point Mori at the production database:

```bash
# PostgreSQL
./mori init --from "postgres://user:pass@host:5432/mydb?sslmode=require"

# MySQL
./mori init --from "mysql://user:pass@host:3306/mydb"

# Redis
./mori init --from "redis://:password@host:6379/0"

# SQLite
./mori init --from "/path/to/database.db"

# DuckDB
./mori init --from "/path/to/analytics.duckdb"

# Firestore
./mori init --from "firestore://project-id?credentials=./sa.json"

# MSSQL
./mori init --from "sqlserver://sa:password@host:1433?database=mydb"

# CockroachDB (uses postgres:// scheme)
./mori init --from "postgres://user:pass@host:26257/mydb?sslmode=require"
```

This connects to production, dumps the schema, spins up a local shadow database, and offsets sequences to prevent PK collisions.

## Start the Proxy

```bash
./mori start
```

The proxy listens on a local port (auto-assigned from 9002, or use `--port <N>`).

### With MCP Server (recommended for AI agents)

```bash
./mori start --mcp --mcp-port 9000
```

This starts an MCP (Model Context Protocol) server at `http://127.0.0.1:9000/mcp`. Connect to it using the MCP protocol to get database tools:

- **SQL engines**: `db_query` tool — execute any SQL query
- **Redis**: `redis_command`, `redis_get`, `redis_hgetall`, `redis_keys` tools
- **Firestore**: `firestore_get`, `firestore_list`, `firestore_query` tools

All queries go through the proxy. Reads return real production data. Writes are isolated in the shadow.

## Connect Your Application

Swap the connection string to point at `127.0.0.1` on the proxy port:

```bash
# Before
DATABASE_URL=postgres://user:pass@prod-host:5432/mydb

# After
DATABASE_URL=postgres://user:pass@127.0.0.1:5432/mydb
```

Same driver, same protocol. The application won't know the difference.

## Key Commands

```bash
./mori start              # Start proxy
./mori stop               # Stop proxy, persist state
./mori reset              # Wipe all local state, start fresh
./mori status             # Show delta/tombstone counts
./mori inspect            # Show detailed state per table
./mori dash               # Open TUI dashboard
./mori log                # Tail proxy logs
```

## How It Works

- **Reads**: Unmodified tables go straight to production. Tables with local changes merge results from production + shadow.
- **Writes**: All INSERTs, UPDATEs, DELETEs execute on shadow only. Production is never written to.
- **DDL**: Schema changes (CREATE TABLE, ALTER TABLE, etc.) execute on shadow only. A schema registry tracks divergence.
- **Transactions**: BEGIN/COMMIT/ROLLBACK coordinate across both connections. Staged deltas promote on commit, discard on rollback.
- **Reset**: `./mori reset` wipes the shadow and all metadata. Instant clean slate.

## Safety

Production is read-only. A multi-layer write guard prevents writes from reaching production even if bugs exist in any single layer. There is no code path that writes to prod.
