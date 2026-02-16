# Mori Testing Tracker

Testing status for all engines and auth providers.

---

## Engine Testing

### PostgreSQL — Done

- **Unit tests**: Pass (all packages)
- **E2E tests**: 202+ tests passing
- **Build tag**: `e2e`
- **Command**: `go test -tags e2e -v -count=1 -timeout 15m ./e2e/`
- **Docker image**: `postgres:16`
- **Status**: Complete. Reference implementation.

---

### CockroachDB — Pending: Write E2E suite

- **Unit tests**: Pass (shares PostgreSQL packages)
- **E2E tests**: Not written
- **Docker image**: `cockroachdb/cockroach:latest`
- **What's needed**: Create `e2e/cockroachdb/` directory. CockroachDB speaks pgwire, so many PostgreSQL E2E tests can be reused with minimal adaptation (different Docker image, insecure mode startup, CockroachDB-specific SQL dialect differences).
- **Notes**: Thin wrapper over pgEngine (`internal/engine/postgres/cockroach.go`). Low effort.

---

### MySQL — Pending: Write E2E suite

- **Unit tests**: Pass (classify, connstr, proxy, schema packages)
- **E2E tests**: Not written
- **Build tag**: `e2e_mysql`
- **Docker image**: `mysql:8.0`
- **What's needed**: Create `e2e/mysql/` directory with test harness, MySQL-dialect seed data, and CRUD tests. Follow the pattern from `e2e/redis/e2e_test.go` for harness structure (build binary, start Docker, seed, write mori.yaml, start proxy, test, teardown).
- **Notes**: Shadow container uses `MYSQL_ROOT_PASSWORD=mori`.

---

### MariaDB — Pending: Write E2E suite

- **Unit tests**: Pass (shares MySQL packages)
- **E2E tests**: Not written
- **Build tag**: `e2e_mariadb`
- **Docker image**: Auto-detected from `SELECT VERSION()` (e.g. `mariadb:10.11`); fallback `mariadb:11`
- **What's needed**: Clone MySQL E2E tests with MariaDB Docker image. Thin MySQL variant (`internal/engine/mysql/mariadb.go`).
- **Notes**: Depends on MySQL E2E suite being written first.

---

### MSSQL — Pending: Run E2E suite

- **Unit tests**: Pass (4 packages: classify, connstr, proxy, schema)
- **E2E tests**: Written at `e2e/mssql/` (5 test files)
- **Build tag**: `e2e_mssql`
- **Command**: `go test -tags e2e_mssql -v -count=1 -timeout 15m ./e2e/mssql/`
- **Docker image**: `mcr.microsoft.com/mssql/server:2022-latest` (~1.5GB, ARM emulated)
- **What's needed**: Run E2E suite. Previous P0 bugs (TDS proxy shadow login) may have been fixed.
- **Notes**: ARM hosts face emulation overhead. Shadow readiness timeout set to 300s.

---

### SQLite — Pending: Run E2E suite

- **Unit tests**: Pass (5 packages: classify, connstr, proxy, schema, shadow)
- **E2E tests**: Written at `e2e/sqlite/` (5 test files)
- **Build tag**: `e2e_sqlite`
- **Command**: `go test -tags e2e_sqlite -v -count=1 -timeout 10m ./e2e/sqlite/`
- **Docker image**: None (file-based)
- **What's needed**: Run E2E suite. Previous P0 bug (proxy not merging shadow reads) may have been fixed.
- **Notes**: No Docker needed. Fastest engine to test.

---

### Redis — Pending: Run E2E suite

- **Unit tests**: Pass
- **E2E tests**: Written at `e2e/redis/` (4 test files: lifecycle, basic CRUD, data types, write guard)
- **Build tag**: `e2e_redis`
- **Command**: `go test -tags e2e_redis -v -count=1 -timeout 10m ./e2e/redis/`
- **Docker image**: `redis:7` (~30MB)
- **What's needed**: Run E2E suite.
- **Notes**: Smallest Docker image. Fastest to validate after SQLite.

---

### DuckDB — Pending: Write E2E suite

- **Unit tests**: Pass (3 packages: classify, connstr, proxy)
- **E2E tests**: Not written
- **Build tag**: `e2e_duckdb`
- **Docker image**: None (file-based, embedded)
- **What's needed**: Create `e2e/duckdb/` directory. DuckDB uses pgwire, so many SQLite/PostgreSQL E2E patterns apply. File-based shadow (copies `.duckdb` file).
- **Notes**: Embedded engine, no Docker needed. Uses `github.com/marcboeker/go-duckdb` (CGo-based driver). 47 unit tests passing (classify: 23, connstr: 12, proxy: 12).

---

### Firestore — Pending: Run E2E suite

- **Unit tests**: Pass
- **E2E tests**: Written at `e2e/firestore/` (3 test files: lifecycle, CRUD, isolation)
- **Build tag**: `e2e_firestore`
- **Command**: `go test -tags e2e_firestore -v -count=1 -timeout 10m ./e2e/firestore/`
- **Docker image**: `gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators` (~1.5GB)
- **What's needed**: Run E2E suite. Uses Firestore emulator (two containers: prod on 18080, shadow on 18081).
- **Notes**: No GCP account needed — emulator runs locally.

---

## Auth Provider Testing

All providers have passing unit tests (`internal/auth/providers/`). Integration testing against real cloud databases is pending.

| Provider | Compatible Engines | Unit Tests | Integration Test | What's Needed |
|----------|-------------------|-----------|-----------------|---------------|
| **Direct / Self-Hosted** | All | Pass | Covered by E2E | Covered by engine E2E tests (all use Direct) |
| **GCP Cloud SQL** | Postgres, MySQL, MSSQL | Pass | Pending | GCP project (free tier available) |
| **AWS RDS / Aurora** | Postgres, MySQL, MariaDB, MSSQL | Pass | Pending | AWS account (free tier for some engines) |
| **Neon** | Postgres | Pass | Pending | Neon project (free tier available) |
| **Supabase** | Postgres | Pass | Pending | Supabase project (free tier available) |
| **Azure Database** | Postgres, MySQL, MariaDB, MSSQL | Pass | Pending | Azure account |
| **PlanetScale** | MySQL | Pass | Pending | PlanetScale organization |
| **Vercel Postgres** | Postgres | Pass | Pending | Vercel project |
| **MongoDB Atlas** | MongoDB | Pass | Pending | MongoDB Atlas account (free tier), MongoDB engine not yet implemented |
| **DigitalOcean** | Postgres, MySQL, Redis, MongoDB | Pass | Pending | DigitalOcean account |
| **Railway** | Postgres, MySQL, Redis, MongoDB | Pass | Pending | Railway project |
| **Upstash** | Redis | Pass | Pending | Upstash account (free tier available) |
| **Cloudflare D1 / KV** | SQLite, Redis | Pass | Pending | Cloudflare account |
| **Firebase** | Firestore | Pass | Pending | Covered by Firestore emulator E2E |

### Notes

- **Direct** provider is implicitly tested by all engine E2E tests (they all connect directly).
- **Firebase** provider is implicitly tested by Firestore emulator E2E tests.
- All other providers require cloud accounts for integration testing. The connection-string generation and field validation logic is covered by unit tests. Integration testing verifies that Mori can actually connect to and proxy real cloud-hosted databases.
- Free tier is available for: Neon, Supabase, Upstash, GCP, AWS (limited), MongoDB Atlas.
