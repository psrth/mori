# mori -- Branding & Positioning

## (a) One-Liners

1. **Copy-on-write for production databases.**
2. **A transparent proxy that makes production databases safe to write to.**
3. **Read from production. Write to a shadow. Reset in milliseconds.**
4. **Database virtualization: your app sees one database, production stays untouched.**
5. **The layer between your code and production that catches every write before it lands.**

---

## (b) Motivation

### The problem

Every team that works with databases hits the same wall: you need production data to test properly, but you cannot write to production to test properly. The workarounds are all bad.

Staging environments drift. Seed data is fake. Database dumps are stale before the restore finishes. And the team that maintains the "production-like" test environment is always behind.

The result: developers test against fake data and hope for the best. Migrations run against empty schemas and break on real data. QA signs off on a shadow of what production actually looks like.

### What mori does about it

Mori is a transparent proxy that sits between your application and a production database. It intercepts every query at the wire protocol level, classifies it, and routes it. Reads go to production -- real data, real volumes, real schemas. Writes go to a local Shadow database -- a schema-only clone that captures every mutation. Your application sees one unified database. Production is write-protected at all times.

When you are done, `mori reset` wipes the Shadow. Clean slate. No teardown scripts, no snapshot restores, no waiting.

### Who needs this

**Testing against production data.** Point your test suite at Mori. Every test writes to Shadow, reads merge seamlessly with production. Tear down is instant. No fixtures, no factories, no "we need to refresh staging."

**QA with real data volumes.** QA environments with synthetic data miss the bugs that only show up at scale -- slow queries on large tables, edge cases in real user data, constraint violations from actual data distributions. Mori gives QA the real thing without the risk.

**Schema migration testing.** Run `ALTER TABLE` through Mori. It executes on Shadow while reads still come from production (with live row adaptation). If the migration breaks something, you see it immediately against real data. Reset and iterate.

**Staging environments that actually reflect production.** Instead of maintaining a separate staging database that is always weeks behind, point staging at Mori. Reads are current. Writes are isolated. The environment reflects production because it *is* production, with a write buffer.

### Why this matters now

The rise of AI coding agents changes the equation.

AI agents -- Cursor, Claude Code, Copilot, custom agents built on LLMs -- are increasingly capable of writing and executing code autonomously. The test-verify loop is the core of how they work: write code, run it, check the output, iterate. For any application that touches a database, that loop requires database access.

You cannot give an AI agent write access to production. But you cannot give it useful test results against empty or synthetic data either. The agent needs to read real data to understand the schema, the volumes, the edge cases. It needs to write to verify that its code works. And it needs to reset when it is done.

Mori solves this directly:

- **Read from production.** The agent sees real data. Schema introspection returns the actual schema. Queries return actual results.
- **Write to Shadow.** INSERTs, UPDATEs, DELETEs, ALTER TABLEs -- all captured locally. Production is untouched.
- **Reset instantly.** The agent finishes a task, wipes the Shadow, starts the next one clean.

The MCP (Model Context Protocol) server makes this native. AI agents connect to Mori over MCP, execute queries, inspect results, and reset state -- all through a standardized protocol. No custom tooling, no brittle integrations.

This is not a nice-to-have. As AI agents become the primary authors of code, the infrastructure to let them interact with production data safely becomes foundational. Mori is that infrastructure.

---

## (c) The Six Hardest Things We Built

### 1. Multi-Protocol Wire Protocol Implementation

Mori does not use database drivers. It implements wire protocols natively: PostgreSQL's pgwire, MySQL's wire protocol, Microsoft's TDS (Tabular Data Stream), Redis RESP, and Firestore's gRPC interface.

This is hard because wire protocols are not simple request-response. pgwire has a separate "extended query" sub-protocol with Parse/Bind/Describe/Execute message sequences and a per-connection prepared statement cache. MySQL wire has capability negotiation and COM_STMT_PREPARE with binary result encoding. TDS multiplexes RPC calls like `sp_executesql` and `sp_prepare` within token streams that require stateful binary parsing -- a single response can contain multiple result sets interleaved with metadata tokens, error tokens, and environment change tokens.

Each protocol also has its own authentication handshake. Mori handles SCRAM-SHA-256 for Postgres, native password and caching_sha2_password for MySQL, NTLM/SQL auth for MSSQL, and AUTH for Redis. The proxy must authenticate the client *and* maintain authenticated sessions to both production and Shadow backends, with different credentials for each.

Getting any one of these wrong -- a mishandled protocol state, a wrong byte offset in TDS token parsing, a broken prepared statement lifecycle -- means the client driver sees protocol errors and the proxy is useless. There is no partial credit for wire protocol implementation.

### 2. Transparent Result Merging with Over-Fetching

When a `SELECT` hits a table with local modifications, Mori cannot just forward to production. It has to query both backends, reconcile the results, and return a single result set that is indistinguishable from querying a database where those writes actually happened.

The merge process: query production, query Shadow, filter tombstoned and delta rows from the production result (by primary key), adapt schema differences between the two result sets (inject NULLs for columns added on Shadow, strip columns dropped on Shadow, rename columns that were renamed), concatenate, re-apply ORDER BY, re-apply LIMIT.

The hard part is LIMIT semantics. If the query has `LIMIT 10` and Mori filters 3 tombstoned rows from the production result, the result now has 7 rows plus whatever Shadow contributed. That might not be 10. So Mori over-fetches: it requests more rows from production, filters again, merges again. But over-fetching can cascade -- each round might filter more rows. Mori caps this at 3 iterations to bound latency while still producing correct results for the vast majority of cases.

For JOINs, the problem is harder. Mori executes the JOIN on production, scans the result for rows whose primary keys appear in the delta map, replaces those with patched versions from Shadow, then runs the JOIN on Shadow to catch inserts (rows that only exist locally), and deduplicates. All of this must preserve the original query's ordering and projection.

### 3. AST-Based Query Classification Across Five SQL Dialects

Every query Mori handles must be classified: what does it do, what tables does it touch, can we extract primary keys from the WHERE clause. This classification drives routing, and getting it wrong means either data corruption (routing a write to production) or incorrect results (routing a read to the wrong backend).

Mori does not use regex or keyword matching. Each engine has a real parser. PostgreSQL classification uses `pg_query_go`, which embeds the actual PostgreSQL parser (C code compiled to Go via cgo). MySQL uses Vitess's production SQL parser. MSSQL handles T-SQL, which is a different language with different syntax for CTEs, temp tables, and variable declarations.

Redis and Firestore are not SQL at all. Redis classification works by command name and arity -- `GET` is a read, `SET` is a write, `EVAL` requires key analysis. Firestore parses document paths and structured query filters.

The two-phase classification for prepared statements adds another layer. At `PREPARE` time, Mori parses the query template and extracts table references and structural properties. At `BIND` time, it resolves parameter values to extract primary keys. The classification is split across two protocol messages that may arrive in different TCP packets.

### 4. Schema Divergence Tracking and Live Row Adaptation

When you `ALTER TABLE ADD COLUMN` through Mori, the DDL runs on Shadow but not production. Now the two backends have different schemas for the same table. Every subsequent read must account for this.

The Schema Registry tracks every DDL divergence: added columns (with default values and types), dropped columns, renamed columns, type changes. When a merged read returns rows from production, Mori transforms them in-flight: injecting NULL (or the declared default) for added columns, stripping dropped columns, renaming columns that were renamed, casting values for type changes.

This also affects hydration. When an UPDATE requires copying a production row to Shadow, the row must be transformed to match Shadow's schema before the INSERT. And when Shadow rows are returned, they must be projected to match what the application expects given the full sequence of DDL changes.

The hard part is correctness across sequences of DDL operations. `ADD COLUMN x`, then `RENAME COLUMN x TO y`, then `ALTER COLUMN y TYPE bigint` -- the registry must compose these transformations correctly, in order, and apply them to every row that passes through the merge engine. A single missed transformation means the application sees wrong column names, missing data, or type errors.

### 5. Transaction Isolation with Staged Delta Semantics

Mori's copy-on-write model interacts with transactions in subtle ways. Consider: a transaction INSERTs a row, then a SELECT within the same transaction should see that row. But if the transaction ROLLBACKs, the row must disappear from the delta map. And concurrent connections should not see uncommitted changes from other connections.

Mori implements per-connection, per-transaction staging buffers. When a write occurs inside a transaction, the delta is recorded in a staging area, not the global delta map. Reads within the same transaction consult both the global map and the local staging buffer. On COMMIT, staged deltas are promoted to the global map atomically. On ROLLBACK, they are discarded.

The production backend gets a REPEATABLE READ transaction to ensure consistent snapshots during merged reads. Shadow gets a standard read-write transaction. The two must be coordinated: if the production transaction fails, Shadow must also roll back, and vice versa. Each engine implements this coordination adapted to its wire protocol -- pgwire sends `BEGIN`/`COMMIT` as simple queries, MySQL uses `START TRANSACTION`, TDS uses `BEGIN TRAN` with special attention to implicit transaction mode.

Without staged semantics, a failed transaction would leave phantom deltas -- the delta map would show a modification that does not exist in either backend. Every subsequent merged read on that table would try to filter or patch a row that is not there.

### 6. Redis Pub/Sub Fan-In and Lua Script Hydration

The Redis engine is architecturally different from the SQL engines. Redis is not a relational database. There are no tables, no schemas, no JOINs, no SQL. The copy-on-write model must be reimagined for key-value semantics.

**Pub/Sub fan-in.** When a client subscribes to a channel, Mori subscribes on both production and Shadow, then multiplexes messages from both into a single stream back to the client. Messages from production and Shadow must be deduplicated (Shadow might republish production messages) and ordered correctly. The subscription must be maintained across reconnections to either backend.

**SCAN merging.** Redis `SCAN` uses a cursor-based iterator. Mori maintains separate cursors for production and Shadow, merges the results (filtering keys that exist in Shadow's delta set from the production scan), and synthesizes a single cursor that the client can use to paginate through the combined keyspace. The cursor encoding must be stateless -- the client might not call SCAN again for minutes.

**Lua script hydration.** `EVAL` and `EVALSHA` execute Lua scripts server-side with access to specified keys. Mori parses the KEYS argument, identifies which keys have local modifications, hydrates those keys from production to Shadow before execution, then runs the script entirely on Shadow. This ensures the script sees a consistent view: production data for unmodified keys, local data for modified keys. The hydration must happen atomically and handle type mismatches (a key that is a string on production but was deleted and recreated as a hash on Shadow).

This is a fundamentally different proxying paradigm. SQL engines classify queries and route rows. The Redis engine classifies commands and routes keys.
