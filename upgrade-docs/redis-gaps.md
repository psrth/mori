# Redis Engine — Gap Analysis vs. PostgreSQL Engine

This document provides an exhaustive gap analysis of the Redis engine implementation compared to the PostgreSQL engine (the most mature implementation). Redis is fundamentally different from SQL databases, so many PostgreSQL features have no direct equivalent. This analysis focuses on conceptual parallels (read/write routing, delta tracking, tombstoning, hydration) and how well they are implemented for Redis's data model.

---

## Table of Contents

1. [Engine Overview](#1-engine-overview)
2. [Feature Parity Matrix](#2-feature-parity-matrix)
3. [Command Classification Gaps](#3-command-classification-gaps)
4. [Read Operation Gaps](#4-read-operation-gaps)
5. [Write Operation Gaps](#5-write-operation-gaps)
6. [Data Structure Gaps](#6-data-structure-gaps)
7. [Transaction Management Gaps](#7-transaction-management-gaps)
8. [Protocol-Level Gaps](#8-protocol-level-gaps)
9. [Pub/Sub Gaps](#9-pubsub-gaps)
10. [Guard Rail Gaps](#10-guard-rail-gaps)
11. [Initialization Gaps](#11-initialization-gaps)
12. [Priority Recommendations](#12-priority-recommendations)

---

## 1. Engine Overview

The Redis engine is in an **early-but-functional** state. It implements the core proxy architecture (client -> classify -> route -> dispatch -> merge -> client) and correctly plugs into the shared `engine.Engine` interface. The fundamental copy-on-write mechanics are present: a shadow Redis container receives writes, a delta map tracks modified keys, a tombstone set tracks deleted keys, and merged reads check delta/tombstone state to decide per-key routing.

**What works well:**
- Connection string parsing (`connstr/`) -- robust, handles `redis://`, `rediss://`, bare host:port, IPv6
- Command classification (`classify/`) -- comprehensive coverage of 150+ Redis commands across all data types
- RESP protocol handling (`proxy/resp.go`) -- full RESP2 read/write with all five types (simple string, error, integer, bulk string, array)
- Three-layer write guard (`proxy/guard.go`) -- L1 routing assertion, L2 SafeProdConn wrapper, L3 final dispatch check
- Shadow container lifecycle (`shadow/`) -- version-matched image, Docker management, readiness polling
- Initialization (`init.go`) -- full pipeline from connection parsing through key detection to config persistence
- Basic merged reads for single-key and MGET commands
- DUMP/RESTORE-based hydration for key-level copy-on-write
- Pub/Sub fan-in multiplexing from both backends
- EVAL/EVALSHA handling with key hydration
- SCAN merging with tombstone filtering

**What is missing or incomplete:**
- No merged reads for complex data structures (lists, sets, sorted sets, hashes with partial field modifications, streams)
- No TTL/expiration tracking or synchronization
- No transaction state management (MULTI/EXEC treated as simple forward-both)
- No staged delta system for transactional atomicity
- No WATCH/optimistic-locking awareness
- No RESP3 protocol support
- No SSL/TLS connection support to prod
- No blocking command handling (BLPOP, BRPOP, etc.)
- No cluster/sentinel awareness
- No KEYS/SCAN pattern-aware merged reads
- Minimal schema registry integration (no DDL concept for Redis)

---

## 2. Feature Parity Matrix

| PostgreSQL Concept | Redis Equivalent | Status | Notes |
|---|---|---|---|
| **Architecture** | | | |
| Dual-backend proxy (Prod + Shadow) | Dual-backend RESP proxy | Done | `proxy/proxy.go` lines 36-60 |
| Delta Map (table, pk) tracking | Delta Map (prefix, key) tracking | Done | Uses shared `core/delta` package |
| Tombstone Set (table, pk) tracking | Tombstone Set (prefix, key) tracking | Done | Uses shared `core/delta` package |
| Schema Registry (DDL tracking) | N/A for Redis | N/A | Redis has no schema; registry is injected but unused |
| **Classification** | | | |
| AST-based SQL parser (pg_query_go) | String-based command tokenizer | Done | `classify/classifier.go` -- adequate for Redis's simple command grammar |
| Statement type classification | Command type classification | Done | 150+ commands classified |
| PK extraction from WHERE clauses | Key extraction from command args | Done | Key position and multi-key awareness |
| Feature flags (IsJoin, HasAggregate, etc.) | N/A | N/A | Redis commands are atomic, not composable |
| **Read Operations** | | | |
| Single-table merged read | Single-key merged read (GET, HGETALL) | Done | `proxy/proxy.go` lines 450-501 |
| MGET merged read | Per-key MGET routing | Done | `proxy/proxy.go` lines 504-566 |
| Aggregate queries (COUNT, SUM, etc.) | N/A | N/A | Redis has no aggregate queries |
| JOIN handling | N/A | N/A | Redis has no joins |
| Complex queries (CTEs, derived tables) | N/A | N/A | Redis has no query language |
| Set operations (UNION/INTERSECT/EXCEPT) | SDIFF/SINTER/SUNION merged reads | Missing | Multi-key set operations not merged |
| Window functions | N/A | N/A | |
| LIMIT/ORDER BY re-application | N/A | N/A | |
| Schema adaptation (added/dropped cols) | N/A | N/A | |
| SCAN consistency | Merged SCAN from both backends | Done | `proxy/proxy.go` lines 907-1003 |
| **Write Operations** | | | |
| INSERT (shadow-only) | SET/HSET/etc. (shadow-only) | Done | All writes route to shadow |
| Upsert (INSERT ON CONFLICT) | SET with NX/XX flags | Partial | Hydration for SET exists, but no NX/XX-aware logic |
| UPDATE (hydrate-and-write) | Key modification (hydrate-and-write) | Done | DUMP/RESTORE hydration at `proxy/proxy.go` lines 569-618 |
| DELETE (shadow + tombstone) | DEL/UNLINK (shadow + tombstone) | Done | `trackWriteEffects` at line 621 |
| RETURNING clause | N/A | N/A | Redis commands return their own results |
| Bulk writes (MSET, etc.) | MSET key extraction | Partial | Keys extracted but only first key hydrated |
| Generated column filtering | N/A | N/A | |
| FK constraint enforcement | N/A | N/A | Redis has no foreign keys |
| Cross-table writes (UPDATE...FROM) | N/A | N/A | |
| Hydration system (DUMP/RESTORE) | DUMP/RESTORE copy | Done | Preserves TTL via PTTL |
| **DDL** | | | |
| CREATE/ALTER/DROP TABLE | FLUSHDB/FLUSHALL/SWAPDB | Partial | Classified as DDL, routed to shadow, but no post-DDL state tracking |
| Schema change tracking | N/A | N/A | |
| Query rewriting for schema diffs | N/A | N/A | |
| **Transactions** | | | |
| BEGIN/COMMIT/ROLLBACK | MULTI/EXEC/DISCARD | Partial | Forward-both only; no staged deltas |
| SAVEPOINT/RELEASE/ROLLBACK TO | N/A | N/A | Redis has no savepoints |
| Staged delta system | Missing | Missing | No Stage/Commit/Rollback on delta map |
| Schema registry snapshots | N/A | N/A | |
| Repeatable read on prod | N/A | N/A | Redis is single-threaded, reads are already consistent |
| **Protocol** | | | |
| pgwire simple protocol | RESP2 protocol | Done | `proxy/resp.go` -- full RESP2 implementation |
| pgwire extended protocol (Parse/Bind/etc.) | N/A | N/A | Redis has no extended protocol |
| SSL/TLS negotiation | Missing | Missing | `connstr.ConnInfo.SSL` parsed but not used |
| Inline command support | Inline command parsing | Done | `readInlineCommand` in resp.go |
| **Guard Rails** | | | |
| L1: validateRouteDecision | L1: validateRouteDecision | Done | `proxy/guard.go` lines 22-47 |
| L2: SafeProdConn wrapper | L2: SafeProdConn wrapper | Done | `proxy/guard.go` lines 55-95 |
| L3: classifyAndRoute final check | L3: dispatch check in routeLoop | Done | `proxy/proxy.go` lines 316-323 |
| SQLSTATE error codes | RESP error responses | Done | `buildGuardErrorResponse` |
| **Initialization** | | | |
| Prod connection + version detect | Prod PING + INFO version | Done | `init.go` lines 37-51 |
| Shadow container creation | Shadow Redis container | Done | `shadow/shadow.go` |
| Schema dump + apply | Key metadata detection (SCAN + TYPE) | Done | `schema/schema.go` DetectKeyMetadata |
| Extension handling | N/A | N/A | |
| Sequence offset calculation | N/A | N/A | Redis keys are strings, no sequences |
| FK discovery | N/A | N/A | |
| Read-replica retry logic | Missing | Missing | No retry on connection errors |
| **Pub/Sub** | | | |
| LISTEN/UNLISTEN (prod-only) | SUBSCRIBE/PSUBSCRIBE fan-in | Done | `proxy/proxy.go` lines 700-814 |
| N/A | PUBLISH (shadow-only) | Done | Classified as write, routed to shadow |
| N/A | UNSUBSCRIBE handling | Done | Forward to both backends |
| **Lua Scripting** | | | |
| N/A (PostgreSQL has PL/pgSQL) | EVAL/EVALSHA with key hydration | Done | `proxy/proxy.go` lines 822-899 |
| N/A | EVALRO/EVALSHA_RO (read-only) | Partial | Classified as read but no special routing |
| N/A | SCRIPT LOAD/EXISTS/FLUSH | Partial | Classified as OpOther, forwarded to prod only |

---

## 3. Command Classification Gaps

The classifier at `/Users/psrth/Development/mori/internal/engine/redis/classify/classifier.go` is comprehensive but has several issues:

### 3.1 SubType Misclassifications

Many write commands that are conceptually updates or deletes are classified as `SubInsert`:

| Command | Current SubType | Correct SubType | Impact |
|---------|----------------|-----------------|--------|
| `HDEL` (line 115) | SubInsert | SubDelete | No tombstone tracking for hash field deletion |
| `SREM` (line 136) | SubInsert | SubDelete | No tombstone tracking for set member removal |
| `ZREM` (line 145) | SubInsert | SubDelete | No tombstone tracking for sorted set member removal |
| `LREM` (line 127) | SubInsert | SubDelete | No tombstone tracking for list element removal |
| `LPOP`/`RPOP` (lines 122-123) | SubInsert | SubDelete | Pop is destructive; should track removal |
| `SPOP` (line 138) | SubInsert | SubDelete | Pop is destructive |
| `ZPOPMIN`/`ZPOPMAX` (lines 152-153) | SubInsert | SubDelete | Pop is destructive |
| `XDEL` (line 159) | SubInsert | SubDelete | Stream entry deletion |
| `XTRIM` (line 160) | SubInsert | SubTruncate | Stream trimming is a truncation |
| `LTRIM` (line 126) | SubInsert | SubTruncate | List trimming is a truncation |
| `INCR`/`DECR`/`INCRBY` etc. (lines 101-105) | SubInsert | SubUpdate | These modify existing values |
| `APPEND` (line 100) | SubInsert | SubUpdate | Appends to existing string |
| `SETRANGE` (line 107) | SubInsert | SubUpdate | Modifies part of existing string |
| `GETSET` (line 106) | SubInsert | SubUpdate | Reads old and writes new |
| `LSET` (line 124) | SubInsert | SubUpdate | Modifies existing list element |
| `LINSERT` (line 125) | SubInsert | SubUpdate | Modifies existing list |
| `ZINCRBY` (line 146) | SubInsert | SubUpdate | Modifies existing score |
| `HINCRBY`/`HINCRBYFLOAT` (lines 113-114) | SubInsert | SubUpdate | Modifies existing hash field |

**Impact**: Because the Router uses SubType to decide between `StrategyShadowWrite` (no hydration), `StrategyHydrateAndWrite` (hydrate first), and `StrategyShadowDelete` (tombstone), these misclassifications mean:
- Commands like `INCR` on a key only in prod will execute on an empty shadow (wrong result)
- Commands like `HDEL` won't create tombstone entries
- Commands like `SREM` won't properly track member removal

### 3.2 Missing SubUpdate Classification

The classifier never uses `SubUpdate`, which means the Router's `StrategyHydrateAndWrite` path (triggered by `SubUpdate`) is never activated through normal classification. Currently, `StrategyHydrateAndWrite` is only selected for `SubInsert` with `HasOnConflict`, which doesn't apply to Redis.

The Router at `/Users/psrth/Development/mori/internal/core/router.go` line 69 routes `SubUpdate` to `StrategyHydrateAndWrite`. The Redis classifier should use `SubUpdate` for modify-in-place commands (INCR, APPEND, HSET on existing key, etc.) so they get proper hydration.

### 3.3 Missing Commands

Commands not in the classification map (classified as `OpOther` by default):

| Command | Category | Correct Classification |
|---------|----------|----------------------|
| `GETEX` | Read+Write (GET + set expiry) | OpWrite/SubUpdate |
| `LMPOP` (Redis 7.0+) | Write (pop from multiple lists) | OpWrite/SubDelete |
| `ZMPOP` (Redis 7.0+) | Write (pop from multiple sorted sets) | OpWrite/SubDelete |
| `ZDIFF` (Redis 6.2+) | Read (sorted set difference) | OpRead/SubSelect |
| `ZDIFFSTORE` (Redis 6.2+) | Write (store sorted set difference) | OpWrite/SubInsert |
| `ZINTER` (Redis 6.2+) | Read (sorted set intersection) | OpRead/SubSelect |
| `ZUNION` (Redis 6.2+) | Read (sorted set union) | OpRead/SubSelect |
| `ZRANGESTORE` (Redis 6.2+) | Write (store range result) | OpWrite/SubInsert |
| `SINTERCARD` (Redis 7.0+) | Read (cardinality of intersection) | OpRead/SubSelect |
| `LCS` (Redis 7.0+) | Read (longest common substring) | OpRead/SubSelect |
| `EXPIRETIME` / `PEXPIRETIME` (Redis 7.0+) | Read | OpRead/SubSelect |
| `OBJECT ENCODING/FREQ/HELP/IDLETIME/REFCOUNT` | Read | OpRead/SubSelect |
| `WAIT` / `WAITAOF` | Other | OpOther |
| `ACL` subcommands | Other/DDL | Depends on subcommand |
| `FUNCTION LOAD/DELETE/DUMP/LIST/STATS` (Redis 7.0+) | DDL/Read | Depends on subcommand |
| `FCALL` / `FCALL_RO` (Redis 7.0+) | Write/Read | Like EVAL/EVALRO |
| `CLIENT SETNAME/GETNAME/ID/INFO/LIST/KILL/NO-EVICT/NO-TOUCH/PAUSE/UNPAUSE` | Other | OpOther |
| `SHUTDOWN` | DDL | Should be blocked |
| `BGSAVE` / `BGREWRITEAOF` / `SAVE` | Other | Should be blocked or forwarded to prod only |
| `REPLICAOF` / `SLAVEOF` | DDL | Should be blocked |
| `FAILOVER` | DDL | Should be blocked |
| `DEBUG` subcommands | DDL | Should be blocked |
| `XAUTOCLAIM` (Redis 6.2+) | Write | OpWrite/SubUpdate |
| `XREADGROUP` | Write (acknowledges) | OpWrite/SubUpdate |
| `OBJECT HELP` | Read | OpRead |

### 3.4 SORT with STORE Detection

The SORT command detection (lines 274-283) only extracts the key prefix from the source key (`args[1:2]`), not the destination key after STORE. The destination key should also be tracked in the delta map.

---

## 4. Read Operation Gaps

### 4.1 Single-Key Merged Read

**Status**: Implemented for simple commands (GET, HGETALL, TYPE, TTL, etc.)

**Implementation** (`proxy/proxy.go` lines 450-501):
- Checks tombstone set -- returns null if tombstoned
- Checks delta map -- reads from shadow if delta exists
- Falls back to prod

**Gaps**:
1. **No data-type-aware null responses**: A tombstoned hash key returns a null bulk string (`$-1`), but `HGETALL` on a non-existent key should return an empty array (`*0`), `LLEN` should return `:0`, `SCARD` should return `:0`, etc. The current implementation always returns `BuildNullBulkString()` regardless of command type.

2. **No EXISTS merging**: EXISTS on multiple keys (`EXISTS key1 key2 key3`) is classified as multi-key read but falls through to the default prod-only path at line 494. It should count keys across both backends minus tombstoned keys.

3. **No TYPE merging**: If a key is in the delta map, TYPE should read from shadow. Currently works for single-key but the response is not validated against the original prod type.

4. **No TTL/PTTL synchronization**: When a key is hydrated via DUMP/RESTORE, TTL is preserved. But if the application later calls EXPIRE on a shadowed key, the TTL in shadow diverges. Reads of TTL/PTTL on delta keys correctly go to shadow, but there is no mechanism to detect TTL expiration discrepancies.

5. **No RANDOMKEY merging**: RANDOMKEY returns a random key from a single backend. If both backends have keys, this should draw from the merged keyspace.

6. **No DBSIZE merging**: DBSIZE returns the key count from a single backend. Should return `prod_count + shadow_only_count - tombstone_count`.

### 4.2 Multi-Key Read Operations

**MGET**: Implemented with per-key routing (`executeMergedMGET`, lines 504-566). This is well-done -- keys are partitioned into prod/shadow/tombstone groups, batched, and results reassembled in order.

**Missing multi-key merged reads**:

| Command | Gap Description |
|---------|----------------|
| `SDIFF key1 key2` | Both keys might be in different backends. No cross-backend set diff. |
| `SINTER key1 key2` | Same issue. No cross-backend set intersection. |
| `SUNION key1 key2` | Same issue. No cross-backend set union. |
| `SMISMEMBER key member...` | Works for single key, but tombstone check needed. |
| `EXISTS key1 key2...` | Should sum existence across backends, subtract tombstones. |

### 4.3 Scan-Family Commands

**SCAN**: Implemented with merging (`executeMergedScan`, lines 907-1003). However, there are issues:

1. **Cursor consistency**: The merged SCAN uses prod's cursor as the return cursor, falling back to shadow's cursor if prod's is "0". This is incorrect because SCAN cursors are opaque iteration state tied to a specific hash table. A prod cursor cannot be used to resume iteration on shadow or vice versa. This can cause duplicate keys or missed keys across iterations.

2. **HSCAN/SSCAN/ZSCAN**: These per-key scan commands have no merged handling. If a hash/set/sorted-set key has partial modifications in shadow, HSCAN on that key should return the shadow version (since the key is in the delta map). Currently these would fall through to the single-key merged read path, which works at the key level but doesn't handle partial field-level modifications.

3. **MATCH pattern handling**: The merged SCAN does not extract or apply the MATCH pattern to shadow keys. If SCAN is called with `MATCH user:*`, shadow keys that don't match this pattern would still be included.

4. **COUNT parameter**: The COUNT hint is forwarded to both backends, but the merged result may return up to 2x COUNT keys.

### 4.4 Missing Read Strategies

| PostgreSQL Strategy | Redis Equivalent Needed | Status |
|---|---|---|
| Aggregate re-computation | N/A | N/A |
| JOIN patch | N/A (no joins in Redis) | N/A |
| CTE/derived table materialization | N/A | N/A |
| Set operation decomposition | Cross-backend set ops (SDIFF, SINTER, SUNION) | Missing |
| Window function materialization | N/A | N/A |
| Cursor declare/fetch/close | N/A | N/A |
| WHERE clause re-evaluation | N/A | N/A |
| Schema-adapted reads (col add/drop/rename) | N/A | N/A |

---

## 5. Write Operation Gaps

### 5.1 Hydration Gaps

**Current hydration** (`proxy/proxy.go` lines 569-618): Uses DUMP/RESTORE to copy the entire key from prod to shadow, preserving TTL.

**Gaps**:

1. **Single-key-only hydration**: The `hydrateKeys` function at line 578 extracts only `args[1]` (the first key argument) from the raw command string. Multi-key write commands like MSET, RENAME (which has source and destination keys), SMOVE, RPOPLPUSH/LMOVE, SDIFFSTORE/SINTERSTORE/SUNIONSTORE, ZUNIONSTORE/ZINTERSTORE, and COPY are not fully hydrated.

2. **No read-check before hydration**: For commands like SETNX (set-if-not-exists), the proxy should check if the key exists in prod before hydrating, since the behavior depends on key existence. Currently, SETNX is classified as `SubInsert` and gets `StrategyShadowWrite` (no hydration), which means it might succeed on shadow even if the key exists in prod.

3. **No partial hydration for hash operations**: HSET/HDEL operate on individual hash fields. The current DUMP/RESTORE hydration copies the entire hash, which is correct but expensive. There's no incremental field-level tracking.

4. **RENAME hydration gap**: RENAME affects two keys (source and destination). Only the source key (args[1]) is hydrated. The destination key should also be considered (it might need tombstoning if it existed in prod).

5. **Blocking commands**: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX are classified as writes but are blocking operations. The proxy would hang waiting for a response from shadow that may never have the key. These need special handling -- either hydrate first and use non-blocking equivalents on shadow, or route to prod.

6. **GETDEL / GETSET hydration**: GETDEL is classified as `SubDelete` which goes through `StrategyShadowDelete` (tombstone path). But it needs to return the value before deleting, which requires hydration from prod first if the key is not in shadow.

### 5.2 Write Tracking Gaps

**Current tracking** (`trackWriteEffects`, lines 621-662):

1. **StrategyShadowWrite path** (line 627-635): Calls `MarkInserted` on the table (prefix) and `Add` on each key. However, `MarkInserted` is designed for insert-count tracking when exact PKs are unknown -- for Redis, we always know the key, so this is redundant noise.

2. **Delta persistence inconsistency**: `StrategyHydrateAndWrite` and `StrategyShadowDelete` persist the delta map to disk immediately (lines 641, 653), but `StrategyShadowWrite` does not. This means shadow-only writes (SET on new keys) could be lost on crash.

3. **Multi-key write tracking**: `extractWriteKeys` (lines 667-680) correctly handles MSET/MSETNX by extracting keys at odd positions. However, it only returns `args[1]` for all other commands. Commands that affect multiple keys are not handled:
   - `RENAME key1 key2` -- only key1 tracked, key2 not tracked as delta
   - `RPOPLPUSH src dst` -- only src tracked
   - `LMOVE src dst ...` -- only src tracked
   - `SMOVE src dst member` -- only src tracked
   - `COPY src dst` -- only src tracked
   - `SDIFFSTORE dst key1 key2...` -- only dst tracked, but key1/key2 are read dependencies
   - `SUNIONSTORE/SINTERSTORE` -- same issue
   - `ZUNIONSTORE/ZINTERSTORE` -- same issue
   - `SORT key STORE dst` -- key tracked but dst not tracked

4. **No expiration tracking**: When EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT is called on a key, the key is added to the delta map but there's no mechanism to track that only the TTL changed (not the value). If the TTL expires on shadow but not on prod (or vice versa), reads become inconsistent.

5. **PERSIST tracking**: PERSIST removes a TTL. The key is tracked as a delta, but if it was previously hydrated with a TTL, the shadow and prod states diverge in TTL without value change.

### 5.3 Missing Write Strategies

| PostgreSQL Feature | Redis Equivalent | Status |
|---|---|---|
| Bulk UPDATE hydration (no PK in WHERE) | N/A (Redis always has explicit keys) | N/A |
| Cross-table UPDATE | Multi-key writes (RENAME, SMOVE, etc.) | Missing |
| TRUNCATE (fully-shadow table) | FLUSHDB handling | Missing |
| INSERT with RETURNING | N/A (Redis returns results inline) | N/A |
| DELETE with RETURNING | GETDEL | Missing (needs hydration) |
| FK cascade delete | N/A | N/A |

### 5.4 FLUSHDB/FLUSHALL Handling

FLUSHDB and FLUSHALL are classified as `OpDDL` and routed to shadow via `StrategyShadowDDL`. However:

1. After FLUSHDB on shadow, all subsequent reads for keys in that database should go to shadow (returning empty), but the current implementation would still read from prod for keys not in the delta map, causing phantom reads of data that the application thinks it flushed.

2. There's no equivalent of PostgreSQL's `IsFullyShadowed` flag being set after FLUSHDB. The schema registry's `IsFullyShadowed` method exists in the core router but is never called from the Redis proxy.

3. FLUSHALL is even more problematic -- it flushes all databases, but the proxy only operates on one database.

---

## 6. Data Structure Gaps

Redis supports multiple data types, each with unique semantics. The current implementation treats all keys as opaque blobs via DUMP/RESTORE. This works for full-key operations but breaks for partial operations on complex data types.

### 6.1 Strings

**Status**: Well-supported. GET/SET/MGET/MSET are the primary use case and work correctly.

**Gaps**:
- INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT on a key only in prod: These are classified as `SubInsert` -> `StrategyShadowWrite` (no hydration). The shadow key won't exist, so INCR will start from 0 instead of the prod value. **This is a correctness bug.**
- APPEND on a key only in prod: Same issue -- will start from empty string.
- GETRANGE on a delta key: Works (reads from shadow), but SETRANGE on a key only in prod needs hydration.

### 6.2 Hashes

**Status**: Key-level routing works. HGETALL, HGET on delta keys read from shadow.

**Gaps**:
- **No field-level delta tracking**: If HSET modifies one field of a large hash, the entire hash is hydrated via DUMP/RESTORE. This is correct but inefficient.
- **HDEL not tombstoned**: HDEL is classified as `SubInsert`, so deleted hash fields are not tracked. After HDEL on a hydrated key, the field is removed from shadow but still exists in prod. If the key is later re-read, the shadow version (missing the field) is returned, which is correct as long as the key stays in the delta map. But there's no field-level tombstone.
- **HSETNX race**: HSETNX (set field only if not exists) is classified as `SubInsert` -> `StrategyShadowWrite`. If the hash key only exists in prod, the shadow hash won't have the field, so HSETNX will always succeed, even if the field exists in prod.

### 6.3 Lists

**Status**: Key-level routing works for reads (LRANGE, LLEN, LINDEX).

**Gaps**:
- **LPUSH/RPUSH on existing prod list**: Classified as `SubInsert` -> `StrategyShadowWrite`. If the list key only exists in prod, the push creates a new list in shadow with only the pushed elements, losing the existing prod elements. **This is a correctness bug.**
- **LPOP/RPOP on prod-only list**: Classified as `SubInsert` -> `StrategyShadowWrite`. Shadow has no list, so pop returns nil. **Correctness bug.**
- **RPOPLPUSH/LMOVE**: Affects two keys. Only source key is hydrated.
- **BLPOP/BRPOP**: Blocking operations. If the list is in prod and not shadow, the blocking pop will wait indefinitely on an empty shadow list.
- **LINSERT/LSET**: Modify specific elements. Need hydration of the full list first.

### 6.4 Sets

**Status**: Key-level routing works for single-key reads (SMEMBERS, SISMEMBER, SCARD).

**Gaps**:
- **SADD on existing prod set**: Same issue as LPUSH -- adds to an empty shadow set instead of the prod set.
- **SREM on prod-only set**: Removes from non-existent shadow set (no-op). Does not track the removal.
- **SMOVE src dst member**: Moves member between sets. Both keys need hydration.
- **Cross-backend set operations**: SDIFF, SINTER, SUNION with keys across prod and shadow are not handled. These fall through to prod-only reads, which miss shadow modifications.
- **SDIFFSTORE/SINTERSTORE/SUNIONSTORE**: Destination key written to shadow, but source keys may be in prod. No cross-backend computation.

### 6.5 Sorted Sets

**Status**: Key-level routing works for single-key reads (ZRANGE, ZSCORE, ZCARD, etc.).

**Gaps**:
- **ZADD on existing prod sorted set**: Same hydration issue.
- **ZREM on prod-only set**: Removes from non-existent shadow set.
- **ZINCRBY on prod-only set**: Starts from score 0 instead of prod score.
- **Cross-backend sorted set operations**: ZUNIONSTORE, ZINTERSTORE with source keys in different backends.
- **ZRANGEBYSCORE/ZRANGEBYLEX range queries**: If the sorted set is partially modified, range queries on shadow may return incorrect results (missing members that are only in prod).

### 6.6 Streams

**Status**: Key-level routing works for reads (XRANGE, XLEN).

**Gaps**:
- **XADD on existing prod stream**: Creates new stream in shadow, losing existing entries.
- **XDEL**: Classified as `SubInsert` instead of `SubDelete`.
- **XREADGROUP**: Involves consumer groups and acknowledgment. No consumer group state synchronization between prod and shadow.
- **XAUTOCLAIM/XCLAIM**: Involve consumer group ownership transfer. Not classified.
- **XGROUP CREATE/SETID/DELCONSUMER/DESTROY**: Consumer group management commands. Classified as `SubInsert` (line 161) but should be DDL-like operations that potentially need shadow routing.
- **Stream ID generation**: XADD with `*` auto-generates IDs. Shadow and prod would generate different IDs for the same logical entry.

### 6.7 HyperLogLog, Bitmaps, Geospatial

These data types are completely missing from the classifier:

| Data Type | Commands Missing |
|-----------|-----------------|
| HyperLogLog | PFADD, PFCOUNT, PFMERGE |
| Bitmaps | BITCOUNT, BITFIELD, BITOP, BITPOS, GETBIT, SETBIT |
| Geospatial | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH, GEOSEARCHSTORE |

---

## 7. Transaction Management Gaps

### 7.1 Current State

MULTI/EXEC/DISCARD are classified as `OpTransaction` and routed to `targetBoth` (lines 361-373 of `proxy/proxy.go`). Both backends receive the transaction commands. Commands within the transaction are individually classified and routed.

### 7.2 Fundamental Problems

1. **No queued command buffering**: In Redis, commands between MULTI and EXEC are queued (returning `+QUEUED`) and executed atomically on EXEC. The current proxy routes each command individually as it arrives, meaning:
   - A SET inside a MULTI goes to shadow immediately (not queued)
   - The EXEC on prod has no queued commands (it only got MULTI, not the intermediate commands)
   - The prod EXEC returns an empty array, shadow EXEC returns results
   - The client gets prod's empty EXEC response

   **This is a critical correctness bug.** The proxy needs to buffer commands between MULTI and EXEC, classify them, and then:
   - Queue read commands on prod
   - Queue write commands on shadow
   - Or: hydrate all keys, queue all commands on shadow only, and synthesize prod-equivalent responses

2. **No staged delta system**: PostgreSQL uses `Stage()` / `Commit()` / `Rollback()` on the delta map within transactions. The Redis engine doesn't use staging at all. If a transaction is DISCARDed, delta entries from writes within that transaction persist incorrectly.

3. **WATCH not handled**: WATCH is classified as `OpOther` (line 189) and forwarded to prod only. But WATCH provides optimistic locking -- if a watched key is modified by another client before EXEC, the transaction fails. The proxy needs to:
   - Forward WATCH to both backends (or at least track watched keys)
   - If any watched key is in the delta map, WATCH should go to shadow
   - UNWATCH should go to both

4. **No atomicity guarantee**: Even if individual commands are correctly routed, the atomic guarantee of MULTI/EXEC is broken because commands go to different backends.

### 7.3 What's Missing vs. PostgreSQL

| PostgreSQL Feature | Redis Equivalent | Status |
|---|---|---|
| BEGIN with REPEATABLE READ on prod | MULTI on both | Partial (fundamentally broken) |
| COMMIT promoting staged deltas | EXEC promoting staged deltas | Missing |
| ROLLBACK discarding staged deltas | DISCARD discarding staged deltas | Missing |
| SAVEPOINT snapshots | N/A (Redis has no savepoints) | N/A |
| Schema registry snapshot/restore | N/A | N/A |

---

## 8. Protocol-Level Gaps

### 8.1 RESP2 Protocol

**Status**: Fully implemented in `proxy/resp.go`. All five RESP2 types are supported:
- Simple Strings (+)
- Errors (-)
- Integers (:)
- Bulk Strings ($)
- Arrays (*)

Read/write round-trip tested. Inline command parsing supported.

### 8.2 RESP3 Protocol

**Status**: Not implemented.

RESP3 (introduced in Redis 6.0) adds several new types:
- Map (%)
- Set (~)
- Null (_)
- Boolean (#)
- Double (,)
- Big Number (()
- Verbatim String (=)
- Push (>)
- Attribute (|)

The HELLO command is classified as `OpOther` and forwarded to prod, but if a client negotiates RESP3 via HELLO, the proxy's RESP2-only parser will fail on responses from prod/shadow that use RESP3 types.

### 8.3 Connection Handling

**Gaps**:

1. **No SSL/TLS**: The `connstr.ConnInfo` parses `rediss://` and sets `SSL: true` (line 16), but the proxy never establishes TLS connections. All connections use plain TCP (`net.Dial("tcp", ...)` at `proxy/proxy.go` line 183).

2. **No AUTH forwarding for client connections**: When a client connects to the proxy with AUTH, it's forwarded to prod (classified as OpOther -> targetProd). But the client also needs to be authenticated against shadow if shadow has a password. Currently shadow connections are unauthenticated.

3. **No SELECT database forwarding**: When a client sends SELECT to change databases, it goes to prod only (classified as OpOther -> targetProd). Shadow always uses db 0, so SELECT would cause a database mismatch.

4. **No CLIENT command handling**: CLIENT SETNAME, CLIENT ID, etc. are forwarded to prod only. Shadow doesn't get these, which could cause issues with connection tracking.

5. **No connection pooling**: Each client connection creates exactly one prod and one shadow connection. There's no connection pooling or multiplexing.

6. **No pipeline support**: Redis supports pipelining (sending multiple commands without waiting for responses). The current `routeLoop` processes one command at a time, which works but loses the pipelining performance benefit. This is not a correctness issue but a significant performance gap.

### 8.4 Error Handling

1. **No error propagation from shadow**: When shadow returns an error for a write command (e.g., wrong type, OOM), the error is forwarded to the client. But there's no recovery -- the delta map may already have been updated before the error was detected (since `trackWriteEffects` is called after reading the response, and the response could be an error).

2. **No prod error handling on reads**: If prod returns an error during a merged read, the error is returned as-is. There's no fallback to shadow.

---

## 9. Pub/Sub Gaps

### 9.1 Current Implementation

The pub/sub fan-in multiplexer at `proxy/proxy.go` lines 700-814 is well-implemented:
- SUBSCRIBE/PSUBSCRIBE forwarded to both backends
- Messages from both backends multiplexed to client via goroutines and channels
- UNSUBSCRIBE forwarded to both backends
- Clean shutdown on disconnect

### 9.2 Gaps

1. **Duplicate messages**: If the same message is published on a channel that exists on both prod and shadow, the client receives it twice. There's no deduplication. This is somewhat mitigated by PUBLISH being routed to shadow only, but if prod has other publishers, messages from prod will arrive alongside shadow messages.

2. **No message attribution**: The client cannot distinguish whether a message came from prod or shadow. For debugging purposes, it would be useful to tag messages.

3. **Channel subscription count**: The subscription confirmation from prod is returned to the client, but shadow's confirmation is discarded (line 723). If prod and shadow have different channel counts, the reported count is wrong.

4. **Pattern subscription merging**: PSUBSCRIBE with patterns could match different keys on prod vs. shadow. There's no pattern-aware filtering.

5. **No SUBSCRIBE in transaction context**: If SUBSCRIBE is called within a MULTI/EXEC block, behavior is undefined.

6. **SSUBSCRIBE/SUNSUBSCRIBE (Redis 7.0+ sharded pub/sub)**: Not classified or handled.

---

## 10. Guard Rail Gaps

### 10.1 Current Implementation

The three-layer write guard is well-implemented and mirrors the PostgreSQL engine:

- **L1** (`validateRouteDecision`, `guard.go` lines 22-47): Asserts write/DDL never gets `StrategyProdDirect`. Overrides to `StrategyShadowWrite`.
- **L2** (`SafeProdConn`, `guard.go` lines 55-95): Wraps prod connection, inspects outgoing RESP commands via `extractCommandFromRESP`, blocks writes.
- **L3** (inline in `routeLoop`, `proxy.go` lines 316-323): Final assertion before prod dispatch.

### 10.2 Gaps

1. **L2 bypass via raw prod connection**: The `routeLoop` uses both `safeProd` (for L2-guarded writes) and `rawProdConn` (for direct writes). Several code paths use `rawProdConn` directly:
   - `hydrateKeys` at line 592: `prodConn.Write(dumpCmd.Bytes())` -- this is a read (DUMP), so it's safe
   - `executeMergedRead` at line 481: `prodConn.Write(cmdBytes)` -- this is a read, safe
   - `executeMergedMGET` at line 535: `prodConn.Write(cmd.Bytes())` -- read, safe
   - `handleEval` at line 841: `prodConn.Write(dumpCmd.Bytes())` -- read, safe
   - `handlePubSubSubscribe` at line 711: `prodConn.Write(cmdBytes)` -- SUBSCRIBE, safe
   - `executeMergedScan` at line 928: `prodConn.Write(cmdBytes)` -- SCAN, safe

   All current direct uses are reads, so L2 is not actually bypassed for writes. This is correctly implemented.

2. **L2 RESP parsing fragility**: `extractCommandFromRESP` (guard.go lines 138-176) parses raw RESP bytes to extract the command name. It assumes the command is in a standard `*N\r\n$M\r\ncmd\r\n...` format. If a client sends an inline command or a pipelined batch, the parsing could fail and allow a write through. However, inline commands are converted to RESP arrays by `ReadRESPValue`, so this is unlikely in practice.

3. **No max hydration cap**: PostgreSQL has a max rows cap on hydration to prevent runaway copies. The Redis engine has no equivalent limit. A DUMP on a very large key could cause memory issues.

4. **No command size limits**: There's no limit on command size, key size, or value size passing through the proxy.

5. **Dangerous commands not explicitly blocked**: Commands like SHUTDOWN, REPLICAOF, SLAVEOF, DEBUG SEGFAULT, CONFIG SET (which can change persistence settings), BGSAVE, SAVE are not explicitly handled. CONFIG SET is classified as DDL and routed to shadow, which is correct. SHUTDOWN and REPLICAOF are not classified and would fall through as OpOther -> prod, which could be catastrophic.

---

## 11. Initialization Gaps

### 11.1 Current Implementation

The init pipeline at `/Users/psrth/Development/mori/internal/engine/redis/init.go` is functional:
1. Parse connection string
2. PING production Redis
3. Get Redis version via INFO
4. Create .mori directory
5. Start shadow Redis container (version-matched)
6. Detect key metadata via SCAN + TYPE
7. Persist config and tables

### 11.2 Gaps

1. **No key metadata for empty databases**: If the production Redis has no keys, initialization succeeds but with empty metadata. This is handled gracefully (line 94-98), but the first SCAN-based detection might miss keys if the database is very large (SCAN with COUNT 500 might take many iterations).

2. **No data seeding**: PostgreSQL dumps the full schema to shadow. Redis has no schema, but there's no option to seed shadow with a subset of prod data for testing. The shadow always starts empty.

3. **No extension/module detection**: Redis supports modules (RediSearch, RedisJSON, RedisGraph, etc.). If prod uses modules, shadow should have them too. No module detection or installation is implemented.

4. **No persistence configuration**: Shadow Redis is started with default persistence settings (RDB snapshots enabled). For a throwaway shadow, persistence should be disabled (`--save ""`) to improve performance.

5. **No maxmemory configuration**: Shadow Redis has no memory limits. If the application writes a large amount of data, shadow could consume unlimited memory.

6. **No SSL/TLS support for prod connections**: If prod requires SSL (detected via `rediss://`), initialization fails silently because `net.DialTimeout("tcp", ...)` is used instead of TLS.

7. **No cluster/sentinel support**: If prod is a Redis Cluster or behind Sentinel, initialization connects to a single node. There's no cluster topology discovery or slot-aware routing.

8. **No health check on container reuse**: Unlike the PostgreSQL engine (per recent commit `787c700`), there's no health check when reusing an existing shadow container.

9. **No ACL detection**: If prod uses Redis ACLs (Redis 6.0+), the proxy might not have the required permissions. No ACL validation is performed.

10. **Fixed proxy port**: The proxy port is hardcoded to 9002 (`init.go` line 109). Multiple Redis connections or conflict with other services could fail. PostgreSQL uses dynamic port allocation.

---

## 12. Priority Recommendations

### P0 -- Critical Correctness (Must Fix)

1. **Fix SubType misclassifications for modify-in-place commands** (INCR, APPEND, SETRANGE, etc. should be SubUpdate; HDEL, SREM, ZREM, etc. should be SubDelete). Without this, data-modifying commands on prod-only keys produce wrong results.
   - Files: `classify/classifier.go` lines 93-165
   - Estimated effort: Small (reclassify ~25 commands)

2. **Fix MULTI/EXEC transaction handling**: Buffer commands between MULTI and EXEC, hydrate all keys, execute on shadow only, return shadow results. Current implementation is fundamentally broken.
   - Files: `proxy/proxy.go` routeLoop and transaction handling
   - Estimated effort: Large (new transaction state machine)

3. **Fix list/set/sorted-set write hydration**: LPUSH, SADD, ZADD on existing prod keys create empty structures in shadow instead of appending to the existing data. Add hydration for all modify-in-place data structure commands.
   - Files: `proxy/proxy.go` hydrateKeys
   - Estimated effort: Medium (extend hydration to cover all SubUpdate commands)

### P1 -- Important Correctness

4. **Fix tombstone response types**: Return type-appropriate empty responses for tombstoned keys (empty array for HGETALL, 0 for LLEN, etc.) instead of always returning null bulk string.
   - Files: `proxy/proxy.go` executeMergedRead
   - Estimated effort: Medium (command-aware response generation)

5. **Fix multi-key write tracking**: Track all affected keys for RENAME, RPOPLPUSH, LMOVE, SMOVE, COPY, SDIFFSTORE, SUNIONSTORE, SINTERSTORE, ZUNIONSTORE, ZINTERSTORE.
   - Files: `proxy/proxy.go` extractWriteKeys
   - Estimated effort: Small-Medium (per-command key extraction)

6. **Fix SCAN cursor consistency**: The current merged SCAN cursor logic is incorrect. Consider maintaining a proxy-side cursor map or doing a full two-pass scan.
   - Files: `proxy/proxy.go` executeMergedScan
   - Estimated effort: Medium

7. **Implement FLUSHDB fully-shadowed marking**: After FLUSHDB on shadow, all subsequent reads should go to shadow. Use the existing `IsFullyShadowed` mechanism from the schema registry.
   - Files: `proxy/proxy.go` trackWriteEffects (StrategyShadowDDL case)
   - Estimated effort: Small

8. **Add staged delta support for transactions**: Use `Stage()` / `Commit()` / `Rollback()` from the existing delta.Map within MULTI/EXEC/DISCARD.
   - Files: `proxy/proxy.go`
   - Estimated effort: Medium (depends on P0 #2)

### P2 -- Important but Non-Blocking

9. **Add SSL/TLS support**: Use `tls.Dial` when `ConnInfo.SSL` is true.
   - Files: `proxy/proxy.go` handleConn, `schema/schema.go` DetectKeyMetadata
   - Estimated effort: Small

10. **Add missing commands to classifier**: HyperLogLog (PFADD, PFCOUNT, PFMERGE), Bitmaps (BITCOUNT, BITFIELD, etc.), Geospatial (GEOADD, etc.), Redis 7.0+ commands (LMPOP, ZMPOP, FUNCTION, FCALL).
    - Files: `classify/classifier.go` commandMap
    - Estimated effort: Small

11. **Add RESP3 support**: Extend RESPValue to handle RESP3 types. Required for Redis 6.0+ clients that negotiate RESP3 via HELLO.
    - Files: `proxy/resp.go`
    - Estimated effort: Large

12. **Fix delta persistence inconsistency**: Persist delta map on all write paths, not just HydrateAndWrite and ShadowDelete.
    - Files: `proxy/proxy.go` trackWriteEffects
    - Estimated effort: Small

13. **Add blocking command handling**: Route BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX through hydration and timeout-aware execution.
    - Files: `proxy/proxy.go`
    - Estimated effort: Medium

14. **Disable persistence on shadow**: Start shadow with `--save ""` and `--appendonly no`.
    - Files: `shadow/shadow.go` Create
    - Estimated effort: Small

15. **Block dangerous commands**: Explicitly block SHUTDOWN, REPLICAOF/SLAVEOF, DEBUG SEGFAULT, SAVE, BGSAVE from reaching prod through the proxy.
    - Files: `classify/classifier.go`
    - Estimated effort: Small

### P3 -- Nice to Have

16. **Add cross-backend set operations**: Implement merged SDIFF/SINTER/SUNION when source keys span both backends.
17. **Add connection pipelining**: Support Redis pipelining for better throughput.
18. **Add cluster/sentinel support**: Topology discovery and slot-aware routing.
19. **Add max hydration cap**: Limit DUMP size to prevent memory issues on large keys.
20. **Add module detection**: Detect and install Redis modules on shadow.
21. **Add dynamic proxy port**: Use OS-allocated port instead of hardcoded 9002.
22. **Add pub/sub message deduplication**: Track message IDs to prevent duplicate delivery.
23. **Fix EXISTS merging**: Count existence across both backends for multi-key EXISTS.
24. **Fix DBSIZE merging**: Return merged key count from both backends.
25. **Fix RANDOMKEY merging**: Return keys from merged keyspace.
