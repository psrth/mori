# Redis Engine Update Plan

This document is the implementation plan for bringing the Redis engine to full feature parity with the PostgreSQL engine's conceptual equivalents. Agents implementing these changes should **always refer to the PostgreSQL implementation** as the reference for patterns and strategies, adapting them to Redis's key-value data model.

---

## Guiding Principles

1. **Mirror the PostgreSQL patterns conceptually.** The same strategies apply: hydrate-then-write, tombstone tracking, staged deltas, merged reads. Adapt to Redis's data model.
2. **Key = row, prefix = table.** Redis keys map to postgres rows. Key prefixes (e.g., `user:`) map to tables. DUMP/RESTORE is the hydration mechanism.
3. **Prod safety is non-negotiable.** Never mutate Prod. All writes go to Shadow.
4. **Classification correctness drives everything.** The SubType determines the routing strategy. Wrong SubType = wrong strategy = wrong results.

---

## Table of Contents

1. [P0 — Critical Correctness](#p0--critical-correctness)
2. [P1 — Important Correctness](#p1--important-correctness)
3. [P2 — Important Non-Blocking](#p2--important-non-blocking)
4. [P3 — Nice to Have](#p3--nice-to-have)

---

## P0 — Critical Correctness

These cause **wrong data** in the current implementation. Fix first.

### 1.1 Fix SubType Misclassifications

**Problem:** ~25 commands that modify or delete data are classified as `SubInsert`, causing them to route through `StrategyShadowWrite` (no hydration). This means commands like INCR on a prod-only key start from 0 instead of the prod value.

**Resolution:** Reclassify commands to their correct SubType so the Router picks the correct strategy.

**Implementation:**
- In `classify/classifier.go`, update the `commandMap`:

**Reclassify to SubUpdate (triggers StrategyHydrateAndWrite):**
```
INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT → SubUpdate
APPEND → SubUpdate
SETRANGE → SubUpdate
GETSET → SubUpdate
LSET → SubUpdate
LINSERT → SubUpdate
ZINCRBY → SubUpdate
HINCRBY, HINCRBYFLOAT → SubUpdate
HSET (on existing key) → SubUpdate (see note below)
LPUSH, RPUSH → SubUpdate
LPUSHX, RPUSHX → SubUpdate
SADD → SubUpdate
ZADD → SubUpdate
XADD → SubUpdate
```

**Reclassify to SubDelete (triggers StrategyShadowDelete with tombstoning):**
```
HDEL → SubDelete
SREM → SubDelete
ZREM → SubDelete
LREM → SubDelete
LPOP, RPOP → SubDelete
SPOP → SubDelete
ZPOPMIN, ZPOPMAX → SubDelete
XDEL → SubDelete
```

**Reclassify to SubTruncate:**
```
XTRIM → SubTruncate
LTRIM → SubTruncate
```

**Important note on SubUpdate routing:** The core Router at `core/router.go` maps `SubUpdate` → `StrategyHydrateAndWrite`. This means all SubUpdate commands will trigger DUMP/RESTORE hydration before execution. The hydration is conditional — if DUMP returns nil (key doesn't exist in Prod), skip hydration and proceed with the write on Shadow. This handles both "modify existing key" and "create new key" cases correctly.

**Note on HSET/LPUSH/RPUSH/SADD/ZADD:** These commands both create new keys AND modify existing ones. Classifying as SubUpdate means they always attempt hydration, which is correct: if the key exists in Prod, it gets hydrated first; if not, hydration is a no-op and the command creates a new key on Shadow.

- **Reference:** `postgres/classify/classifier.go` SubType classification, `core/router.go` Route() method

### 1.2 Fix MULTI/EXEC Transaction Handling

**Problem:** Commands within MULTI/EXEC are individually routed instead of being buffered and executed atomically. Prod gets MULTI + EXEC with nothing in between. This is fundamentally broken.

**Resolution:** Buffer + Hydrate + Shadow-only with staged deltas (matching the postgres transaction pattern).

**Implementation:**
- In `proxy/proxy.go`, add transaction state tracking:
  ```
  type txnState struct {
      inTxn       bool
      cmdQueue    []RESPCommand   // buffered commands
      writeKeys   []string        // all keys that will be written
      readKeys    []string        // all keys that will be read
  }
  ```

- **On MULTI:**
  1. Forward MULTI to Shadow only (Prod gets nothing)
  2. Set `inTxn = true`
  3. Call `deltaMap.Stage()` equivalent — begin staged delta tracking
  4. Return `+OK` to client

- **On each command between MULTI and EXEC:**
  1. Do NOT execute the command
  2. Classify the command
  3. Add to `cmdQueue`
  4. Extract keys and add to `writeKeys` or `readKeys`
  5. Return `+QUEUED` to client (matching Redis behavior)

- **On EXEC:**
  1. Hydrate all `writeKeys` from Prod to Shadow (DUMP/RESTORE for each key that exists in Prod and is not already in delta map)
  2. For each command in `cmdQueue`:
     - Forward to Shadow (within the MULTI context)
  3. Forward EXEC to Shadow
  4. Read Shadow's EXEC response (array of results)
  5. Track all write effects (update delta map, tombstone set) using staged deltas
  6. Commit staged deltas
  7. Persist state
  8. Return Shadow's EXEC response to client

- **On DISCARD:**
  1. Forward DISCARD to Shadow
  2. Rollback staged deltas
  3. Clear txnState
  4. Return `+OK` to client

- **Reference:** `postgres/proxy/txn.go` — staged delta system, BEGIN/COMMIT/ROLLBACK pattern

### 1.3 Fix Data Structure Write Hydration

**Problem:** LPUSH, SADD, ZADD, XADD on existing prod keys create empty structures in shadow instead of appending to existing data. This is a direct consequence of 1.1 — once commands are reclassified as SubUpdate, `StrategyHydrateAndWrite` is triggered, and DUMP/RESTORE hydration copies the full key before the write executes.

**Resolution:** This is automatically fixed by 1.1. Once LPUSH/SADD/ZADD/XADD are classified as SubUpdate:
1. Router returns `StrategyHydrateAndWrite`
2. Proxy calls `hydrateKeys()` which does DUMP on Prod, RESTORE on Shadow
3. Shadow now has the full data structure
4. Command executes on Shadow, correctly appending to existing data

**Verification:** After implementing 1.1, add tests for:
- LPUSH on a list that only exists in Prod → list should contain both Prod elements and new element
- SADD on a set that only exists in Prod → set should contain both Prod members and new member
- INCR on a key that only exists in Prod → value should be Prod value + 1
- ZADD on a sorted set that only exists in Prod → sorted set should contain both Prod and new members

---

## P1 — Important Correctness

### 2.1 Fix Tombstone Response Types

**Problem:** Tombstoned keys always return null bulk string (`$-1`), but different commands expect different empty responses.

**Resolution:** Return the correct empty response based on the command type, matching what Redis returns for non-existent keys.

**Implementation:**
- In `proxy/proxy.go` `executeMergedRead()`, instead of always returning `BuildNullBulkString()` for tombstoned keys, check the command and return the appropriate response:

| Command | Correct Tombstone Response |
|---------|---------------------------|
| GET, GETRANGE, HGET, LINDEX | `$-1` (null bulk string) |
| HGETALL, HMGET, LRANGE, SMEMBERS, ZRANGE, ZRANGEBYSCORE, XRANGE | `*0` (empty array) |
| LLEN, SCARD, ZCARD, XLEN, STRLEN, HLEN | `:0` (integer 0) |
| EXISTS | `:0` (integer 0) |
| TTL, PTTL | `:-2` (key does not exist) |
| TYPE | `+none` (simple string) |
| SISMEMBER, SISMEMBERX | `:0` (integer 0) |
| ZSCORE | `$-1` (null bulk string) |
| HEXISTS | `:0` (integer 0) |
| OBJECT ENCODING | `$-1` (null bulk string) |

- Build a `tombstoneResponse map[string]func() []byte` lookup in the proxy initialization
- **Reference:** Redis documentation for each command's "key does not exist" behavior

### 2.2 Fix Multi-Key Write Tracking

**Problem:** Only the first key is tracked for multi-key write commands. RENAME, RPOPLPUSH, SMOVE, etc. affect multiple keys.

**Resolution:** Extract and track ALL affected keys per command.

**Implementation:**
- In `proxy/proxy.go` `extractWriteKeys()`, add per-command key extraction:

| Command | Keys to Track |
|---------|--------------|
| `RENAME key1 key2` | key1 (delta), key2 (delta) |
| `RPOPLPUSH src dst` | src (delta), dst (delta) |
| `LMOVE src dst ...` | src (delta), dst (delta) |
| `SMOVE src dst member` | src (delta), dst (delta) |
| `COPY src dst` | dst (delta) |
| `SDIFFSTORE dst k1 k2...` | dst (delta) |
| `SINTERSTORE dst k1 k2...` | dst (delta) |
| `SUNIONSTORE dst k1 k2...` | dst (delta) |
| `ZUNIONSTORE dst k1 k2...` | dst (delta) |
| `ZINTERSTORE dst k1 k2...` | dst (delta) |
| `SORT key STORE dst` | dst (delta) |
| `BITOP op dst k1 k2...` | dst (delta) |
| `GEORADIUS ... STORE key` | key (delta) |
| `GEOSEARCHSTORE dst src ...` | dst (delta) |

- For commands that read from source keys (SDIFFSTORE, SUNIONSTORE, etc.), the source keys also need hydration if they're not already in Shadow
- **Reference:** `postgres/proxy/write_update.go` cross-table update tracking

### 2.3 Fix SCAN Cursor Merging

**Problem:** Current merged SCAN uses Prod's opaque cursor for Shadow iteration, which is incorrect and causes duplicate/missed keys.

**Resolution:** Sequential two-phase scan with cursor high-bit to distinguish phases.

**Implementation:**
- In `proxy/proxy.go` `executeMergedScan()`:
  - Use bit 63 of the cursor to track phase: `0` = scanning Prod, `1` = scanning Shadow
  - **Phase 0 (Prod scan):**
    1. Forward SCAN with client's cursor (mask off bit 63) to Prod
    2. Filter results: remove tombstoned keys, remove keys in delta map
    3. If Prod cursor returns "0" (done), switch to Phase 1: set bit 63, start Shadow scan at cursor "0"
    4. Otherwise return Prod cursor (with bit 63 = 0) and filtered results
  - **Phase 1 (Shadow scan):**
    1. Forward SCAN with cursor (mask off bit 63) to Shadow
    2. Return all Shadow keys (these are all delta/new keys)
    3. If Shadow cursor returns "0" (done), return "0" to client
    4. Otherwise return Shadow cursor (with bit 63 = 1) and results
  - Apply MATCH pattern filtering to both phases
  - Apply COUNT hint to both phases
- No functionality loss — the merged keyspace is complete and correct. Every key appears exactly once.
- **Reference:** New approach specific to Redis. The conceptual parallel is postgres's merged read filtering (remove delta/tombstoned rows from Prod, include all Shadow rows).

### 2.4 Implement FLUSHDB Fully-Shadowed Marking

**Problem:** After FLUSHDB on Shadow, all subsequent reads for keys in that database should return empty, but reads still fall through to Prod.

**Resolution:** Use the existing `IsFullyShadowed` mechanism from the schema registry.

**Implementation:**
- In `proxy/proxy.go` `trackWriteEffects()` for `StrategyShadowDDL`:
  - If command is FLUSHDB: call `schemaReg.MarkFullyShadowed("*")` (use `"*"` or a sentinel value to indicate all prefixes)
  - In the routing logic, before checking delta/tombstone for a key, check if the database is fully shadowed → if yes, always read from Shadow
- For FLUSHALL: same treatment but log a warning (multi-database implications)
- **Reference:** `postgres/proxy/conn.go` StrategyTruncate → `MarkFullyShadowed(table)`

### 2.5 Add Staged Delta Support

**Problem:** No staged delta system means DISCARD doesn't properly undo delta tracking.

**Resolution:** Use the existing `Stage()` / `Commit()` / `Rollback()` from `core/delta.Map` and `core/delta.TombstoneSet`.

**Implementation:**
- This is automatically handled by 1.2 (MULTI/EXEC fix) which uses staged deltas
- Outside of MULTI/EXEC (single-command auto-commit), continue using direct `Add()` (no staging needed, same as postgres auto-commit)
- **Reference:** `postgres/proxy/txn.go` staged delta usage

### 2.6 Multi-Key Write Hydration

**Problem:** `hydrateKeys` only hydrates `args[1]` (the first key). Multi-key writes need all affected keys hydrated.

**Implementation:**
- In `proxy/proxy.go` `hydrateKeys()`, accept a list of keys instead of extracting just one
- For multi-key writes, extract all source/destination keys and hydrate each one:

| Command | Keys to Hydrate |
|---------|----------------|
| `RENAME key1 key2` | key1, key2 (key2 might be overwritten) |
| `RPOPLPUSH src dst` | src, dst |
| `LMOVE src dst ...` | src, dst |
| `SMOVE src dst member` | src, dst |
| `SDIFFSTORE dst k1 k2...` | k1, k2, ... (source keys need to be in Shadow for the operation) |
| `SINTERSTORE dst k1 k2...` | k1, k2, ... |
| `SUNIONSTORE dst k1 k2...` | k1, k2, ... |
| `ZUNIONSTORE dst k1 k2...` | k1, k2, ... |
| `ZINTERSTORE dst k1 k2...` | k1, k2, ... |
| `BITOP op dst k1 k2...` | k1, k2, ... |
| `SORT key STORE dst` | key |

- Skip hydration for keys already in delta map
- **Reference:** `postgres/proxy/write_update.go` cross-table hydration

### 2.7 GETDEL / GETSET Hydration

**Problem:** GETDEL needs to return the value before deleting, which requires the key to exist in Shadow first. GETSET needs the old value.

**Implementation:**
- GETDEL: Reclassify as SubDelete. In the ShadowDelete handler, before tombstoning, hydrate the key from Prod if not in Shadow. Execute GETDEL on Shadow (returns the value and deletes). Tombstone the key. Return the value to client.
- GETSET: Already reclassified as SubUpdate in 1.1. StrategyHydrateAndWrite hydrates first, then GETSET executes on Shadow correctly.
- **Reference:** `postgres/proxy/write_delete.go` DELETE with RETURNING

---

## P2 — Important Non-Blocking

### 3.1 Add SSL/TLS Support

**Problem:** `rediss://` connections are parsed but TLS is never established. Cloud Redis (ElastiCache, Cloud Memorystore, Azure Cache) require TLS.

**Implementation:**
- In `proxy/proxy.go` `handleConn()`, when connecting to Prod:
  - If `ConnInfo.SSL` is true, use `tls.Dial("tcp", addr, &tls.Config{})` instead of `net.Dial("tcp", addr)`
  - Support optional CA cert, client cert, client key from connection string or config
- In `schema/schema.go` `DetectKeyMetadata()`, same TLS handling for init-time connections
- Shadow connections remain plain TCP (local Docker container)
- **Reference:** `postgres/proxy/conn.go` SSL/TLS negotiation (different protocol but same concept)

### 3.2 Add Missing Commands to Classifier

**Problem:** Several command categories are not classified at all.

**Implementation:**
- In `classify/classifier.go`, add to `commandMap`:

**HyperLogLog:**
```
PFADD → OpWrite/SubInsert (creates/modifies HLL)
PFCOUNT → OpRead/SubSelect
PFMERGE → OpWrite/SubInsert (destination key)
```

**Bitmaps:**
```
SETBIT → OpWrite/SubUpdate (modifies existing bit)
GETBIT → OpRead/SubSelect
BITCOUNT → OpRead/SubSelect
BITPOS → OpRead/SubSelect
BITFIELD → OpWrite/SubUpdate (can modify bits)
BITOP → OpWrite/SubInsert (destination key)
```

**Geospatial:**
```
GEOADD → OpWrite/SubUpdate
GEODIST → OpRead/SubSelect
GEOHASH → OpRead/SubSelect
GEOPOS → OpRead/SubSelect
GEOSEARCH → OpRead/SubSelect
GEOSEARCHSTORE → OpWrite/SubInsert
```

**Redis 7.0+ commands:**
```
LMPOP → OpWrite/SubDelete
ZMPOP → OpWrite/SubDelete
GETEX → OpWrite/SubUpdate (modifies TTL)
FUNCTION LOAD → OpDDL/SubCreate
FUNCTION DELETE → OpDDL/SubDrop
FUNCTION DUMP/LIST/STATS → OpRead/SubSelect
FCALL → OpWrite/SubInsert (like EVAL)
FCALL_RO → OpRead/SubSelect (like EVALRO)
SINTERCARD → OpRead/SubSelect
LCS → OpRead/SubSelect
EXPIRETIME, PEXPIRETIME → OpRead/SubSelect
```

**Dangerous commands to block (SubNotSupported):**
```
SHUTDOWN → SubNotSupported ("SHUTDOWN is not supported through Mori")
REPLICAOF/SLAVEOF → SubNotSupported ("replication commands are not supported through Mori")
DEBUG → SubNotSupported ("DEBUG is not supported through Mori")
```

### 3.3 Add RESP3 Support

**Problem:** Redis 6.0+ clients may negotiate RESP3 via HELLO. The proxy's RESP2-only parser will fail.

**Implementation:**
- In `proxy/resp.go`, extend `RESPValue` and `ReadRESPValue()` to handle RESP3 types:
  - `%` (Map) → read as array of key-value pairs
  - `~` (Set) → read as array
  - `_` (Null) → read as nil
  - `#` (Boolean) → read as `t` or `f`
  - `,` (Double) → read as string (float representation)
  - `(` (Big Number) → read as string
  - `=` (Verbatim String) → read as bulk string (strip encoding prefix)
  - `>` (Push) → read as array (async server push)
  - `|` (Attribute) → read and discard (metadata)
- Intercept HELLO command: if client negotiates RESP3, track protocol version per connection
- Use correct serialization format for responses based on negotiated protocol
- **Reference:** Redis protocol specification (https://redis.io/docs/reference/protocol-spec/)

### 3.4 Fix Delta Persistence Inconsistency

**Problem:** `StrategyShadowWrite` doesn't persist delta map to disk, but other paths do. Shadow-only writes could be lost on crash.

**Implementation:**
- In `proxy/proxy.go` `trackWriteEffects()`, add persistence call in the `StrategyShadowWrite` case:
  ```go
  case core.StrategyShadowWrite:
      // ... existing tracking code ...
      if err := delta.WriteDeltaMap(p.moriDir, p.deltaMap); err != nil {
          p.logger.Error("failed to persist delta map", "err", err)
      }
  ```
- **Reference:** `postgres/proxy/conn.go` delta persistence after every write

### 3.5 Blocking Command Handling (BLPOP, BRPOP, etc.)

**Problem:** Blocking commands on prod-only keys wait indefinitely on empty Shadow lists.

**Resolution:** Hydrate the key, then implement proxy-side polling with timeout.

**Implementation:**
- Detect blocking commands in classifier: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX, BRPOPLPUSH
- In the write handler:
  1. Extract timeout from the command's last argument
  2. Hydrate all source keys from Prod
  3. Convert to non-blocking equivalent on Shadow:
     - BLPOP → LPOP (try once)
     - BRPOP → RPOP (try once)
     - BLMOVE → LMOVE
     - BZPOPMIN → ZPOPMIN
     - BZPOPMAX → ZPOPMAX
  4. If non-blocking equivalent returns a result → return it
  5. If nil → poll Shadow with short sleep intervals (100ms) until timeout
  6. If timeout expires → return nil array (matching Redis BLPOP timeout behavior)
- Track affected keys in delta map (these are destructive reads)
- **Reference:** Conceptually similar to `postgres/proxy/cursor.go` (materialized reads with client-side iteration)

### 3.6 Disable Persistence on Shadow

**Problem:** Shadow Redis runs with default persistence (RDB snapshots), which is unnecessary overhead for a throwaway container.

**Implementation:**
- In `shadow/shadow.go` `Create()`, add Redis config flags to the Docker run command:
  ```
  --save "" --appendonly no
  ```
- This disables both RDB and AOF persistence, improving Shadow performance
- **Reference:** `postgres/shadow/container.go` container configuration

### 3.7 Block Dangerous Commands

**Problem:** SHUTDOWN, REPLICAOF, DEBUG SEGFAULT could be catastrophic if forwarded to Prod.

**Implementation:**
- In `classify/classifier.go`, classify these as `SubNotSupported`:
  - SHUTDOWN → "SHUTDOWN is not supported through Mori — it would stop the production Redis server"
  - REPLICAOF / SLAVEOF → "replication commands are not supported through Mori"
  - DEBUG SEGFAULT / DEBUG SLEEP → "DEBUG is not supported through Mori"
  - SAVE / BGSAVE / BGREWRITEAOF → Route to Shadow only (not Prod), or block
  - FAILOVER → Block
- **Reference:** `postgres/classify/classifier.go` SubNotSupported handling (COPY, LOCK, DO, CALL, EXPLAIN ANALYZE)

---

## P3 — Nice to Have

### 4.1 Cross-Backend Set Operations

**Problem:** SDIFF, SINTER, SUNION with keys across Prod and Shadow produce incorrect results.

**Implementation:**
- Detect multi-key set operations where keys span both backends (some in delta, some not)
- Hydrate all source keys to Shadow, execute operation on Shadow
- This leverages the existing hydration mechanism — no new algorithm needed
- **Reference:** Conceptually similar to `postgres/proxy/read_join.go` (materializing dirty tables before JOIN)

### 4.2 Connection Pipelining

**Problem:** Commands are processed one at a time, losing Redis pipelining performance.

**Implementation:**
- In `routeLoop`, batch-read multiple commands when available on the client connection
- Classify and route each command independently
- Batch responses and write back together
- Challenge: commands within a pipeline may depend on each other (rare but possible)
- **Reference:** `postgres/proxy/extended.go` batch processing (Parse/Bind/Execute batching)

### 4.3 EXISTS Merging for Multi-Key

**Problem:** `EXISTS key1 key2 key3` doesn't properly count across backends.

**Implementation:**
- Split keys into three groups: tombstoned (count 0), delta (check Shadow), clean (check Prod)
- Sum existence counts from Shadow and Prod, subtract tombstoned
- Return total count

### 4.4 DBSIZE Merging

**Problem:** DBSIZE returns count from single backend.

**Implementation:**
- Query DBSIZE on both Prod and Shadow
- Return: `prod_count + shadow_only_delta_count - tombstone_count`
- `shadow_only_delta_count` = keys in delta map that don't exist in Prod (new keys)

### 4.5 RANDOMKEY Merging

**Problem:** RANDOMKEY draws from single backend.

**Implementation:**
- Probabilistically choose Prod or Shadow based on approximate key ratio
- If chosen key is tombstoned, retry
- Acceptable to have slight bias — this is a dev proxy

### 4.6 Pub/Sub Improvements

- Channel subscription count correction (return merged count from both backends)
- SSUBSCRIBE/SUNSUBSCRIBE for Redis 7.0+ sharded pub/sub
- Message deduplication deferred — edge case since PUBLISH routes to Shadow only

### 4.7 WATCH Support

**Problem:** WATCH provides optimistic locking but is forwarded to Prod only.

**Implementation:**
- If WATCH'ed key is in delta map → watch on Shadow
- If WATCH'ed key is not in delta map → watch on Prod
- On EXEC, if watch fails on either backend → return nil (transaction aborted)
- UNWATCH → forward to both
- **Reference:** `postgres/proxy/txn.go` dual-backend transaction management

### 4.8 Dynamic Proxy Port

**Problem:** Proxy port hardcoded to 9002.

**Implementation:**
- Use OS-allocated port (like postgres does): `net.Listen("tcp", "127.0.0.1:0")`
- Store allocated port in config
- **Reference:** `postgres/shadow/container.go` dynamic port allocation

### 4.9 Max Hydration Cap

**Problem:** DUMP on very large keys could cause memory issues.

**Implementation:**
- Before DUMP, check key size via `DEBUG OBJECT key` or `MEMORY USAGE key` (Redis 4.0+)
- If size exceeds `max_hydration_bytes` config, skip hydration and log a warning
- Return error to client: "key too large for hydration (X bytes, limit Y bytes)"
- **Reference:** `postgres/proxy/write_update.go` `capSQL()` and max_rows_hydrate

### 4.10 Module Detection

**Problem:** If Prod uses Redis modules (RediSearch, RedisJSON, etc.), Shadow should have them too.

**Implementation:**
- During init, run `MODULE LIST` on Prod
- If modules detected, pull a Redis image with those modules (e.g., `redis/redis-stack`) or log a warning
- **Reference:** `postgres/schema/dumper.go` extension detection and installation

### 4.11 Health Check on Container Reuse

**Problem:** No health check when reusing an existing Shadow container (unlike postgres per commit `787c700`).

**Implementation:**
- In `shadow/shadow.go`, when detecting an existing container, verify it's healthy:
  - Check container is running
  - PING the Redis instance
  - If unhealthy, restart or recreate
- **Reference:** `postgres/shadow/container.go` health check on reuse

### 4.12 HSCAN/SSCAN/ZSCAN Handling

**Problem:** Per-key scan commands on delta keys don't properly read from Shadow.

**Implementation:**
- If the key is in delta map → forward scan to Shadow (the hydrated copy has all data)
- If the key is tombstoned → return empty scan result (`*2\r\n$1\r\n0\r\n*0\r\n`)
- If the key is clean → forward to Prod
- This follows the same routing logic as other single-key reads

### 4.13 AUTH and SELECT Forwarding

**Problem:** AUTH and SELECT go to Prod only, causing Shadow auth/db mismatch.

**Implementation:**
- AUTH: Forward to both Prod and Shadow (Shadow may have a different password — use Shadow's configured password)
- SELECT db: Forward to both backends (ensure Shadow uses the same database number)
- **Reference:** `postgres/proxy/conn.go` StrategyForwardBoth pattern

---

## Implementation Order

```
Phase 1 (P0 — critical correctness):
  1.1 SubType reclassifications (~25 commands)
  1.2 MULTI/EXEC fix (buffer + hydrate + shadow-only + staged deltas)
  1.3 Data structure write hydration (automatic from 1.1)
  → Test thoroughly: INCR, LPUSH, SADD, ZADD on prod-only keys

Phase 2 (P1 — important correctness):
  2.1 Tombstone response types
  2.2 Multi-key write tracking
  2.3 SCAN cursor merging (sequential two-phase)
  2.4 FLUSHDB fully-shadowed marking
  2.5 Staged delta support (comes with 1.2)
  2.6 Multi-key write hydration
  2.7 GETDEL/GETSET hydration

Phase 3 (P2 — important non-blocking):
  3.1 SSL/TLS support
  3.2 Missing command classification
  3.3 RESP3 support
  3.4 Delta persistence fix
  3.5 Blocking command handling
  3.6 Disable Shadow persistence
  3.7 Block dangerous commands

Phase 4 (P3 — nice to have):
  4.1-4.13 in any order based on user demand
```

---

## Key Redis vs PostgreSQL Conceptual Mappings

Agents should use these mappings when translating postgres patterns to Redis:

| PostgreSQL Concept | Redis Equivalent |
|-------------------|-----------------|
| Table | Key prefix (e.g., `user:`) |
| Row | Key |
| Primary Key | Key name |
| Column | Hash field (for hashes) or N/A |
| INSERT | SET, HSET, LPUSH, SADD, ZADD, XADD |
| UPDATE | INCR, APPEND, HSET (existing), LSET |
| DELETE | DEL, UNLINK, HDEL, SREM, ZREM |
| TRUNCATE | FLUSHDB |
| SELECT | GET, HGETALL, LRANGE, SMEMBERS, ZRANGE |
| SELECT with JOIN | N/A (no joins) |
| SELECT with WHERE | SCAN with MATCH pattern |
| BEGIN/COMMIT/ROLLBACK | MULTI/EXEC/DISCARD |
| SAVEPOINT | N/A |
| Foreign Key | N/A |
| Schema change (DDL) | N/A (schemaless) |
| Hydration (row copy) | DUMP/RESTORE (key copy with TTL) |
| Tombstone (logical delete) | Key in tombstone set |
| Delta (modified row) | Key in delta map |
| Merged read | Check tombstone → check delta → route to Shadow or Prod |
| ctid (PK-less identity) | Key name (always exists in Redis) |
| pg_dump | SCAN + TYPE (key metadata) |
| Extension | Redis Module |
| LISTEN/NOTIFY | SUBSCRIBE/PUBLISH |
| EXPLAIN | N/A |
| Cursor (DECLARE/FETCH) | SCAN cursor (opaque iteration state) |
