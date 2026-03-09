# Firestore Engine — Gap Analysis

This document provides an exhaustive analysis of the Firestore engine implementation compared to the PostgreSQL engine (the most mature implementation). Since Firestore is a document database with a fundamentally different data model from SQL, this analysis focuses on conceptual parallels: read/write routing, delta tracking, tombstoning, merged reads, and safety mechanisms.

**Reference files examined:**
- `/internal/engine/firestore/engine.go`
- `/internal/engine/firestore/init.go`
- `/internal/engine/firestore/classify/classifier.go`
- `/internal/engine/firestore/connstr/connstr.go`
- `/internal/engine/firestore/proxy/proxy.go`
- `/internal/engine/firestore/proxy/handler.go`
- `/internal/engine/firestore/proxy/guard.go`
- `/internal/engine/firestore/proxy/read.go`
- `/internal/engine/firestore/proxy/write.go`
- `/internal/engine/firestore/proxy/sdk.go`
- `/internal/engine/firestore/schema/schema.go`
- `/internal/engine/firestore/schema/persist.go`
- `/internal/engine/firestore/shadow/shadow.go`

---

## 1. Engine Overview

The Firestore engine implements a gRPC reverse proxy that intercepts Firestore API calls and applies copy-on-write semantics using a local Firestore emulator as the shadow backend. It supports:

- **Initialization**: Connecting to production Firestore, detecting root-level collections, spinning up a Docker-based emulator, and seeding it with production data.
- **Classification**: Mapping Firestore gRPC method names to operation types (read/write/transaction).
- **Routing**: Using the shared `core.Router` with SDK-level upgrades for merged reads and delta-tracked writes.
- **Merged Reads**: SDK-based document-level routing (GetDocument, ListDocuments, BatchGetDocuments, RunQuery) with delta/tombstone filtering.
- **Write Tracking**: CreateDocument, UpdateDocument, DeleteDocument, Commit, and BatchWrite operations routed to shadow with delta/tombstone persistence.
- **Write Guards**: Three-layer protection preventing writes from reaching production.
- **Raw Forwarding Fallback**: Opaque gRPC frame forwarding when SDK clients are unavailable.

The engine is in a functional but early state. Core read/write routing and delta tracking work for basic operations, but many features from the PostgreSQL engine either do not apply directly or have no Firestore-native equivalent yet.

---

## 2. Feature Parity Matrix

| PostgreSQL Concept | Firestore Equivalent | Status | Notes |
|---|---|---|---|
| **Core Architecture** | | | |
| Prod (read-only reference) | Prod Firestore project | ✅ Implemented | SDK + raw gRPC connections |
| Shadow (local clone) | Firestore emulator container | ✅ Implemented | Docker-based, seeded from prod |
| Proxy (protocol interception) | gRPC reverse proxy | ✅ Implemented | `grpc.UnknownServiceHandler` |
| Delta Map | Delta Map (shared core) | ✅ Implemented | Per-document tracking via `(collection, docID)` |
| Tombstone Set | Tombstone Set (shared core) | ✅ Implemented | Per-document tombstoning |
| Schema Registry | Schema Registry (shared core) | ⚠️ Partial | Injected but not actively used for Firestore-specific schema tracking |
| **Classification** | | | |
| SQL parser (pg_query_go) | gRPC method name matching | ✅ Implemented | `classify/classifier.go` lines 29-78 |
| Statement type detection | Method-based classification | ✅ Implemented | Read, Write, Transaction, Other |
| Feature flags (IsJoin, HasLimit, etc.) | HasAggregate only | ⚠️ Partial | Only `HasAggregate` set for `RunAggregationQuery`; no other flags |
| PK extraction from WHERE | Document path parsing | ✅ Implemented | `splitDocPath()` in `read.go` lines 374-390 |
| Table extraction from AST | Collection extraction from proto | ✅ Implemented | `extractCollectionFromQuery()`, `extractCollectionFromParent()` |
| Parameterized query handling | N/A (no SQL params) | ➖ N/A | `ClassifyWithParams` delegates to `Classify` |
| **Routing Strategies** | | | |
| StrategyProdDirect | Raw forward to prod | ✅ Implemented | Default for clean reads |
| StrategyMergedRead | SDK merged read | ✅ Implemented | Upgraded from ProdDirect when deltas exist |
| StrategyJoinPatch | N/A | ➖ N/A | Firestore has no server-side JOINs |
| StrategyShadowWrite | SDK write to shadow | ✅ Implemented | CreateDocument, UpdateDocument |
| StrategyHydrateAndWrite | Not implemented | ❌ Missing | No hydration system; updates go directly to shadow |
| StrategyShadowDelete | SDK delete + tombstone | ✅ Implemented | DeleteDocument with tombstone tracking |
| StrategyShadowDDL | N/A | ➖ N/A | Firestore is schema-less; no DDL concept |
| StrategyTransaction | Forward to shadow | ⚠️ Partial | BeginTransaction classified but no dual-backend coordination |
| StrategyNotSupported | Default for unknown methods | ✅ Implemented | Falls through to OpOther |
| StrategyForwardBoth | Not implemented | ❌ Missing | No dual-backend forwarding |
| StrategyTruncate | Not implemented | ❌ Missing | No collection-level truncation |
| StrategyListenOnly | Not implemented | ❌ Missing | `Listen` classified as read, not special-cased |
| **Read Operations** | | | |
| Single-table merged read | Document-level merged read | ✅ Implemented | GetDocument, ListDocuments, RunQuery, BatchGetDocuments |
| PK injection for dedup | Document path as natural key | ✅ Implemented | `splitDocPath()` used for dedup |
| LIMIT over-fetch | Not implemented | ❌ Missing | No LIMIT adjustment for merged reads |
| Schema adaptation (drop/rename/add cols) | N/A | ➖ N/A | Firestore is schema-less |
| Aggregate queries (re-aggregation) | Not implemented | ❌ Missing | `RunAggregationQuery` not handled via SDK merged read |
| JOIN handling | N/A | ➖ N/A | No server-side JOINs in Firestore |
| Complex queries (CTEs, derived tables) | N/A | ➖ N/A | No SQL-level query composition |
| Set operations (UNION, etc.) | N/A | ➖ N/A | No set operations in Firestore |
| Window functions | N/A | ➖ N/A | Not applicable |
| Cursor operations | N/A | ➖ N/A | Firestore uses pagination tokens instead |
| **Write Operations** | | | |
| Simple INSERT | CreateDocument | ✅ Implemented | `write.go` lines 24-42 |
| UPDATE (point) | UpdateDocument | ✅ Implemented | `write.go` lines 45-62 |
| UPDATE (bulk/hydrate) | Not implemented | ❌ Missing | No hydration from prod before update |
| DELETE (point) | DeleteDocument + tombstone | ✅ Implemented | `write.go` lines 65-85 |
| DELETE (bulk) | Not implemented | ❌ Missing | No bulk delete with PK discovery |
| INSERT ... ON CONFLICT (upsert) | Not implemented | ❌ Missing | No upsert handling |
| TRUNCATE | Not implemented | ❌ Missing | No collection-level truncation |
| Commit (batch writes) | Commit + BatchWrite | ✅ Implemented | `write.go` lines 89-120, with per-Write tracking |
| Transform tracking | Delta tracking | ✅ Implemented | `trackWrite()` handles `Write_Transform` at line 145 |
| RETURNING clause | N/A | ➖ N/A | Firestore returns the created/updated document directly |
| Hydration system | Not implemented | ❌ Missing | No prod-to-shadow row copying before writes |
| **DDL & Schema Tracking** | | | |
| DDL execution on shadow | N/A | ➖ N/A | Firestore is schema-less |
| Schema change tracking | N/A (schema-less) | ➖ N/A | No column add/drop/rename/type-change concept |
| Query rewriting for schema diffs | N/A | ➖ N/A | Not applicable |
| **Transaction Management** | | | |
| BEGIN (dual-backend) | BeginTransaction (shadow only) | ⚠️ Partial | Classified but forwarded to shadow only; no prod transaction |
| COMMIT (dual-backend, promote staged) | Commit (shadow only) | ⚠️ Partial | Commit classified as write; no staged delta promotion |
| ROLLBACK (dual-backend, discard staged) | Rollback (shadow only) | ⚠️ Partial | No staged delta rollback |
| Savepoint system | Not implemented | ❌ Missing | No savepoint support |
| Staged delta system | Not implemented | ❌ Missing | All deltas committed immediately; no staging |
| **Foreign Key Enforcement** | | | |
| FK discovery | N/A | ➖ N/A | Firestore has no foreign key constraints |
| Parent row validation | N/A | ➖ N/A | Not applicable |
| CASCADE/RESTRICT enforcement | N/A | ➖ N/A | Not applicable |
| **Protocol Handling** | | | |
| pgwire protocol (simple + extended) | gRPC protocol (unary + streaming) | ✅ Implemented | Bidirectional stream forwarding |
| Extended query protocol (Parse/Bind/Execute) | N/A | ➖ N/A | gRPC uses protobuf, not SQL protocol |
| SSL/TLS negotiation | TLS + OAuth2 credentials | ✅ Implemented | `proxy.go` lines 206-231 |
| Statement cache | N/A | ➖ N/A | No prepared statements in Firestore gRPC |
| **Guard Rails** | | | |
| L1: validateRouteDecision | validateRouteDecision | ✅ Implemented | `guard.go` lines 15-40 |
| L2: SafeProdConn / method check | guardProdMethod | ✅ Implemented | `guard.go` lines 45-64 |
| L3: Final assertion before dispatch | OpType check in handler | ✅ Implemented | `handler.go` lines 83-88 |
| **Initialization** | | | |
| Connection string parsing | Firestore URI parsing | ✅ Implemented | `connstr/connstr.go` |
| Prod connection | Firestore SDK client | ✅ Implemented | ADC + service account + emulator support |
| Shadow container creation | Docker emulator container | ✅ Implemented | `shadow/shadow.go` |
| Schema dump & apply | Collection detection + seeding | ✅ Implemented | Full doc seeding from prod |
| Extension handling | N/A | ➖ N/A | No extensions in Firestore |
| Sequence offset calculation | N/A | ➖ N/A | Firestore uses auto-generated doc IDs |
| Version detection | N/A | ➖ N/A | Emulator is version-agnostic |
| Read-replica resilience | Not implemented | ❌ Missing | No retry logic for transient errors |
| **State Persistence** | | | |
| Delta map persistence | Delta map persistence | ✅ Implemented | `delta.WriteDeltaMap()` called in `write.go` |
| Tombstone persistence | Tombstone persistence | ✅ Implemented | `delta.WriteTombstoneSet()` called in `write.go` |
| Schema registry persistence | Not implemented | ❌ Missing | Schema registry not written/loaded for Firestore |
| Config persistence | Config persistence | ✅ Implemented | `config.WriteConnConfig()` in `init.go` |
| Table metadata persistence | Collection metadata persistence | ✅ Implemented | `schema.WriteTables()` / `schema.ReadTables()` |

---

## 3. Operation Classification Gaps

### What is implemented
The classifier (`classify/classifier.go`) handles all core Firestore gRPC methods:
- **Read**: GetDocument, ListDocuments, RunQuery, RunAggregationQuery, BatchGetDocuments, Listen, PartitionQuery, ListCollectionIds
- **Write**: CreateDocument, UpdateDocument, DeleteDocument, Commit, Write, BatchWrite
- **Transaction**: BeginTransaction, Rollback

### Gaps

**3.1 No collection/table extraction from request bodies** (Medium priority)
- The classifier does not extract collection names from the gRPC request payloads. The `Classification.Tables` slice is always empty.
- This means the `core.Router` can never determine if specific collections are "affected" (have deltas/tombstones), causing it to always return `StrategyProdDirect` for reads.
- The proxy works around this in `resolveTargetSDK()` (`handler.go` lines 112-118) by checking `HasAnyDelta()` globally, but this is imprecise -- any delta in any collection triggers merged reads for all collections.
- **Impact**: Unnecessary merged reads on clean collections, or missed merged reads if the global check is too broad.

**3.2 RunAggregationQuery not handled in SDK path** (Medium priority)
- `RunAggregationQuery` is classified with `HasAggregate = true` but is not listed in the `resolveTargetSDK()` switch cases (`handler.go` lines 103-105, 113-114).
- It falls through to prod-direct raw forwarding, meaning aggregation results will not reflect shadow writes.

**3.3 Listen (real-time) not handled** (Low priority)
- `Listen` is classified as `OpRead/SubSelect` but is not handled in the SDK read path.
- It falls through to raw prod forwarding, which means real-time listeners will not see shadow changes.
- The PostgreSQL engine has `StrategyListenOnly` which forwards to prod only; Firestore does the same implicitly but without explicit handling.

**3.4 PartitionQuery not handled** (Low priority)
- `PartitionQuery` is classified as a read but not handled in the SDK path. Falls through to raw forwarding.
- This is used for parallel reads and may not need merged read treatment, but it should be explicitly considered.

**3.5 Write method not handled in SDK path** (Low priority)
- The `Write` gRPC method (real-time write stream) is classified as `OpWrite/SubOther` but is not included in `handleSDKWrite()` (`handler.go` line 125 only lists `Commit` and `BatchWrite`, not `Write`).
- Falls back to shadow raw forwarding, losing delta tracking.

---

## 4. Read Operation Gaps

### What is implemented
The `readHandler` in `read.go` implements merged reads for four methods:
1. **GetDocument** (lines 32-57): Check tombstone, then delta map, then prod. Single-document routing.
2. **BatchGetDocuments** (lines 62-123): Per-document tombstone/delta check. Iterates through requested documents.
3. **ListDocuments** (lines 127-151): Queries both backends, merges by document path, filters tombstones. Shadow wins on conflicts.
4. **RunQuery** (lines 172-193): Same dual-query + merge approach for structured queries.

### Gaps

**4.1 No query-level filter re-evaluation** (High priority)
- When RunQuery or ListDocuments merge results from prod and shadow, the merged result set may not correctly satisfy the original query's filters.
- Example: A document in shadow was updated such that it no longer matches the query's WHERE clause, but the merged read includes it because it came from shadow.
- The PostgreSQL engine re-evaluates WHERE clauses on patched rows (`where_eval.go`). Firestore has no equivalent.
- **Impact**: Stale or incorrect query results after updates.

**4.2 No ORDER BY preservation** (High priority)
- `mergeDocuments()` sorts by document path (`read.go` line 278), which is a lexicographic sort on the full resource path.
- `mergeQueryResults()` does not sort at all -- shadow results come first, then prod results.
- If the original query had an `orderBy` clause, the merged result will not respect it.
- The PostgreSQL engine re-applies ORDER BY after merging (`read_single.go`). No Firestore equivalent exists.
- **Impact**: Incorrect ordering for queries with explicit ordering.

**4.3 No LIMIT/OFFSET handling** (High priority)
- When merging ListDocuments or RunQuery results, there is no adjustment for `limit` or `offset` parameters.
- The PostgreSQL engine over-fetches from prod by `delta_count + tombstone_count` to account for filtered rows. Firestore does not.
- **Impact**: Merged queries with LIMIT may return fewer results than expected.

**4.4 RunAggregationQuery not merged** (Medium priority)
- `RunAggregationQuery` (COUNT, SUM, AVG over collections) is classified but not handled in the SDK read path.
- The PostgreSQL engine has an entire aggregate re-computation system. Firestore has nothing equivalent.
- **Impact**: Aggregation queries return stale prod-only results, ignoring shadow writes.

**4.5 Subcollection queries not fully handled** (Medium priority)
- `splitDocPath()` extracts the last `collection/docID` pair from a path, which works for subcollections.
- However, `extractCollectionFromQuery()` only reads the first `From` selector, which may not capture nested subcollection context.
- Delta/tombstone lookups use the extracted collection name, but subcollection names may collide across parent documents (e.g., `users/a/posts` and `users/b/posts` both map to collection `posts`).
- **Impact**: Ambiguous delta/tombstone tracking for subcollections.

**4.6 No pagination token handling** (Medium priority)
- Firestore uses `page_token` for paginated reads (ListDocuments) and cursor-based pagination for RunQuery (`startAt`/`endAt`).
- The merged read implementation does not handle pagination tokens from either backend.
- `ListDocumentsResponse.NextPageToken` is not set in the merged response.
- **Impact**: Paginated reads will not work correctly across merged backends.

**4.7 No read_time / transaction consistency** (Medium priority)
- BatchGetDocuments and RunQuery support `read_time` and `transaction` parameters for consistent reads.
- The merged read implementation does not coordinate read timestamps between prod and shadow.
- **Impact**: Potential inconsistency between prod and shadow snapshots during merged reads.

---

## 5. Write Operation Gaps

### What is implemented
The `writeHandler` in `write.go` handles:
1. **CreateDocument** (lines 24-42): Forward to shadow, track delta + mark inserted.
2. **UpdateDocument** (lines 45-62): Forward to shadow, track delta.
3. **DeleteDocument** (lines 65-85): Forward to shadow, track tombstone, remove from delta map.
4. **Commit** (lines 89-103): Forward to shadow, iterate through writes to track deltas/tombstones.
5. **BatchWrite** (lines 106-120): Same as Commit.

### Gaps

**5.1 No hydration system** (High priority)
- The PostgreSQL engine hydrates rows from prod into shadow before updates/deletes to ensure the shadow has a complete picture.
- For Firestore, UpdateDocument on shadow will fail if the document does not exist in shadow (it was not seeded, or was created after seeding).
- There is no mechanism to fetch a document from prod and insert it into shadow before applying an update.
- **Impact**: Updates to documents not in shadow (created in prod after init) will fail.

**5.2 UpdateDocument does not read-before-write** (High priority)
- When a Firestore UpdateDocument request uses `update_mask` (partial field update), the shadow emulator needs the full document to apply the mask correctly.
- If the document was not seeded into shadow, the update will fail or create a sparse document.
- The PostgreSQL engine's hydration path handles this; Firestore has no equivalent.
- **Impact**: Partial updates on non-seeded documents will produce incorrect results.

**5.3 No precondition handling** (Medium priority)
- Firestore write operations support preconditions (`current_document` with `exists` or `update_time` checks).
- UpdateDocument and DeleteDocument preconditions reference the document's current state in prod, but the operation executes against shadow.
- There is no logic to evaluate preconditions against the correct backend (prod if not delta, shadow if delta).
- **Impact**: Precondition checks may pass/fail incorrectly when the document state differs between backends.

**5.4 Commit transaction ID not translated** (Medium priority)
- `CommitRequest.Transaction` contains a transaction ID from `BeginTransaction`.
- If `BeginTransaction` was forwarded to shadow, the transaction ID is valid for shadow. But if it was forwarded to prod (in a pass-through scenario), the Commit to shadow would fail.
- There is no transaction ID translation or dual-backend transaction management.
- **Impact**: Transactional writes may fail due to invalid transaction IDs.

**5.5 No document transform hydration** (Medium priority)
- `Write_Transform` operations (server-side field transforms like `Increment`, `ArrayUnion`, `ServerTimestamp`) are tracked in the delta map but execute on shadow.
- If the base document is not in shadow, the transform may fail or produce unexpected results (e.g., incrementing a field that does not exist).
- **Impact**: Field transforms on non-seeded documents will fail or produce wrong values.

**5.6 Write stream (real-time) not handled** (Low priority)
- The `Write` gRPC method is a bidirectional streaming RPC for real-time writes.
- It is classified as `OpWrite/SubOther` but not listed in the `handleSDKWrite()` method switch (`handler.go` line 125).
- Falls back to raw shadow forwarding, losing delta/tombstone tracking.
- **Impact**: Real-time write stream mutations are invisible to merged reads.

---

## 6. Collection/Schema Gaps

### What is implemented
- **Collection detection** (`schema/schema.go` lines 14-33): Lists root-level collections from prod via `client.Collections()`.
- **Collection metadata** (`schema/persist.go`): Persists `CollectionMeta` with PKColumns (`["__name__"]`) and PKType (`"uuid"`).
- **Seeding** (`schema/schema.go` lines 41-91): Copies all documents from prod collections to shadow, batched in groups of 500.

### Gaps

**6.1 No subcollection detection** (High priority)
- `DetectCollections()` only lists root-level collections. Firestore's nested data model supports arbitrary subcollection hierarchies.
- Subcollection documents will not be seeded into shadow.
- Delta/tombstone tracking for subcollections may partially work (via `splitDocPath`), but there is no metadata or seeding for them.
- **Impact**: Subcollection data is missing from shadow; reads and writes to subcollections may behave incorrectly.

**6.2 No collection group support** (Medium priority)
- Firestore supports collection group queries that span all subcollections with the same ID across the entire database.
- There is no handling for `allDescendants` in `CollectionSelector` within `extractCollectionFromQuery()`.
- **Impact**: Collection group queries will not be correctly merged.

**6.3 No field-level schema tracking** (Low priority)
- Firestore is schema-less, but in practice documents in a collection tend to have consistent field shapes.
- The `CollectionMeta` struct only tracks PK info; there is no field-level metadata.
- This is arguably N/A since Firestore documents are self-describing (protobuf `MapValue`), but field-level tracking could enable smarter merge behavior.

**6.4 No seeding of large collections** (Medium priority)
- `DefaultSeedLimit` is set to 0 (unlimited), meaning all documents in every collection are seeded.
- For large collections, this could be extremely slow or cause memory/disk issues on the emulator.
- The PostgreSQL engine only does schema-only shadow; Firestore seeds all data because the emulator has no schema dump/restore.
- **Impact**: Init may time out or fail on large Firestore databases.

**6.5 PKType always set to "uuid"** (Low priority)
- `DetectCollections()` sets `PKType = "uuid"` for all collections (`schema/schema.go` line 28), but Firestore document IDs are arbitrary strings, not UUIDs.
- This is a cosmetic issue but may cause confusion if PKType is used for logic elsewhere.

---

## 7. Transaction Management Gaps

### What is implemented
- `BeginTransaction` classified as `OpTransaction/SubBegin` (routed to shadow via `resolveTargetLegacy`).
- `Rollback` classified as `OpTransaction/SubRollback` (routed to shadow).
- `Commit` classified as `OpWrite/SubOther` (handled by SDK write path).

### Gaps

**7.1 No dual-backend transaction coordination** (High priority)
- The PostgreSQL engine begins transactions on both backends (shadow gets the original BEGIN; prod gets `BEGIN ISOLATION LEVEL REPEATABLE READ`).
- Firestore only forwards `BeginTransaction` to shadow. Reads from prod during the transaction are not transactionally isolated.
- **Impact**: Read-your-writes within transactions may be inconsistent when reads hit prod.

**7.2 No staged delta system** (High priority)
- The PostgreSQL engine uses `Stage()` / `Commit()` / `Rollback()` on the delta map to ensure transactional delta visibility.
- Firestore writes immediately add to the delta map (non-staged). If a transaction is rolled back, the delta entries remain.
- **Impact**: Rolled-back writes remain visible in subsequent merged reads.

**7.3 No schema registry snapshot/restore** (Medium priority)
- The PostgreSQL engine snapshots the schema registry on BEGIN and restores on ROLLBACK.
- While Firestore is schema-less, the schema registry is still injected (`handler.go`). If it were ever used, there would be no snapshot/restore on rollback.
- **Impact**: Currently N/A since schema registry is not actively used, but becomes relevant if schema tracking is added.

**7.4 No savepoint support** (Low priority)
- Firestore does not natively support savepoints.
- The PostgreSQL engine has a stack-based savepoint system. This is not applicable to Firestore's transaction model.
- **Impact**: N/A -- Firestore transactions are all-or-nothing.

**7.5 BeginTransaction response not surfaced** (Medium priority)
- `BeginTransaction` is forwarded to shadow via raw forwarding, but the resulting transaction ID is not intercepted.
- The proxy cannot track which transaction IDs are active or map them between backends.
- **Impact**: No ability to correlate transaction IDs for hybrid read/write operations.

---

## 8. Protocol-Level Gaps

### What is implemented
- **gRPC reverse proxy** (`proxy.go`): `grpc.UnknownServiceHandler` catches all methods.
- **Raw codec** (`proxy.go` lines 245-267): Opaque frame forwarding via `rawCodec`.
- **Bidirectional streaming** (`handler.go` lines 413-457): `forward()` copies frames in both directions.
- **TLS + OAuth2** (`proxy.go` lines 206-231): Production authentication with service account credentials.
- **SDK clients** (`sdk.go`): Parallel SDK connections for typed operations.

### Gaps

**8.1 No streaming RPC handling for Listen** (Medium priority)
- `Listen` is a server-streaming RPC that maintains a persistent connection for real-time updates.
- Raw forwarding works for prod-only listeners, but merged reads cannot intercept live snapshot changes.
- **Impact**: Real-time listeners will not reflect shadow writes.

**8.2 No client-side streaming for Write** (Medium priority)
- The `Write` RPC is a bidirectional streaming method used for real-time writes.
- The proxy's frame forwarder handles this at the raw level, but delta tracking is lost.
- **Impact**: Write stream mutations are not tracked.

**8.3 No gRPC metadata propagation** (Low priority)
- The proxy does not explicitly propagate gRPC metadata (headers, trailers) between client and backend.
- `grpc.ServerStream` and `grpc.ClientStream` handle metadata somewhat implicitly, but there may be edge cases where auth tokens, request IDs, or other metadata are dropped.
- **Impact**: Potential auth failures or missing tracing context.

**8.4 No gRPC health checking** (Low priority)
- The proxy does not implement the gRPC health checking protocol (`grpc.health.v1.Health/Check`).
- Client libraries that depend on health checks for load balancing may not work correctly.
- **Impact**: Client-side health-check-based routing may fail.

---

## 9. Security Rules / Access Gaps

### What is implemented
- **Service account authentication** (`proxy.go` lines 218-226): OAuth2 per-RPC credentials from service account JSON.
- **ADC support** (`engine.go` line 154, `init.go`): Falls back to Application Default Credentials when no explicit credentials file.
- **Emulator detection** (`connstr.go` line 126-128): Disables TLS/auth for local emulator connections.

### Gaps

**9.1 No Firestore Security Rules awareness** (Medium priority)
- Firestore Security Rules govern access at the document/collection level in production.
- The proxy bypasses security rules because it uses admin SDK credentials (service account), not client-side auth.
- If the application under test relies on security rules to restrict access, the proxy may return documents that the real client would not be able to read.
- **Impact**: Security rules are effectively bypassed; test behavior may differ from production.

**9.2 No per-document access control simulation** (Low priority)
- Related to 9.1 -- there is no mechanism to simulate security rule evaluation on the proxy side.
- This is a fundamental limitation since the emulator does not enforce security rules by default.
- **Impact**: Low -- security rules testing is typically done separately.

**9.3 No token refresh handling** (Low priority)
- OAuth2 tokens from `oauth.NewServiceAccountFromFile()` have a limited lifetime.
- If the proxy runs for an extended period, tokens may expire without refresh.
- The gRPC `PerRPCCredentials` interface should handle auto-refresh, but this has not been explicitly verified.
- **Impact**: Potential auth failures on long-running proxy sessions.

---

## 10. Guard Rail Gaps

### What is implemented
The Firestore engine implements all three layers of the PostgreSQL write guard:

1. **L1 - `validateRouteDecision()`** (`guard.go` lines 15-40): Blocks writes/DDL from `StrategyProdDirect`, overrides to `StrategyShadowWrite`.
2. **L2 - `guardProdMethod()`** (`guard.go` lines 45-64): Blocks known write gRPC methods from reaching prod by method name.
3. **L3 - OpType check** (`handler.go` lines 83-88): Final assertion before prod dispatch checks `OpWrite` / `OpDDL`.

### Gaps

**10.1 No guard for Write (streaming) method** (Medium priority)
- The `Write` gRPC method is included in `IsWriteMethod()` (`classifier.go` line 101), so L2 would block it.
- However, since `Write` is a streaming RPC, the frame forwarder would need to intercept it before the first frame is forwarded.
- If the `Write` stream is opened and frames are forwarded before classification occurs, the guard may not fire.
- **Impact**: Potential write leak to prod via streaming Write RPC.

**10.2 No audit log for blocked operations** (Low priority)
- The PostgreSQL engine uses SQLSTATE `MR001` for mori write guard errors with structured error responses.
- The Firestore engine uses `gRPC PermissionDenied` with message strings, but does not persist an audit log of blocked operations.
- **Impact**: Harder to debug write guard activations in production.

---

## 11. Initialization Gaps

### What is implemented
The init pipeline (`init.go`) follows a 7-step process:
1. Parse connection string
2. Connect to prod Firestore
3. Detect root-level collections
4. Create `.mori` directory
5. Pull and start emulator Docker container
6. Seed shadow emulator from prod
7. Persist config and collection metadata

### Gaps

**11.1 No subcollection seeding** (High priority)
- `DetectCollections()` only lists root-level collections.
- Subcollection data is not seeded into the shadow emulator.
- This is arguably the most impactful initialization gap -- any application using subcollections will have incomplete shadow data.
- **Impact**: Shadow emulator is missing all subcollection data.

**11.2 No incremental seeding / re-seeding** (Medium priority)
- If the proxy is restarted, there is no mechanism to detect new documents or collections in prod and seed them.
- The PostgreSQL engine does schema-only shadow and hydrates on demand; Firestore seeds all data upfront with no refresh mechanism.
- **Impact**: Shadow becomes stale if new documents are added to prod after initialization.

**11.3 No health check on emulator reuse** (Medium priority)
- The PostgreSQL engine has health check logic when reusing containers (`optimize/docker` as seen in recent commits).
- The Firestore shadow manager has `IsRunning()` but the init pipeline always creates a new container.
- There is no logic to reuse an existing container or verify its health.
- **Impact**: Each init creates a new container, even if one already exists for the same project.

**11.4 No image pull caching** (Low priority)
- `Pull()` always runs `docker pull`, even if the image is already cached.
- The PostgreSQL engine skips pulls for cached images.
- **Impact**: Slower initialization on repeated runs.

**11.5 No database ID support in seeding** (Low priority)
- The connection string supports `database_id` for multi-database Firestore projects, but the init pipeline always uses the client's default database.
- If the `database_id` is specified, it should be used when creating the Firestore client.
- The `databaseID` field in `readHandler` is hardcoded to `"(default)"` (`handler.go` line 176).
- **Impact**: Multi-database Firestore projects may not work correctly.

---

## 12. Priority Recommendations

### P0 -- Critical (blocks basic correctness)

1. **Implement hydration for UpdateDocument** (`write.go`)
   - Before forwarding an UpdateDocument to shadow, check if the document exists in shadow. If not, fetch from prod and insert into shadow first.
   - This is the Firestore analog of the PostgreSQL `StrategyHydrateAndWrite`.
   - Files: `/internal/engine/firestore/proxy/write.go` lines 45-62.

2. **Implement staged delta system for transactions** (`write.go`, `handler.go`)
   - Use the existing `delta.Map.Stage()` / `Commit()` / `Rollback()` methods.
   - On `BeginTransaction`, start staging. On `Commit`, promote. On `Rollback`, discard.
   - Files: `/internal/engine/firestore/proxy/write.go`, `/internal/engine/firestore/proxy/handler.go`.

3. **Fix ORDER BY preservation in merged reads** (`read.go`)
   - Parse the `StructuredQuery.OrderBy` field from RunQuery requests and re-sort merged results.
   - Files: `/internal/engine/firestore/proxy/read.go` lines 172-193, 286-335.

4. **Fix LIMIT handling in merged reads** (`read.go`)
   - Over-fetch from prod to account for tombstoned/delta'd rows that will be filtered.
   - Apply the original limit after merging.
   - Files: `/internal/engine/firestore/proxy/read.go` lines 127-151, 172-193.

### P1 -- High (significant functionality gaps)

5. **Add collection name extraction to classifier**
   - Parse the gRPC request body in `Classify()` to extract collection names and populate `Classification.Tables`.
   - This enables precise per-collection delta checking in the router instead of the global `HasAnyDelta()` workaround.
   - Files: `/internal/engine/firestore/classify/classifier.go`, `/internal/engine/firestore/proxy/handler.go` lines 95-133.

6. **Add subcollection detection and seeding**
   - Walk the document tree during init to discover subcollections.
   - Seed subcollection documents into shadow.
   - Files: `/internal/engine/firestore/schema/schema.go`, `/internal/engine/firestore/init.go`.

7. **Handle RunAggregationQuery in SDK read path**
   - Add `RunAggregationQuery` to the merged read handler.
   - Either forward to both backends and re-aggregate, or materialize base data and re-run the aggregation.
   - Files: `/internal/engine/firestore/proxy/handler.go`, `/internal/engine/firestore/proxy/read.go`.

8. **Add query-level filter re-evaluation for merged results**
   - After merging RunQuery results, re-evaluate the `StructuredQuery.Where` filter on merged documents.
   - Remove documents that no longer match the filter after patching from shadow.
   - Files: `/internal/engine/firestore/proxy/read.go`.

### P2 -- Medium (correctness improvements)

9. **Handle pagination tokens in merged reads**
   - Track pagination state across both backends for ListDocuments.
   - Files: `/internal/engine/firestore/proxy/read.go`.

10. **Add precondition evaluation against correct backend**
    - For UpdateDocument/DeleteDocument with preconditions, check the precondition against prod (if not delta) or shadow (if delta).
    - Files: `/internal/engine/firestore/proxy/write.go`.

11. **Add delta tracking for Write (streaming) RPC**
    - Intercept the bidirectional Write stream, parse write operations, and track deltas/tombstones.
    - Files: `/internal/engine/firestore/proxy/handler.go`.

12. **Add emulator container reuse and health checking**
    - Check for existing containers before creating new ones during init.
    - Files: `/internal/engine/firestore/shadow/shadow.go`, `/internal/engine/firestore/init.go`.

13. **Add read-time consistency for merged reads**
    - Coordinate read timestamps between prod and shadow queries.
    - Files: `/internal/engine/firestore/proxy/read.go`.

### P3 -- Low (nice-to-have improvements)

14. **Add collection group query handling**
    - Detect `allDescendants: true` in `CollectionSelector` and handle merged reads across collection groups.
    - Files: `/internal/engine/firestore/proxy/read.go`.

15. **Add Listen (real-time) shadow awareness**
    - Intercept Listen streams to inject shadow changes into real-time updates.
    - Files: `/internal/engine/firestore/proxy/handler.go`.

16. **Add gRPC metadata propagation**
    - Explicitly copy headers/trailers between client and backend streams.
    - Files: `/internal/engine/firestore/proxy/handler.go`.

17. **Fix PKType in collection metadata**
    - Use `"string"` instead of `"uuid"` for Firestore document IDs.
    - Files: `/internal/engine/firestore/schema/schema.go` line 28.

18. **Add Docker image pull caching**
    - Skip `docker pull` if the image exists locally.
    - Files: `/internal/engine/firestore/shadow/shadow.go`.

19. **Add multi-database support**
    - Use `databaseID` from connection string throughout the proxy, not hardcoded `"(default)"`.
    - Files: `/internal/engine/firestore/proxy/handler.go` line 176, `/internal/engine/firestore/init.go`.
