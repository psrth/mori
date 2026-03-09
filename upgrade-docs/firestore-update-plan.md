# Firestore Engine Update Plan

This document is the implementation plan for bringing the Firestore engine to full feature parity with the PostgreSQL engine's **conceptual equivalents**. Since Firestore is a NoSQL document database with a fundamentally different data model, this plan maps PostgreSQL features to their Firestore-native equivalents. Agents implementing these changes should **always refer to the PostgreSQL implementation** as the reference for understanding the *intent* behind each feature, then adapt for gRPC protocol and document semantics.

---

## Guiding Principles

1. **Map PostgreSQL concepts to Firestore equivalents.** Not every postgres feature has a 1:1 mapping. Rows → documents, tables → collections, PKs → document paths, SQL WHERE → StructuredQuery filters. Focus on the same *outcomes*: correct merged reads, accurate write tracking, and prod safety.
2. **Hydrate before mutate.** The core postgres principle applies: before any shadow write, ensure the document exists in the shadow emulator. Fetch from prod if needed.
3. **Prod safety is non-negotiable.** The three-layer guard is already solid — maintain it.
4. **Document path is the universal key.** Firestore's `projects/{project}/databases/{db}/documents/{collection}/{docID}` path serves as both the PK and the routing key. The `(collection, docID)` pair maps to `(table, pk)` in SQL engines.
5. **Schema-less doesn't mean no tracking.** While Firestore has no DDL, collection-level metadata (which collections have deltas/tombstones) must still be tracked precisely.

---

## P0 — Critical Correctness

### 1.1 Document Hydration System (HydrateAndWrite)

**Problem:** UpdateDocument on shadow fails if the document doesn't exist in shadow (not seeded, or created in prod after init). Partial updates via `update_mask` produce sparse documents without the full base state.

**Implementation:**
- New file: `proxy/hydrate.go`
- `hydrateDocument(collection, docID string) error`:
  1. Check delta map — if document is already tracked, skip (already in shadow)
  2. Check tombstone set — if tombstoned, skip (intentionally deleted)
  3. Fetch document from prod via SDK: `prodClient.Collection(collection).Doc(docID).Get(ctx)`
  4. If document exists in prod, write to shadow emulator via SDK: `shadowClient.Collection(collection).Doc(docID).Set(ctx, data)`
  5. Do NOT add to delta map (hydration is not a user mutation)
- Call `hydrateDocument()` before:
  - `UpdateDocument` (always)
  - `DeleteDocument` (for return data if needed)
  - Transform operations in Commit/BatchWrite
- Handle subcollection paths: extract collection and docID from full resource path
- **Reference:** `postgres/proxy/write_update.go` `hydrateRow()` — same concept, different protocol

### 1.2 Staged Delta System for Transactions

**Problem:** All deltas are committed immediately. If a transaction is rolled back, delta entries remain, causing merged reads to return stale shadow data.

**Implementation:**
- In `proxy/handler.go`, intercept `BeginTransaction`:
  1. Forward to shadow
  2. Call `deltaMap.Stage()` and `tombstoneSet.Stage()`
  3. Store the transaction ID → staged state mapping
- In `proxy/write.go`, during a transaction:
  - All delta/tombstone writes go to the staged area (this happens automatically after `Stage()`)
- On `Commit`:
  1. Forward to shadow
  2. Call `deltaMap.Commit()` and `tombstoneSet.Commit()`
  3. Persist delta/tombstone state
- On `Rollback`:
  1. Forward to shadow
  2. Call `deltaMap.Rollback()` and `tombstoneSet.Rollback()`
- Track active transaction IDs to correlate Commit/Rollback with the correct staged state
- **Reference:** `postgres/proxy/txn.go` staged delta system

### 1.3 ORDER BY Preservation in Merged Reads

**Problem:** `mergeQueryResults()` does not respect the original query's `orderBy` clause. Shadow results come first, then prod results, in arbitrary order.

**Implementation:**
- In `proxy/read.go`, for `RunQuery`:
  1. Extract `StructuredQuery.OrderBy` from the request protobuf
  2. After merging results, re-sort the merged document list according to the `OrderBy` fields
  3. Handle ascending/descending per field
  4. Handle DynamicValue comparison for different Firestore value types (string, number, timestamp, reference, etc.)
- For `ListDocuments`:
  - Default sort is by document path (already done via `sort.Slice` in `mergeDocuments`)
  - If `orderBy` is specified in the request, apply it
- **Reference:** `postgres/proxy/read_single.go` ORDER BY re-sort logic

### 1.4 LIMIT/OFFSET Handling in Merged Reads

**Problem:** No adjustment for `limit`/`offset` when merging. Merged queries with limits may return fewer results than expected.

**Implementation:**
- In `proxy/read.go`, for `RunQuery` and `ListDocuments`:
  1. Extract `limit` (and `offset`/`startAt`/`endAt`) from the request
  2. Over-fetch from prod: request `limit + delta_count + tombstone_count` rows
  3. After merging and filtering (tombstones removed, deltas patched), apply the original `limit`
  4. For pagination: adjust `startAt`/`endAt` cursors based on merged result boundaries
- For `ListDocuments`: extract `page_size` from request, over-fetch accordingly
- **Reference:** `postgres/proxy/read_single.go` LIMIT overfetch logic

### 1.5 Collection Name Extraction in Classifier

**Problem:** `Classification.Tables` is always empty. The router can never determine if specific collections are affected. The proxy works around this with `HasAnyDelta()` globally, causing unnecessary merged reads.

**Implementation:**
- In `classify/classifier.go`, extract collection names from the gRPC method + request path:
  - For `GetDocument`, `UpdateDocument`, `DeleteDocument`: extract from the `name` field (document resource path)
  - For `ListDocuments`, `RunQuery`: extract from the `parent` field + `CollectionSelector`
  - For `BatchGetDocuments`: extract from the `documents` field (list of paths)
  - For `Commit`, `BatchWrite`: extract from each `Write.document.name` path
  - For `CreateDocument`: extract from the `parent` + `collection_id` fields
- Populate `Classification.Tables` with extracted collection names
- Remove `HasAnyDelta()` workaround in `resolveTargetSDK()` — use per-collection delta check via standard router
- Helper: `extractCollectionFromPath(resourcePath string) string` — parse `projects/x/databases/y/documents/{collection}/...`
- **Reference:** `postgres/classify/classifier.go` table extraction — conceptual parallel

---

## P1 — High Priority

### 2.1 Subcollection Detection and Seeding

**Problem:** `DetectCollections()` only lists root-level collections. Subcollection data is missing from shadow, causing hydration failures and incomplete merged reads.

**Implementation:**
- In `schema/schema.go`, add `DetectSubcollections()`:
  1. For each root-level collection, list all documents
  2. For each document, call `doc.Ref.Collections()` to discover subcollections
  3. Recursively walk to a configurable depth (default: 3 levels)
  4. Track discovered subcollection paths in metadata
- In `schema/schema.go` `SeedShadow()`:
  1. After seeding root collections, seed discovered subcollections
  2. Use the same batch-of-500 approach
  3. Track subcollection parent paths for delta/tombstone routing
- Add `MaxSeedDepth int` configuration (default 3)
- **Reference:** No direct postgres equivalent — Firestore-specific

### 2.2 RunAggregationQuery Handling

**Problem:** `RunAggregationQuery` is classified with `HasAggregate = true` but falls through to prod-direct raw forwarding. Aggregation results don't reflect shadow writes.

**Implementation:**
- In `proxy/handler.go` `resolveTargetSDK()`, add `RunAggregationQuery` to the SDK read methods
- In `proxy/read.go`, add `handleRunAggregationQuery()`:
  1. Extract the base `StructuredQuery` from the aggregation request
  2. Run the base query through the existing merged read pipeline (RunQuery path)
  3. Apply the aggregation in-memory on the merged results:
     - COUNT: count merged documents
     - SUM: sum the specified field values
     - AVG: compute average of field values
  4. Build and return an `RunAggregationQueryResponse` with the computed results
- **Reference:** `postgres/proxy/aggregate.go` — same re-aggregation concept

### 2.3 Query Filter Re-Evaluation on Merged Results

**Problem:** After merging RunQuery results from prod and shadow, documents that were updated in shadow may no longer match the original query's WHERE filter, but are still included.

**Implementation:**
- In `proxy/read.go`, after merging RunQuery results:
  1. Extract the `StructuredQuery.Where` composite filter from the request
  2. For each merged document (especially shadow-sourced ones), evaluate the filter against the document's field values
  3. Remove documents that don't satisfy the filter
- Implement `evaluateFilter(doc *firestore.DocumentSnapshot, filter *StructuredQuery_Filter) bool`:
  - Handle `CompositeFilter` (AND/OR)
  - Handle `UnaryFilter` (IS NULL, IS NOT NULL, IS NAN)
  - Handle `FieldFilter` with operators: EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, ARRAY_CONTAINS, IN, NOT_IN, ARRAY_CONTAINS_ANY
  - Type-aware comparison for Firestore value types
- **Reference:** `postgres/proxy/where_eval.go` — same concept, different data model

### 2.4 Precondition Evaluation Against Correct Backend

**Problem:** UpdateDocument/DeleteDocument preconditions (`current_document.exists`, `current_document.update_time`) reference the document's state in prod, but the operation executes against shadow.

**Implementation:**
- In `proxy/write.go`, before forwarding UpdateDocument/DeleteDocument:
  1. Check if preconditions are set in the request
  2. Determine the correct backend to check:
     - If document is in delta map → check shadow
     - If document is tombstoned → treat as not existing
     - Otherwise → check prod
  3. Evaluate precondition:
     - `exists = true`: document must exist in the determined backend
     - `exists = false`: document must not exist
     - `update_time`: document's update time must match
  4. If precondition fails, return `gRPC FailedPrecondition` error without forwarding
- **Reference:** No direct postgres equivalent — Firestore-specific

### 2.5 Transaction ID Tracking and Coordination

**Problem:** `BeginTransaction` is forwarded to shadow, but the transaction ID is not tracked. No ability to correlate transactions for hybrid read/write operations.

**Implementation:**
- In `proxy/handler.go`, intercept `BeginTransaction` response:
  1. Parse the `BeginTransactionResponse` to extract the transaction ID
  2. Store in a `activeTransactions map[string]bool` (transaction ID → active)
- In `proxy/write.go`, on `Commit`:
  1. Extract `CommitRequest.Transaction` ID
  2. Look up in `activeTransactions` to verify it's a known shadow transaction
  3. After commit, remove from `activeTransactions`
- In `proxy/read.go`, for reads within a transaction:
  1. If the request includes a `transaction` field, check if it's in `activeTransactions`
  2. If so, route reads through the merged read path (shadow data is transactionally consistent)
- **Reference:** `postgres/proxy/txn.go` transaction tracking

### 2.6 Write Stream (Bidirectional) Delta Tracking

**Problem:** The `Write` gRPC method (real-time bidirectional streaming) is classified as `OpWrite/SubOther` but not handled in `handleSDKWrite()`. Falls back to raw forwarding, losing delta tracking.

**Implementation:**
- In `proxy/handler.go`, add `Write` to the SDK write method switch
- Implement `handleWriteStream()`:
  1. For each incoming write frame in the stream, parse the `WriteRequest`
  2. Extract writes (same as `Commit` path: `Write_Update`, `Write_Delete`, `Write_Transform`)
  3. Forward to shadow stream
  4. Track deltas/tombstones for each write operation
  5. Forward shadow's response frames back to client
- Note: This is a persistent bidirectional stream — need careful goroutine management
- **Reference:** `proxy/write.go` Commit handler — same delta tracking logic, adapted for streaming

---

## P2 — Advanced Features

### 3.1 Pagination Token Handling in Merged Reads

**Problem:** `ListDocumentsResponse.NextPageToken` is not set in merged responses. Paginated reads break.

**Implementation:**
- In `proxy/read.go` `handleListDocuments()`:
  1. Track pagination state per collection: maintain a cursor position for both prod and shadow
  2. When building merged response, compute the next page token based on the last document in the merged result
  3. Set `NextPageToken` on the response if more results exist in either backend
- For `RunQuery` with `startAt`/`endAt`:
  1. After merging and sorting, compute cursor values from the last returned document
  2. Encode cursor position for subsequent requests
- **Reference:** No direct postgres equivalent — Firestore pagination uses tokens, not OFFSET

### 3.2 Collection Group Query Support

**Problem:** Collection group queries (`allDescendants: true` in `CollectionSelector`) span all subcollections with the same ID. Not handled in merged reads.

**Implementation:**
- In `proxy/read.go`, when processing `RunQuery`:
  1. Check if `CollectionSelector.AllDescendants` is true
  2. If so, query both prod and shadow for the collection group
  3. Merge results across all subcollections, using full document path as the dedup key
  4. Filter tombstones and patch deltas across all subcollection instances
- Delta/tombstone lookups must use the full path (not just collection name) to avoid cross-subcollection confusion
- **Reference:** No postgres equivalent — Firestore-specific

### 3.3 Read-Time Consistency for Merged Reads

**Problem:** No coordination of read timestamps between prod and shadow. Potential inconsistency during merged reads.

**Implementation:**
- In `proxy/read.go`, for `BatchGetDocuments` and `RunQuery`:
  1. Extract `read_time` from the request if present
  2. Apply the same `read_time` to both prod and shadow queries
  3. For shadow (emulator), set the request's read_time on the shadow SDK call
  4. For prod, forward the original `read_time`
- If no `read_time` is specified, capture a consistent timestamp before querying either backend
- **Reference:** `postgres/proxy/txn.go` REPEATABLE READ — conceptual equivalent

### 3.4 Emulator Container Reuse and Health Checking

**Problem:** Each init creates a new emulator container even if one already exists. No health check on existing containers.

**Implementation:**
- In `shadow/shadow.go`:
  1. Before creating a new container, check if one already exists for the project:
     `docker ps -a --filter name=mori-firestore-{project}`
  2. If found and running, verify health via HTTP health endpoint
  3. If found but stopped, restart it
  4. Only create new container if none exists
- In `init.go`:
  1. Add `--reuse` flag or auto-detect existing containers
  2. Skip seeding if container already has data (check for marker file or collection count)
- **Reference:** `postgres/shadow` Docker container reuse logic

### 3.5 Incremental/On-Demand Seeding

**Problem:** Shadow seeding is all-or-nothing at init. Documents created in prod after init are missing from shadow, causing hydration to be the only path.

**Implementation:**
- Option A (Lazy hydration — recommended): Rely on the hydration system (1.1) to fetch documents on demand. Accept that the first access to a non-seeded document triggers a prod fetch.
- Option B (Background re-seed): Implement a background goroutine that periodically checks prod for new documents and seeds them into shadow.
- For Option A: No additional implementation needed beyond 1.1
- For Option B: Add `proxy/seeder.go` with configurable interval
- **Reference:** No direct postgres equivalent — postgres shadow is schema-only and always hydrates on demand

### 3.6 Document Transform Hydration

**Problem:** `Write_Transform` operations (Increment, ArrayUnion, ArrayRemove, ServerTimestamp) execute on shadow but may fail if the base document isn't in shadow.

**Implementation:**
- In `proxy/write.go` `trackWrite()`, when processing `Write_Transform`:
  1. Extract the document path from the transform
  2. Call `hydrateDocument()` before forwarding the transform to shadow
  3. This ensures the base document exists so transforms (e.g., increment a field) have valid base values
- **Reference:** `proxy/hydrate.go` (from 1.1)

---

## P3 — Polish & Edge Cases

### 4.1 Listen (Real-Time) Shadow Awareness

**Problem:** `Listen` is classified as a read but falls through to raw prod forwarding. Real-time listeners don't see shadow changes.

**Implementation:**
- Option A (Conservative — recommended): Keep `Listen` as prod-only. Document that real-time listeners reflect prod state only.
- Option B (Full): Intercept Listen stream, inject shadow deltas as synthetic `DocumentChange` events. Complex and error-prone.
- Recommend Option A for now. Add explicit `StrategyListenOnly` classification to make the routing intentional.
- **Reference:** `postgres/proxy/conn.go` StrategyListenOnly

### 4.2 PartitionQuery Handling

- `PartitionQuery` is used for parallel reads (splits a query into cursor ranges)
- Forward to prod only — partitioning metadata doesn't need shadow awareness
- Add explicit classification as `SubOther` → `StrategyProdDirect`
- **Reference:** No postgres equivalent

### 4.3 gRPC Metadata Propagation

- Explicitly copy gRPC headers and trailers between client and backend
- Use `grpc.SetHeader()` and `grpc.SetTrailer()` on the server stream
- Copy auth-related metadata from client requests to prod backend
- **Reference:** No postgres equivalent — gRPC-specific

### 4.4 PKType Correction

- Change `PKType = "uuid"` to `PKType = "string"` in `DetectCollections()`
- Firestore document IDs are arbitrary strings, not UUIDs
- **File:** `schema/schema.go` line 28
- **Reference:** Cosmetic fix

### 4.5 Docker Image Pull Caching

- Before `docker pull`, check if the image exists locally: `docker image inspect`
- Skip pull if image is already cached
- **File:** `shadow/shadow.go`
- **Reference:** `postgres/shadow` image caching

### 4.6 Multi-Database Support

- Replace hardcoded `"(default)"` database ID with value from connection string
- Use `databaseID` from `ConnConfig` throughout proxy
- **File:** `proxy/handler.go` line 176
- **Reference:** Firestore-specific

### 4.7 Seed Limit Configuration

- Add configurable `MaxSeedDocuments int` per collection
- For large collections, seed only the most recently updated N documents
- Order by `update_time` descending, take top N
- Default: 10,000 per collection
- **Reference:** No postgres equivalent — postgres shadow is schema-only

### 4.8 Guard for Write Stream

- Ensure the `Write` streaming RPC is properly guarded
- The L2 guard (`guardProdMethod`) blocks it by method name, but verify the frame forwarder doesn't race with classification
- Add explicit test: attempt a Write stream to prod and verify it's blocked
- **Reference:** `proxy/guard.go`

### 4.9 Audit Logging for Blocked Operations

- When a write operation is blocked by any guard layer, log:
  - Timestamp, method name, document path, guard layer that caught it
- Use structured logging (JSON)
- **Reference:** `postgres/proxy/guard.go` SQLSTATE MR001 error responses

### 4.10 Schema Registry Persistence

- The schema registry is injected but never persisted for Firestore
- Add `schema.WriteSchemaRegistry()` / `schema.ReadSchemaRegistry()` for collection-level state
- Persist: which collections have been truncated (fully shadowed), any future field-level tracking
- **Reference:** `postgres/schema/registry.go`

---

## Implementation Order

```
Phase 1 (P0 — correctness):
  1.1 Document hydration system
  1.2 Staged delta system for transactions
  1.3 ORDER BY preservation in merged reads
  1.4 LIMIT/OFFSET handling in merged reads
  1.5 Collection name extraction in classifier

Phase 2 (P1 — features):
  2.1 Subcollection detection and seeding
  2.2 RunAggregationQuery handling
  2.3 Query filter re-evaluation
  2.4 Precondition evaluation
  2.5 Transaction ID tracking
  2.6 Write stream delta tracking

Phase 3 (P2 — advanced):
  3.1 Pagination token handling
  3.2 Collection group query support
  3.3 Read-time consistency
  3.4 Emulator container reuse
  3.5 Incremental/on-demand seeding
  3.6 Document transform hydration

Phase 4 (P3 — polish):
  4.1-4.10 in any order
```

---

## Key Firestore vs PostgreSQL Concept Mapping

| PostgreSQL Concept | Firestore Equivalent | Notes |
|---|---|---|
| Table | Collection | Flat namespace (plus subcollections) |
| Row | Document | Self-describing (protobuf MapValue) |
| Primary Key | Document path (`collection/docID`) | Always a string; `__name__` in queries |
| Column | Field | Schema-less — fields vary per document |
| SQL WHERE | StructuredQuery.Where (CompositeFilter/FieldFilter) | Limited operators vs SQL |
| SQL ORDER BY | StructuredQuery.OrderBy | Must have index support |
| SQL LIMIT | StructuredQuery.Limit | Integer value wrapper |
| INSERT | CreateDocument | Creates new document with auto/manual ID |
| UPDATE | UpdateDocument + update_mask | Partial field updates |
| DELETE | DeleteDocument | With optional preconditions |
| UPSERT (ON CONFLICT) | Set() with merge option | Not directly in gRPC API — SDK concept |
| TRUNCATE | Delete all docs in collection | No atomic collection-level delete |
| BEGIN/COMMIT/ROLLBACK | BeginTransaction/Commit/Rollback | Single-document transactions or batched writes |
| Savepoints | N/A | Firestore transactions are all-or-nothing |
| DDL (CREATE/ALTER TABLE) | N/A | Schema-less — no DDL concept |
| Foreign Keys | N/A | No referential integrity in Firestore |
| pgwire protocol | gRPC protocol | Protobuf-based, not SQL wire protocol |
| SQL parser (pg_query_go) | gRPC method name matching | Method-based classification, not SQL parsing |
| RETURNING clause | Document returned in CreateDocument/UpdateDocument response | Built into Firestore API responses |
| CTID/ROWID | Document resource path | Natural unique identifier |
| Temp table materialization | N/A | Not applicable — no SQL execution layer |
| Window functions | N/A | Not applicable |
| CTEs/Derived tables | N/A | Not applicable |
| JOINs | N/A | No server-side joins in Firestore |
| Set operations | N/A | Not applicable |

**Key insight:** Many PostgreSQL features (JOINs, window functions, CTEs, set operations, DDL, FKs) simply don't apply to Firestore. The focus should be on: hydration correctness, transaction atomicity, merged read accuracy (ORDER BY, LIMIT, filter re-evaluation), and subcollection support. These are the features that translate to the document model.
