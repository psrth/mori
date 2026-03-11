package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"

	firestore "cloud.google.com/go/firestore/apiv1"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/mori-dev/mori/internal/core/delta"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

// readHandler implements document-level merged reads for Firestore.
type readHandler struct {
	prodClient          *firestore.Client
	shadowClient        *firestore.Client
	deltaMap            *delta.Map
	tombstones          *delta.TombstoneSet
	projectID           string
	databaseID          string
	verbose             bool
	activeTransactionFn func(string) bool // checks if a txn ID is active

	// readTime, when set, is used for all prod-side reads to provide snapshot
	// isolation within a transaction. Captured at BeginTransaction time.
	readTime *timestamppb.Timestamp
}

// getDocument performs a merged GetDocument: if the doc path is in the delta map,
// read from shadow; if tombstoned, return NOT_FOUND; otherwise read from prod.
func (h *readHandler) getDocument(ctx context.Context, req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	docPath := req.GetName()
	collection, fullDocKey := splitDocPathFull(docPath)

	// Check tombstone first — deleted docs should return NOT_FOUND.
	if collection != "" && h.tombstones.IsTombstoned(collection, fullDocKey) {
		if h.verbose {
			log.Printf("[firestore-read] GetDocument %s: tombstoned → NOT_FOUND", docPath)
		}
		return nil, status.Errorf(codes.NotFound, "document %q not found (tombstoned)", docPath)
	}

	// Check delta map — if modified, read from shadow.
	if collection != "" && h.deltaMap.IsDelta(collection, fullDocKey) {
		if h.verbose {
			log.Printf("[firestore-read] GetDocument %s: delta → shadow", fullDocKey)
		}
		return h.shadowClient.GetDocument(ctx, req)
	}

	// Default: read from prod.
	if h.verbose {
		log.Printf("[firestore-read] GetDocument %s: prod", docPath)
	}
	if h.readTime != nil {
		req.ConsistencySelector = &firestorepb.GetDocumentRequest_ReadTime{ReadTime: h.readTime}
	}
	return h.prodClient.GetDocument(ctx, req)
}

// batchGetDocuments performs merged reads for multiple documents at once.
// For each document, it checks tombstones and delta map to decide whether
// to read from shadow or prod.
func (h *readHandler) batchGetDocuments(ctx context.Context, req *firestorepb.BatchGetDocumentsRequest) ([]*firestorepb.BatchGetDocumentsResponse, error) {
	var results []*firestorepb.BatchGetDocumentsResponse

	for _, docPath := range req.GetDocuments() {
		collection, fullDocKey := splitDocPathFull(docPath)

		// Check tombstone — return "missing" result.
		if collection != "" && h.tombstones.IsTombstoned(collection, fullDocKey) {
			if h.verbose {
				log.Printf("[firestore-read] BatchGetDocuments %s: tombstoned → missing", fullDocKey)
			}
			results = append(results, &firestorepb.BatchGetDocumentsResponse{
				Result:  &firestorepb.BatchGetDocumentsResponse_Missing{Missing: docPath},
				ReadTime: nil,
			})
			continue
		}

		// Check delta — read from shadow.
		if collection != "" && h.deltaMap.IsDelta(collection, fullDocKey) {
			if h.verbose {
				log.Printf("[firestore-read] BatchGetDocuments %s: delta → shadow", fullDocKey)
			}
			getReq := &firestorepb.GetDocumentRequest{Name: docPath}
			doc, err := h.shadowClient.GetDocument(ctx, getReq)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					results = append(results, &firestorepb.BatchGetDocumentsResponse{
						Result: &firestorepb.BatchGetDocumentsResponse_Missing{Missing: docPath},
					})
					continue
				}
				return nil, err
			}
			results = append(results, &firestorepb.BatchGetDocumentsResponse{
				Result: &firestorepb.BatchGetDocumentsResponse_Found{Found: doc},
			})
			continue
		}

		// Default: read from prod.
		if h.verbose {
			log.Printf("[firestore-read] BatchGetDocuments %s: prod", docPath)
		}
		getReq := &firestorepb.GetDocumentRequest{Name: docPath}
		if h.readTime != nil {
			getReq.ConsistencySelector = &firestorepb.GetDocumentRequest_ReadTime{ReadTime: h.readTime}
		}
		doc, err := h.prodClient.GetDocument(ctx, getReq)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				results = append(results, &firestorepb.BatchGetDocumentsResponse{
					Result: &firestorepb.BatchGetDocumentsResponse_Missing{Missing: docPath},
				})
				continue
			}
			return nil, err
		}
		results = append(results, &firestorepb.BatchGetDocumentsResponse{
			Result: &firestorepb.BatchGetDocumentsResponse_Found{Found: doc},
		})
	}

	return results, nil
}

// listDocuments performs a merged ListDocuments: queries both backends, merges
// by document path (shadow wins on conflicts), filters tombstoned docs.
func (h *readHandler) listDocuments(ctx context.Context, req *firestorepb.ListDocumentsRequest) (*firestorepb.ListDocumentsResponse, error) {
	collection := extractCollectionFromParent(req.GetParent(), req.GetCollectionId())
	origPageSize := req.GetPageSize()

	// If no deltas or inserts for this collection, just query prod and filter tombstones.
	if !h.deltaMap.AnyTableDelta([]string{collection}) && !h.deltaMap.HasInserts(collection) {
		h.applyListDocumentsReadTime(req)
		prodDocs, err := collectListDocuments(ctx, h.prodClient, req)
		if err != nil {
			return nil, err
		}
		filtered := filterTombstoned(prodDocs, collection, h.tombstones)
		if origPageSize > 0 && int32(len(filtered)) > origPageSize {
			nextToken := ""
			if len(filtered) > int(origPageSize) {
				nextToken = filtered[origPageSize].GetName()
			}
			return &firestorepb.ListDocumentsResponse{
				Documents:     filtered[:origPageSize],
				NextPageToken: nextToken,
			}, nil
		}
		return &firestorepb.ListDocumentsResponse{Documents: filtered}, nil
	}

	// Over-fetch from prod if there's a page_size limit.
	prodReq := req
	if origPageSize > 0 {
		deltaCount := h.deltaMap.CountForTable(collection)
		tombstoneCount := h.tombstones.CountForTable(collection)
		overFetch := origPageSize + int32(deltaCount) + int32(tombstoneCount)
		prodReq = cloneListDocumentsRequestWithPageSize(req, overFetch)
	}

	// Query both backends via iterators.
	h.applyListDocumentsReadTime(prodReq)
	prodDocs, prodErr := collectListDocuments(ctx, h.prodClient, prodReq)
	shadowDocs, shadowErr := collectListDocuments(ctx, h.shadowClient, req)

	if prodErr != nil && shadowErr != nil {
		return nil, fmt.Errorf("both backends failed: prod=%v, shadow=%v", prodErr, shadowErr)
	}

	merged := mergeDocuments(prodDocs, shadowDocs, collection, h.deltaMap, h.tombstones)

	// Apply original page_size after merge.
	resp := &firestorepb.ListDocumentsResponse{}
	if origPageSize > 0 && int32(len(merged)) > origPageSize {
		resp.Documents = merged[:origPageSize]
		if len(merged) > int(origPageSize) {
			resp.NextPageToken = merged[origPageSize].GetName()
		}
	} else {
		resp.Documents = merged
	}

	return resp, nil
}

// listCollectionIds performs a merged ListCollectionIds: queries both prod and
// shadow, deduplicates collection IDs, and returns the union. This ensures that
// shadow-only collections (created by writes to new collections) are visible.
func (h *readHandler) listCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest) (*firestorepb.ListCollectionIdsResponse, error) {
	origPageSize := req.GetPageSize()

	// Collect from both backends via iterators.
	prodIds, prodErr := collectCollectionIds(ctx, h.prodClient, req)
	shadowIds, shadowErr := collectCollectionIds(ctx, h.shadowClient, req)

	if prodErr != nil && shadowErr != nil {
		return nil, fmt.Errorf("both backends failed: prod=%v, shadow=%v", prodErr, shadowErr)
	}

	// Merge: union of collection IDs from both backends.
	seen := make(map[string]bool)
	var merged []string

	for _, id := range prodIds {
		if !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}
	for _, id := range shadowIds {
		if !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}

	// Sort for deterministic ordering.
	sort.Strings(merged)

	// Apply original page_size after merge.
	resp := &firestorepb.ListCollectionIdsResponse{}
	if origPageSize > 0 && int32(len(merged)) > origPageSize {
		resp.CollectionIds = merged[:origPageSize]
		if len(merged) > int(origPageSize) {
			resp.NextPageToken = merged[origPageSize]
		}
	} else {
		resp.CollectionIds = merged
	}

	return resp, nil
}

// collectCollectionIds iterates through a ListCollectionIds result and collects all IDs.
func collectCollectionIds(ctx context.Context, client *firestore.Client, req *firestorepb.ListCollectionIdsRequest) ([]string, error) {
	iter := client.ListCollectionIds(ctx, req)
	var ids []string
	for {
		id, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// collectListDocuments iterates through a ListDocuments result and collects all documents.
func collectListDocuments(ctx context.Context, client *firestore.Client, req *firestorepb.ListDocumentsRequest) ([]*firestorepb.Document, error) {
	iter := client.ListDocuments(ctx, req)
	var docs []*firestorepb.Document
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return docs, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// runQuery performs a merged RunQuery by querying both backends and merging results.
// Returns a slice of RunQueryResponse (one per document), suitable for streaming back.
func (h *readHandler) runQuery(ctx context.Context, req *firestorepb.RunQueryRequest) ([]*firestorepb.RunQueryResponse, error) {
	collection := extractCollectionFromQuery(req)

	// Extract ORDER BY and LIMIT from the structured query for post-merge application.
	sq := req.GetStructuredQuery()
	var orderBy []*firestorepb.StructuredQuery_Order
	var origLimit int32
	if sq != nil {
		orderBy = sq.GetOrderBy()
		if sq.GetLimit() != nil {
			origLimit = sq.GetLimit().GetValue()
		}
	}

	// Collection group queries are handled correctly by splitDocPathFull:
	// delta/tombstone keys use the full document path, preventing collisions
	// when the same collection ID appears under different parent paths.

	// If no deltas for this collection, just query prod and filter tombstones.
	if !h.deltaMap.AnyTableDelta([]string{collection}) && !h.deltaMap.HasInserts(collection) {
		h.applyRunQueryReadTime(req)
		results, err := collectQueryResults(ctx, h.prodClient, req)
		if err != nil {
			return nil, err
		}
		filtered := filterTombstonedQueryResults(results, collection, h.tombstones)
		// Re-sort and apply limit if needed (tombstone removal may affect ordering).
		if len(orderBy) > 0 {
			sortQueryResults(filtered, orderBy)
		}
		if origLimit > 0 && int32(len(filtered)) > origLimit {
			filtered = filtered[:origLimit]
		}
		return filtered, nil
	}

	// Over-fetch from prod if there's a limit: request more to compensate for
	// merged/tombstoned documents.
	prodReq := req
	if origLimit > 0 {
		deltaCount := h.deltaMap.CountForTable(collection)
		tombstoneCount := h.tombstones.CountForTable(collection)
		overFetch := origLimit + int32(deltaCount) + int32(tombstoneCount)
		prodReq = cloneRunQueryRequestWithLimit(req, overFetch)
	}

	// Query both backends.
	h.applyRunQueryReadTime(prodReq)
	prodResults, prodErr := collectQueryResults(ctx, h.prodClient, prodReq)
	shadowResults, shadowErr := collectQueryResults(ctx, h.shadowClient, req)

	if prodErr != nil && shadowErr != nil {
		return nil, fmt.Errorf("both backends failed: prod=%v, shadow=%v", prodErr, shadowErr)
	}

	merged := mergeQueryResults(prodResults, shadowResults, collection, h.deltaMap, h.tombstones)

	// Re-evaluate WHERE filter on merged results: shadow documents may no longer
	// match the original query's filter after being updated.
	if sq != nil && sq.GetWhere() != nil {
		merged = filterByQuery(merged, sq.GetWhere())
	}

	// Re-sort merged results according to original ORDER BY.
	if len(orderBy) > 0 {
		sortQueryResults(merged, orderBy)
	}

	// Apply original LIMIT after merge.
	if origLimit > 0 && int32(len(merged)) > origLimit {
		merged = merged[:origLimit]
	}

	return merged, nil
}

// runAggregationQuery performs a merged RunAggregationQuery by running the base
// query through the merged read pipeline and re-computing aggregations in memory.
func (h *readHandler) runAggregationQuery(ctx context.Context, req *firestorepb.RunAggregationQueryRequest) ([]*firestorepb.RunAggregationQueryResponse, error) {
	aggQuery := req.GetStructuredAggregationQuery()
	if aggQuery == nil {
		// No structured aggregation query — forward to prod directly.
		h.applyAggregationQueryReadTime(req)
		return collectAggregationResults(ctx, h.prodClient, req)
	}

	baseQuery := aggQuery.GetStructuredQuery()
	if baseQuery == nil {
		h.applyAggregationQueryReadTime(req)
		return collectAggregationResults(ctx, h.prodClient, req)
	}

	// Build a RunQueryRequest from the base query.
	runReq := &firestorepb.RunQueryRequest{
		Parent:    req.GetParent(),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: baseQuery},
	}

	// Run through the merged query pipeline.
	queryResults, err := h.runQuery(ctx, runReq)
	if err != nil {
		return nil, err
	}

	// Compute aggregations in memory.
	aggResults := computeAggregations(queryResults, aggQuery.GetAggregations())

	return []*firestorepb.RunAggregationQueryResponse{
		{
			Result: aggResults,
		},
	}, nil
}

// collectAggregationResults runs an aggregation query and collects results.
func collectAggregationResults(ctx context.Context, client *firestore.Client, req *firestorepb.RunAggregationQueryRequest) ([]*firestorepb.RunAggregationQueryResponse, error) {
	stream, err := client.RunAggregationQuery(ctx, req)
	if err != nil {
		return nil, err
	}

	var results []*firestorepb.RunAggregationQueryResponse
	for {
		resp, err := stream.Recv()
		if err != nil {
			if isStreamDone(err) {
				break
			}
			return results, err
		}
		if resp != nil {
			results = append(results, resp)
		}
	}
	return results, nil
}

// computeAggregations applies aggregation operations over merged query results.
func computeAggregations(results []*firestorepb.RunQueryResponse, aggregations []*firestorepb.StructuredAggregationQuery_Aggregation) *firestorepb.AggregationResult {
	fields := make(map[string]*firestorepb.Value)

	for i, agg := range aggregations {
		alias := agg.GetAlias()
		if alias == "" {
			alias = fmt.Sprintf("field_%d", i)
		}

		switch agg.GetOperator().(type) {
		case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
			// COUNT: count the number of documents.
			count := int64(0)
			for _, r := range results {
				if r.GetDocument() != nil {
					count++
				}
			}
			fields[alias] = &firestorepb.Value{
				ValueType: &firestorepb.Value_IntegerValue{IntegerValue: count},
			}

		case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
			// SUM: sum the specified field.
			sumAgg := agg.GetSum()
			fieldPath := sumAgg.GetField().GetFieldPath()
			sum := 0.0
			hasValues := false
			for _, r := range results {
				doc := r.GetDocument()
				if doc == nil {
					continue
				}
				val := doc.GetFields()[fieldPath]
				if val == nil {
					continue
				}
				switch v := val.GetValueType().(type) {
				case *firestorepb.Value_IntegerValue:
					sum += float64(v.IntegerValue)
					hasValues = true
				case *firestorepb.Value_DoubleValue:
					sum += v.DoubleValue
					hasValues = true
				}
			}
			if hasValues {
				fields[alias] = &firestorepb.Value{
					ValueType: &firestorepb.Value_DoubleValue{DoubleValue: sum},
				}
			} else {
				fields[alias] = &firestorepb.Value{
					ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 0},
				}
			}

		case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
			// AVG: average the specified field.
			avgAgg := agg.GetAvg()
			fieldPath := avgAgg.GetField().GetFieldPath()
			sum := 0.0
			count := 0
			for _, r := range results {
				doc := r.GetDocument()
				if doc == nil {
					continue
				}
				val := doc.GetFields()[fieldPath]
				if val == nil {
					continue
				}
				switch v := val.GetValueType().(type) {
				case *firestorepb.Value_IntegerValue:
					sum += float64(v.IntegerValue)
					count++
				case *firestorepb.Value_DoubleValue:
					sum += v.DoubleValue
					count++
				}
			}
			if count > 0 {
				fields[alias] = &firestorepb.Value{
					ValueType: &firestorepb.Value_DoubleValue{DoubleValue: sum / float64(count)},
				}
			} else {
				fields[alias] = &firestorepb.Value{
					ValueType: &firestorepb.Value_NullValue{},
				}
			}
		}
	}

	return &firestorepb.AggregationResult{
		AggregateFields: fields,
	}
}

// collectQueryResults runs a query and collects all streaming results.
func collectQueryResults(ctx context.Context, client *firestore.Client, req *firestorepb.RunQueryRequest) ([]*firestorepb.RunQueryResponse, error) {
	stream, err := client.RunQuery(ctx, req)
	if err != nil {
		return nil, err
	}

	var results []*firestorepb.RunQueryResponse
	for {
		resp, err := stream.Recv()
		if err != nil {
			// gRPC stream EOF or done.
			if isStreamDone(err) {
				break
			}
			return results, err
		}
		if resp != nil {
			results = append(results, resp)
		}
	}
	return results, nil
}

// isStreamDone checks if a stream error indicates the stream is finished.
func isStreamDone(err error) bool {
	if err == io.EOF {
		return true
	}
	if status.Code(err) == codes.OK {
		return true
	}
	return false
}

// mergeDocuments merges documents from prod and shadow. Shadow wins on conflicts.
// Tombstoned and delta-tracked docs from prod are excluded (shadow version used instead).
func mergeDocuments(prodDocs, shadowDocs []*firestorepb.Document, collection string, dm *delta.Map, ts *delta.TombstoneSet) []*firestorepb.Document {
	// Index shadow docs by path for O(1) lookup.
	shadowByPath := make(map[string]*firestorepb.Document, len(shadowDocs))
	for _, doc := range shadowDocs {
		shadowByPath[doc.GetName()] = doc
	}

	seen := make(map[string]bool)
	var merged []*firestorepb.Document

	// Add all shadow docs first (they always win).
	for _, doc := range shadowDocs {
		path := doc.GetName()
		_, fullDocKey := splitDocPathFull(path)

		// Skip tombstoned docs that somehow appear in shadow.
		if ts.IsTombstoned(collection, fullDocKey) {
			continue
		}
		merged = append(merged, doc)
		seen[path] = true
	}

	// Add prod docs not in shadow and not tombstoned.
	for _, doc := range prodDocs {
		path := doc.GetName()
		if seen[path] {
			continue
		}
		_, fullDocKey := splitDocPathFull(path)

		// Skip tombstoned docs.
		if ts.IsTombstoned(collection, fullDocKey) {
			continue
		}
		// Skip docs that have been modified (delta) — shadow version should be used,
		// but if shadow didn't return it, it means it was deleted there.
		if dm.IsDelta(collection, fullDocKey) {
			continue
		}

		merged = append(merged, doc)
		seen[path] = true
	}

	// Sort by document path for deterministic ordering.
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].GetName() < merged[j].GetName()
	})

	return merged
}

// mergeQueryResults merges RunQuery results from prod and shadow.
func mergeQueryResults(prodResults, shadowResults []*firestorepb.RunQueryResponse, collection string, dm *delta.Map, ts *delta.TombstoneSet) []*firestorepb.RunQueryResponse {
	// Index shadow results by document path.
	shadowByPath := make(map[string]*firestorepb.RunQueryResponse)
	for _, r := range shadowResults {
		if r.GetDocument() != nil {
			shadowByPath[r.GetDocument().GetName()] = r
		}
	}

	seen := make(map[string]bool)
	var merged []*firestorepb.RunQueryResponse

	// Shadow results first.
	for _, r := range shadowResults {
		doc := r.GetDocument()
		if doc == nil {
			continue
		}
		path := doc.GetName()
		_, fullDocKey := splitDocPathFull(path)
		if ts.IsTombstoned(collection, fullDocKey) {
			continue
		}
		merged = append(merged, r)
		seen[path] = true
	}

	// Prod results not in shadow and not tombstoned.
	for _, r := range prodResults {
		doc := r.GetDocument()
		if doc == nil {
			continue
		}
		path := doc.GetName()
		if seen[path] {
			continue
		}
		_, fullDocKey := splitDocPathFull(path)
		if ts.IsTombstoned(collection, fullDocKey) {
			continue
		}
		if dm.IsDelta(collection, fullDocKey) {
			continue
		}
		merged = append(merged, r)
		seen[path] = true
	}

	return merged
}

// filterTombstoned removes tombstoned documents from a slice.
func filterTombstoned(docs []*firestorepb.Document, collection string, ts *delta.TombstoneSet) []*firestorepb.Document {
	if ts.CountForTable(collection) == 0 {
		return docs
	}
	var out []*firestorepb.Document
	for _, doc := range docs {
		_, fullDocKey := splitDocPathFull(doc.GetName())
		if !ts.IsTombstoned(collection, fullDocKey) {
			out = append(out, doc)
		}
	}
	return out
}

// filterTombstonedQueryResults removes tombstoned documents from query results.
func filterTombstonedQueryResults(results []*firestorepb.RunQueryResponse, collection string, ts *delta.TombstoneSet) []*firestorepb.RunQueryResponse {
	if ts.CountForTable(collection) == 0 {
		return results
	}
	var out []*firestorepb.RunQueryResponse
	for _, r := range results {
		doc := r.GetDocument()
		if doc == nil {
			out = append(out, r)
			continue
		}
		_, fullDocKey := splitDocPathFull(doc.GetName())
		if !ts.IsTombstoned(collection, fullDocKey) {
			out = append(out, r)
		}
	}
	return out
}

// applyListDocumentsReadTime sets the ReadTime consistency selector on a
// ListDocumentsRequest if the readHandler has a transaction read timestamp.
func (h *readHandler) applyListDocumentsReadTime(req *firestorepb.ListDocumentsRequest) {
	if h.readTime != nil {
		req.ConsistencySelector = &firestorepb.ListDocumentsRequest_ReadTime{ReadTime: h.readTime}
	}
}

// applyRunQueryReadTime sets the ReadTime consistency selector on a
// RunQueryRequest if the readHandler has a transaction read timestamp.
func (h *readHandler) applyRunQueryReadTime(req *firestorepb.RunQueryRequest) {
	if h.readTime != nil {
		req.ConsistencySelector = &firestorepb.RunQueryRequest_ReadTime{ReadTime: h.readTime}
	}
}

// applyAggregationQueryReadTime sets the ReadTime consistency selector on a
// RunAggregationQueryRequest if the readHandler has a transaction read timestamp.
func (h *readHandler) applyAggregationQueryReadTime(req *firestorepb.RunAggregationQueryRequest) {
	if h.readTime != nil {
		req.ConsistencySelector = &firestorepb.RunAggregationQueryRequest_ReadTime{ReadTime: h.readTime}
	}
}

// splitDocPath extracts the collection name and document ID from a full document path.
// e.g. "projects/p/databases/d/documents/users/abc123" → ("users", "abc123")
func splitDocPath(path string) (collection, docID string) {
	const marker = "/documents/"
	_, rest, found := strings.Cut(path, marker)
	if !found {
		return "", ""
	}
	// rest is e.g. "users/abc123" or "users/abc123/subcol/doc2"
	// We extract the last collection/docID pair.
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return rest, ""
	}
	// For top-level: collection = parts[0], docID = parts[1]
	// For subcollections: collection = parts[len-2], docID = parts[len-1]
	return parts[len(parts)-2], parts[len(parts)-1]
}

// splitDocPathFull extracts the immediate collection name and the full relative
// document path for use as delta/tombstone keys. Using the full relative path
// (everything after "/documents/") prevents key collisions in subcollections.
//
// Examples:
//
//	"projects/p/databases/d/documents/users/abc123"           → ("users", "users/abc123")
//	"projects/p/databases/d/documents/users/u1/comments/c1"   → ("comments", "users/u1/comments/c1")
//
// The collection return value is the immediate parent collection (last collection
// segment), used for AnyTableDelta/HasInserts checks. The fullDocKey is the full
// relative path, used for IsDelta/IsTombstoned per-document lookups.
func splitDocPathFull(path string) (collection, fullDocKey string) {
	const marker = "/documents/"
	_, rest, found := strings.Cut(path, marker)
	if !found {
		return "", ""
	}
	parts := strings.Split(rest, "/")
	if len(parts) < 2 {
		return rest, ""
	}
	collection = parts[len(parts)-2]
	return collection, rest
}

// extractCollectionFromParent extracts the collection name from a ListDocuments parent + collectionId.
func extractCollectionFromParent(parent, collectionID string) string {
	if collectionID != "" {
		return collectionID
	}
	// Fallback: try to extract from parent path.
	const marker = "/documents/"
	_, rest, found := strings.Cut(parent, marker)
	if !found {
		return ""
	}
	parts := strings.Split(rest, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// extractCollectionFromQuery extracts the collection name from a RunQuery request.
func extractCollectionFromQuery(req *firestorepb.RunQueryRequest) string {
	sq := req.GetStructuredQuery()
	if sq == nil {
		// Try parent path.
		return extractCollectionFromParent(req.GetParent(), "")
	}
	from := sq.GetFrom()
	if len(from) > 0 {
		return from[0].GetCollectionId()
	}
	return extractCollectionFromParent(req.GetParent(), "")
}

// sortQueryResults sorts RunQuery results according to the given ORDER BY clauses.
func sortQueryResults(results []*firestorepb.RunQueryResponse, orderBy []*firestorepb.StructuredQuery_Order) {
	if len(orderBy) == 0 || len(results) < 2 {
		return
	}

	sort.SliceStable(results, func(i, j int) bool {
		docI := results[i].GetDocument()
		docJ := results[j].GetDocument()
		if docI == nil || docJ == nil {
			return docI != nil
		}

		for _, order := range orderBy {
			fieldPath := order.GetField().GetFieldPath()
			dir := order.GetDirection()

			var valI, valJ *firestorepb.Value
			if fieldPath == "__name__" {
				// Sort by document path.
				valI = &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: docI.GetName()}}
				valJ = &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: docJ.GetName()}}
			} else {
				valI = docI.GetFields()[fieldPath]
				valJ = docJ.GetFields()[fieldPath]
			}

			cmp := compareValues(valI, valJ)
			if cmp == 0 {
				continue
			}

			ascending := dir != firestorepb.StructuredQuery_DESCENDING
			if ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
}

// compareValues compares two Firestore values. Returns -1, 0, or 1.
// Follows Firestore's value type ordering: null < bool < number < timestamp < string < bytes < ref < geo < array < map.
func compareValues(a, b *firestorepb.Value) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	typeOrderA := valueTypeOrder(a)
	typeOrderB := valueTypeOrder(b)
	if typeOrderA != typeOrderB {
		if typeOrderA < typeOrderB {
			return -1
		}
		return 1
	}

	// Same type — compare within type.
	switch a.GetValueType().(type) {
	case *firestorepb.Value_NullValue:
		return 0

	case *firestorepb.Value_BooleanValue:
		aB := a.GetBooleanValue()
		bB := b.GetBooleanValue()
		if aB == bB {
			return 0
		}
		if !aB {
			return -1 // false < true
		}
		return 1

	case *firestorepb.Value_IntegerValue:
		aI := a.GetIntegerValue()
		bVal := b
		// Handle comparison with double.
		if _, ok := bVal.GetValueType().(*firestorepb.Value_DoubleValue); ok {
			aF := float64(aI)
			bF := bVal.GetDoubleValue()
			return compareFloat(aF, bF)
		}
		bI := bVal.GetIntegerValue()
		if aI < bI {
			return -1
		}
		if aI > bI {
			return 1
		}
		return 0

	case *firestorepb.Value_DoubleValue:
		aF := a.GetDoubleValue()
		bVal := b
		if _, ok := bVal.GetValueType().(*firestorepb.Value_IntegerValue); ok {
			bF := float64(bVal.GetIntegerValue())
			return compareFloat(aF, bF)
		}
		bF := bVal.GetDoubleValue()
		return compareFloat(aF, bF)

	case *firestorepb.Value_TimestampValue:
		aT := a.GetTimestampValue()
		bT := b.GetTimestampValue()
		if aT.GetSeconds() != bT.GetSeconds() {
			if aT.GetSeconds() < bT.GetSeconds() {
				return -1
			}
			return 1
		}
		if aT.GetNanos() < bT.GetNanos() {
			return -1
		}
		if aT.GetNanos() > bT.GetNanos() {
			return 1
		}
		return 0

	case *firestorepb.Value_StringValue:
		aS := a.GetStringValue()
		bS := b.GetStringValue()
		if aS < bS {
			return -1
		}
		if aS > bS {
			return 1
		}
		return 0

	case *firestorepb.Value_BytesValue:
		aB := a.GetBytesValue()
		bB := b.GetBytesValue()
		aStr := string(aB)
		bStr := string(bB)
		if aStr < bStr {
			return -1
		}
		if aStr > bStr {
			return 1
		}
		return 0

	case *firestorepb.Value_ReferenceValue:
		aR := a.GetReferenceValue()
		bR := b.GetReferenceValue()
		if aR < bR {
			return -1
		}
		if aR > bR {
			return 1
		}
		return 0

	default:
		// For arrays, maps, geopoints — fall back to string comparison of document name.
		return 0
	}
}

// valueTypeOrder returns the Firestore ordering rank for a value type.
func valueTypeOrder(v *firestorepb.Value) int {
	switch v.GetValueType().(type) {
	case *firestorepb.Value_NullValue:
		return 0
	case *firestorepb.Value_BooleanValue:
		return 1
	case *firestorepb.Value_IntegerValue:
		return 2
	case *firestorepb.Value_DoubleValue:
		return 2 // numbers share the same rank
	case *firestorepb.Value_TimestampValue:
		return 3
	case *firestorepb.Value_StringValue:
		return 4
	case *firestorepb.Value_BytesValue:
		return 5
	case *firestorepb.Value_ReferenceValue:
		return 6
	case *firestorepb.Value_GeoPointValue:
		return 7
	case *firestorepb.Value_ArrayValue:
		return 8
	case *firestorepb.Value_MapValue:
		return 9
	default:
		return 10
	}
}

// compareFloat compares two float64 values. Returns -1, 0, or 1.
func compareFloat(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// filterByQuery re-evaluates the WHERE filter against each merged document,
// removing documents that no longer match the filter.
func filterByQuery(results []*firestorepb.RunQueryResponse, where *firestorepb.StructuredQuery_Filter) []*firestorepb.RunQueryResponse {
	if where == nil {
		return results
	}
	var filtered []*firestorepb.RunQueryResponse
	for _, r := range results {
		doc := r.GetDocument()
		if doc == nil {
			// Non-document responses (e.g., stats) are passed through.
			filtered = append(filtered, r)
			continue
		}
		if evaluateFilter(doc, where) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// evaluateFilter evaluates a StructuredQuery filter against a document.
func evaluateFilter(doc *firestorepb.Document, filter *firestorepb.StructuredQuery_Filter) bool {
	switch ft := filter.GetFilterType().(type) {
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		return evaluateCompositeFilter(doc, ft.CompositeFilter)
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		return evaluateFieldFilter(doc, ft.FieldFilter)
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		return evaluateUnaryFilter(doc, ft.UnaryFilter)
	default:
		// Unknown filter type — include the document (conservative).
		return true
	}
}

// evaluateCompositeFilter evaluates a composite (AND/OR) filter.
func evaluateCompositeFilter(doc *firestorepb.Document, cf *firestorepb.StructuredQuery_CompositeFilter) bool {
	if cf == nil {
		return true
	}

	switch cf.GetOp() {
	case firestorepb.StructuredQuery_CompositeFilter_AND:
		for _, sub := range cf.GetFilters() {
			if !evaluateFilter(doc, sub) {
				return false
			}
		}
		return true
	case firestorepb.StructuredQuery_CompositeFilter_OR:
		for _, sub := range cf.GetFilters() {
			if evaluateFilter(doc, sub) {
				return true
			}
		}
		return len(cf.GetFilters()) == 0
	default:
		return true
	}
}

// evaluateFieldFilter evaluates a field comparison filter.
func evaluateFieldFilter(doc *firestorepb.Document, ff *firestorepb.StructuredQuery_FieldFilter) bool {
	if ff == nil {
		return true
	}

	fieldPath := ff.GetField().GetFieldPath()
	var docValue *firestorepb.Value

	if fieldPath == "__name__" {
		docValue = &firestorepb.Value{
			ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: doc.GetName()},
		}
	} else {
		docValue = doc.GetFields()[fieldPath]
	}

	filterValue := ff.GetValue()

	switch ff.GetOp() {
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		return compareValues(docValue, filterValue) == 0

	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return compareValues(docValue, filterValue) != 0

	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return compareValues(docValue, filterValue) < 0

	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return compareValues(docValue, filterValue) <= 0

	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return compareValues(docValue, filterValue) > 0

	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return compareValues(docValue, filterValue) >= 0

	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		return arrayContains(docValue, filterValue)

	case firestorepb.StructuredQuery_FieldFilter_IN:
		return valueInArray(docValue, filterValue)

	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return !valueInArray(docValue, filterValue)

	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return arrayContainsAny(docValue, filterValue)

	default:
		return true // Unknown operator — include (conservative).
	}
}

// evaluateUnaryFilter evaluates a unary filter (IS NULL, IS NOT NULL, IS NAN, IS NOT NAN).
func evaluateUnaryFilter(doc *firestorepb.Document, uf *firestorepb.StructuredQuery_UnaryFilter) bool {
	if uf == nil {
		return true
	}

	fieldRef := uf.GetField()
	if fieldRef == nil {
		return true
	}
	fieldPath := fieldRef.GetFieldPath()
	docValue := doc.GetFields()[fieldPath]

	switch uf.GetOp() {
	case firestorepb.StructuredQuery_UnaryFilter_IS_NAN:
		if docValue == nil {
			return false
		}
		if dv, ok := docValue.GetValueType().(*firestorepb.Value_DoubleValue); ok {
			return dv.DoubleValue != dv.DoubleValue // NaN check
		}
		return false

	case firestorepb.StructuredQuery_UnaryFilter_IS_NULL:
		if docValue == nil {
			return true
		}
		_, isNull := docValue.GetValueType().(*firestorepb.Value_NullValue)
		return isNull

	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
		if docValue == nil {
			return true
		}
		if dv, ok := docValue.GetValueType().(*firestorepb.Value_DoubleValue); ok {
			return dv.DoubleValue == dv.DoubleValue // Not NaN
		}
		return true

	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
		if docValue == nil {
			return false
		}
		_, isNull := docValue.GetValueType().(*firestorepb.Value_NullValue)
		return !isNull

	default:
		return true
	}
}

// arrayContains checks if a document field (which should be an array) contains the given value.
func arrayContains(docValue, searchValue *firestorepb.Value) bool {
	if docValue == nil {
		return false
	}
	arr, ok := docValue.GetValueType().(*firestorepb.Value_ArrayValue)
	if !ok || arr.ArrayValue == nil {
		return false
	}
	for _, elem := range arr.ArrayValue.GetValues() {
		if compareValues(elem, searchValue) == 0 {
			return true
		}
	}
	return false
}

// valueInArray checks if a value is contained in a filter array.
func valueInArray(docValue, filterArray *firestorepb.Value) bool {
	if filterArray == nil {
		return false
	}
	arr, ok := filterArray.GetValueType().(*firestorepb.Value_ArrayValue)
	if !ok || arr.ArrayValue == nil {
		return false
	}
	for _, elem := range arr.ArrayValue.GetValues() {
		if compareValues(docValue, elem) == 0 {
			return true
		}
	}
	return false
}

// arrayContainsAny checks if a document array contains any of the values in the filter array.
func arrayContainsAny(docValue, filterArray *firestorepb.Value) bool {
	if docValue == nil || filterArray == nil {
		return false
	}
	docArr, ok := docValue.GetValueType().(*firestorepb.Value_ArrayValue)
	if !ok || docArr.ArrayValue == nil {
		return false
	}
	filterArr, ok := filterArray.GetValueType().(*firestorepb.Value_ArrayValue)
	if !ok || filterArr.ArrayValue == nil {
		return false
	}
	for _, docElem := range docArr.ArrayValue.GetValues() {
		for _, filterElem := range filterArr.ArrayValue.GetValues() {
			if compareValues(docElem, filterElem) == 0 {
				return true
			}
		}
	}
	return false
}

// cloneListDocumentsRequestWithPageSize creates a copy of the ListDocuments request with a modified page_size.
func cloneListDocumentsRequestWithPageSize(req *firestorepb.ListDocumentsRequest, newPageSize int32) *firestorepb.ListDocumentsRequest {
	return &firestorepb.ListDocumentsRequest{
		Parent:       req.GetParent(),
		CollectionId: req.GetCollectionId(),
		PageSize:     newPageSize,
		PageToken:    req.GetPageToken(),
		OrderBy:      req.GetOrderBy(),
		Mask:         req.GetMask(),
		ShowMissing:  req.GetShowMissing(),
	}
}

// cloneRunQueryRequestWithLimit creates a copy of the RunQuery request with a modified limit.
func cloneRunQueryRequestWithLimit(req *firestorepb.RunQueryRequest, newLimit int32) *firestorepb.RunQueryRequest {
	// We need to clone the structured query with the new limit.
	sq := req.GetStructuredQuery()
	if sq == nil {
		return req
	}

	// Clone the structured query.
	newSQ := &firestorepb.StructuredQuery{
		Select:  sq.GetSelect(),
		From:    sq.GetFrom(),
		Where:   sq.GetWhere(),
		OrderBy: sq.GetOrderBy(),
		StartAt: sq.GetStartAt(),
		EndAt:   sq.GetEndAt(),
		Offset:  sq.GetOffset(),
		Limit:   &wrapperspb.Int32Value{Value: newLimit},
	}

	return &firestorepb.RunQueryRequest{
		Parent:    req.GetParent(),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: newSQ},
	}
}
