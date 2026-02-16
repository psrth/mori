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
)

// readHandler implements document-level merged reads for Firestore.
type readHandler struct {
	prodClient   *firestore.Client
	shadowClient *firestore.Client
	deltaMap     *delta.Map
	tombstones   *delta.TombstoneSet
	projectID    string
	databaseID   string
	verbose      bool
}

// getDocument performs a merged GetDocument: if the doc path is in the delta map,
// read from shadow; if tombstoned, return NOT_FOUND; otherwise read from prod.
func (h *readHandler) getDocument(ctx context.Context, req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	docPath := req.GetName()
	collection, docID := splitDocPath(docPath)

	// Check tombstone first — deleted docs should return NOT_FOUND.
	if collection != "" && h.tombstones.IsTombstoned(collection, docID) {
		if h.verbose {
			log.Printf("[firestore-read] GetDocument %s: tombstoned → NOT_FOUND", docPath)
		}
		return nil, status.Errorf(codes.NotFound, "document %q not found (tombstoned)", docPath)
	}

	// Check delta map — if modified, read from shadow.
	if collection != "" && h.deltaMap.IsDelta(collection, docID) {
		if h.verbose {
			log.Printf("[firestore-read] GetDocument %s: delta → shadow", docPath)
		}
		return h.shadowClient.GetDocument(ctx, req)
	}

	// Default: read from prod.
	if h.verbose {
		log.Printf("[firestore-read] GetDocument %s: prod", docPath)
	}
	return h.prodClient.GetDocument(ctx, req)
}

// batchGetDocuments performs merged reads for multiple documents at once.
// For each document, it checks tombstones and delta map to decide whether
// to read from shadow or prod.
func (h *readHandler) batchGetDocuments(ctx context.Context, req *firestorepb.BatchGetDocumentsRequest) ([]*firestorepb.BatchGetDocumentsResponse, error) {
	var results []*firestorepb.BatchGetDocumentsResponse

	for _, docPath := range req.GetDocuments() {
		collection, docID := splitDocPath(docPath)

		// Check tombstone — return "missing" result.
		if collection != "" && h.tombstones.IsTombstoned(collection, docID) {
			if h.verbose {
				log.Printf("[firestore-read] BatchGetDocuments %s: tombstoned → missing", docPath)
			}
			results = append(results, &firestorepb.BatchGetDocumentsResponse{
				Result:  &firestorepb.BatchGetDocumentsResponse_Missing{Missing: docPath},
				ReadTime: nil,
			})
			continue
		}

		// Check delta — read from shadow.
		if collection != "" && h.deltaMap.IsDelta(collection, docID) {
			if h.verbose {
				log.Printf("[firestore-read] BatchGetDocuments %s: delta → shadow", docPath)
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

	// If no deltas or inserts for this collection, just query prod and filter tombstones.
	if !h.deltaMap.AnyTableDelta([]string{collection}) && !h.deltaMap.HasInserts(collection) {
		prodDocs, err := collectListDocuments(ctx, h.prodClient, req)
		if err != nil {
			return nil, err
		}
		filtered := filterTombstoned(prodDocs, collection, h.tombstones)
		return &firestorepb.ListDocumentsResponse{Documents: filtered}, nil
	}

	// Query both backends via iterators.
	prodDocs, prodErr := collectListDocuments(ctx, h.prodClient, req)
	shadowDocs, shadowErr := collectListDocuments(ctx, h.shadowClient, req)

	if prodErr != nil && shadowErr != nil {
		return nil, fmt.Errorf("both backends failed: prod=%v, shadow=%v", prodErr, shadowErr)
	}

	merged := mergeDocuments(prodDocs, shadowDocs, collection, h.deltaMap, h.tombstones)

	return &firestorepb.ListDocumentsResponse{Documents: merged}, nil
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

	// If no deltas for this collection, just query prod and filter tombstones.
	if !h.deltaMap.AnyTableDelta([]string{collection}) && !h.deltaMap.HasInserts(collection) {
		results, err := collectQueryResults(ctx, h.prodClient, req)
		if err != nil {
			return nil, err
		}
		return filterTombstonedQueryResults(results, collection, h.tombstones), nil
	}

	// Query both backends.
	prodResults, prodErr := collectQueryResults(ctx, h.prodClient, req)
	shadowResults, shadowErr := collectQueryResults(ctx, h.shadowClient, req)

	if prodErr != nil && shadowErr != nil {
		return nil, fmt.Errorf("both backends failed: prod=%v, shadow=%v", prodErr, shadowErr)
	}

	return mergeQueryResults(prodResults, shadowResults, collection, h.deltaMap, h.tombstones), nil
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
		_, docID := splitDocPath(path)

		// Skip tombstoned docs that somehow appear in shadow.
		if ts.IsTombstoned(collection, docID) {
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
		_, docID := splitDocPath(path)

		// Skip tombstoned docs.
		if ts.IsTombstoned(collection, docID) {
			continue
		}
		// Skip docs that have been modified (delta) — shadow version should be used,
		// but if shadow didn't return it, it means it was deleted there.
		if dm.IsDelta(collection, docID) {
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
		_, docID := splitDocPath(path)
		if ts.IsTombstoned(collection, docID) {
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
		_, docID := splitDocPath(path)
		if ts.IsTombstoned(collection, docID) {
			continue
		}
		if dm.IsDelta(collection, docID) {
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
		_, docID := splitDocPath(doc.GetName())
		if !ts.IsTombstoned(collection, docID) {
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
		_, docID := splitDocPath(doc.GetName())
		if !ts.IsTombstoned(collection, docID) {
			out = append(out, r)
		}
	}
	return out
}

// splitDocPath extracts the collection name and document ID from a full document path.
// e.g. "projects/p/databases/d/documents/users/abc123" → ("users", "abc123")
func splitDocPath(path string) (collection, docID string) {
	const marker = "/documents/"
	idx := strings.Index(path, marker)
	if idx < 0 {
		return "", ""
	}
	rest := path[idx+len(marker):]
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

// extractCollectionFromParent extracts the collection name from a ListDocuments parent + collectionId.
func extractCollectionFromParent(parent, collectionID string) string {
	if collectionID != "" {
		return collectionID
	}
	// Fallback: try to extract from parent path.
	const marker = "/documents/"
	idx := strings.Index(parent, marker)
	if idx < 0 {
		return ""
	}
	rest := parent[idx+len(marker):]
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
