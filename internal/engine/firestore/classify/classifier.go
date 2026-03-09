package classify

import (
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/firestore/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*FirestoreClassifier)(nil)

// FirestoreClassifier classifies Firestore gRPC method names into core.Classification.
type FirestoreClassifier struct {
	collections map[string]schema.CollectionMeta
}

// New creates a FirestoreClassifier.
func New(collections map[string]schema.CollectionMeta) *FirestoreClassifier {
	if collections == nil {
		collections = make(map[string]schema.CollectionMeta)
	}
	return &FirestoreClassifier{collections: collections}
}

// Classify parses a Firestore gRPC method name and returns its classification.
// The "query" string is the full gRPC method name, e.g.
// "/google.firestore.v1.Firestore/GetDocument".
func (c *FirestoreClassifier) Classify(query string) (*core.Classification, error) {
	cl := &core.Classification{RawSQL: query}

	method := extractMethodName(query)

	switch method {
	// Read operations.
	case "GetDocument", "ListDocuments", "RunQuery",
		"RunAggregationQuery", "BatchGetDocuments",
		"PartitionQuery", "ListCollectionIds":
		cl.OpType = core.OpRead
		cl.SubType = core.SubSelect

		if method == "RunAggregationQuery" {
			cl.HasAggregate = true
		}

	// Listen is classified as a listen-only operation (prod-only, no shadow awareness).
	case "Listen":
		cl.OpType = core.OpOther
		cl.SubType = core.SubListen

	// Write operations.
	case "CreateDocument":
		cl.OpType = core.OpWrite
		cl.SubType = core.SubInsert

	case "UpdateDocument":
		cl.OpType = core.OpWrite
		cl.SubType = core.SubUpdate

	case "DeleteDocument":
		cl.OpType = core.OpWrite
		cl.SubType = core.SubDelete

	case "Commit", "Write", "BatchWrite":
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther

	// Transaction control.
	case "BeginTransaction":
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubBegin

	case "Rollback":
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRollback

	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}

	return cl, nil
}

// ClassifyWithParams classifies a parameterized query. For Firestore,
// this delegates directly to Classify since there are no SQL parameters.
func (c *FirestoreClassifier) ClassifyWithParams(query string, params []any) (*core.Classification, error) {
	return c.Classify(query)
}

// extractMethodName extracts the method name from a fully qualified gRPC method string.
// "/google.firestore.v1.Firestore/GetDocument" -> "GetDocument"
// "GetDocument" -> "GetDocument"
func extractMethodName(fullMethod string) string {
	if idx := strings.LastIndex(fullMethod, "/"); idx >= 0 {
		return fullMethod[idx+1:]
	}
	return fullMethod
}

// IsWriteMethod reports whether the given gRPC method is a write operation.
func IsWriteMethod(method string) bool {
	m := extractMethodName(method)
	switch m {
	case "CreateDocument", "UpdateDocument", "DeleteDocument",
		"Commit", "Write", "BatchWrite":
		return true
	default:
		return false
	}
}

// IsTransactionMethod reports whether the given gRPC method is transaction control.
func IsTransactionMethod(method string) bool {
	m := extractMethodName(method)
	switch m {
	case "BeginTransaction", "Rollback":
		return true
	// Commit is classified as write since it executes mutations.
	default:
		return false
	}
}

// ExtractCollectionFromPath extracts the root collection name from a Firestore
// document resource path.
// e.g. "projects/p/databases/d/documents/users/abc123" → "users"
// e.g. "projects/p/databases/d/documents/users/abc/posts/p1" → "users" (root collection)
func ExtractCollectionFromPath(resourcePath string) string {
	const marker = "/documents/"
	_, rest, found := strings.Cut(resourcePath, marker)
	if !found {
		return ""
	}
	// The root collection is always the first segment.
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return ""
}

// ExtractCollectionsFromPaths extracts unique collection names from multiple resource paths.
func ExtractCollectionsFromPaths(paths []string) []string {
	seen := make(map[string]bool)
	var collections []string
	for _, p := range paths {
		c := ExtractCollectionFromPath(p)
		if c != "" && !seen[c] {
			seen[c] = true
			collections = append(collections, c)
		}
	}
	return collections
}

// ExtractCollectionsFromWrites extracts collection names from a list of Write protos.
func ExtractCollectionsFromWrites(writes []WriteInfo) []string {
	seen := make(map[string]bool)
	var collections []string
	for _, w := range writes {
		c := ExtractCollectionFromPath(w.DocumentPath)
		if c != "" && !seen[c] {
			seen[c] = true
			collections = append(collections, c)
		}
	}
	return collections
}

// WriteInfo is a minimal representation of a write operation for classification purposes.
type WriteInfo struct {
	DocumentPath string
}
