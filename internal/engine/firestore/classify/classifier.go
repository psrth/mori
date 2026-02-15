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
		"Listen", "PartitionQuery", "ListCollectionIds":
		cl.OpType = core.OpRead
		cl.SubType = core.SubSelect

		if method == "RunAggregationQuery" {
			cl.HasAggregate = true
		}

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
func (c *FirestoreClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
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
