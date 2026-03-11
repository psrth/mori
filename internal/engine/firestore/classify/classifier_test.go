package classify

import (
	"testing"

	"github.com/psrth/mori/internal/core"
)

func TestClassify(t *testing.T) {
	c := New(nil)

	tests := []struct {
		name       string
		method     string
		wantOp     core.OpType
		wantSub    core.SubType
		wantAggr   bool
	}{
		// Read operations.
		{"GetDocument", "/google.firestore.v1.Firestore/GetDocument", core.OpRead, core.SubSelect, false},
		{"ListDocuments", "/google.firestore.v1.Firestore/ListDocuments", core.OpRead, core.SubSelect, false},
		{"RunQuery", "/google.firestore.v1.Firestore/RunQuery", core.OpRead, core.SubSelect, false},
		{"RunAggregationQuery", "/google.firestore.v1.Firestore/RunAggregationQuery", core.OpRead, core.SubSelect, true},
		{"BatchGetDocuments", "/google.firestore.v1.Firestore/BatchGetDocuments", core.OpRead, core.SubSelect, false},
		{"Listen", "/google.firestore.v1.Firestore/Listen", core.OpOther, core.SubListen, false},
		{"PartitionQuery", "/google.firestore.v1.Firestore/PartitionQuery", core.OpRead, core.SubSelect, false},
		{"ListCollectionIds", "/google.firestore.v1.Firestore/ListCollectionIds", core.OpRead, core.SubSelect, false},

		// Write operations.
		{"CreateDocument", "/google.firestore.v1.Firestore/CreateDocument", core.OpWrite, core.SubInsert, false},
		{"UpdateDocument", "/google.firestore.v1.Firestore/UpdateDocument", core.OpWrite, core.SubUpdate, false},
		{"DeleteDocument", "/google.firestore.v1.Firestore/DeleteDocument", core.OpWrite, core.SubDelete, false},
		{"Commit", "/google.firestore.v1.Firestore/Commit", core.OpWrite, core.SubOther, false},
		{"Write", "/google.firestore.v1.Firestore/Write", core.OpWrite, core.SubOther, false},
		{"BatchWrite", "/google.firestore.v1.Firestore/BatchWrite", core.OpWrite, core.SubOther, false},

		// Transaction control.
		{"BeginTransaction", "/google.firestore.v1.Firestore/BeginTransaction", core.OpTransaction, core.SubBegin, false},
		{"Rollback", "/google.firestore.v1.Firestore/Rollback", core.OpTransaction, core.SubRollback, false},

		// Unknown.
		{"unknown method", "/some.service/UnknownMethod", core.OpOther, core.SubOther, false},
		{"bare method name", "GetDocument", core.OpRead, core.SubSelect, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.method)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
			if cl.HasAggregate != tt.wantAggr {
				t.Errorf("HasAggregate = %v, want %v", cl.HasAggregate, tt.wantAggr)
			}
			if cl.RawSQL != tt.method {
				t.Errorf("RawSQL = %q, want %q", cl.RawSQL, tt.method)
			}
		})
	}
}

func TestClassifyWithParams(t *testing.T) {
	c := New(nil)
	cl, err := c.ClassifyWithParams("/google.firestore.v1.Firestore/GetDocument", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.OpType != core.OpRead {
		t.Errorf("OpType = %v, want OpRead", cl.OpType)
	}
}

func TestIsWriteMethod(t *testing.T) {
	writes := []string{
		"/google.firestore.v1.Firestore/CreateDocument",
		"/google.firestore.v1.Firestore/UpdateDocument",
		"/google.firestore.v1.Firestore/DeleteDocument",
		"/google.firestore.v1.Firestore/Commit",
		"/google.firestore.v1.Firestore/Write",
		"/google.firestore.v1.Firestore/BatchWrite",
	}
	for _, m := range writes {
		if !IsWriteMethod(m) {
			t.Errorf("IsWriteMethod(%q) = false, want true", m)
		}
	}

	reads := []string{
		"/google.firestore.v1.Firestore/GetDocument",
		"/google.firestore.v1.Firestore/ListDocuments",
		"/google.firestore.v1.Firestore/RunQuery",
		"/google.firestore.v1.Firestore/BeginTransaction",
		"/google.firestore.v1.Firestore/Rollback",
	}
	for _, m := range reads {
		if IsWriteMethod(m) {
			t.Errorf("IsWriteMethod(%q) = true, want false", m)
		}
	}
}

func TestExtractMethodName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/google.firestore.v1.Firestore/GetDocument", "GetDocument"},
		{"GetDocument", "GetDocument"},
		{"/a/b/c", "c"},
		{"", ""},
	}
	for _, tt := range tests {
		got := extractMethodName(tt.input)
		if got != tt.want {
			t.Errorf("extractMethodName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
