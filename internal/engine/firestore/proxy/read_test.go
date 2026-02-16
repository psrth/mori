package proxy

import (
	"testing"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/mori-dev/mori/internal/core/delta"
)

func TestSplitDocPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantColl   string
		wantDocID  string
	}{
		{
			name:      "top-level document",
			path:      "projects/p/databases/d/documents/users/abc123",
			wantColl:  "users",
			wantDocID: "abc123",
		},
		{
			name:      "subcollection document",
			path:      "projects/p/databases/d/documents/users/abc123/posts/post1",
			wantColl:  "posts",
			wantDocID: "post1",
		},
		{
			name:      "no documents marker",
			path:      "projects/p/databases/d",
			wantColl:  "",
			wantDocID: "",
		},
		{
			name:      "empty path",
			path:      "",
			wantColl:  "",
			wantDocID: "",
		},
		{
			name:      "collection only (no doc ID)",
			path:      "projects/p/databases/d/documents/users",
			wantColl:  "users",
			wantDocID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coll, docID := splitDocPath(tt.path)
			if coll != tt.wantColl {
				t.Errorf("collection = %q, want %q", coll, tt.wantColl)
			}
			if docID != tt.wantDocID {
				t.Errorf("docID = %q, want %q", docID, tt.wantDocID)
			}
		})
	}
}

func TestExtractCollectionFromParent(t *testing.T) {
	tests := []struct {
		name         string
		parent       string
		collectionID string
		want         string
	}{
		{
			name:         "explicit collection ID",
			parent:       "projects/p/databases/d/documents",
			collectionID: "users",
			want:         "users",
		},
		{
			name:         "collection from parent path",
			parent:       "projects/p/databases/d/documents/users",
			collectionID: "",
			want:         "users",
		},
		{
			name:         "no collection info",
			parent:       "projects/p/databases/d",
			collectionID: "",
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractCollectionFromParent(tt.parent, tt.collectionID)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractCollectionFromQuery(t *testing.T) {
	tests := []struct {
		name string
		req  *firestorepb.RunQueryRequest
		want string
	}{
		{
			name: "structured query with from",
			req: &firestorepb.RunQueryRequest{
				QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
					StructuredQuery: &firestorepb.StructuredQuery{
						From: []*firestorepb.StructuredQuery_CollectionSelector{
							{CollectionId: "orders"},
						},
					},
				},
			},
			want: "orders",
		},
		{
			name: "parent path only",
			req: &firestorepb.RunQueryRequest{
				Parent: "projects/p/databases/d/documents/users",
			},
			want: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractCollectionFromQuery(tt.req)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMergeDocuments(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	prodDocs := []*firestorepb.Document{
		{Name: "projects/p/databases/d/documents/users/a"},
		{Name: "projects/p/databases/d/documents/users/b"},
		{Name: "projects/p/databases/d/documents/users/c"},
		{Name: "projects/p/databases/d/documents/users/d"},
	}

	// doc "b" was modified in shadow (delta).
	dm.Add("users", "b")

	// doc "c" was deleted (tombstone).
	ts.Add("users", "c")

	shadowDocs := []*firestorepb.Document{
		{Name: "projects/p/databases/d/documents/users/b"}, // modified version
		{Name: "projects/p/databases/d/documents/users/e"}, // new doc (insert)
	}

	merged := mergeDocuments(prodDocs, shadowDocs, "users", dm, ts)

	// Expected: a (prod), b (shadow), d (prod), e (shadow)
	// Not: c (tombstoned), b from prod (delta → shadow wins)
	wantNames := []string{
		"projects/p/databases/d/documents/users/a",
		"projects/p/databases/d/documents/users/b",
		"projects/p/databases/d/documents/users/d",
		"projects/p/databases/d/documents/users/e",
	}

	if len(merged) != len(wantNames) {
		t.Fatalf("got %d docs, want %d", len(merged), len(wantNames))
	}

	for i, doc := range merged {
		if doc.GetName() != wantNames[i] {
			t.Errorf("doc[%d] = %q, want %q", i, doc.GetName(), wantNames[i])
		}
	}
}

func TestMergeDocuments_NoDeltas(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	prodDocs := []*firestorepb.Document{
		{Name: "projects/p/databases/d/documents/users/a"},
		{Name: "projects/p/databases/d/documents/users/b"},
	}

	merged := mergeDocuments(prodDocs, nil, "users", dm, ts)

	if len(merged) != 2 {
		t.Fatalf("got %d docs, want 2", len(merged))
	}
}

func TestFilterTombstoned(t *testing.T) {
	ts := delta.NewTombstoneSet()
	ts.Add("users", "b")

	docs := []*firestorepb.Document{
		{Name: "projects/p/databases/d/documents/users/a"},
		{Name: "projects/p/databases/d/documents/users/b"},
		{Name: "projects/p/databases/d/documents/users/c"},
	}

	filtered := filterTombstoned(docs, "users", ts)

	if len(filtered) != 2 {
		t.Fatalf("got %d docs, want 2", len(filtered))
	}
	if filtered[0].GetName() != "projects/p/databases/d/documents/users/a" {
		t.Errorf("doc[0] = %q, want users/a", filtered[0].GetName())
	}
	if filtered[1].GetName() != "projects/p/databases/d/documents/users/c" {
		t.Errorf("doc[1] = %q, want users/c", filtered[1].GetName())
	}
}

func TestFilterTombstoned_Empty(t *testing.T) {
	ts := delta.NewTombstoneSet()

	docs := []*firestorepb.Document{
		{Name: "projects/p/databases/d/documents/users/a"},
	}

	filtered := filterTombstoned(docs, "users", ts)
	if len(filtered) != 1 {
		t.Fatalf("got %d docs, want 1", len(filtered))
	}
}

func TestMergeQueryResults(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	dm.Add("orders", "o2")
	ts.Add("orders", "o3")

	prodResults := []*firestorepb.RunQueryResponse{
		{Document: &firestorepb.Document{Name: "projects/p/databases/d/documents/orders/o1"}},
		{Document: &firestorepb.Document{Name: "projects/p/databases/d/documents/orders/o2"}},
		{Document: &firestorepb.Document{Name: "projects/p/databases/d/documents/orders/o3"}},
	}

	shadowResults := []*firestorepb.RunQueryResponse{
		{Document: &firestorepb.Document{Name: "projects/p/databases/d/documents/orders/o2"}},
		{Document: &firestorepb.Document{Name: "projects/p/databases/d/documents/orders/o4"}},
	}

	merged := mergeQueryResults(prodResults, shadowResults, "orders", dm, ts)

	// Expected: o2 (shadow), o4 (shadow), o1 (prod)
	// Not: o2 from prod (delta), o3 (tombstoned)
	if len(merged) != 3 {
		t.Fatalf("got %d results, want 3", len(merged))
	}

	// Shadow results come first, then prod.
	names := make([]string, len(merged))
	for i, r := range merged {
		names[i] = r.GetDocument().GetName()
	}

	// o2 from shadow, o4 from shadow, o1 from prod.
	wantContains := map[string]bool{
		"projects/p/databases/d/documents/orders/o1": true,
		"projects/p/databases/d/documents/orders/o2": true,
		"projects/p/databases/d/documents/orders/o4": true,
	}
	for _, n := range names {
		if !wantContains[n] {
			t.Errorf("unexpected doc %q in merged results", n)
		}
		delete(wantContains, n)
	}
	for n := range wantContains {
		t.Errorf("missing doc %q from merged results", n)
	}
}
