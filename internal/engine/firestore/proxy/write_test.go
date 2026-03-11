package proxy

import (
	"testing"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/psrth/mori/internal/core/delta"
)

func TestTrackWrite_Update(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	wh := &writeHandler{
		deltaMap:   dm,
		tombstones: ts,
		verbose:    false,
	}

	w := &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{
				Name: "projects/p/databases/d/documents/users/abc",
			},
		},
	}

	wh.trackWrite(w)

	if !dm.IsDelta("users", "users/abc") {
		t.Error("expected delta for users/abc after update")
	}
}

func TestTrackWrite_Delete(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	// Pre-populate delta with full doc key.
	dm.Add("users", "users/abc")

	wh := &writeHandler{
		deltaMap:   dm,
		tombstones: ts,
		verbose:    false,
	}

	w := &firestorepb.Write{
		Operation: &firestorepb.Write_Delete{
			Delete: "projects/p/databases/d/documents/users/abc",
		},
	}

	wh.trackWrite(w)

	if !ts.IsTombstoned("users", "users/abc") {
		t.Error("expected tombstone for users/abc after delete")
	}
	if dm.IsDelta("users", "users/abc") {
		t.Error("expected delta to be removed for users/abc after delete")
	}
}

func TestTrackWrite_Transform(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	wh := &writeHandler{
		deltaMap:   dm,
		tombstones: ts,
		verbose:    false,
	}

	w := &firestorepb.Write{
		Operation: &firestorepb.Write_Transform{
			Transform: &firestorepb.DocumentTransform{
				Document: "projects/p/databases/d/documents/counters/visits",
			},
		},
	}

	wh.trackWrite(w)

	if !dm.IsDelta("counters", "counters/visits") {
		t.Error("expected delta for counters/visits after transform")
	}
}

func TestTrackWrite_MultipleWrites(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	wh := &writeHandler{
		deltaMap:   dm,
		tombstones: ts,
		verbose:    false,
	}

	writes := []*firestorepb.Write{
		{
			Operation: &firestorepb.Write_Update{
				Update: &firestorepb.Document{
					Name: "projects/p/databases/d/documents/users/u1",
				},
			},
		},
		{
			Operation: &firestorepb.Write_Update{
				Update: &firestorepb.Document{
					Name: "projects/p/databases/d/documents/users/u2",
				},
			},
		},
		{
			Operation: &firestorepb.Write_Delete{
				Delete: "projects/p/databases/d/documents/users/u3",
			},
		},
	}

	for _, w := range writes {
		wh.trackWrite(w)
	}

	if !dm.IsDelta("users", "users/u1") {
		t.Error("expected delta for users/u1")
	}
	if !dm.IsDelta("users", "users/u2") {
		t.Error("expected delta for users/u2")
	}
	if !ts.IsTombstoned("users", "users/u3") {
		t.Error("expected tombstone for users/u3")
	}
	if dm.IsDelta("users", "users/u3") {
		t.Error("users/u3 should not be in delta map after delete")
	}
}

func TestTrackWrite_InsertCount(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	wh := &writeHandler{
		deltaMap:   dm,
		tombstones: ts,
		verbose:    false,
	}

	// Simulate a Commit-based create: Write_Update with Precondition{Exists: false}.
	w := &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{
				Name: "projects/p/databases/d/documents/items/new1",
			},
		},
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_Exists{Exists: false},
		},
	}

	wh.trackWrite(w)

	if !dm.IsDelta("items", "items/new1") {
		t.Error("expected delta for items/new1 after create-via-commit")
	}
	if !dm.HasInserts("items") {
		t.Error("expected HasInserts(items) to be true after create-via-commit")
	}
}

func TestTrackWrite_Subcollection(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	wh := &writeHandler{
		deltaMap:   dm,
		tombstones: ts,
		verbose:    false,
	}

	w := &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{
				Name: "projects/p/databases/d/documents/users/u1/comments/c1",
			},
		},
	}

	wh.trackWrite(w)

	// Full doc key should include entire relative path.
	if !dm.IsDelta("comments", "users/u1/comments/c1") {
		t.Error("expected delta with full path key for subcollection document")
	}

	// A different parent path with the same docID should NOT collide.
	if dm.IsDelta("comments", "teams/t1/comments/c1") {
		t.Error("different parent path should not collide in delta map")
	}
}
