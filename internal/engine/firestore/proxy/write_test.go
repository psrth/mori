package proxy

import (
	"testing"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/mori-dev/mori/internal/core/delta"
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

	if !dm.IsDelta("users", "abc") {
		t.Error("expected delta for users/abc after update")
	}
}

func TestTrackWrite_Delete(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	// Pre-populate delta.
	dm.Add("users", "abc")

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

	if !ts.IsTombstoned("users", "abc") {
		t.Error("expected tombstone for users/abc after delete")
	}
	if dm.IsDelta("users", "abc") {
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

	if !dm.IsDelta("counters", "visits") {
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

	if !dm.IsDelta("users", "u1") {
		t.Error("expected delta for users/u1")
	}
	if !dm.IsDelta("users", "u2") {
		t.Error("expected delta for users/u2")
	}
	if !ts.IsTombstoned("users", "u3") {
		t.Error("expected tombstone for users/u3")
	}
	if dm.IsDelta("users", "u3") {
		t.Error("users/u3 should not be in delta map after delete")
	}
}
