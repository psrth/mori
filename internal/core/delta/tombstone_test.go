package delta

import "testing"

func TestNewTombstoneSetNotNil(t *testing.T) {
	ts := NewTombstoneSet()
	if ts == nil {
		t.Fatal("NewTombstoneSet() returned nil")
	}
}

func TestTombstoneAddAndIsTombstoned(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("users", "42")
	if !ts.IsTombstoned("users", "42") {
		t.Error("IsTombstoned(users, 42) = false, want true")
	}
}

func TestTombstoneIsTombstonedFalseForClean(t *testing.T) {
	ts := NewTombstoneSet()
	if ts.IsTombstoned("users", "42") {
		t.Error("IsTombstoned(users, 42) = true for clean set, want false")
	}
}

func TestTombstonedPKs(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("users", "3")
	ts.Add("users", "1")
	ts.Add("users", "2")
	got := ts.TombstonedPKs("users")
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("TombstonedPKs len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("TombstonedPKs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestTombstoneCountForTable(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("users", "1")
	ts.Add("users", "2")
	if got := ts.CountForTable("users"); got != 2 {
		t.Errorf("CountForTable(users) = %d, want 2", got)
	}
}

func TestTombstoneStageCommit(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Stage("users", "42")
	ts.Commit()
	if !ts.IsTombstoned("users", "42") {
		t.Error("IsTombstoned(users, 42) = false after Stage+Commit, want true")
	}
}

func TestTombstoneStageRollback(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Stage("users", "42")
	ts.Rollback()
	if ts.IsTombstoned("users", "42") {
		t.Error("IsTombstoned(users, 42) = true after Stage+Rollback, want false")
	}
}

func TestTombstoneTables(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("orders", "1")
	ts.Add("users", "1")
	got := ts.Tables()
	want := []string{"orders", "users"}
	if len(got) != len(want) {
		t.Fatalf("Tables() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Tables()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestAnyTableTombstone(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("users", "1")

	if !ts.AnyTableTombstone([]string{"users"}) {
		t.Error("AnyTableTombstone([users]) = false, want true")
	}
	if ts.AnyTableTombstone([]string{"orders"}) {
		t.Error("AnyTableTombstone([orders]) = true, want false")
	}
	if !ts.AnyTableTombstone([]string{"orders", "users"}) {
		t.Error("AnyTableTombstone([orders, users]) = false, want true")
	}

	empty := NewTombstoneSet()
	if empty.AnyTableTombstone([]string{"users"}) {
		t.Error("AnyTableTombstone on empty set = true, want false")
	}
}

func TestTombstoneRenameTable(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("users", "1")
	ts.Add("users", "2")
	ts.Add("orders", "10")

	ts.RenameTable("users", "customers")

	if !ts.IsTombstoned("customers", "1") || !ts.IsTombstoned("customers", "2") {
		t.Error("tombstone entries not moved to new name")
	}
	if ts.IsTombstoned("users", "1") {
		t.Error("tombstone entries still under old name")
	}
	if !ts.IsTombstoned("orders", "10") {
		t.Error("unrelated table affected by rename")
	}
}

func TestTombstoneRenameTableMerge(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("old_t", "1")
	ts.Add("new_t", "2")

	ts.RenameTable("old_t", "new_t")

	if got := ts.CountForTable("new_t"); got != 2 {
		t.Errorf("CountForTable(new_t) = %d, want 2", got)
	}
	if ts.CountForTable("old_t") != 0 {
		t.Error("old_t still has entries after rename")
	}
}

func TestTombstoneSnapshotAndLoad(t *testing.T) {
	ts := NewTombstoneSet()
	ts.Add("users", "1")
	ts.Add("orders", "10")

	snap := ts.Snapshot()

	ts2 := NewTombstoneSet()
	ts2.Load(snap)

	if !ts2.IsTombstoned("users", "1") || !ts2.IsTombstoned("orders", "10") {
		t.Error("Load did not restore all entries")
	}
}
