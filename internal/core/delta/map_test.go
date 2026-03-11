package delta

import "testing"

func TestNewMapNotNil(t *testing.T) {
	m := NewMap()
	if m == nil {
		t.Fatal("NewMap() returned nil")
	}
}

func TestMapAddAndIsDelta(t *testing.T) {
	m := NewMap()
	m.Add("users", "42")
	if !m.IsDelta("users", "42") {
		t.Error("IsDelta(users, 42) = false, want true")
	}
}

func TestMapIsDeltaFalseForClean(t *testing.T) {
	m := NewMap()
	if m.IsDelta("users", "42") {
		t.Error("IsDelta(users, 42) = true for clean map, want false")
	}
}

func TestMapRemove(t *testing.T) {
	m := NewMap()
	m.Add("users", "42")
	m.Remove("users", "42")
	if m.IsDelta("users", "42") {
		t.Error("IsDelta(users, 42) = true after Remove, want false")
	}
}

func TestMapDeltaPKs(t *testing.T) {
	m := NewMap()
	m.Add("users", "3")
	m.Add("users", "1")
	m.Add("users", "2")
	got := m.DeltaPKs("users")
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("DeltaPKs len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("DeltaPKs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMapCountForTable(t *testing.T) {
	m := NewMap()
	m.Add("users", "1")
	m.Add("users", "2")
	if got := m.CountForTable("users"); got != 2 {
		t.Errorf("CountForTable(users) = %d, want 2", got)
	}
}

func TestMapAnyTableDelta(t *testing.T) {
	m := NewMap()
	m.Add("orders", "1")

	if !m.AnyTableDelta([]string{"users", "orders"}) {
		t.Error("AnyTableDelta([users, orders]) = false, want true")
	}
	if m.AnyTableDelta([]string{"users", "products"}) {
		t.Error("AnyTableDelta([users, products]) = true, want false")
	}
}

func TestMapStageCommit(t *testing.T) {
	m := NewMap()
	m.Stage("users", "42")
	m.Commit()
	if !m.IsDelta("users", "42") {
		t.Error("IsDelta(users, 42) = false after Stage+Commit, want true")
	}
}

func TestMapStageRollback(t *testing.T) {
	m := NewMap()
	m.Stage("users", "42")
	m.Rollback()
	if m.IsDelta("users", "42") {
		t.Error("IsDelta(users, 42) = true after Stage+Rollback, want false")
	}
}

func TestMapTables(t *testing.T) {
	m := NewMap()
	m.Add("orders", "1")
	m.Add("users", "1")
	got := m.Tables()
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

func TestMapRenameTable(t *testing.T) {
	m := NewMap()
	m.Add("users", "1")
	m.Add("users", "2")
	m.AddInsertCount("users", 5)

	m.RenameTable("users", "customers")

	// Delta entries moved.
	if !m.IsDelta("customers", "1") || !m.IsDelta("customers", "2") {
		t.Error("delta entries not moved to new name")
	}
	if m.IsDelta("users", "1") || m.IsDelta("users", "2") {
		t.Error("delta entries still under old name")
	}
	// Insert count moved.
	if got := m.InsertCountForTable("customers"); got != 5 {
		t.Errorf("InsertCountForTable(customers) = %d, want 5", got)
	}
	if m.HasInserts("users") {
		t.Error("old table still has insert count after rename")
	}
}

func TestMapRenameTableMerge(t *testing.T) {
	m := NewMap()
	m.Add("old_t", "1")
	m.AddInsertCount("old_t", 3)
	m.Add("new_t", "2")
	m.AddInsertCount("new_t", 7)

	m.RenameTable("old_t", "new_t")

	if got := m.CountForTable("new_t"); got != 2 {
		t.Errorf("CountForTable(new_t) = %d, want 2", got)
	}
	if got := m.InsertCountForTable("new_t"); got != 10 {
		t.Errorf("InsertCountForTable(new_t) = %d, want 10", got)
	}
}

func TestMapRenameTableWithStagedInserts(t *testing.T) {
	m := NewMap()
	m.Add("users", "1")
	m.StageInsertCount("users", 3)

	m.RenameTable("users", "customers")

	// Staged inserts should also be moved.
	m.CommitInsertCounts()
	if got := m.InsertCountForTable("customers"); got != 3 {
		t.Errorf("InsertCountForTable(customers) = %d after commit, want 3", got)
	}
}

func TestMapSnapshotAndLoad(t *testing.T) {
	m := NewMap()
	m.Add("users", "1")
	m.Add("users", "2")
	m.Add("orders", "10")

	snap := m.Snapshot()

	m2 := NewMap()
	m2.Load(snap)

	if !m2.IsDelta("users", "1") || !m2.IsDelta("users", "2") || !m2.IsDelta("orders", "10") {
		t.Error("Load did not restore all entries")
	}
	if m2.IsDelta("users", "99") {
		t.Error("Load introduced phantom entry")
	}
}
