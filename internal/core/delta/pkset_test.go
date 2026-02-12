package delta

import (
	"sync"
	"testing"
)

func TestNewPKSetEmpty(t *testing.T) {
	s := newPKSet()
	if got := s.tables(); got != nil {
		t.Errorf("tables() = %v, want nil", got)
	}
}

func TestPKSetAddAndHas(t *testing.T) {
	s := newPKSet()
	s.add("users", "42")
	if !s.has("users", "42") {
		t.Error("has(users, 42) = false, want true")
	}
}

func TestPKSetHasNonExistent(t *testing.T) {
	s := newPKSet()
	if s.has("users", "99") {
		t.Error("has(users, 99) = true for empty set, want false")
	}
}

func TestPKSetRemove(t *testing.T) {
	s := newPKSet()
	s.add("users", "42")
	s.remove("users", "42")
	if s.has("users", "42") {
		t.Error("has(users, 42) = true after remove, want false")
	}
	// Inner map should be cleaned up.
	if len(s.entries) != 0 {
		t.Errorf("entries has %d tables after removing last pk, want 0", len(s.entries))
	}
}

func TestPKSetRemoveNonExistent(t *testing.T) {
	s := newPKSet()
	s.remove("users", "99") // should not panic
}

func TestPKSetPKsSorted(t *testing.T) {
	s := newPKSet()
	s.add("users", "3")
	s.add("users", "1")
	s.add("users", "2")
	got := s.pks("users")
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("pks(users) len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pks(users)[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPKSetPKsEmptyTable(t *testing.T) {
	s := newPKSet()
	if got := s.pks("users"); got != nil {
		t.Errorf("pks(users) = %v for unknown table, want nil", got)
	}
}

func TestPKSetCountForTable(t *testing.T) {
	s := newPKSet()
	s.add("users", "1")
	s.add("users", "2")
	s.add("orders", "10")
	if got := s.countForTable("users"); got != 2 {
		t.Errorf("countForTable(users) = %d, want 2", got)
	}
	if got := s.countForTable("orders"); got != 1 {
		t.Errorf("countForTable(orders) = %d, want 1", got)
	}
	if got := s.countForTable("missing"); got != 0 {
		t.Errorf("countForTable(missing) = %d, want 0", got)
	}
}

func TestPKSetTables(t *testing.T) {
	s := newPKSet()
	s.add("orders", "1")
	s.add("users", "1")
	got := s.tables()
	want := []string{"orders", "users"}
	if len(got) != len(want) {
		t.Fatalf("tables() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tables()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPKSetStageAndCommit(t *testing.T) {
	s := newPKSet()
	s.add("users", "1") // committed
	s.stage("users", "2")
	s.stage("orders", "10")

	// Staged entries should NOT appear in pks (committed only).
	if got := s.pks("users"); len(got) != 1 {
		t.Errorf("pks(users) before commit = %v, want [1]", got)
	}

	s.commit()

	// After commit, staged entries are promoted.
	got := s.pks("users")
	if len(got) != 2 {
		t.Fatalf("pks(users) after commit len = %d, want 2", len(got))
	}
	if got[0] != "1" || got[1] != "2" {
		t.Errorf("pks(users) after commit = %v, want [1 2]", got)
	}
	if got := s.pks("orders"); len(got) != 1 || got[0] != "10" {
		t.Errorf("pks(orders) after commit = %v, want [10]", got)
	}
}

func TestPKSetStageAndRollback(t *testing.T) {
	s := newPKSet()
	s.add("users", "1")
	s.stage("users", "2")

	s.rollback()

	// Staged entry should be discarded.
	got := s.pks("users")
	if len(got) != 1 || got[0] != "1" {
		t.Errorf("pks(users) after rollback = %v, want [1]", got)
	}
}

func TestPKSetStageHasVisibility(t *testing.T) {
	s := newPKSet()
	s.stage("users", "42")

	// has() should see staged entries.
	if !s.has("users", "42") {
		t.Error("has(users, 42) = false for staged entry, want true")
	}
}

func TestPKSetSnapshotAndLoad(t *testing.T) {
	s := newPKSet()
	s.add("users", "1")
	s.add("users", "2")
	s.add("orders", "10")

	snap := s.snapshot()

	s2 := newPKSet()
	s2.load(snap)

	// Verify identical state.
	for _, table := range []string{"users", "orders"} {
		orig := s.pks(table)
		loaded := s2.pks(table)
		if len(orig) != len(loaded) {
			t.Fatalf("pks(%s) len mismatch: orig=%d loaded=%d", table, len(orig), len(loaded))
		}
		for i := range orig {
			if orig[i] != loaded[i] {
				t.Errorf("pks(%s)[%d] mismatch: orig=%q loaded=%q", table, i, orig[i], loaded[i])
			}
		}
	}
}

func TestPKSetConcurrentAccess(t *testing.T) {
	s := newPKSet()
	var wg sync.WaitGroup

	// Writers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				pk := string(rune('0' + j%10))
				s.add("users", pk)
				s.has("users", pk)
			}
		}(i)
	}

	// Readers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.pks("users")
				s.tables()
				s.countForTable("users")
			}
		}()
	}

	wg.Wait()
}
