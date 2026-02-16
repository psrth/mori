package proxy

import (
	"testing"

	"github.com/mori-dev/mori/internal/core/delta"
	"github.com/mori-dev/mori/internal/logging"
)

func TestTxnHandler_InTxn_Default(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	txh := &TxnHandler{
		deltaMap:   dm,
		tombstones: ts,
		logger:     (*logging.Logger)(nil),
	}

	if txh.InTxn() {
		t.Error("InTxn should be false initially")
	}
}

func TestTxnHandler_CommitPromotesDeltas(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	// Stage some deltas (simulating what WriteHandler would do during a txn).
	dm.Stage("users", "1")
	ts.Stage("orders", "99")

	// Staged entries are visible via IsDelta/IsTombstoned (has() checks both
	// committed + staged) but not yet in the committed DeltaPKs list.
	if !dm.IsDelta("users", "1") {
		t.Error("staged delta should be visible via IsDelta during txn")
	}
	if len(dm.DeltaPKs("users")) != 0 {
		t.Error("staged delta should not appear in committed DeltaPKs before commit")
	}

	// Commit promotes staged entries into the committed set.
	dm.Commit()
	ts.Commit()

	if !dm.IsDelta("users", "1") {
		t.Error("delta should be visible after commit")
	}
	if len(dm.DeltaPKs("users")) != 1 || dm.DeltaPKs("users")[0] != "1" {
		t.Error("committed delta should appear in DeltaPKs after commit")
	}
	if !ts.IsTombstoned("orders", "99") {
		t.Error("tombstone should be visible after commit")
	}
}

func TestTxnHandler_RollbackDiscardsDeltas(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()

	dm.Stage("users", "1")
	ts.Stage("orders", "99")

	// Rollback discards staged entries.
	dm.Rollback()
	ts.Rollback()

	if dm.IsDelta("users", "1") {
		t.Error("delta should not be visible after rollback")
	}
	if ts.IsTombstoned("orders", "99") {
		t.Error("tombstone should not be visible after rollback")
	}
}

func TestTxnHandler_StageThenCommitThenRollback(t *testing.T) {
	dm := delta.NewMap()

	// First transaction: stage and commit.
	dm.Stage("t1", "1")
	dm.Commit()
	if !dm.IsDelta("t1", "1") {
		t.Error("committed delta should be visible")
	}

	// Second transaction: stage and rollback.
	dm.Stage("t1", "2")
	dm.Rollback()
	if dm.IsDelta("t1", "2") {
		t.Error("rolled-back delta should not be visible")
	}

	// First transaction's delta should still be visible.
	if !dm.IsDelta("t1", "1") {
		t.Error("previously committed delta should still be visible after another rollback")
	}
}
