package proxy

import (
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
)

func TestTxnHandler_InTxn(t *testing.T) {
	p := &Proxy{
		deltaMap:   delta.NewMap(),
		tombstones: delta.NewTombstoneSet(),
	}
	th := &TxnHandler{proxy: p, connID: 1}

	if th.InTxn() {
		t.Error("expected InTxn() = false before BEGIN")
	}
}

func TestTrackWriteEffects_StagedDeltas(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()
	moriDir := t.TempDir()
	p := &Proxy{
		deltaMap:   dm,
		tombstones: ts,
		moriDir:    moriDir,
	}
	txh := &TxnHandler{proxy: p, connID: 1, inTxn: true}

	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "42"}},
	}

	p.trackWriteEffects(cl, core.StrategyHydrateAndWrite, 1, txh)

	// IsDelta returns true for staged entries (visible within txn).
	if !dm.IsDelta("users", "42") {
		t.Error("expected staged delta to be visible via IsDelta during txn")
	}

	// Rollback should discard staged entry.
	dm.Rollback()
	if dm.IsDelta("users", "42") {
		t.Error("expected delta to be discarded after Rollback()")
	}

	// Stage again and commit.
	p.trackWriteEffects(cl, core.StrategyHydrateAndWrite, 1, txh)
	dm.Commit()
	if !dm.IsDelta("users", "42") {
		t.Error("expected delta to persist after Commit()")
	}
}

func TestTrackWriteEffects_StagedTombstones(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()
	moriDir := t.TempDir()
	p := &Proxy{
		deltaMap:   dm,
		tombstones: ts,
		moriDir:    moriDir,
	}
	txh := &TxnHandler{proxy: p, connID: 1, inTxn: true}

	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubDelete,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "7"}},
	}

	p.trackWriteEffects(cl, core.StrategyShadowDelete, 1, txh)

	if !ts.IsTombstoned("users", "7") {
		t.Error("expected staged tombstone to be visible via IsTombstoned during txn")
	}

	ts.Rollback()
	if ts.IsTombstoned("users", "7") {
		t.Error("expected tombstone to be discarded after Rollback()")
	}
}

func TestTrackWriteEffects_OutsideTxn(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()
	moriDir := t.TempDir()
	p := &Proxy{
		deltaMap:   dm,
		tombstones: ts,
		moriDir:    moriDir,
	}

	cl := &core.Classification{
		OpType:  core.OpWrite,
		SubType: core.SubUpdate,
		Tables:  []string{"users"},
		PKs:     []core.TablePK{{Table: "users", PK: "1"}},
	}

	p.trackWriteEffects(cl, core.StrategyHydrateAndWrite, 1, nil)

	if !dm.IsDelta("users", "1") {
		t.Error("expected delta to be immediately committed outside txn")
	}
}

func TestParseAlterRenameColumn(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantTable   string
		wantOldName string
		wantNewName string
	}{
		{
			name:        "basic_rename",
			sql:         `ALTER TABLE users RENAME COLUMN name TO full_name`,
			wantTable:   "users",
			wantOldName: "name",
			wantNewName: "full_name",
		},
		{
			name:        "quoted_table",
			sql:         `ALTER TABLE "my_table" RENAME COLUMN old_col TO new_col`,
			wantTable:   "my_table",
			wantOldName: "old_col",
			wantNewName: "new_col",
		},
		{
			name:        "no_column_keyword",
			sql:         `ALTER TABLE users RENAME name TO full_name`,
			wantTable:   "users",
			wantOldName: "name",
			wantNewName: "full_name",
		},
		{
			name:        "too_short",
			sql:         `ALTER TABLE users`,
			wantTable:   "",
			wantOldName: "",
			wantNewName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, oldName, newName := parseAlterRenameColumn(tt.sql)
			if table != tt.wantTable {
				t.Errorf("table = %q, want %q", table, tt.wantTable)
			}
			if oldName != tt.wantOldName {
				t.Errorf("oldName = %q, want %q", oldName, tt.wantOldName)
			}
			if newName != tt.wantNewName {
				t.Errorf("newName = %q, want %q", newName, tt.wantNewName)
			}
		})
	}
}

func TestExtHandler_Accumulate_Parse(t *testing.T) {
	p := &Proxy{}
	eh := &ExtHandler{
		proxy:     p,
		connID:    1,
		stmtCache: make(map[string]string),
	}

	payload := []byte("stmt1\x00SELECT * FROM users\x00\x00\x00")
	msg := &pgMsg{Type: 'P', Payload: payload}

	eh.Accumulate(msg)

	if !eh.batchHasParse {
		t.Error("expected batchHasParse = true")
	}
	if eh.batchSQL != "SELECT * FROM users" {
		t.Errorf("batchSQL = %q, want %q", eh.batchSQL, "SELECT * FROM users")
	}
	if cached, ok := eh.stmtCache["stmt1"]; !ok || cached != "SELECT * FROM users" {
		t.Errorf("stmtCache[\"stmt1\"] = %q, want %q", cached, "SELECT * FROM users")
	}
}

func TestExtHandler_ClearBatch(t *testing.T) {
	p := &Proxy{}
	eh := &ExtHandler{
		proxy:     p,
		connID:    1,
		stmtCache: make(map[string]string),
	}

	eh.batchSQL = "SELECT 1"
	eh.batchHasParse = true
	eh.batchHasBind = true
	eh.batchHasExec = true

	eh.clearBatch()

	if eh.batchSQL != "" {
		t.Error("expected batchSQL to be cleared")
	}
	if eh.batchHasParse || eh.batchHasBind || eh.batchHasExec {
		t.Error("expected batch flags to be cleared")
	}
}

func TestReconstructSQL(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		params []string
		want   string
	}{
		{
			name:   "single_param",
			sql:    "SELECT * FROM users WHERE id = $1",
			params: []string{"42"},
			want:   "SELECT * FROM users WHERE id = '42'",
		},
		{
			name:   "multiple_params",
			sql:    "SELECT * FROM users WHERE name = $1 AND id = $2",
			params: []string{"alice", "1"},
			want:   "SELECT * FROM users WHERE name = 'alice' AND id = '1'",
		},
		{
			name:   "null_param",
			sql:    "INSERT INTO users (name) VALUES ($1)",
			params: []string{""},
			want:   "INSERT INTO users (name) VALUES (NULL)",
		},
		{
			name:   "escape_single_quote",
			sql:    "INSERT INTO users (name) VALUES ($1)",
			params: []string{"O'Brien"},
			want:   "INSERT INTO users (name) VALUES ('O''Brien')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reconstructSQL(tt.sql, tt.params)
			if got != tt.want {
				t.Errorf("reconstructSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsExtendedProtocolMsg(t *testing.T) {
	extended := []byte{'P', 'B', 'D', 'E', 'C', 'S', 'H'}
	for _, b := range extended {
		if !isExtendedProtocolMsg(b) {
			t.Errorf("expected isExtendedProtocolMsg(%c) = true", b)
		}
	}

	nonExtended := []byte{'Q', 'X', 'Z', 'R'}
	for _, b := range nonExtended {
		if isExtendedProtocolMsg(b) {
			t.Errorf("expected isExtendedProtocolMsg(%c) = false", b)
		}
	}
}

func TestParseParseMsgPayload(t *testing.T) {
	payload := []byte("\x00SELECT 1\x00")
	name, sql, err := parseParseMsgPayload(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "" {
		t.Errorf("name = %q, want empty string (unnamed)", name)
	}
	if sql != "SELECT 1" {
		t.Errorf("sql = %q, want %q", sql, "SELECT 1")
	}
}

func TestParseCloseMsgPayload(t *testing.T) {
	payload := []byte("Sstmt1\x00")
	closeType, name, err := parseCloseMsgPayload(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if closeType != 'S' {
		t.Errorf("closeType = %c, want S", closeType)
	}
	if name != "stmt1" {
		t.Errorf("name = %q, want %q", name, "stmt1")
	}
}
