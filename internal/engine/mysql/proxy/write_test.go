package proxy

import (
	"testing"

	"github.com/mori-dev/mori/internal/engine/mysql/schema"
)

// ---------------------------------------------------------------------------
// isReplaceInto (Fix 1)
// ---------------------------------------------------------------------------

func TestIsReplaceInto(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"REPLACE INTO users (id, name) VALUES (1, 'alice')", true},
		{"replace into users (id) values (1)", true},
		{"  REPLACE INTO t VALUES (1)", true},
		{"INSERT INTO users (id) VALUES (1)", false},
		{"SELECT * FROM users", false},
		{"UPDATE users SET name = 'bob'", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := isReplaceInto(tt.sql); got != tt.want {
				t.Errorf("isReplaceInto(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isAutoIncrement (Fix 1)
// ---------------------------------------------------------------------------

func TestIsAutoIncrement(t *testing.T) {
	tests := []struct {
		name string
		meta schema.TableMeta
		want bool
	}{
		{"serial", schema.TableMeta{PKType: "serial"}, true},
		{"bigserial", schema.TableMeta{PKType: "bigserial"}, true},
		{"uuid", schema.TableMeta{PKType: "uuid"}, false},
		{"text", schema.TableMeta{PKType: "text"}, false},
		{"empty", schema.TableMeta{PKType: ""}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAutoIncrement(tt.meta); got != tt.want {
				t.Errorf("isAutoIncrement(%+v) = %v, want %v", tt.meta, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// extractInsertPKs (Fix 1)
// ---------------------------------------------------------------------------

func TestExtractInsertPKs(t *testing.T) {
	// WriteHandler with minimal config — only tables map is needed.
	w := &WriteHandler{
		tables: map[string]schema.TableMeta{
			"users":  {PKColumns: []string{"id"}, PKType: "text"},
			"events": {PKColumns: []string{"event_id", "user_id"}, PKType: "text"},
		},
	}

	tests := []struct {
		name     string
		sql      string
		table    string
		meta     schema.TableMeta
		wantPKs  []string
		wantNone bool
	}{
		{
			"single row single PK",
			"INSERT INTO users (id, name) VALUES ('u1', 'Alice')",
			"users",
			schema.TableMeta{PKColumns: []string{"id"}, PKType: "text"},
			[]string{"u1"},
			false,
		},
		{
			"multiple rows single PK",
			"INSERT INTO users (id, name) VALUES ('u1', 'Alice'), ('u2', 'Bob')",
			"users",
			schema.TableMeta{PKColumns: []string{"id"}, PKType: "text"},
			[]string{"u1", "u2"},
			false,
		},
		{
			"composite PK",
			"INSERT INTO events (event_id, user_id, data) VALUES ('e1', 'u1', 'hello')",
			"events",
			schema.TableMeta{PKColumns: []string{"event_id", "user_id"}, PKType: "text"},
			[]string{`{"event_id":"e1","user_id":"u1"}`},
			false,
		},
		{
			"integer PK",
			"INSERT INTO users (id, name) VALUES (42, 'Alice')",
			"users",
			schema.TableMeta{PKColumns: []string{"id"}, PKType: "integer"},
			[]string{"42"},
			false,
		},
		{
			"function call PK — should return nil",
			"INSERT INTO users (id, name) VALUES (UUID(), 'Alice')",
			"users",
			schema.TableMeta{PKColumns: []string{"id"}, PKType: "text"},
			nil,
			true,
		},
		{
			"no column list — should return nil",
			"INSERT INTO users VALUES ('u1', 'Alice')",
			"users",
			schema.TableMeta{PKColumns: []string{"id"}, PKType: "text"},
			nil,
			true,
		},
		{
			"PK column missing from insert — should return nil",
			"INSERT INTO users (name) VALUES ('Alice')",
			"users",
			schema.TableMeta{PKColumns: []string{"id"}, PKType: "text"},
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pks := w.extractInsertPKs(tt.sql, tt.table, tt.meta)
			if tt.wantNone {
				if len(pks) != 0 {
					t.Errorf("expected nil/empty PKs, got %v", pks)
				}
				return
			}
			if len(pks) != len(tt.wantPKs) {
				t.Fatalf("PKs = %v (len %d), want %v (len %d)", pks, len(pks), tt.wantPKs, len(tt.wantPKs))
			}
			for i, want := range tt.wantPKs {
				if pks[i] != want {
					t.Errorf("PKs[%d] = %q, want %q", i, pks[i], want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// serializeCompositePK (Fix 1)
// ---------------------------------------------------------------------------

func TestSerializeCompositePK(t *testing.T) {
	tests := []struct {
		name   string
		cols   []string
		vals   []string
		want   string
	}{
		{
			"single PK",
			[]string{"id"},
			[]string{"42"},
			"42",
		},
		{
			"composite PK",
			[]string{"tenant_id", "user_id"},
			[]string{"t1", "u1"},
			`{"tenant_id":"t1","user_id":"u1"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := serializeCompositePK(tt.cols, tt.vals)
			if got != tt.want {
				t.Errorf("serializeCompositePK(%v, %v) = %q, want %q", tt.cols, tt.vals, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// readLenEncUint64 and lenEncIntSize (Fix 1 — OK packet parsing)
// ---------------------------------------------------------------------------

func TestReadLenEncUint64(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want uint64
	}{
		{"single byte 0", []byte{0}, 0},
		{"single byte 42", []byte{42}, 42},
		{"single byte 250 (max 1-byte)", []byte{250}, 250},
		{"two byte (0xfc prefix)", []byte{0xfc, 0x01, 0x00}, 1},
		{"two byte 300", []byte{0xfc, 0x2c, 0x01}, 300},
		{"empty", []byte{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := readLenEncUint64(tt.data)
			if got != tt.want {
				t.Errorf("readLenEncUint64(%v) = %d, want %d", tt.data, got, tt.want)
			}
		})
	}
}

func TestLenEncIntSize(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want int
	}{
		{"single byte", []byte{42}, 1},
		{"two byte prefix", []byte{0xfc, 0x01, 0x00}, 3},
		{"three byte prefix", []byte{0xfd, 0x01, 0x00, 0x00}, 4},
		{"eight byte prefix", []byte{0xfe, 0, 0, 0, 0, 0, 0, 0, 0}, 9},
		{"empty", []byte{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lenEncIntSize(tt.data)
			if got != tt.want {
				t.Errorf("lenEncIntSize(%v) = %d, want %d", tt.data, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// vitessRefActionToString (Fix 2 — FK extraction)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// extractPKsFromResult (Fix 3 — RETURNING write handling)
// ---------------------------------------------------------------------------

func TestExtractPKsFromResult(t *testing.T) {
	t.Run("single PK column", func(t *testing.T) {
		result := &QueryResult{
			Columns: []ColumnInfo{
				{Name: "id"},
				{Name: "name"},
			},
			RowValues: [][]string{
				{"1", "foo"},
				{"2", "bar"},
			},
		}
		pks := extractPKsFromResult(result, []string{"id"})
		if len(pks) != 2 {
			t.Fatalf("pks len = %d, want 2", len(pks))
		}
		if pks[0] != "1" {
			t.Errorf("pks[0] = %q, want %q", pks[0], "1")
		}
		if pks[1] != "2" {
			t.Errorf("pks[1] = %q, want %q", pks[1], "2")
		}
	})

	t.Run("composite PK", func(t *testing.T) {
		result := &QueryResult{
			Columns: []ColumnInfo{
				{Name: "tenant_id"},
				{Name: "user_id"},
				{Name: "name"},
			},
			RowValues: [][]string{
				{"t1", "u1", "foo"},
			},
		}
		pks := extractPKsFromResult(result, []string{"tenant_id", "user_id"})
		if len(pks) != 1 {
			t.Fatalf("pks len = %d, want 1", len(pks))
		}
		want := `{"tenant_id":"t1","user_id":"u1"}`
		if pks[0] != want {
			t.Errorf("pks[0] = %q, want %q", pks[0], want)
		}
	})

	t.Run("missing PK column", func(t *testing.T) {
		result := &QueryResult{
			Columns: []ColumnInfo{
				{Name: "name"},
				{Name: "email"},
			},
			RowValues: [][]string{
				{"foo", "foo@example.com"},
			},
		}
		pks := extractPKsFromResult(result, []string{"id"})
		if pks != nil {
			t.Errorf("expected nil, got %v", pks)
		}
	})

	t.Run("empty result", func(t *testing.T) {
		result := &QueryResult{
			Columns:   []ColumnInfo{{Name: "id"}},
			RowValues: [][]string{},
		}
		pks := extractPKsFromResult(result, []string{"id"})
		if pks != nil {
			t.Errorf("expected nil, got %v", pks)
		}
	})
}

// ---------------------------------------------------------------------------
// vitessRefActionToString (Fix 2 — FK extraction)
// ---------------------------------------------------------------------------

func TestVitessRefActionToString(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		// We test a few key conversions.
		{"no_action", "NO ACTION"},
	}
	// The default case also returns "NO ACTION".
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := vitessRefActionToString(0) // 0 = DefaultAction → "NO ACTION"
			if got != tt.want {
				t.Errorf("vitessRefActionToString(0) = %q, want %q", got, tt.want)
			}
		})
	}
}
