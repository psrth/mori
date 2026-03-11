package proxy

import (
	"testing"

	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/engine/postgres/schema"
)

func TestParseInsertValues(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string
		wantRows int
	}{
		{
			"single row",
			"INSERT INTO orders (id, user_id, total) VALUES (1, 42, 100)",
			[]string{"id", "user_id", "total"},
			1,
		},
		{
			"multiple rows",
			"INSERT INTO orders (id, user_id) VALUES (1, 42), (2, 43)",
			[]string{"id", "user_id"},
			2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, cols, err := parseInsertValues(tt.sql)
			if err != nil {
				t.Fatalf("parseInsertValues() error: %v", err)
			}
			if len(cols) != len(tt.wantCols) {
				t.Errorf("cols len = %d, want %d", len(cols), len(tt.wantCols))
			}
			for i, c := range tt.wantCols {
				if i < len(cols) && cols[i] != c {
					t.Errorf("col[%d] = %q, want %q", i, cols[i], c)
				}
			}
			if len(rows) != tt.wantRows {
				t.Errorf("rows len = %d, want %d", len(rows), tt.wantRows)
			}
		})
	}
}

func TestParseUpdateSetValues(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantCol string
		wantVal string
	}{
		{
			"simple set",
			"UPDATE orders SET user_id = 42 WHERE id = 1",
			"user_id",
			"42",
		},
		{
			"string value",
			"UPDATE orders SET status = 'active' WHERE id = 1",
			"status",
			"active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vals, err := parseUpdateSetValues(tt.sql)
			if err != nil {
				t.Fatalf("parseUpdateSetValues() error: %v", err)
			}
			v, ok := vals[tt.wantCol]
			if !ok {
				t.Fatalf("column %q not found in SET values", tt.wantCol)
			}
			if v != tt.wantVal {
				t.Errorf("SET %s = %q, want %q", tt.wantCol, v, tt.wantVal)
			}
		})
	}
}

func TestFKEnforcerEnforceInsertParentExists(t *testing.T) {
	// Set up: parent exists in prod.
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()
	reg.RecordForeignKey("orders", coreSchema.ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	})

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       delta.NewMap(),
		tombstones:     delta.NewTombstoneSet(),
		tables:         map[string]schema.TableMeta{},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// Shadow returns no rows (parent not in shadow).
	shadowBackend.respondWith(emptySelectResponse([]string{"?column?"}))
	// Prod returns one row (parent exists).
	prodBackend.respondWith(selectResponse([]string{"?column?"}, [][]interface{}{{"1"}}, "SELECT 1"))

	err := fke.EnforceInsert("orders", "INSERT INTO orders (id, user_id, total) VALUES (1, 42, 100)")
	if err != nil {
		t.Fatalf("EnforceInsert() should pass when parent exists, got: %v", err)
	}
}

func TestFKEnforcerEnforceInsertParentMissing(t *testing.T) {
	// Set up: parent does NOT exist.
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()
	reg.RecordForeignKey("orders", coreSchema.ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	})

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       delta.NewMap(),
		tombstones:     delta.NewTombstoneSet(),
		tables:         map[string]schema.TableMeta{},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// Shadow returns no rows.
	shadowBackend.respondWith(emptySelectResponse([]string{"?column?"}))
	// Prod returns no rows.
	prodBackend.respondWith(emptySelectResponse([]string{"?column?"}))

	err := fke.EnforceInsert("orders", "INSERT INTO orders (id, user_id, total) VALUES (1, 999, 100)")
	if err == nil {
		t.Fatal("EnforceInsert() should fail when parent is missing")
	}
	if !containsStr(err.Error(), "foreign key constraint") {
		t.Errorf("error message should mention foreign key: %v", err)
	}
}

func TestFKEnforcerEnforceInsertNoFKs(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       delta.NewMap(),
		tombstones:     delta.NewTombstoneSet(),
		tables:         map[string]schema.TableMeta{},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// No FKs defined — should pass without any queries.
	err := fke.EnforceInsert("orders", "INSERT INTO orders (id, total) VALUES (1, 100)")
	if err != nil {
		t.Fatalf("EnforceInsert() should pass when no FKs defined, got: %v", err)
	}
	// Verify no backend queries were made.
	if received := prodBackend.getReceived(); len(received) > 0 {
		t.Error("no backend queries expected when no FKs defined")
	}
}

func TestFKEnforcerEnforceInsertParentInDelta(t *testing.T) {
	// Parent is in the delta map (already in shadow).
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()
	reg.RecordForeignKey("orders", coreSchema.ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	})

	dm := delta.NewMap()
	dm.Add("users", "42")

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       dm,
		tombstones:     delta.NewTombstoneSet(),
		tables: map[string]schema.TableMeta{
			"users": {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// Parent is in delta — no queries should be needed.
	err := fke.EnforceInsert("orders", "INSERT INTO orders (id, user_id, total) VALUES (1, 42, 100)")
	if err != nil {
		t.Fatalf("EnforceInsert() should pass when parent is in delta, got: %v", err)
	}
}

func TestFKEnforcerEnforceInsertParentTombstoned(t *testing.T) {
	// Parent is tombstoned (deleted).
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()
	reg.RecordForeignKey("orders", coreSchema.ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	})

	ts := delta.NewTombstoneSet()
	ts.Add("users", "42")

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       delta.NewMap(),
		tombstones:     ts,
		tables: map[string]schema.TableMeta{
			"users": {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// Parent is tombstoned — should fail.
	err := fke.EnforceInsert("orders", "INSERT INTO orders (id, user_id, total) VALUES (1, 42, 100)")
	if err == nil {
		t.Fatal("EnforceInsert() should fail when parent is tombstoned")
	}
}

func TestFKEnforcerEnforceUpdateRelevantColumn(t *testing.T) {
	// UPDATE changes an FK column to a non-existent parent.
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()
	reg.RecordForeignKey("orders", coreSchema.ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	})

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       delta.NewMap(),
		tombstones:     delta.NewTombstoneSet(),
		tables:         map[string]schema.TableMeta{},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// Shadow returns no rows.
	shadowBackend.respondWith(emptySelectResponse([]string{"?column?"}))
	// Prod returns no rows.
	prodBackend.respondWith(emptySelectResponse([]string{"?column?"}))

	err := fke.EnforceUpdate("orders", "UPDATE orders SET user_id = 999 WHERE id = 1")
	if err == nil {
		t.Fatal("EnforceUpdate() should fail when FK column updated to non-existent parent")
	}
}

func TestFKEnforcerEnforceUpdateIrrelevantColumn(t *testing.T) {
	prodConn, prodBackend := newMockBackend()
	defer prodConn.Close()
	defer prodBackend.conn.Close()

	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	reg := coreSchema.NewRegistry()
	reg.RecordForeignKey("orders", coreSchema.ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	})

	fke := &FKEnforcer{
		prodConn:       prodConn,
		shadowConn:     shadowConn,
		deltaMap:       delta.NewMap(),
		tombstones:     delta.NewTombstoneSet(),
		tables:         map[string]schema.TableMeta{},
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// Updating a non-FK column — should pass without any queries.
	err := fke.EnforceUpdate("orders", "UPDATE orders SET total = 200 WHERE id = 1")
	if err != nil {
		t.Fatalf("EnforceUpdate() should pass when non-FK column is updated, got: %v", err)
	}
}

func TestBuildFKErrorResponse(t *testing.T) {
	resp := buildFKErrorResponse("test violation")
	if len(resp) == 0 {
		t.Fatal("buildFKErrorResponse() returned empty response")
	}
	// Should start with 'E' (ErrorResponse).
	if resp[0] != 'E' {
		t.Errorf("first byte = %q, want 'E'", resp[0])
	}
	// Should contain the error message.
	if !containsBytes(resp, []byte("test violation")) {
		t.Error("response should contain the error message")
	}
	// Should contain SQLSTATE 23503.
	if !containsBytes(resp, []byte("23503")) {
		t.Error("response should contain SQLSTATE 23503")
	}
}

func TestBuildINClause(t *testing.T) {
	tests := []struct {
		pks  []string
		want string
	}{
		{[]string{"1"}, "'1'"},
		{[]string{"1", "2", "3"}, "'1', '2', '3'"},
		{[]string{"O'Brien"}, "'O''Brien'"},
	}

	for _, tt := range tests {
		got := buildINClause(tt.pks)
		if got != tt.want {
			t.Errorf("buildINClause(%v) = %q, want %q", tt.pks, got, tt.want)
		}
	}
}

func TestFkActionToString(t *testing.T) {
	tests := []struct {
		action string
		want   string
	}{
		{"c", "CASCADE"},
		{"r", "RESTRICT"},
		{"n", "SET NULL"},
		{"d", "SET DEFAULT"},
		{"a", "NO ACTION"},
		{"", "NO ACTION"},
		{"x", "NO ACTION"},
	}

	for _, tt := range tests {
		got := fkActionToString(tt.action)
		if got != tt.want {
			t.Errorf("fkActionToString(%q) = %q, want %q", tt.action, got, tt.want)
		}
	}
}

func containsStr(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || len(needle) == 0 || findSubstring(haystack, needle))
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func containsBytes(haystack, needle []byte) bool {
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
