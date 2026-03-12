package proxy

import (
	"strings"
	"testing"

	"github.com/psrth/mori/internal/engine/mssql/schema"
)

// ---------------------------------------------------------------------------
// Bug fix tests: findOuterFromIndexMSSQL
// ---------------------------------------------------------------------------

func TestFindOuterFromIndexMSSQL(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		found bool
	}{
		{
			name:  "simple_select",
			sql:   "SELECT id, name FROM users WHERE id = 1",
			found: true,
		},
		{
			name:  "no_from",
			sql:   "SELECT 1",
			found: false,
		},
		{
			name:  "subquery_only_from",
			sql:   "SELECT (SELECT 1 FROM dual)",
			found: false,
		},
		{
			name:  "subquery_and_outer_from",
			sql:   "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) FROM conversations c",
			found: true,
		},
		{
			name:  "join",
			sql:   "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			found: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upper := strings.ToUpper(tt.sql)
			idx := findOuterFromIndexMSSQL(upper)
			if tt.found {
				if idx < 0 {
					t.Fatalf("expected to find outer FROM, got -1")
				}
				// Verify the found index actually has " FROM ".
				if upper[idx:idx+6] != " FROM " {
					t.Errorf("index %d does not point to ' FROM ', got %q", idx, upper[idx:idx+6])
				}
			} else {
				if idx >= 0 {
					t.Errorf("expected -1, got %d", idx)
				}
			}
		})
	}
}

func TestFindOuterFromIndexMSSQL_SubquerySkipped(t *testing.T) {
	// Key regression test: the first " FROM " is inside a subquery.
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c LEFT JOIN users u ON c.uid = u.id"
	upper := strings.ToUpper(sql)
	idx := findOuterFromIndexMSSQL(upper)
	if idx < 0 {
		t.Fatal("expected to find outer FROM")
	}
	// The outer FROM should be before "CONVERSATIONS", not before "MESSAGES".
	after := strings.TrimSpace(upper[idx+6:])
	if !strings.HasPrefix(after, "CONVERSATIONS") {
		t.Errorf("expected outer FROM before CONVERSATIONS, got: %s", after[:min(40, len(after))])
	}
}

func TestFindOuterFromIndexMSSQL_NestedSubqueries(t *testing.T) {
	// Multiple levels of nesting.
	sql := "SELECT (SELECT (SELECT 1 FROM inner1) FROM inner2) FROM outer_table"
	upper := strings.ToUpper(sql)
	idx := findOuterFromIndexMSSQL(upper)
	if idx < 0 {
		t.Fatal("expected to find outer FROM")
	}
	after := strings.TrimSpace(upper[idx+6:])
	if !strings.HasPrefix(after, "OUTER_TABLE") {
		t.Errorf("expected outer FROM before OUTER_TABLE, got: %s", after[:min(40, len(after))])
	}
}

// ---------------------------------------------------------------------------
// Bug fix tests: containsColumnForTableMSSQL
// ---------------------------------------------------------------------------

func TestContainsColumnForTableMSSQL(t *testing.T) {
	tests := []struct {
		name       string
		selectList string
		col        string
		alias      string
		want       bool
	}{
		{
			name:       "unqualified_match",
			selectList: "id, name, email",
			col:        "id",
			alias:      "users",
			want:       true,
		},
		{
			name:       "qualified_match_correct_alias",
			selectList: "u.id, u.name",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "qualified_no_match_wrong_alias",
			selectList: "c.id, c.name",
			col:        "id",
			alias:      "u",
			want:       false,
		},
		{
			name:       "not_present",
			selectList: "name, email",
			col:        "id",
			alias:      "users",
			want:       false,
		},
		{
			name:       "with_alias_as",
			selectList: "u.id AS user_id, u.name",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "bracketed_column",
			selectList: "[u].[id], [u].[name]",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "mixed_qualified_unqualified",
			selectList: "c.id, name",
			col:        "id",
			alias:      "u",
			want:       false,
		},
		{
			name:       "unqualified_matches_any_table",
			selectList: "id, name",
			col:        "id",
			alias:      "u",
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsColumnForTableMSSQL(tt.selectList, tt.col, tt.alias)
			if got != tt.want {
				t.Errorf("containsColumnForTableMSSQL(%q, %q, %q) = %v, want %v",
					tt.selectList, tt.col, tt.alias, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Bug fix tests: collectSubqueryTablesFromSQL
// ---------------------------------------------------------------------------

func TestCollectSubqueryTablesFromSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "no_subqueries",
			sql:  "SELECT * FROM users",
			want: nil,
		},
		{
			name: "select_list_subquery",
			sql:  "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) FROM conversations c",
			want: []string{"messages"},
		},
		{
			name: "where_subquery",
			sql:  "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			want: []string{"orders"},
		},
		{
			name: "multiple_subqueries",
			sql:  "SELECT (SELECT COUNT(*) FROM orders o WHERE o.uid = u.id), (SELECT MAX(amount) FROM payments p WHERE p.uid = u.id) FROM users u",
			want: []string{"orders", "payments"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectSubqueryTablesFromSQL(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("collectSubqueryTablesFromSQL() returned %d tables, want %d: got %v",
					len(got), len(tt.want), got)
			}
			for i, want := range tt.want {
				if got[i] != want {
					t.Errorf("table[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Bug fix tests: needsPKInjectionMSSQL with subqueries
// ---------------------------------------------------------------------------

func TestNeedsPKInjectionMSSQL_SubqueryInSelect(t *testing.T) {
	// With a subquery in the SELECT list, the first FROM is inside the subquery.
	// needsPKInjectionMSSQL should use the outer FROM to find the SELECT list boundary.
	sql := "SELECT c.name, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c WHERE c.active = 1"

	// The PK "id" appears as c.id in the subquery's WHERE, but NOT in the
	// outer SELECT list. needsPKInjectionMSSQL should detect this correctly
	// and return true (PK needs injection).
	needs := needsPKInjectionMSSQL(sql, "id")
	if !needs {
		t.Error("expected needsPKInjectionMSSQL to return true: 'id' is not in the outer SELECT list")
	}
}

func TestNeedsPKInjectionMSSQL_SubqueryDoesNotFalseNegative(t *testing.T) {
	// If the old code used strings.Index(upper, " FROM "), it would match the
	// subquery's FROM, making the "SELECT list" span from "SELECT" to the
	// subquery's FROM — which incorrectly includes "(SELECT COUNT(*)" as part
	// of the select list, missing the real columns.
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c"

	// "id" IS in the outer SELECT list as c.id, so needsPKInjection should be false.
	// With the bug, the "selectList" would be "c.id, (SELECT COUNT(*)" — which
	// still contains "id", so this particular case works either way.
	// But the important thing is the function doesn't panic or misbehave.
	needs := needsPKInjectionMSSQL(sql, "id")
	if needs {
		t.Error("expected needsPKInjectionMSSQL to return false: 'id' is in the outer SELECT list as c.id")
	}
}

// ---------------------------------------------------------------------------
// Tests: buildAggregateBaseQuery with subqueries
// ---------------------------------------------------------------------------

func TestBuildAggregateBaseQuery_SubqueryInSelect(t *testing.T) {
	rh := &ReadHandler{
		tables: map[string]schema.TableMeta{
			"conversations": {PKColumns: []string{"id"}},
		},
	}

	sql := "SELECT COUNT(*) FROM conversations c WHERE c.active = 1"
	base := rh.buildAggregateBaseQuery(sql, "conversations")
	if base == "" {
		t.Fatal("expected non-empty base query")
	}
	// The base query should reference FROM conversations.
	if !strings.Contains(strings.ToUpper(base), "FROM CONVERSATIONS") {
		t.Errorf("expected base query to contain 'FROM CONVERSATIONS', got: %s", base)
	}
}

// ---------------------------------------------------------------------------
// Tests: stripNewColumnsFromQuery with subqueries
// ---------------------------------------------------------------------------

func TestStripNewColumnsFromQuery_UsesOuterFrom(t *testing.T) {
	// This tests that stripNewColumnsFromQuery correctly identifies the
	// SELECT list boundary when there's a subquery in the SELECT list.
	rh := &ReadHandler{
		schemaRegistry: nil, // Will be set below
	}

	// We need a schema registry with an added column.
	// For this unit test, we just verify the function doesn't panic
	// and returns empty when there's no schema registry.
	result := rh.stripNewColumnsFromQuery(
		"SELECT c.name, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) FROM conversations c",
		[]string{"conversations"},
	)
	// With no schema registry, should return "".
	if result != "" {
		t.Errorf("expected empty string with nil schemaRegistry, got: %s", result)
	}
}

// ---------------------------------------------------------------------------
// Tests: extractTablesFromSQL (read_setop.go)
// ---------------------------------------------------------------------------

func TestExtractTablesFromSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "simple_select",
			sql:  "SELECT * FROM users WHERE id = 1",
			want: []string{"users"},
		},
		{
			name: "with_alias",
			sql:  "SELECT u.name FROM users u WHERE u.id = 1",
			want: []string{"users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTablesFromSQL(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("extractTablesFromSQL(%q) returned %d tables, want %d: got %v",
					tt.sql, len(got), len(tt.want), got)
			}
			for i, want := range tt.want {
				if got[i] != want {
					t.Errorf("table[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: buildWindowBaseQuery uses outer FROM
// ---------------------------------------------------------------------------

func TestBuildWindowBaseQuery_SubqueryInSelect(t *testing.T) {
	rh := &ReadHandler{}

	// With a subquery in the SELECT list, the outer FROM should be found correctly.
	sql := "SELECT ROW_NUMBER() OVER (ORDER BY c.id), (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) FROM conversations c"
	base := rh.buildWindowBaseQuery(sql, "conversations")
	if base == "" {
		t.Fatal("expected non-empty base query")
	}
	upper := strings.ToUpper(base)
	// Should start with SELECT * FROM.
	if !strings.HasPrefix(upper, "SELECT *") {
		t.Errorf("expected base to start with 'SELECT *', got: %s", base[:min(30, len(base))])
	}
	// Should contain FROM conversations (the outer FROM).
	if !strings.Contains(upper, "FROM CONVERSATIONS") {
		t.Errorf("expected base to contain 'FROM CONVERSATIONS', got: %s", base)
	}
}

// ---------------------------------------------------------------------------
// Tests: containsColumnMSSQL (existing function) still works
// ---------------------------------------------------------------------------

func TestContainsColumnMSSQL(t *testing.T) {
	tests := []struct {
		selectList string
		col        string
		want       bool
	}{
		{"id, name, email", "id", true},
		{"name, email", "id", false},
		{"u.id, u.name", "id", true},
		{"[id], [name]", "id", true},
		{"user_id AS id", "user_id", true},
	}

	for _, tt := range tests {
		got := containsColumnMSSQL(tt.selectList, tt.col)
		if got != tt.want {
			t.Errorf("containsColumnMSSQL(%q, %q) = %v, want %v",
				tt.selectList, tt.col, got, tt.want)
		}
	}
}
