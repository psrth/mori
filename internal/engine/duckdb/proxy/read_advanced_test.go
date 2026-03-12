package proxy

import (
	"strings"
	"testing"
)

// --- Bug fix tests: findOuterFromIndex ---

func TestFindOuterFromIndex(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		found bool
	}{
		{
			name:  "simple query",
			sql:   "SELECT id, name FROM users",
			found: true,
		},
		{
			name:  "subquery in SELECT list",
			sql:   "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c",
			found: true,
		},
		{
			name:  "no FROM",
			sql:   "SELECT 1",
			found: false,
		},
		{
			name:  "nested subqueries",
			sql:   "SELECT (SELECT (SELECT 1 FROM a) FROM b) FROM c",
			found: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upper := strings.ToUpper(tt.sql)
			idx := findOuterFromIndex(upper)
			if tt.found {
				if idx < 0 {
					t.Fatalf("expected to find outer FROM, got -1")
				}
			} else if idx >= 0 {
				t.Errorf("expected -1, got %d", idx)
			}
		})
	}
}

func TestFindOuterFromIndex_SubquerySkipped(t *testing.T) {
	// Key regression test: the first " FROM " is inside a subquery.
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c LEFT JOIN users u ON c.uid = u.id"
	upper := strings.ToUpper(sql)
	idx := findOuterFromIndex(upper)
	if idx < 0 {
		t.Fatal("expected to find outer FROM")
	}
	// The outer FROM should be before "CONVERSATIONS", not before "MESSAGES".
	after := strings.TrimSpace(upper[idx+6:])
	if !strings.HasPrefix(after, "CONVERSATIONS") {
		t.Errorf("expected outer FROM before CONVERSATIONS, got: %s", after[:min(40, len(after))])
	}
}

func TestFindOuterFromIndex_MultipleSubqueries(t *testing.T) {
	sql := "SELECT (SELECT 1 FROM a), (SELECT 2 FROM b) FROM real_table"
	upper := strings.ToUpper(sql)
	idx := findOuterFromIndex(upper)
	if idx < 0 {
		t.Fatal("expected to find outer FROM")
	}
	after := strings.TrimSpace(upper[idx+6:])
	if !strings.HasPrefix(after, "REAL_TABLE") {
		t.Errorf("expected outer FROM before REAL_TABLE, got: %s", after[:min(40, len(after))])
	}
}

// --- Bug fix tests: containsColumnForTable ---

func TestContainsColumnForTable(t *testing.T) {
	tests := []struct {
		name       string
		selectList string
		col        string
		alias      string
		want       bool
	}{
		{
			name:       "qualified match",
			selectList: "u.id, u.name, o.total",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "qualified mismatch - different alias",
			selectList: "c.id, u.name",
			col:        "id",
			alias:      "u",
			want:       false,
		},
		{
			name:       "unqualified match",
			selectList: "id, name",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "no match",
			selectList: "c.id, u.name, o.total",
			col:        "email",
			alias:      "u",
			want:       false,
		},
		{
			name:       "with AS alias",
			selectList: "u.name AS user_name, c.id",
			col:        "name",
			alias:      "u",
			want:       true,
		},
		{
			name:       "subquery in list - qualified column not matching",
			selectList: "c.id, c.user_id, (select count(*)",
			col:        "id",
			alias:      "u",
			want:       false,
		},
		{
			name:       "case insensitive match",
			selectList: "U.ID, u.name",
			col:        "id",
			alias:      "u",
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsColumnForTable(tt.selectList, tt.col, tt.alias)
			if got != tt.want {
				t.Errorf("containsColumnForTable(%q, %q, %q) = %v, want %v",
					tt.selectList, tt.col, tt.alias, got, tt.want)
			}
		})
	}
}

// --- Bug fix tests: needsPKInjection with subqueries ---

func TestNeedsPKInjection_SubqueryInSelect(t *testing.T) {
	// Before the fix, strings.Index(upper, " FROM ") would match the subquery's
	// FROM, causing incorrect SELECT list analysis. The outer FROM is what matters.
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c"
	// "id" is in the SELECT list as c.id, so PK injection should NOT be needed.
	got := needsPKInjection(sql, "id")
	if got {
		t.Error("expected needsPKInjection to return false (id is in SELECT list as c.id)")
	}
}

func TestNeedsPKInjection_SubqueryMissingPK(t *testing.T) {
	// The PK "user_id" is NOT in the outer SELECT list at all.
	sql := "SELECT c.name, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c"
	got := needsPKInjection(sql, "user_id")
	if !got {
		t.Error("expected needsPKInjection to return true (user_id not in SELECT list)")
	}
}

func TestNeedsPKInjection_SimpleQuery(t *testing.T) {
	sql := "SELECT name, email FROM users"
	got := needsPKInjection(sql, "id")
	if !got {
		t.Error("expected needsPKInjection to return true (id not in SELECT list)")
	}
}

func TestNeedsPKInjection_SelectStar(t *testing.T) {
	sql := "SELECT * FROM users"
	got := needsPKInjection(sql, "id")
	if got {
		t.Error("expected needsPKInjection to return false (SELECT *)")
	}
}

// --- Bug fix tests: collectSubLinkTablesFromSQL ---

func TestCollectSubLinkTablesFromSQL(t *testing.T) {
	sql := `SELECT c.id, u.name,
		(SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS message_count
		FROM conversations c LEFT JOIN users u ON c.user_id = u.id`

	tables := collectSubLinkTablesFromSQL(sql)
	found := false
	for _, tbl := range tables {
		if tbl == "messages" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'messages' in SubLink tables, got: %v", tables)
	}
}

func TestCollectSubLinkTablesFromSQL_NoSubLink(t *testing.T) {
	sql := "SELECT c.id, u.name FROM conversations c LEFT JOIN users u ON c.user_id = u.id"
	tables := collectSubLinkTablesFromSQL(sql)
	if len(tables) != 0 {
		t.Errorf("expected no SubLink tables, got: %v", tables)
	}
}

func TestCollectSubLinkTablesFromSQL_MultipleSubLinks(t *testing.T) {
	sql := `SELECT c.id,
		(SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS msg_count,
		(SELECT MAX(created_at) FROM replies r WHERE r.cid = c.id) AS last_reply
		FROM conversations c`

	tables := collectSubLinkTablesFromSQL(sql)
	foundMessages := false
	foundReplies := false
	for _, tbl := range tables {
		if tbl == "messages" {
			foundMessages = true
		}
		if tbl == "replies" {
			foundReplies = true
		}
	}
	if !foundMessages {
		t.Errorf("expected 'messages' in SubLink tables, got: %v", tables)
	}
	if !foundReplies {
		t.Errorf("expected 'replies' in SubLink tables, got: %v", tables)
	}
}

func TestCollectSubLinkTablesFromSQL_WhereSubLink(t *testing.T) {
	sql := "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"
	tables := collectSubLinkTablesFromSQL(sql)
	found := false
	for _, tbl := range tables {
		if tbl == "orders" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'orders' in SubLink tables, got: %v", tables)
	}
}

// --- Bug fix tests: buildWindowBaseQuery with subqueries ---

func TestBuildWindowBaseQuery_SubqueryInSelect(t *testing.T) {
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt, ROW_NUMBER() OVER () FROM conversations c WHERE c.active = true"
	result := buildWindowBaseQuery(sql, "conversations")
	if result == "" {
		t.Fatal("expected non-empty result")
	}
	upper := strings.ToUpper(result)
	// The base query should use the outer FROM (conversations), not the subquery FROM (messages)
	if !strings.Contains(upper, "CONVERSATIONS") {
		t.Errorf("expected result to reference conversations table, got: %s", result)
	}
}

// --- Bug fix tests: buildAggregateBaseSQL with subqueries ---

func TestBuildAggregateBaseSQL_SubqueryInSelect(t *testing.T) {
	sql := "SELECT COUNT(*), (SELECT MAX(id) FROM orders) FROM users WHERE active = true"
	result := buildAggregateBaseSQL(sql, "users")
	if result == "" {
		t.Fatal("expected non-empty result")
	}
	upper := strings.ToUpper(result)
	// Should build SELECT pk FROM users WHERE active = true
	// The FROM should be the outer users table, not the subquery's orders table
	if !strings.Contains(upper, "USERS") {
		t.Errorf("expected result to reference users table, got: %s", result)
	}
}

// --- selectListContainsColumn tests for backwards compatibility ---

func TestSelectListContainsColumn(t *testing.T) {
	tests := []struct {
		name       string
		selectList string
		col        string
		want       bool
	}{
		{
			name:       "simple match",
			selectList: "id, name, email",
			col:        "id",
			want:       true,
		},
		{
			name:       "no match",
			selectList: "name, email",
			col:        "id",
			want:       false,
		},
		{
			name:       "qualified match",
			selectList: "u.id, u.name",
			col:        "id",
			want:       true,
		},
		{
			name:       "with AS alias",
			selectList: "name AS user_name",
			col:        "name",
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectListContainsColumn(tt.selectList, tt.col)
			if got != tt.want {
				t.Errorf("selectListContainsColumn(%q, %q) = %v, want %v",
					tt.selectList, tt.col, got, tt.want)
			}
		})
	}
}
