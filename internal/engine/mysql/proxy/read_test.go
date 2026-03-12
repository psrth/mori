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
			name:  "multiple subqueries in SELECT",
			sql:   "SELECT (SELECT COUNT(*) FROM a), (SELECT MAX(x) FROM b) FROM main_table",
			found: true,
		},
		{
			name:  "nested subqueries",
			sql:   "SELECT (SELECT (SELECT 1 FROM inner_t) FROM mid_t) FROM outer_t",
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
				// Verify the found FROM is the outer one (not inside parens).
				after := strings.TrimSpace(upper[idx+6:])
				if strings.HasPrefix(after, "MESSAGES") || strings.HasPrefix(after, "A)") || strings.HasPrefix(after, "INNER_T") {
					t.Errorf("found subquery FROM instead of outer FROM at idx %d", idx)
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
			name:       "backtick quoted match",
			selectList: "`u`.`id`, `u`.`name`",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "backtick quoted mismatch",
			selectList: "`c`.`id`, `u`.`name`",
			col:        "id",
			alias:      "u",
			want:       false,
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
	// Regression test: a correlated subquery in the SELECT list contains " FROM ".
	// The old code would match the subquery's FROM and extract an incorrect select list.
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c WHERE c.active = 1"
	// "id" is the PK of conversations. It IS in the select list as "c.id".
	// needsPKInjection should return false because c.id is present.
	got := needsPKInjection(sql, "id")
	if got {
		t.Error("needsPKInjection should return false: c.id is in the SELECT list")
	}
}

func TestNeedsPKInjection_SubqueryInSelect_NoPK(t *testing.T) {
	// The SELECT list does NOT contain the PK "user_id", only a subquery.
	sql := "SELECT c.name, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c WHERE c.active = 1"
	got := needsPKInjection(sql, "user_id")
	if !got {
		t.Error("needsPKInjection should return true: user_id is NOT in the SELECT list")
	}
}

// --- Bug fix tests: buildWindowBaseQueryMySQL with subqueries ---

func TestBuildWindowBaseQueryMySQL_SubqueryInSelect(t *testing.T) {
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt, ROW_NUMBER() OVER (ORDER BY c.id) FROM conversations c"
	result := buildWindowBaseQueryMySQL(sql)
	if result == "" {
		t.Fatal("expected non-empty base query")
	}
	// The base query should start with "SELECT *" and include " FROM conversations".
	upper := strings.ToUpper(result)
	if !strings.HasPrefix(upper, "SELECT *") {
		t.Errorf("expected base query to start with SELECT *, got: %s", result[:min(30, len(result))])
	}
	if !strings.Contains(upper, "CONVERSATIONS") {
		t.Error("expected base query to contain CONVERSATIONS")
	}
}

// --- Bug fix tests: buildMaterializationBaseQueryMySQL with subqueries ---

func TestBuildMaterializationBaseQueryMySQL_SubqueryInSelect(t *testing.T) {
	sql := "SELECT (SELECT COUNT(*) FROM orders o WHERE o.uid = u.id) AS order_count, COUNT(*) FROM users u GROUP BY u.id"
	result := buildMaterializationBaseQueryMySQL(sql)
	if result == "" {
		t.Fatal("expected non-empty base query")
	}
	upper := strings.ToUpper(result)
	if !strings.HasPrefix(upper, "SELECT *") {
		t.Errorf("expected base query to start with SELECT *, got: %s", result[:min(30, len(result))])
	}
	// Should reference users, not orders (orders is in subquery).
	if !strings.Contains(upper, "USERS") {
		t.Error("expected base query to contain USERS")
	}
}

// --- Bug fix tests: collectSubqueryTablesFromSQL ---

func TestCollectSubqueryTablesFromSQL(t *testing.T) {
	sql := `SELECT c.id, u.name,
		(SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count
		FROM conversations c
		LEFT JOIN users u ON c.user_id = u.id`

	tables := collectSubqueryTablesFromSQL(sql)

	// Should find "messages" from the correlated subquery.
	found := false
	for _, t := range tables {
		if t == "messages" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected to find 'messages' in subquery tables, got: %v", tables)
	}
}

func TestCollectSubqueryTablesFromSQL_NoSubquery(t *testing.T) {
	sql := "SELECT c.id, u.name FROM conversations c LEFT JOIN users u ON c.user_id = u.id"

	tables := collectSubqueryTablesFromSQL(sql)
	if len(tables) != 0 {
		t.Errorf("expected no subquery tables, got: %v", tables)
	}
}

func TestCollectSubqueryTablesFromSQL_WhereSubquery(t *testing.T) {
	sql := `SELECT c.id FROM conversations c
		WHERE c.user_id IN (SELECT u.id FROM admins u WHERE u.active = 1)`

	tables := collectSubqueryTablesFromSQL(sql)

	found := false
	for _, t := range tables {
		if t == "admins" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected to find 'admins' in subquery tables, got: %v", tables)
	}
}

// --- Bug fix tests: rewriteTableRefsInSQL ---

func TestRewriteTableRefsInSQL_SubqueryTables(t *testing.T) {
	sql := `SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c LEFT JOIN users u ON c.user_id = u.id`
	tableMap := map[string]string{
		"conversations": "_mori_util_conv",
		"users":         "_mori_util_users",
		"messages":      "_mori_util_msgs",
	}

	rewritten := rewriteTableRefsInSQL(sql, tableMap)
	upper := strings.ToUpper(rewritten)

	// All three table names should be rewritten to util names.
	if strings.Contains(upper, " CONVERSATIONS ") || strings.Contains(upper, "`CONVERSATIONS`") {
		t.Error("expected 'conversations' to be rewritten")
	}
	if strings.Contains(upper, " USERS ") && !strings.Contains(upper, "_MORI_UTIL_USERS") {
		t.Error("expected 'users' to be rewritten")
	}
	if strings.Contains(upper, " MESSAGES ") && !strings.Contains(upper, "_MORI_UTIL_MSGS") {
		t.Error("expected 'messages' to be rewritten")
	}

	// The rewritten SQL should contain the util table names.
	if !strings.Contains(rewritten, "_mori_util_conv") {
		t.Error("expected rewritten SQL to contain _mori_util_conv")
	}
	if !strings.Contains(rewritten, "_mori_util_msgs") {
		t.Error("expected rewritten SQL to contain _mori_util_msgs")
	}
}

func TestRewriteTableRefsInSQL_EmptyMap(t *testing.T) {
	sql := "SELECT * FROM users"
	rewritten := rewriteTableRefsInSQL(sql, map[string]string{})
	if rewritten != sql {
		t.Errorf("expected unchanged SQL, got: %s", rewritten)
	}
}
