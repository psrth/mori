package proxy

import (
	"testing"

	"github.com/psrth/mori/internal/core"
)

func TestValidateRouteDecision_AllowsReadToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpRead, SubType: core.SubSelect}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for read to prod, got: %v", err)
	}
}

func TestValidateRouteDecision_BlocksWriteToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for write to prod, got nil")
	}
}

func TestValidateRouteDecision_BlocksDDLToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpDDL, SubType: core.SubAlter}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for DDL to prod, got nil")
	}
}

func TestValidateRouteDecision_AllowsWriteToShadow(t *testing.T) {
	cl := &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert}
	err := validateRouteDecision(cl, core.StrategyShadowWrite, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for write to shadow, got: %v", err)
	}
}

func TestValidateRouteDecision_AllowsTransactionToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpTransaction, SubType: core.SubBegin}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for transaction to prod, got: %v", err)
	}
}

func TestValidateRouteDecision_NilClassification(t *testing.T) {
	err := validateRouteDecision(nil, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for nil classification, got: %v", err)
	}
}

func TestValidateRouteDecision_AllowsOtherToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpOther, SubType: core.SubOther}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for other to prod, got: %v", err)
	}
}

func TestLooksLikeWrite(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		// Safe
		{"select", "SELECT * FROM users", false},
		{"pragma", "PRAGMA table_info(users)", false},
		{"explain", "EXPLAIN SELECT 1", false},
		{"begin", "BEGIN", false},
		{"commit", "COMMIT", false},
		{"end", "END", false},
		{"rollback", "ROLLBACK", false},
		{"savepoint", "SAVEPOINT sp1", false},
		{"release", "RELEASE SAVEPOINT sp1", false},
		{"analyze", "ANALYZE", false},
		{"attach", "ATTACH DATABASE 'other.db' AS other", false},

		// Write
		{"insert", "INSERT INTO users (name) VALUES ('alice')", true},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", true},
		{"delete", "DELETE FROM users WHERE id = 1", true},
		{"replace", "REPLACE INTO users (id, name) VALUES (1, 'alice')", true},
		{"create", "CREATE TABLE foo (id INTEGER)", true},
		{"alter", "ALTER TABLE users ADD COLUMN age INTEGER", true},
		{"drop", "DROP TABLE users", true},

		// CTE writes
		{"cte_insert", "WITH data AS (SELECT 1) INSERT INTO users SELECT * FROM data", true},
		{"cte_select_only", "WITH data AS (SELECT 1) SELECT * FROM data", false},

		// Multi-statement detection
		{"multi_stmt_delete", "SELECT 1; DELETE FROM users", true},
		{"multi_stmt_insert", "SELECT 1; INSERT INTO t VALUES (1)", true},
		{"multi_stmt_drop", "SELECT 1; DROP TABLE t", true},
		{"multi_stmt_safe", "SELECT 1; SELECT 2", false},
		{"multi_stmt_trailing_semi", "SELECT 1;", false},

		// String literal false positives (semicolons inside quotes are not delimiters)
		{"string_literal_semicolon", "SELECT 'x;DELETE FROM t'", false},
		{"string_literal_escaped_quote", "SELECT 'it''s;here'", false},

		// Comment bypass (comments before write keywords must be stripped)
		{"line_comment_before_write", "SELECT 1; --comment\nDELETE FROM t", true},
		{"block_comment_before_write", "SELECT 1; /* comment */ DELETE FROM t", true},

		// Block comment with apostrophe must not poison semicolon detection
		{"block_comment_apostrophe", "SELECT /* it's */ 1; DELETE FROM users", true},

		// CTE name containing keyword substring must not cause false positive
		{"cte_name_insert_log", "WITH insert_log AS (SELECT 1) SELECT * FROM insert_log", false},

		// Edge cases
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := looksLikeWrite(tt.sql)
			if got != tt.want {
				t.Errorf("looksLikeWrite(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}
