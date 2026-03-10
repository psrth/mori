package proxy

import "testing"

func TestNeedsVirtualPKInjection(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		colName string
		want    bool
	}{
		{"ctid not present", "SELECT name FROM users", "ctid", true},
		{"ctid already present", "SELECT ctid, name FROM users", "ctid", false},
		{"rowid not present", "SELECT name FROM users", "rowid", true},
		{"rowid already present", "SELECT rowid, name FROM users", "rowid", false},
		{"select star ctid", "SELECT * FROM users", "ctid", true},
		{"select star rowid", "SELECT * FROM users", "rowid", true},
		{"union skipped", "SELECT name FROM users UNION SELECT name FROM admins", "ctid", false},
		{"CTE skipped", "WITH cte AS (SELECT 1) SELECT * FROM cte", "ctid", false},
		{"subquery skipped", "SELECT * FROM (SELECT 1) t", "ctid", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := needsVirtualPKInjection(tt.sql, tt.colName)
			if got != tt.want {
				t.Errorf("needsVirtualPKInjection(%q, %q) = %v, want %v", tt.sql, tt.colName, got, tt.want)
			}
		})
	}
}

func TestInjectVirtualPKColumn(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		colName string
		want    string
	}{
		{
			"inject ctid",
			"SELECT name FROM users",
			"ctid",
			"SELECT ctid, name FROM users",
		},
		{
			"inject rowid",
			"SELECT name FROM users",
			"rowid",
			"SELECT rowid, name FROM users",
		},
		{
			"inject ctid with star",
			"SELECT * FROM users",
			"ctid",
			"SELECT ctid, * FROM users",
		},
		{
			"inject rowid with distinct",
			"SELECT DISTINCT name FROM users",
			"rowid",
			"SELECT DISTINCT rowid, name FROM users",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := injectVirtualPKColumn(tt.sql, tt.colName)
			if got != tt.want {
				t.Errorf("injectVirtualPKColumn(%q, %q) = %q, want %q", tt.sql, tt.colName, got, tt.want)
			}
		})
	}
}
