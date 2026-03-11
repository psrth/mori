package proxy

import (
	"strings"
	"testing"
)

// TestStripMySQLFKConstraints_CreateTable verifies FK stripping from CREATE TABLE (Fix 2).
func TestStripMySQLFKConstraints_CreateTable(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantStripped  bool   // whether stripping should occur
		wantRefTables []string
	}{
		{
			"create with FK",
			"CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			true,
			[]string{"users"},
		},
		{
			"create with FK cascade",
			"CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL)",
			true,
			[]string{"users"},
		},
		{
			"create with multiple FKs",
			"CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, product_id INT, FOREIGN KEY (order_id) REFERENCES orders(id), FOREIGN KEY (product_id) REFERENCES products(id))",
			true,
			[]string{"orders", "products"},
		},
		{
			"create without FK",
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))",
			false,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strippedSQL, refTables := stripMySQLFKConstraints(tt.sql)
			if tt.wantStripped {
				if refTables == nil {
					t.Fatal("expected FK stripping but refTables is nil")
				}
				if len(refTables) != len(tt.wantRefTables) {
					t.Fatalf("refTables = %v, want %v", refTables, tt.wantRefTables)
				}
				for i, want := range tt.wantRefTables {
					if refTables[i] != want {
						t.Errorf("refTables[%d] = %q, want %q", i, refTables[i], want)
					}
				}
				// Stripped SQL should not contain FOREIGN KEY.
				upper := strings.ToUpper(strippedSQL)
				if strings.Contains(upper, "FOREIGN KEY") {
					t.Errorf("stripped SQL still contains FOREIGN KEY: %s", strippedSQL)
				}
				// Should still be valid SQL (contain CREATE TABLE).
				if !strings.Contains(upper, "CREATE TABLE") {
					t.Errorf("stripped SQL lost CREATE TABLE: %s", strippedSQL)
				}
			} else {
				if refTables != nil {
					t.Errorf("expected no stripping but got refTables = %v", refTables)
				}
				if strippedSQL != "" {
					t.Errorf("expected empty strippedSQL but got %q", strippedSQL)
				}
			}
		})
	}
}

// TestStripMySQLFKConstraints_AlterTable verifies FK stripping from ALTER TABLE.
func TestStripMySQLFKConstraints_AlterTable(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantStripped  bool
		wantRefTables []string
		wantSelectOne bool // if all clauses are FK, should return "SELECT 1"
	}{
		{
			"alter add FK",
			"ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)",
			true,
			[]string{"users"},
			true, // only clause is FK → SELECT 1
		},
		{
			"alter add FK with other clause",
			"ALTER TABLE orders ADD COLUMN status VARCHAR(20), ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)",
			true,
			[]string{"users"},
			false,
		},
		{
			"alter without FK",
			"ALTER TABLE users ADD COLUMN age INT",
			false,
			nil,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strippedSQL, refTables := stripMySQLFKConstraints(tt.sql)
			if tt.wantStripped {
				if refTables == nil {
					t.Fatal("expected FK stripping but refTables is nil")
				}
				if len(refTables) != len(tt.wantRefTables) {
					t.Fatalf("refTables = %v, want %v", refTables, tt.wantRefTables)
				}
				if tt.wantSelectOne {
					if strings.TrimSpace(strippedSQL) != "select 1 from dual" && strings.TrimSpace(strippedSQL) != "SELECT 1" {
						// Vitess may output "select 1 from dual" instead of "SELECT 1"
						if !strings.Contains(strings.ToUpper(strippedSQL), "SELECT 1") &&
							!strings.Contains(strings.ToLower(strippedSQL), "select 1") {
							t.Errorf("expected 'SELECT 1' but got %q", strippedSQL)
						}
					}
				} else {
					upper := strings.ToUpper(strippedSQL)
					if strings.Contains(upper, "FOREIGN KEY") {
						t.Errorf("stripped SQL still contains FOREIGN KEY: %s", strippedSQL)
					}
				}
			} else {
				if refTables != nil {
					t.Errorf("expected no stripping but got refTables = %v", refTables)
				}
			}
		})
	}
}

// TestStripMySQLFKConstraints_InvalidSQL verifies graceful handling of unparseable SQL.
func TestStripMySQLFKConstraints_InvalidSQL(t *testing.T) {
	sql, refs := stripMySQLFKConstraints("NOT VALID SQL AT ALL")
	if sql != "" || refs != nil {
		t.Errorf("expected empty results for invalid SQL, got sql=%q refs=%v", sql, refs)
	}
}
