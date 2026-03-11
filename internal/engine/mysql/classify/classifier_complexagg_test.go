package classify

import (
	"testing"

	"github.com/psrth/mori/internal/engine/mysql/schema"
)

// TestClassify_HasComplexAgg verifies that GROUP_CONCAT, JSON_ARRAYAGG, and
// JSON_OBJECTAGG set HasComplexAgg=true (Fix 5: complex aggregate detection).
func TestClassify_HasComplexAgg(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"orders": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)

	tests := []struct {
		name           string
		sql            string
		wantComplexAgg bool
		wantAggregate  bool
	}{
		// Complex aggregates — should set HasComplexAgg.
		{
			"group_concat",
			"SELECT department, GROUP_CONCAT(name) FROM employees GROUP BY department",
			true, true,
		},
		{
			"json_arrayagg",
			"SELECT department, JSON_ARRAYAGG(name) FROM employees GROUP BY department",
			true, true,
		},
		{
			"json_objectagg",
			"SELECT JSON_OBJECTAGG(name, salary) FROM employees",
			true, true,
		},
		{
			"group_concat case insensitive",
			"SELECT group_concat(name SEPARATOR ', ') FROM users GROUP BY dept",
			true, true,
		},

		// Regular aggregates — should NOT set HasComplexAgg.
		{
			"count",
			"SELECT COUNT(*) FROM users",
			false, true,
		},
		{
			"sum",
			"SELECT SUM(amount) FROM orders GROUP BY user_id",
			false, true,
		},
		{
			"avg",
			"SELECT AVG(price) FROM products",
			false, true,
		},

		// No aggregates at all.
		{
			"plain select",
			"SELECT * FROM users WHERE id = 1",
			false, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.HasComplexAgg != tt.wantComplexAgg {
				t.Errorf("HasComplexAgg = %v, want %v", cl.HasComplexAgg, tt.wantComplexAgg)
			}
			if cl.HasAggregate != tt.wantAggregate {
				t.Errorf("HasAggregate = %v, want %v", cl.HasAggregate, tt.wantAggregate)
			}
		})
	}
}

// TestClassify_HasComplexAgg_InSelectExpr verifies complex aggregate detection
// when the aggregate is directly in the outer SELECT (not hidden in a CTE subquery).
func TestClassify_HasComplexAgg_InSelectExpr(t *testing.T) {
	c := New(nil)

	// GROUP_CONCAT directly in the outer SELECT.
	cl, err := c.Classify("SELECT department, GROUP_CONCAT(name SEPARATOR ', ') FROM employees GROUP BY department")
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	if !cl.HasComplexAgg {
		t.Error("HasComplexAgg should be true for direct GROUP_CONCAT in SELECT")
	}
	if !cl.HasAggregate {
		t.Error("HasAggregate should be true for direct GROUP_CONCAT in SELECT")
	}
}
