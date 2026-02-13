//go:build e2e_mssql

package e2e_mssql

import (
	"testing"
)

func TestJoins(t *testing.T) {
	db := connect(t)

	t.Run("inner_join_users_orders", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT u.id, u.username, o.id AS order_id, o.status FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 1")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 JOIN row, got %d", len(rows))
		}
		t.Logf("User 1 has %d orders", len(rows))
	})

	t.Run("left_join_users_orders", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT TOP 10 u.id, u.username, o.id AS order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id <= 5")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row from LEFT JOIN, got %d", len(rows))
		}
	})

	t.Run("three_table_join", func(t *testing.T) {
		rows := mustQuery(t, db,
			`SELECT TOP 5 u.username, ur.role_id, r.name AS role_name
			 FROM users u
			 INNER JOIN user_roles ur ON u.id = ur.user_id
			 INNER JOIN roles r ON ur.role_id = r.id
			 WHERE u.id = 1`)
		if len(rows) < 1 {
			t.Errorf("expected rows from 3-table JOIN, got %d", len(rows))
		}
		t.Logf("User 1 has %d roles", len(rows))
	})

	t.Run("join_with_where_filter", func(t *testing.T) {
		rows := mustQuery(t, db,
			`SELECT TOP 10 u.username, o.total_amount
			 FROM users u
			 INNER JOIN orders o ON u.id = o.user_id
			 WHERE o.status = 'completed'`)
		if len(rows) < 1 {
			t.Errorf("expected completed orders, got %d", len(rows))
		}
	})
}

func TestSubqueries(t *testing.T) {
	db := connect(t)

	t.Run("subquery_in_where_in", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT username FROM users WHERE id IN (SELECT TOP 5 user_id FROM orders WHERE status = 'completed')")
		if len(rows) < 1 {
			t.Errorf("expected rows from IN subquery, got %d", len(rows))
		}
	})

	t.Run("subquery_in_where_exists", func(t *testing.T) {
		rows := mustQuery(t, db,
			`SELECT TOP 10 u.username FROM users u
			 WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)`)
		if len(rows) < 1 {
			t.Errorf("expected rows from EXISTS subquery, got %d", len(rows))
		}
	})

	t.Run("subquery_scalar", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT username, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u WHERE u.id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})
}

func TestCTEs(t *testing.T) {
	db := connect(t)

	t.Run("simple_cte", func(t *testing.T) {
		rows := mustQuery(t, db,
			`WITH active_users AS (
				SELECT TOP 20 id, username FROM users WHERE is_active = 1
			)
			SELECT * FROM active_users`)
		if len(rows) < 1 {
			t.Errorf("expected rows from CTE, got %d", len(rows))
		}
	})

	t.Run("multiple_ctes", func(t *testing.T) {
		rows := mustQuery(t, db,
			`WITH user_orders AS (
				SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id
			),
			top_users AS (
				SELECT TOP 10 user_id, cnt FROM user_orders ORDER BY cnt DESC
			)
			SELECT u.username, tu.cnt
			FROM top_users tu JOIN users u ON tu.user_id = u.id
			ORDER BY tu.cnt DESC`)
		if len(rows) < 1 {
			t.Errorf("expected rows from multiple CTEs, got %d", len(rows))
		}
	})
}

func TestAggregates(t *testing.T) {
	db := connect(t)

	t.Run("count_group_by", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC")
		if len(rows) < 2 {
			t.Errorf("expected multiple status groups, got %d", len(rows))
		}
	})

	t.Run("sum_group_by", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT user_id, SUM(total_amount) AS total FROM orders WHERE user_id <= 5 GROUP BY user_id ORDER BY user_id")
		if len(rows) < 1 {
			t.Errorf("expected sum results, got %d rows", len(rows))
		}
	})

	t.Run("having_filter", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT TOP 10 user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id HAVING COUNT(*) > 1 ORDER BY cnt DESC")
		for _, row := range rows {
			cnt := row["cnt"].(int64)
			if cnt <= 1 {
				t.Errorf("HAVING filter failed: count=%d should be > 1", cnt)
			}
		}
	})
}
