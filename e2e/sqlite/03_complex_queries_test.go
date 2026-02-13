//go:build e2e_sqlite

package e2e_sqlite

import (
	"testing"
)

func TestJoins(t *testing.T) {
	conn := connect(t)

	t.Run("inner_join_users_orders", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT u.id, u.username, o.id AS order_id, o.status FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 1")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 JOIN row, got %d", len(rows))
		}
		t.Logf("User 1 has %d orders", len(rows))
	})

	t.Run("left_join_users_orders", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT u.id, u.username, o.id AS order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id = 2 LIMIT 10")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row from LEFT JOIN")
		}
	})

	t.Run("three_table_join", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT o.id, ur.role_id, u.username
			 FROM orders o
			 INNER JOIN users u ON o.user_id = u.id
			 INNER JOIN user_roles ur ON u.id = ur.user_id
			 WHERE o.id <= 5
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected rows from 3-table JOIN, got %d", len(rows))
		}
		t.Logf("3-table JOIN returned %d rows", len(rows))
	})

	t.Run("join_with_where_clause", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT u.username, o.total_amount
			 FROM users u
			 INNER JOIN orders o ON u.id = o.user_id
			 WHERE o.status = 'completed'
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected completed orders, got %d", len(rows))
		}
	})
}

func TestSubqueries(t *testing.T) {
	conn := connect(t)

	t.Run("subquery_in_where_in", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT username FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed' LIMIT 5)")
		if len(rows) < 1 {
			t.Errorf("expected rows from IN subquery, got %d", len(rows))
		}
	})

	t.Run("subquery_exists", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT u.username FROM users u
			 WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected rows from EXISTS subquery, got %d", len(rows))
		}
	})

	t.Run("scalar_subquery", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT username, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u WHERE u.id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})
}

func TestCTEs(t *testing.T) {
	conn := connect(t)

	t.Run("simple_cte", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH active_users AS (
				SELECT id, username FROM users WHERE is_active = 1 LIMIT 20
			)
			SELECT * FROM active_users`)
		if len(rows) < 1 {
			t.Errorf("expected rows from CTE, got %d", len(rows))
		}
	})

	t.Run("multiple_ctes", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH user_orders AS (
				SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id
			),
			top_users AS (
				SELECT user_id, cnt FROM user_orders ORDER BY cnt DESC LIMIT 10
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
	conn := connect(t)

	t.Run("count_group_by", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC")
		if len(rows) < 2 {
			t.Errorf("expected multiple status groups, got %d", len(rows))
		}
	})

	t.Run("sum_group_by", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT user_id, SUM(total_amount) AS total FROM orders WHERE user_id <= 5 GROUP BY user_id ORDER BY user_id")
		if len(rows) < 1 {
			t.Errorf("expected sum results, got %d rows", len(rows))
		}
	})

	t.Run("having_clause", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 10")
		for _, row := range rows {
			cnt, ok := row["cnt"].(int64)
			if ok && cnt <= 1 {
				t.Errorf("HAVING filter failed: count=%d should be > 1", cnt)
			}
		}
	})
}
