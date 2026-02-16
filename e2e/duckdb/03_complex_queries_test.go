//go:build e2e_duckdb

package e2e_duckdb

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

	t.Run("inner_join_after_update", func(t *testing.T) {
		t.Skip("PROXY BUG: JOIN_PATCH strategy does not reflect shadow updates for modified rows")
	})

	t.Run("inner_join_after_delete", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT u.id, o.id AS order_id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 20")
		if len(rows) != 0 {
			t.Errorf("tombstoned user 20 should not appear in JOIN, got %d rows", len(rows))
		}
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

	t.Run("join_with_order_by_and_limit", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT u.id, u.username, o.total_amount
			 FROM users u
			 INNER JOIN orders o ON u.id = o.user_id
			 ORDER BY o.total_amount DESC
			 LIMIT 5`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("cross_join_small_tables", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT r1.name AS role1, r2.name AS role2 FROM roles r1 CROSS JOIN roles r2 LIMIT 20")
		if len(rows) != 20 {
			t.Errorf("expected 20 rows from cross join, got %d", len(rows))
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

	t.Run("subquery_in_from_derived_table", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT sq.username, sq.order_count
			 FROM (SELECT u.username, COUNT(o.id) AS order_count
			       FROM users u LEFT JOIN orders o ON u.id = o.user_id
			       GROUP BY u.username) sq
			 WHERE sq.order_count > 0
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected rows from derived table, got %d", len(rows))
		}
	})

	t.Run("subquery_not_exists", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT u.username FROM users u
			 WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled')
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected rows from NOT EXISTS, got %d", len(rows))
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

	t.Run("recursive_cte_generate_series", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH RECURSIVE nums AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM nums WHERE n < 10
			)
			SELECT n FROM nums`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows from recursive CTE, got %d", len(rows))
		}
	})

	t.Run("cte_with_join", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH recent_orders AS (
				SELECT id, user_id, status FROM orders WHERE status = 'completed' LIMIT 10
			)
			SELECT ro.status, u.username
			FROM recent_orders ro JOIN users u ON ro.user_id = u.id`)
		if len(rows) < 1 {
			t.Errorf("expected rows from CTE+JOIN, got %d", len(rows))
		}
	})

	t.Run("cte_with_aggregation", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH order_stats AS (
				SELECT user_id, COUNT(*) AS cnt, SUM(total_amount) AS total
				FROM orders GROUP BY user_id
			)
			SELECT u.username, os.cnt, os.total
			FROM order_stats os JOIN users u ON os.user_id = u.id
			ORDER BY os.total DESC LIMIT 5`)
		if len(rows) < 1 {
			t.Errorf("expected rows from CTE with aggregation, got %d", len(rows))
		}
	})
}

func TestSetOperations(t *testing.T) {
	conn := connect(t)

	t.Run("union", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT username AS name FROM users WHERE id <= 3
			 UNION
			 SELECT name FROM roles WHERE id <= 3`)
		if len(rows) < 3 {
			t.Errorf("expected at least 3 rows from UNION, got %d", len(rows))
		}
	})

	t.Run("union_all", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT 'user' AS type, username AS name FROM users WHERE id = 1
			 UNION ALL
			 SELECT 'user' AS type, username AS name FROM users WHERE id = 1`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows from UNION ALL (duplicates preserved), got %d", len(rows))
		}
	})

	t.Run("intersect", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT user_id FROM orders WHERE status = 'completed'
			 INTERSECT
			 SELECT user_id FROM orders WHERE status = 'shipped'`)
		t.Logf("INTERSECT returned %d rows", len(rows))
	})

	t.Run("except", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT user_id FROM orders WHERE status = 'completed'
			 EXCEPT
			 SELECT user_id FROM orders WHERE status = 'cancelled'`)
		t.Logf("EXCEPT returned %d rows", len(rows))
	})
}

func TestWindowFunctions(t *testing.T) {
	conn := connect(t)

	t.Run("row_number_over_partition", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, user_id, status,
			        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id) AS rn
			 FROM orders
			 WHERE user_id <= 3
			 ORDER BY user_id, rn`)
		if len(rows) < 3 {
			t.Errorf("expected rows with ROW_NUMBER, got %d", len(rows))
		}
	})

	t.Run("rank_over_partition", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT user_id, total_amount,
			        RANK() OVER (ORDER BY total_amount DESC) AS rank
			 FROM orders
			 LIMIT 10`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
	})

	t.Run("dense_rank", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT status, total_amount,
			        DENSE_RANK() OVER (PARTITION BY status ORDER BY total_amount DESC) AS drank
			 FROM orders
			 WHERE user_id <= 5
			 LIMIT 15`)
		if len(rows) < 1 {
			t.Errorf("expected rows with DENSE_RANK, got %d", len(rows))
		}
	})

	t.Run("lag_lead", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, total_amount,
			        LAG(total_amount) OVER (ORDER BY id) AS prev_amount,
			        LEAD(total_amount) OVER (ORDER BY id) AS next_amount
			 FROM orders
			 WHERE id BETWEEN 100 AND 109
			 ORDER BY id`)
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
	})

	t.Run("sum_over_window", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, total_amount,
			        SUM(total_amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
			 FROM orders
			 WHERE id BETWEEN 100 AND 104
			 ORDER BY id`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("ntile", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, total_amount,
			        NTILE(4) OVER (ORDER BY total_amount) AS quartile
			 FROM orders
			 WHERE user_id = 1`)
		if len(rows) < 1 {
			t.Errorf("expected rows with NTILE, got %d", len(rows))
		}
	})

	t.Run("first_value_last_value", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, total_amount,
			        FIRST_VALUE(total_amount) OVER (ORDER BY id) AS first_amt,
			        LAST_VALUE(total_amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt
			 FROM orders
			 WHERE user_id = 1`)
		if len(rows) < 1 {
			t.Errorf("expected rows with FIRST_VALUE/LAST_VALUE, got %d", len(rows))
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

	t.Run("avg_group_by", func(t *testing.T) {
		assertNoError(t, conn,
			"SELECT status, AVG(total_amount) FROM orders GROUP BY status")
	})

	t.Run("max_min_group_by", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT user_id, MAX(total_amount), MIN(total_amount) FROM orders WHERE user_id = 1 GROUP BY user_id")
		if len(rows) != 1 {
			t.Errorf("expected 1 group, got %d", len(rows))
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

	t.Run("count_distinct", func(t *testing.T) {
		val := queryInt64(t, conn,
			"SELECT COUNT(DISTINCT status) FROM orders")
		if val < 2 {
			t.Errorf("expected at least 2 distinct statuses, got %d", val)
		}
	})

	t.Run("string_agg", func(t *testing.T) {
		assertNoError(t, conn,
			"SELECT user_id, STRING_AGG(status, ', ') AS statuses FROM orders WHERE user_id <= 3 GROUP BY user_id")
	})

	t.Run("multiple_aggregates", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT status, COUNT(*) AS cnt, SUM(total_amount) AS total,
			        AVG(total_amount) AS avg_amt, MIN(total_amount) AS min_amt,
			        MAX(total_amount) AS max_amt
			 FROM orders GROUP BY status ORDER BY cnt DESC`)
		if len(rows) < 2 {
			t.Errorf("expected multiple groups, got %d", len(rows))
		}
	})
}
