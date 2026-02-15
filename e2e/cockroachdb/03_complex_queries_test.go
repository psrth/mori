//go:build e2e_cockroachdb

package e2e_cockroachdb

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
		// User 10 was updated in TestBasicUpdate. JOIN should show updated data.
		rows := mustQuery(t, conn,
			"SELECT u.display_name, o.status FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 10 LIMIT 3")
		for _, row := range rows {
			name, _ := row["display_name"].(string)
			if name != "Updated User 10" {
				t.Errorf("JOIN should show updated display_name, got %q", name)
			}
		}
	})

	t.Run("inner_join_after_delete", func(t *testing.T) {
		// User 20 was deleted (tombstoned). JOIN should exclude their orders.
		rows := mustQuery(t, conn,
			"SELECT u.id, o.id AS order_id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.id = 20")
		if len(rows) != 0 {
			t.Errorf("tombstoned user 20 should not appear in JOIN, got %d rows", len(rows))
		}
	})

	t.Run("left_join_users_posts", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT u.id, u.username, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id WHERE u.id = 2 LIMIT 10")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row from LEFT JOIN")
		}
	})

	t.Run("three_table_join", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT o.id, oi.quantity, p.name
			 FROM orders o
			 INNER JOIN order_items oi ON o.id = oi.order_id
			 INNER JOIN products p ON oi.product_id = p.id
			 WHERE o.id = 1
			 LIMIT 5`)
		if len(rows) < 1 {
			t.Errorf("expected rows from 3-table JOIN, got %d", len(rows))
		}
		t.Logf("Order 1 has %d items (showing max 5)", len(rows))
	})

	t.Run("self_join_categories", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT c.name AS child, p.name AS parent
			 FROM categories c
			 INNER JOIN categories p ON c.parent_id = p.id
			 WHERE c.depth = 1
			 ORDER BY c.id`)
		if len(rows) < 5 {
			t.Errorf("expected at least 5 parent-child category pairs, got %d", len(rows))
		}
	})

	t.Run("self_join_comments_threaded", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT c.id, c.body, p.id AS parent_id, p.body AS parent_body
			 FROM comments c
			 INNER JOIN comments p ON c.parent_id = p.id
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected threaded comment rows, got %d", len(rows))
		}
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
			"SELECT r.name, t.name FROM roles r CROSS JOIN tags t LIMIT 20")
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

	t.Run("subquery_in_where_exists", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT u.username FROM users u
			 WHERE EXISTS (SELECT 1 FROM posts p WHERE p.user_id = u.id)
			 LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected rows from EXISTS subquery, got %d", len(rows))
		}
	})

	t.Run("subquery_scalar", func(t *testing.T) {
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
}

func TestCTEs(t *testing.T) {
	conn := connect(t)

	t.Run("simple_cte", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH active_users AS (
				SELECT id, username FROM users WHERE is_active = true LIMIT 20
			)
			SELECT * FROM active_users`)
		if len(rows) < 1 {
			t.Errorf("expected rows from CTE, got %d", len(rows))
		}
	})

	t.Run("recursive_cte_category_tree", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH RECURSIVE cat_tree AS (
				SELECT id, name, parent_id, depth, path
				FROM categories WHERE parent_id IS NULL
				UNION ALL
				SELECT c.id, c.name, c.parent_id, c.depth, c.path
				FROM categories c INNER JOIN cat_tree ct ON c.parent_id = ct.id
			)
			SELECT * FROM cat_tree ORDER BY path`)
		if len(rows) != 30 {
			t.Errorf("expected 30 categories in tree, got %d", len(rows))
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

	t.Run("cte_with_join", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`WITH recent_posts AS (
				SELECT id, user_id, title FROM posts WHERE status = 'published' LIMIT 10
			)
			SELECT rp.title, u.username
			FROM recent_posts rp JOIN users u ON rp.user_id = u.id`)
		if len(rows) < 1 {
			t.Errorf("expected rows from CTE+JOIN, got %d", len(rows))
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
		// Some users may have both completed and shipped orders.
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

	t.Run("lag_lead", func(t *testing.T) {
		// Use higher ID range to avoid rows affected by earlier CRUD tests.
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
		// Use higher ID range to avoid rows affected by earlier CRUD tests.
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
}

func TestAggregatesGroupBy(t *testing.T) {
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

	t.Run("group_by_having", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id HAVING COUNT(*) > 3 ORDER BY cnt DESC LIMIT 10")
		for _, row := range rows {
			cnt := row["cnt"].(int64)
			if cnt <= 3 {
				t.Errorf("HAVING filter failed: count=%d should be > 3", cnt)
			}
		}
	})

	t.Run("count_distinct", func(t *testing.T) {
		val := queryScalar[int64](t, conn,
			"SELECT COUNT(DISTINCT status) FROM orders")
		if val < 2 {
			t.Errorf("expected at least 2 distinct statuses, got %d", val)
		}
	})
}

func TestTypeSpecificOperations(t *testing.T) {
	conn := connect(t)

	t.Run("jsonb_arrow_operator", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id, metadata->'theme' AS theme FROM users WHERE id = 2")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("jsonb_double_arrow_text", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id, metadata->>'lang' AS lang FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("jsonb_contains_operator", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id FROM users WHERE metadata @> '{"theme": "dark"}' LIMIT 10`)
		if len(rows) < 1 {
			t.Errorf("expected rows with dark theme, got %d", len(rows))
		}
	})

	t.Run("array_contains_any", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id, username FROM users WHERE 'admin' = ANY(tags) LIMIT 10")
		if len(rows) < 1 {
			t.Errorf("expected users with admin tag, got %d", len(rows))
		}
	})

	t.Run("string_like", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id FROM users WHERE email LIKE '%@example.com' LIMIT 5")
		if len(rows) < 1 {
			t.Errorf("expected rows matching LIKE pattern, got %d", len(rows))
		}
	})

	t.Run("string_ilike", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id FROM users WHERE username ILIKE 'USER_1%' LIMIT 5")
		if len(rows) < 1 {
			t.Errorf("expected rows matching ILIKE pattern, got %d", len(rows))
		}
	})

	t.Run("date_extract", func(t *testing.T) {
		assertNoError(t, conn,
			"SELECT id, EXTRACT(YEAR FROM created_at) AS yr FROM users WHERE id = 1")
	})

	t.Run("date_trunc", func(t *testing.T) {
		assertNoError(t, conn,
			"SELECT DATE_TRUNC('day', created_at) AS day, COUNT(*) FROM orders GROUP BY day LIMIT 5")
	})

	t.Run("case_expression", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, CASE WHEN is_active THEN 'active' ELSE 'inactive' END AS state
			 FROM users WHERE id <= 5`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("coalesce", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id, COALESCE(website, 'no website') AS site FROM user_profiles WHERE id <= 5")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})
}
