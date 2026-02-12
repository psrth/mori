//go:build e2e

package e2e

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestNoPKTable(t *testing.T) {
	conn := connect(t)

	t.Run("select_from_settings", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM settings")
		if len(rows) < 20 {
			t.Errorf("expected at least 20 settings rows, got %d", len(rows))
		}
	})

	t.Run("insert_into_settings", func(t *testing.T) {
		mustExec(t, conn, "INSERT INTO settings (key, value, category) VALUES ('e2e.edge', 'test', 'edge')")
	})

	t.Run("select_settings_after_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM settings WHERE key = 'e2e.edge'")
		if len(rows) < 1 {
			t.Errorf("expected to find inserted setting, got %d rows", len(rows))
		}
	})
}

func TestUUIDPK(t *testing.T) {
	conn := connect(t)

	t.Run("select_products_by_slug", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id, name, price FROM products WHERE slug = 'product-50'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 product, got %d", len(rows))
		}
	})

	t.Run("insert_uuid_product", func(t *testing.T) {
		var id string
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := conn.QueryRow(ctx,
			"INSERT INTO products (name, slug, price) VALUES ('Edge UUID Product', 'edge-uuid-product', 19.99) RETURNING id",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		if id == "" {
			t.Error("expected UUID, got empty")
		}
		t.Logf("Inserted product UUID: %s", id)
	})

	t.Run("select_uuid_product_after_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM products WHERE slug = 'edge-uuid-product'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("uuid_payments_select", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id, amount, method FROM payments LIMIT 5")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("uuid_sessions_select", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id, token FROM sessions LIMIT 5")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})
}

func TestCompositePK(t *testing.T) {
	conn := connect(t)

	t.Run("select_org_members", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM org_members WHERE org_id = 1 LIMIT 10")
		if len(rows) < 1 {
			t.Errorf("expected org_members rows, got %d", len(rows))
		}
	})

	t.Run("insert_composite_pk", func(t *testing.T) {
		mustExec(t, conn, "INSERT INTO post_tags (post_id, tag_id) VALUES (999, 49) ON CONFLICT DO NOTHING")
	})

	t.Run("select_user_roles", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM user_roles WHERE user_id = 1")
		if len(rows) < 1 {
			t.Errorf("expected user_roles rows, got %d", len(rows))
		}
	})
}

func TestNullHandling(t *testing.T) {
	conn := connect(t)

	t.Run("insert_all_nullable_columns_null", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, password_hash, display_name, metadata, tags, last_login_ip) VALUES ('e2e_null_user', 'null@test.com', 'hash', NULL, NULL, NULL, NULL)")
	})

	t.Run("select_where_null", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE display_name IS NULL AND username = 'e2e_null_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 null-display-name user, got %d", len(rows))
		}
	})

	t.Run("update_set_to_null", func(t *testing.T) {
		mustExec(t, conn, "UPDATE users SET display_name = NULL WHERE id = 3")
		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["display_name"] != nil {
			t.Errorf("expected NULL, got %v", rows[0]["display_name"])
		}
	})

	t.Run("update_null_to_value", func(t *testing.T) {
		mustExec(t, conn, "UPDATE users SET display_name = 'Restored Name' WHERE id = 3")
		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["display_name"].(string)
		if !ok || name != "Restored Name" {
			t.Errorf("expected 'Restored Name', got %v", rows[0]["display_name"])
		}
	})
}

func TestLargeText(t *testing.T) {
	conn := connect(t)

	t.Run("insert_10kb_text", func(t *testing.T) {
		bigText := strings.Repeat("A", 10*1024)
		mustExec(t, conn,
			"INSERT INTO posts (user_id, title, body, slug, status) VALUES (1, '10KB Post', $1, 'big-post-10kb', 'draft')", bigText)
	})

	t.Run("read_10kb_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT body FROM posts WHERE slug = 'big-post-10kb'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		body, ok := rows[0]["body"].(string)
		if !ok || len(body) != 10*1024 {
			t.Errorf("expected 10KB body, got %d bytes", len(body))
		}
	})

	t.Run("insert_100kb_text", func(t *testing.T) {
		bigText := strings.Repeat("B", 100*1024)
		mustExec(t, conn,
			"INSERT INTO posts (user_id, title, body, slug, status) VALUES (1, '100KB Post', $1, 'big-post-100kb', 'draft')", bigText)
	})

	t.Run("read_100kb_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT body FROM posts WHERE slug = 'big-post-100kb'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		body, ok := rows[0]["body"].(string)
		if !ok || len(body) != 100*1024 {
			t.Errorf("expected 100KB body, got %d bytes", len(body))
		}
	})
}

func TestEmptyResults(t *testing.T) {
	conn := connect(t)

	t.Run("select_impossible_where", func(t *testing.T) {
		assertQueryRowCount(t, conn, 0, "SELECT * FROM users WHERE 1 = 0")
	})

	t.Run("select_nonexistent_id", func(t *testing.T) {
		assertQueryRowCount(t, conn, 0, "SELECT * FROM users WHERE id = 999999")
	})
}

func TestConcurrentConnections(t *testing.T) {
	t.Run("two_connections_see_each_others_inserts", func(t *testing.T) {
		conn1 := connect(t)
		conn2 := connect(t)

		// Insert via conn1.
		mustExec(t, conn1, "INSERT INTO tags (name, slug) VALUES ('concurrent-tag-1', 'concurrent-tag-1')")

		// Should be visible via conn2.
		rows := mustQuery(t, conn2, "SELECT * FROM tags WHERE name = 'concurrent-tag-1'")
		if len(rows) != 1 {
			t.Errorf("conn2 should see conn1's insert, got %d rows", len(rows))
		}
	})

	t.Run("parallel_inserts", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make([]error, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				c, err := pgx.Connect(ctx, proxyDSN)
				if err != nil {
					errors[idx] = err
					return
				}
				defer c.Close(ctx)
				_, errors[idx] = c.Exec(ctx,
					"INSERT INTO audit_log (user_id, action, resource, resource_id) VALUES ($1, 'parallel_test', 'test', 'parallel')",
					idx+1)
			}(i)
		}

		wg.Wait()
		for i, err := range errors {
			if err != nil {
				t.Errorf("goroutine %d error: %v", i, err)
			}
		}
	})
}

func TestLargeResultSet(t *testing.T) {
	conn := connect(t)

	t.Run("select_50k_rows_audit_log", func(t *testing.T) {
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM audit_log")
		if count < 50000 {
			t.Errorf("expected at least 50000 audit_log rows, got %d", count)
		}
	})

	t.Run("select_with_large_limit", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id FROM audit_log ORDER BY id LIMIT 1000")
		if len(rows) != 1000 {
			t.Errorf("expected 1000 rows, got %d", len(rows))
		}
	})
}

func TestParameterizedQueries(t *testing.T) {
	conn := connect(t)

	t.Run("parameterized_select_single", func(t *testing.T) {
		var username string
		err := conn.QueryRow(t.Context(), "SELECT username FROM users WHERE id = $1", 5).Scan(&username)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if username != "user_5" {
			t.Errorf("got %q, want user_5", username)
		}
	})

	t.Run("parameterized_select_multiple_params", func(t *testing.T) {
		rows, err := conn.Query(t.Context(),
			"SELECT id, username FROM users WHERE id >= $1 AND id <= $2 ORDER BY id", 1, 5)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			count++
			var id int32
			var name string
			rows.Scan(&id, &name)
		}
		if count != 5 {
			t.Errorf("expected 5 rows, got %d", count)
		}
	})

	t.Run("parameterized_insert", func(t *testing.T) {
		tag, err := conn.Exec(t.Context(),
			"INSERT INTO tags (name, slug) VALUES ($1, $2)", "param-tag", "param-tag")
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("parameterized_update", func(t *testing.T) {
		tag, err := conn.Exec(t.Context(),
			"UPDATE users SET display_name = $1 WHERE id = $2", "Param Updated", 15)
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("parameterized_delete", func(t *testing.T) {
		tag, err := conn.Exec(t.Context(),
			"DELETE FROM users WHERE id = $1", 25)
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("parameterized_null_param", func(t *testing.T) {
		tag, err := conn.Exec(t.Context(),
			"UPDATE users SET display_name = $1 WHERE id = $2", nil, 16)
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("prepared_statement_reuse", func(t *testing.T) {
		ctx := t.Context()
		_, err := conn.Prepare(ctx, "get_user_by_id", "SELECT username FROM users WHERE id = $1")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}

		var name string
		err = conn.QueryRow(ctx, "get_user_by_id", 1).Scan(&name)
		if err != nil {
			t.Fatalf("exec prepared (1): %v", err)
		}
		if name != "user_1" {
			t.Errorf("got %q, want user_1", name)
		}

		err = conn.QueryRow(ctx, "get_user_by_id", 2).Scan(&name)
		if err != nil {
			t.Fatalf("exec prepared (2): %v", err)
		}
		if name != "user_2" {
			t.Errorf("got %q, want user_2", name)
		}
	})

	t.Run("parameterized_select_after_insert", func(t *testing.T) {
		// Insert via parameterized query, then select via parameterized query.
		_, err := conn.Exec(t.Context(),
			"INSERT INTO tags (name, slug) VALUES ($1, $2)", "param-verify", "param-verify")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}

		var name string
		err = conn.QueryRow(t.Context(),
			"SELECT name FROM tags WHERE slug = $1", "param-verify").Scan(&name)
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		if name != "param-verify" {
			t.Errorf("got %q, want param-verify", name)
		}
	})
}

func TestTransactions(t *testing.T) {
	conn := connect(t)

	t.Run("begin_commit_visible", func(t *testing.T) {
		ctx := t.Context()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO tags (name, slug) VALUES ('txn-commit-tag', 'txn-commit-tag')")
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("insert in tx: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}

		// Verify visible after commit.
		rows := mustQuery(t, conn, "SELECT * FROM tags WHERE name = 'txn-commit-tag'")
		if len(rows) != 1 {
			t.Errorf("committed row should be visible, got %d rows", len(rows))
		}
	})

	t.Run("begin_rollback_invisible", func(t *testing.T) {
		ctx := t.Context()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO tags (name, slug) VALUES ('txn-rollback-tag', 'txn-rollback-tag')")
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("insert in tx: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback: %v", err)
		}

		// Verify NOT visible after rollback.
		rows := mustQuery(t, conn, "SELECT * FROM tags WHERE name = 'txn-rollback-tag'")
		if len(rows) != 0 {
			t.Errorf("rolled back row should NOT be visible, got %d rows", len(rows))
		}
	})

	t.Run("transaction_mixed_read_write", func(t *testing.T) {
		ctx := t.Context()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}

		// Read.
		var count int64
		err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM roles").Scan(&count)
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("select in tx: %v", err)
		}

		// Write.
		_, err = tx.Exec(ctx, "INSERT INTO tags (name, slug) VALUES ('txn-mixed-tag', 'txn-mixed-tag')")
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("insert in tx: %v", err)
		}

		// Read again.
		var name string
		err = tx.QueryRow(ctx, "SELECT name FROM tags WHERE slug = 'txn-mixed-tag'").Scan(&name)
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("select in tx (2): %v", err)
		}

		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})

	t.Run("transaction_with_params", func(t *testing.T) {
		ctx := t.Context()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO tags (name, slug) VALUES ($1, $2)", "txn-param-tag", "txn-param-tag")
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("insert: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})
}

func TestSpecialCharacters(t *testing.T) {
	conn := connect(t)

	t.Run("single_quotes", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, password_hash, display_name) VALUES ('e2e_obriens', 'obrien@test.com', 'hash', $1)", "O'Brien")

		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE username = 'e2e_obriens'")
		if len(rows) != 1 || rows[0]["display_name"] != "O'Brien" {
			t.Errorf("unexpected: %v", rows)
		}
	})

	t.Run("unicode", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, password_hash, display_name) VALUES ('e2e_unicode', 'unicode@test.com', 'hash', $1)", "Test: hello world")

		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE username = 'e2e_unicode'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("newlines", func(t *testing.T) {
		body := "Line 1\nLine 2\nLine 3"
		mustExec(t, conn,
			"INSERT INTO posts (user_id, title, body, slug, status) VALUES (1, 'Newline Post', $1, 'newline-post', 'draft')", body)

		rows := mustQuery(t, conn, "SELECT body FROM posts WHERE slug = 'newline-post'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		got, ok := rows[0]["body"].(string)
		if !ok || got != body {
			t.Errorf("body roundtrip failed: got %q, want %q", got, body)
		}
	})
}

func TestKnownLimitations(t *testing.T) {
	conn := connect(t)

	t.Run("multi_column_order_by", func(t *testing.T) {
		// Known limitation: multi-column ORDER BY is not re-applied after merge.
		// This test documents the behavior rather than asserting correctness.
		rows := mustQuery(t, conn,
			"SELECT id, username FROM users ORDER BY is_active DESC, id ASC LIMIT 10")
		if len(rows) != 10 {
			t.Errorf("expected 10 rows, got %d", len(rows))
		}
		t.Log("Multi-column ORDER BY executed (results may not be perfectly re-sorted after merge)")
	})

	t.Run("aggregate_on_merged_table", func(t *testing.T) {
		// Known limitation: aggregates on merged tables may not be perfectly merged.
		// The users table has shadow modifications, so this exercises the limitation.
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM users")
		t.Logf("COUNT(*) on merged users table: %d (known limitation: may not be exact)", count)
	})
}
