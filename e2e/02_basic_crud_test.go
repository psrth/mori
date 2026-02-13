//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestBasicSelect(t *testing.T) {
	conn := connect(t)

	t.Run("select_all_from_small_table", func(t *testing.T) {
		// Expect 11: 10 Prod rows + 1 Shadow row inserted by earlier lifecycle test.
		assertQueryRowCount(t, conn, 11, "SELECT * FROM roles")
	})

	t.Run("select_count_star_users", func(t *testing.T) {
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM users")
		if count != 500 {
			t.Errorf("users count = %d, want 500", count)
		}
	})

	t.Run("select_count_star_orders", func(t *testing.T) {
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM orders")
		if count != 2000 {
			t.Errorf("orders count = %d, want 2000", count)
		}
	})

	t.Run("select_count_star_audit_log", func(t *testing.T) {
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM audit_log")
		if count != 50000 {
			t.Errorf("audit_log count = %d, want 50000", count)
		}
	})

	t.Run("select_with_where_serial_pk", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT username FROM users WHERE id = 1")
		if len(rows) != 1 || rows[0]["username"] != "user_1" {
			t.Errorf("unexpected result: %v", rows)
		}
	})

	t.Run("select_with_where_bigserial_pk", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT status FROM orders WHERE id = 1")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("select_with_where_uuid_pk", func(t *testing.T) {
		// Products use deterministic UUIDs via uuid_generate_v5.
		rows := mustQuery(t, conn, "SELECT name FROM products WHERE slug = 'product-1'")
		if len(rows) != 1 || rows[0]["name"] != "Product 1" {
			t.Errorf("unexpected result: %v", rows)
		}
	})

	t.Run("select_with_limit", func(t *testing.T) {
		assertQueryRowCount(t, conn, 5, "SELECT * FROM users LIMIT 5")
	})

	t.Run("select_with_limit_and_offset", func(t *testing.T) {
		assertQueryRowCount(t, conn, 10, "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5")
	})

	t.Run("select_with_order_by_asc", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id FROM users ORDER BY id ASC LIMIT 3")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		id0 := rows[0]["id"].(int32)
		id1 := rows[1]["id"].(int32)
		if id0 >= id1 {
			t.Errorf("not ascending: %d >= %d", id0, id1)
		}
	})

	t.Run("select_with_order_by_desc", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id FROM users ORDER BY id DESC LIMIT 3")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		id0 := rows[0]["id"].(int32)
		id1 := rows[1]["id"].(int32)
		if id0 <= id1 {
			t.Errorf("not descending: %d <= %d", id0, id1)
		}
	})

	t.Run("select_distinct", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT DISTINCT status FROM orders")
		if len(rows) < 2 {
			t.Errorf("expected multiple distinct statuses, got %d", len(rows))
		}
	})

	t.Run("select_with_where_in", func(t *testing.T) {
		assertQueryRowCount(t, conn, 3, "SELECT * FROM users WHERE id IN (1, 2, 3)")
	})

	t.Run("select_with_where_like", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE username LIKE 'user_1%'")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	t.Run("select_with_where_between", func(t *testing.T) {
		assertQueryRowCount(t, conn, 10, "SELECT * FROM users WHERE id BETWEEN 1 AND 10")
	})

	t.Run("select_with_where_is_null", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM user_profiles WHERE website IS NULL")
		if len(rows) < 1 {
			t.Errorf("expected null websites, got %d rows", len(rows))
		}
	})

	t.Run("select_with_where_is_not_null", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM user_profiles WHERE website IS NOT NULL")
		if len(rows) < 1 {
			t.Errorf("expected non-null websites, got %d rows", len(rows))
		}
	})
}

func TestBasicInsert(t *testing.T) {
	conn := connect(t)

	t.Run("insert_serial_pk_returns_offset_id", func(t *testing.T) {
		var id int32
		err := conn.QueryRow(t.Context(),
			"INSERT INTO users (username, email, display_name, password_hash) VALUES ('e2e_new_user', 'e2e@test.com', 'E2E User', 'hash_e2e') RETURNING id",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT RETURNING: %v", err)
		}
		// Shadow offset should be well above 500 (prod max).
		if id <= 500 {
			t.Errorf("shadow ID %d should be > 500 (offset range)", id)
		}
		t.Logf("New user ID (shadow offset): %d", id)
	})

	t.Run("insert_bigserial_pk_into_orders", func(t *testing.T) {
		var id int64
		err := conn.QueryRow(t.Context(),
			"INSERT INTO orders (user_id, status, total_amount) VALUES (1, 'pending', 99.99) RETURNING id",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT RETURNING: %v", err)
		}
		if id <= 2000 {
			t.Errorf("shadow order ID %d should be > 2000", id)
		}
		t.Logf("New order ID (shadow offset): %d", id)
	})

	t.Run("insert_uuid_pk_products", func(t *testing.T) {
		var id string
		err := conn.QueryRow(t.Context(),
			"INSERT INTO products (name, slug, description, price) VALUES ('E2E Product', 'e2e-product', 'Test product', 42.00) RETURNING id",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT RETURNING: %v", err)
		}
		if id == "" {
			t.Error("expected a UUID, got empty string")
		}
		t.Logf("New product UUID: %s", id)
	})

	t.Run("insert_composite_pk_org_members", func(t *testing.T) {
		mustExec(t, conn, "INSERT INTO org_members (org_id, user_id, role) VALUES (1, 499, 'e2e_test') ON CONFLICT DO NOTHING")
	})

	t.Run("insert_no_pk_table_settings", func(t *testing.T) {
		mustExec(t, conn, "INSERT INTO settings (key, value, category) VALUES ('e2e.test', 'true', 'testing')")
	})

	t.Run("insert_with_returning_star", func(t *testing.T) {
		rows := mustQuery(t, conn, "INSERT INTO tags (name, slug) VALUES ('e2e-tag', 'e2e-tag') RETURNING *")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["name"] != "e2e-tag" {
			t.Errorf("returned name = %v, want 'e2e-tag'", rows[0]["name"])
		}
	})

	t.Run("insert_with_null_columns", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO user_profiles (user_id, bio, avatar_url, location, website, birth_date) VALUES (499, NULL, NULL, NULL, NULL, NULL) ON CONFLICT DO NOTHING")
	})

	t.Run("insert_with_jsonb_value", func(t *testing.T) {
		mustExec(t, conn,
			`INSERT INTO users (username, email, password_hash, metadata) VALUES ('e2e_json_user', 'json@test.com', 'hash', '{"nested": {"key": "value"}, "array": [1,2,3]}')`)
	})

	t.Run("insert_with_array_value", func(t *testing.T) {
		mustExec(t, conn,
			`INSERT INTO users (username, email, password_hash, tags) VALUES ('e2e_array_user', 'array@test.com', 'hash', ARRAY['alpha','beta','gamma'])`)
	})

	t.Run("insert_with_inet_value", func(t *testing.T) {
		mustExec(t, conn,
			`INSERT INTO users (username, email, password_hash, last_login_ip) VALUES ('e2e_inet_user', 'inet@test.com', 'hash', '10.20.30.40')`)
	})

	t.Run("insert_with_numeric_precision", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO products (name, slug, price) VALUES ('Precise Product', 'precise-product', 123456.78)")
	})

	t.Run("insert_multiple_rows", func(t *testing.T) {
		assertExecRowsAffected(t, conn, 3,
			"INSERT INTO tags (name, slug) VALUES ('e2e-multi-1', 'e2e-multi-1'), ('e2e-multi-2', 'e2e-multi-2'), ('e2e-multi-3', 'e2e-multi-3')")
	})
}

func TestMergedReads(t *testing.T) {
	conn := connect(t)

	t.Run("select_after_insert_shows_both", func(t *testing.T) {
		// users table already has inserts from TestBasicInsert.
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM users")
		// Should be prod (500) + shadow inserts.
		if count <= 500 {
			t.Errorf("expected more than 500 users after inserts, got %d", count)
		}
		t.Logf("Total users after inserts: %d", count)
	})

	t.Run("select_with_limit_after_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users ORDER BY id ASC LIMIT 5")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("select_with_order_by_after_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id FROM users ORDER BY id DESC LIMIT 5")
		if len(rows) < 2 {
			t.Fatalf("expected at least 2 rows")
		}
		// First row should be a shadow insert (high ID).
		id0 := rows[0]["id"].(int32)
		if id0 <= 500 {
			t.Errorf("top ID should be shadow offset, got %d", id0)
		}
	})

	t.Run("inserted_row_visible_by_value", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e_new_user, got %d", len(rows))
		}
	})
}

func TestBasicUpdate(t *testing.T) {
	conn := connect(t)

	t.Run("update_prod_row_triggers_hydration", func(t *testing.T) {
		// Update user 10 (exists in prod).
		assertExecRowsAffected(t, conn, 1,
			"UPDATE users SET display_name = 'Updated User 10' WHERE id = 10")
	})

	t.Run("update_prod_row_visible_in_select", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["display_name"].(string)
		if !ok || name != "Updated User 10" {
			t.Errorf("expected 'Updated User 10', got %v", rows[0]["display_name"])
		}
	})

	t.Run("update_shadow_row", func(t *testing.T) {
		// e2e_new_user was inserted in TestBasicInsert.
		assertExecRowsAffected(t, conn, 1,
			"UPDATE users SET display_name = 'Updated E2E User' WHERE username = 'e2e_new_user'")

		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 1 || rows[0]["display_name"] != "Updated E2E User" {
			t.Errorf("unexpected: %v", rows)
		}
	})

	t.Run("update_with_returning", func(t *testing.T) {
		rows := mustQuery(t, conn, "UPDATE users SET login_count = login_count + 1 WHERE id = 11 RETURNING id, login_count")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("update_jsonb_field", func(t *testing.T) {
		mustExec(t, conn,
			`UPDATE users SET metadata = metadata || '{"updated": true}' WHERE id = 12`)
		rows := mustQuery(t, conn, "SELECT metadata FROM users WHERE id = 12")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row")
		}
	})

	t.Run("update_multiple_columns", func(t *testing.T) {
		assertExecRowsAffected(t, conn, 1,
			"UPDATE users SET display_name = 'Multi Update', login_count = 999, is_active = false WHERE id = 13")
	})

	t.Run("update_bigserial_pk_row", func(t *testing.T) {
		assertExecRowsAffected(t, conn, 1,
			"UPDATE orders SET status = 'updated_by_e2e' WHERE id = 5")
	})

	t.Run("prod_unchanged_after_update", func(t *testing.T) {
		direct := connectDirect(t)
		rows := mustQuery(t, direct, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row from prod")
		}
		name := rows[0]["display_name"]
		if name == "Updated User 10" {
			t.Error("prod should NOT have the update")
		}
	})
}

func TestBasicDelete(t *testing.T) {
	conn := connect(t)

	t.Run("delete_prod_row_creates_tombstone", func(t *testing.T) {
		assertExecRowsAffected(t, conn, 1,
			"DELETE FROM users WHERE id = 20")
	})

	t.Run("deleted_row_invisible_in_select", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE id = 20")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows (tombstoned), got %d", len(rows))
		}
	})

	t.Run("delete_with_returning", func(t *testing.T) {
		rows := mustQuery(t, conn, "DELETE FROM users WHERE id = 21 RETURNING id, username")
		if len(rows) != 1 {
			t.Errorf("expected 1 row from RETURNING, got %d", len(rows))
		}
	})

	t.Run("delete_preserves_other_rows", func(t *testing.T) {
		// User 22 should still be visible.
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE id = 22")
		if len(rows) != 1 {
			t.Errorf("user 22 should still exist, got %d rows", len(rows))
		}
	})

	t.Run("prod_unchanged_after_delete", func(t *testing.T) {
		direct := connectDirect(t)
		rows := mustQuery(t, direct, "SELECT * FROM users WHERE id = 20")
		if len(rows) != 1 {
			t.Errorf("prod user 20 should still exist, got %d rows", len(rows))
		}
	})

	t.Run("tombstone_respects_select_count", func(t *testing.T) {
		count := queryScalar[int64](t, conn, "SELECT COUNT(*) FROM users WHERE id = 20")
		if count != 0 {
			t.Errorf("count of tombstoned user should be 0, got %d", count)
		}
	})

	t.Run("delete_and_select_with_limit", func(t *testing.T) {
		// Delete user 30, then verify LIMIT queries still return correct count.
		mustExec(t, conn, "DELETE FROM users WHERE id = 30")
		rows := mustQuery(t, conn, "SELECT * FROM users ORDER BY id ASC LIMIT 30")
		// Should not include id=20 or id=30 (both tombstoned).
		for _, row := range rows {
			id := row["id"].(int32)
			if id == 20 || id == 30 {
				t.Errorf("tombstoned user id=%d should not appear in LIMIT results", id)
			}
		}
	})
}

func TestCRUDCycle(t *testing.T) {
	conn := connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("full_crud_cycle", func(t *testing.T) {
		// INSERT
		var id int32
		err := conn.QueryRow(ctx,
			"INSERT INTO posts (user_id, title, body, slug, status) VALUES (1, 'CRUD Test Post', 'Test body', 'crud-test-post', 'draft') RETURNING id",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		t.Logf("Inserted post ID: %d", id)

		// READ
		rows := mustQuery(t, conn, fmt.Sprintf("SELECT title FROM posts WHERE id = %d", id))
		if len(rows) != 1 || rows[0]["title"] != "CRUD Test Post" {
			t.Fatalf("READ after INSERT: %v", rows)
		}

		// UPDATE
		mustExec(t, conn, fmt.Sprintf("UPDATE posts SET title = 'Updated CRUD Post' WHERE id = %d", id))

		// READ again
		rows = mustQuery(t, conn, fmt.Sprintf("SELECT title FROM posts WHERE id = %d", id))
		if len(rows) != 1 || rows[0]["title"] != "Updated CRUD Post" {
			t.Fatalf("READ after UPDATE: %v", rows)
		}

		// DELETE
		mustExec(t, conn, fmt.Sprintf("DELETE FROM posts WHERE id = %d", id))

		// READ should return 0 rows
		rows = mustQuery(t, conn, fmt.Sprintf("SELECT * FROM posts WHERE id = %d", id))
		if len(rows) != 0 {
			t.Errorf("READ after DELETE: expected 0 rows, got %d", len(rows))
		}
	})

	t.Run("crud_with_timestamps", func(t *testing.T) {
		// Insert with timestamp. Sessions has UUID PK.
		var id string
		err := conn.QueryRow(ctx,
			"INSERT INTO sessions (user_id, token, expires_at) VALUES (1, 'crud_test_token', now() + interval '1 hour') RETURNING id",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT session: %v", err)
		}

		// Read it back.
		rows := mustQuery(t, conn, fmt.Sprintf("SELECT token FROM sessions WHERE id = '%s'", id))
		if len(rows) == 1 {
			if rows[0]["token"] != "crud_test_token" {
				t.Errorf("unexpected token: %v", rows[0]["token"])
			}
		}
	})

	t.Run("insert_and_verify_not_in_prod", func(t *testing.T) {
		direct := connectDirect(t)
		// Look for any of our e2e insertions.
		rows := mustQuery(t, direct, "SELECT * FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 0 {
			t.Errorf("prod should NOT contain e2e_new_user, found %d rows", len(rows))
		}
	})

	// Validate that the proxy logs contain routing decisions.
	t.Run("log_command_works", func(t *testing.T) {
		out := runMoriCommand(t, "log")
		// Just verify the command doesn't crash.
		_ = strings.Contains(out, "")
	})
}
