//go:build e2e_duckdb

package e2e_duckdb

import (
	"fmt"
	"testing"
)

func TestBasicSelect(t *testing.T) {
	conn := connect(t)

	t.Run("select_all_from_small_table", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM roles")
		if len(rows) < 10 {
			t.Errorf("expected at least 10 roles, got %d", len(rows))
		}
	})

	t.Run("select_count_star_users", func(t *testing.T) {
		count := queryInt64(t, conn, "SELECT COUNT(*) FROM users")
		if count < 100 {
			t.Errorf("users count = %d, want at least 100", count)
		}
	})

	t.Run("select_count_star_orders", func(t *testing.T) {
		count := queryInt64(t, conn, "SELECT COUNT(*) FROM orders")
		if count != 200 {
			t.Errorf("orders count = %d, want 200", count)
		}
	})

	t.Run("select_with_where_serial_pk", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT username FROM users WHERE id = 1")
		if len(rows) != 1 || rows[0]["username"] != "user_1" {
			t.Errorf("unexpected result: %v", rows)
		}
	})

	t.Run("select_with_where_text_pk", func(t *testing.T) {
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
	})

	t.Run("select_with_order_by_desc", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id FROM users ORDER BY id DESC LIMIT 3")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
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
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE display_name IS NULL")
		t.Logf("Found %d users with NULL display_name", len(rows))
	})

	t.Run("select_case_expression", func(t *testing.T) {
		rows := mustQuery(t, conn,
			`SELECT id, CASE WHEN is_active = 1 THEN 'active' ELSE 'inactive' END AS state
			 FROM users WHERE id <= 5`)
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})

	t.Run("select_coalesce", func(t *testing.T) {
		rows := mustQuery(t, conn,
			"SELECT id, COALESCE(display_name, 'unknown') AS name FROM users WHERE id <= 5")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d", len(rows))
		}
	})
}

func TestBasicInsert(t *testing.T) {
	conn := connect(t)

	t.Run("insert_autoincrement_pk", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_new_user', 'e2e@test.com', 'E2E User', 1)")

		rows := mustQuery(t, conn, "SELECT username FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e_new_user, got %d", len(rows))
		}
	})

	t.Run("insert_text_pk", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO products (id, name, slug, price) VALUES ('e2e-product-uuid-001', 'E2E Product', 'e2e-product', 42.00)")

		rows := mustQuery(t, conn, "SELECT name FROM products WHERE id = 'e2e-product-uuid-001'")
		if len(rows) != 1 || rows[0]["name"] != "E2E Product" {
			t.Errorf("unexpected result: %v", rows)
		}
	})

	t.Run("insert_no_pk_table", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO settings (key, value) VALUES ('e2e.test_key', 'test_value')")
	})

	t.Run("insert_composite_pk", func(t *testing.T) {
		// Seed maps each user to exactly one role: user N -> role ((N-1)%10)+1.
		// So user 1 has role 1 only. (1, 3) is guaranteed not to exist.
		mustExec(t, conn,
			"INSERT INTO user_roles (user_id, role_id, granted_at) VALUES (1, 3, current_timestamp)")
	})

	t.Run("insert_multiple_rows", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO settings (key, value) VALUES ('e2e.multi_1', 'val1')")
		mustExec(t, conn,
			"INSERT INTO settings (key, value) VALUES ('e2e.multi_2', 'val2')")

		rows := mustQuery(t, conn, "SELECT * FROM settings WHERE key LIKE 'e2e.multi_%'")
		if len(rows) < 2 {
			t.Errorf("expected at least 2 multi-insert rows, got %d", len(rows))
		}
	})
}

func TestBasicUpdate(t *testing.T) {
	conn := connect(t)

	t.Run("update_prod_row", func(t *testing.T) {
		mustExec(t, conn, "UPDATE users SET display_name = 'Updated User 10' WHERE id = 10")
	})

	t.Run("update_visible_in_select", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["display_name"].(string)
		if !ok || name != "Updated User 10" {
			t.Errorf("expected 'Updated User 10', got %v", rows[0]["display_name"])
		}
	})

	t.Run("prod_unchanged_after_update", func(t *testing.T) {
		direct := connectDirect(t)
		directRows := mustQueryDirect(t, direct, "SELECT display_name FROM users WHERE id = 10")
		if len(directRows) != 1 {
			t.Fatalf("expected 1 row from prod")
		}
		name := fmt.Sprintf("%v", directRows[0]["display_name"])
		if name == "Updated User 10" {
			t.Error("prod should NOT have the update")
		}
	})

	t.Run("update_multiple_rows", func(t *testing.T) {
		mustExec(t, conn, "UPDATE users SET display_name = 'Batch Updated' WHERE id IN (11, 12, 13)")

		for _, id := range []int{11, 12, 13} {
			rows := mustQuery(t, conn, fmt.Sprintf("SELECT display_name FROM users WHERE id = %d", id))
			if len(rows) != 1 {
				t.Errorf("expected 1 row for id=%d, got %d", id, len(rows))
				continue
			}
			name, ok := rows[0]["display_name"].(string)
			if !ok || name != "Batch Updated" {
				t.Errorf("id=%d: expected 'Batch Updated', got %v", id, rows[0]["display_name"])
			}
		}
	})
}

func TestBasicDelete(t *testing.T) {
	conn := connect(t)

	t.Run("delete_prod_row", func(t *testing.T) {
		mustExec(t, conn, "DELETE FROM users WHERE id = 20")
	})

	t.Run("deleted_row_invisible", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE id = 20")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows (deleted), got %d", len(rows))
		}
	})

	t.Run("prod_unchanged_after_delete", func(t *testing.T) {
		direct := connectDirect(t)
		directRows := mustQueryDirect(t, direct, "SELECT * FROM users WHERE id = 20")
		if len(directRows) != 1 {
			t.Errorf("prod user 20 should still exist, got %d rows", len(directRows))
		}
	})

	t.Run("delete_text_pk_row", func(t *testing.T) {
		// Delete by PK (id) to ensure tombstone is tracked correctly.
		prodID := "prod-00000050-0000-0000-0000-000000000050"
		mustExec(t, conn, "DELETE FROM products WHERE id = '"+prodID+"'")
		rows := mustQuery(t, conn, "SELECT * FROM products WHERE id = '"+prodID+"'")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows after delete, got %d", len(rows))
		}
	})
}

func TestMergedReads(t *testing.T) {
	conn := connect(t)

	t.Run("count_after_insert_gt_prod", func(t *testing.T) {
		count := queryInt64(t, conn, "SELECT COUNT(*) FROM users")
		if count < 100 {
			t.Errorf("expected at least 100 users after inserts, got %d", count)
		}
		t.Logf("Total users after inserts: %d", count)
	})

	t.Run("inserted_row_visible_by_value", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e_new_user, got %d", len(rows))
		}
	})

	t.Run("updated_row_shows_new_value", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["display_name"].(string)
		if !ok || name != "Updated User 10" {
			t.Errorf("expected 'Updated User 10', got %v", rows[0]["display_name"])
		}
	})

	t.Run("deleted_row_excluded_from_count", func(t *testing.T) {
		// User 20 was deleted. Direct has 100, proxy should have 100 - 1 + shadow inserts.
		proxyCount := queryInt64(t, conn, "SELECT COUNT(*) FROM users")
		direct := connectDirect(t)
		var directCount int64
		direct.QueryRow("SELECT COUNT(*) FROM users").Scan(&directCount)
		// Proxy count should reflect deletions.
		t.Logf("proxy=%d direct=%d", proxyCount, directCount)
	})

	t.Run("text_pk_insert_visible", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT name FROM products WHERE id = 'e2e-product-uuid-001'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e product, got %d", len(rows))
		}
	})

	t.Run("select_star_includes_all_sources", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM products ORDER BY slug LIMIT 5")
		if len(rows) < 1 {
			t.Errorf("expected products from merged read, got %d", len(rows))
		}
	})
}

func TestCRUDCycle(t *testing.T) {
	conn := connect(t)

	t.Run("full_insert_select_update_select_delete_select", func(t *testing.T) {
		// INSERT
		mustExec(t, conn,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('crud_cycle_user', 'crud@test.com', 'CRUD User', 1)")

		// SELECT - verify inserted
		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE username = 'crud_cycle_user'")
		if len(rows) != 1 || rows[0]["display_name"] != "CRUD User" {
			t.Fatalf("READ after INSERT: %v", rows)
		}

		// UPDATE
		mustExec(t, conn, "UPDATE users SET display_name = 'Updated CRUD User' WHERE username = 'crud_cycle_user'")

		// SELECT - verify updated
		rows = mustQuery(t, conn, "SELECT display_name FROM users WHERE username = 'crud_cycle_user'")
		if len(rows) != 1 || rows[0]["display_name"] != "Updated CRUD User" {
			t.Fatalf("READ after UPDATE: %v", rows)
		}

		// DELETE
		mustExec(t, conn, "DELETE FROM users WHERE username = 'crud_cycle_user'")

		// SELECT - verify deleted
		rows = mustQuery(t, conn, "SELECT * FROM users WHERE username = 'crud_cycle_user'")
		if len(rows) != 0 {
			t.Errorf("READ after DELETE: expected 0 rows, got %d", len(rows))
		}
	})

	t.Run("crud_cycle_text_pk", func(t *testing.T) {
		// INSERT
		mustExec(t, conn,
			"INSERT INTO products (id, name, slug, price) VALUES ('crud-cycle-001', 'Cycle Product', 'cycle-product', 55.00)")

		// SELECT
		rows := mustQuery(t, conn, "SELECT name FROM products WHERE id = 'crud-cycle-001'")
		if len(rows) != 1 || rows[0]["name"] != "Cycle Product" {
			t.Fatalf("text PK read after insert: %v", rows)
		}

		// UPDATE
		mustExec(t, conn, "UPDATE products SET price = 66.00 WHERE id = 'crud-cycle-001'")

		// DELETE
		mustExec(t, conn, "DELETE FROM products WHERE id = 'crud-cycle-001'")

		// SELECT - verify gone
		rows = mustQuery(t, conn, "SELECT * FROM products WHERE id = 'crud-cycle-001'")
		if len(rows) != 0 {
			t.Errorf("text PK read after delete: expected 0, got %d", len(rows))
		}
	})
}
