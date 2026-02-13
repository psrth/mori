//go:build e2e_oracle

package e2e_oracle

import (
	"fmt"
	"testing"
)

func TestBasicSelect(t *testing.T) {
	db := connect(t)

	t.Run("select_all_from_small_table", func(t *testing.T) {
		// 10 seeded roles + 1 inserted by lifecycle test.
		rows := mustQuery(t, db, "SELECT * FROM roles")
		if len(rows) < 10 {
			t.Errorf("expected at least 10 roles, got %d", len(rows))
		}
	})

	t.Run("select_count_star_users", func(t *testing.T) {
		count := queryScalar[int64](t, db, "SELECT COUNT(*) FROM users")
		if count != 100 {
			t.Errorf("users count = %d, want 100", count)
		}
	})

	t.Run("select_count_star_orders", func(t *testing.T) {
		count := queryScalar[int64](t, db, "SELECT COUNT(*) FROM orders")
		if count != 200 {
			t.Errorf("orders count = %d, want 200", count)
		}
	})

	t.Run("select_with_where_serial_pk", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT username FROM users WHERE id = :1", 1)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("select_with_where_uuid_pk", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT name FROM products WHERE slug = :1", "product-1")
		if len(rows) != 1 || rows[0]["NAME"] != "Product 1" {
			t.Errorf("unexpected result: %v", rows)
		}
	})

	t.Run("select_fetch_first", func(t *testing.T) {
		assertQueryRowCount(t, db, 5, "SELECT * FROM users FETCH FIRST 5 ROWS ONLY")
	})

	t.Run("select_with_offset_fetch", func(t *testing.T) {
		assertQueryRowCount(t, db, 10,
			"SELECT * FROM users ORDER BY id OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")
	})

	t.Run("select_with_order_by", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT id FROM users ORDER BY id ASC FETCH FIRST 3 ROWS ONLY")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("select_distinct", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT DISTINCT status FROM orders")
		if len(rows) < 2 {
			t.Errorf("expected multiple distinct statuses, got %d", len(rows))
		}
	})

	t.Run("select_with_where_in", func(t *testing.T) {
		assertQueryRowCount(t, db, 3, "SELECT * FROM users WHERE id IN (1, 2, 3)")
	})

	t.Run("select_with_where_like", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE username LIKE 'user_1%'")
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})

	t.Run("select_with_where_between", func(t *testing.T) {
		assertQueryRowCount(t, db, 10, "SELECT * FROM users WHERE id BETWEEN 1 AND 10")
	})

	t.Run("select_with_where_is_null", func(t *testing.T) {
		// display_name is populated for all seed users, but test the IS NULL syntax.
		rows := mustQuery(t, db, "SELECT * FROM users WHERE display_name IS NULL")
		t.Logf("users with NULL display_name: %d", len(rows))
	})
}

func TestBasicInsert(t *testing.T) {
	db := connect(t)

	t.Run("insert_identity_pk", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name) VALUES ('e2e_new_user', 'e2e@test.com', 'E2E User')")

		// Verify the insert is visible via proxy.
		rows := mustQuery(t, db, "SELECT username FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e_new_user, got %d", len(rows))
		}
	})

	t.Run("insert_uuid_pk", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO products (name, slug, price) VALUES ('E2E Product', 'e2e-product', 42.00)")

		rows := mustQuery(t, db, "SELECT name FROM products WHERE slug = 'e2e-product'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e-product, got %d", len(rows))
		}
	})

	t.Run("insert_no_pk_table", func(t *testing.T) {
		mustExec(t, db,
			`INSERT INTO settings ("KEY", value) VALUES ('e2e.test', 'true')`)
	})
}

func TestBasicUpdate(t *testing.T) {
	db := connect(t)

	t.Run("update_prod_row", func(t *testing.T) {
		mustExec(t, db,
			"UPDATE users SET display_name = 'Updated User 10' WHERE id = 10")
	})

	t.Run("update_visible_in_select", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["DISPLAY_NAME"].(string)
		if !ok || name != "Updated User 10" {
			t.Errorf("expected 'Updated User 10', got %v", rows[0]["DISPLAY_NAME"])
		}
	})

	t.Run("prod_unchanged_after_update", func(t *testing.T) {
		direct := connectDirect(t)
		rows := mustQuery(t, direct, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row from prod")
		}
		name := fmt.Sprintf("%v", rows[0]["DISPLAY_NAME"])
		if name == "Updated User 10" {
			t.Error("prod should NOT have the update")
		}
	})
}

func TestBasicDelete(t *testing.T) {
	db := connect(t)

	t.Run("delete_prod_row", func(t *testing.T) {
		mustExec(t, db, "DELETE FROM users WHERE id = 20")
	})

	t.Run("deleted_row_invisible", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id = 20")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows (tombstoned), got %d", len(rows))
		}
	})

	t.Run("prod_unchanged_after_delete", func(t *testing.T) {
		direct := connectDirect(t)
		rows := mustQuery(t, direct, "SELECT * FROM users WHERE id = 20")
		if len(rows) != 1 {
			t.Errorf("prod user 20 should still exist, got %d rows", len(rows))
		}
	})
}

func TestMergedReads(t *testing.T) {
	db := connect(t)

	t.Run("count_after_insert", func(t *testing.T) {
		// Users table already has inserts from TestBasicInsert.
		count := queryScalar[int64](t, db, "SELECT COUNT(*) FROM users")
		// Should be prod (100) + shadow inserts minus tombstones.
		if count < 100 {
			t.Errorf("expected at least 100 users after inserts, got %d", count)
		}
		t.Logf("Total users after inserts: %d", count)
	})

	t.Run("inserted_row_visible_by_value", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE username = 'e2e_new_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for e2e_new_user, got %d", len(rows))
		}
	})
}

func TestCRUDCycle(t *testing.T) {
	db := connect(t)

	t.Run("full_cycle", func(t *testing.T) {
		// INSERT
		mustExec(t, db,
			"INSERT INTO roles (name, description) VALUES ('crud_test_role', 'CRUD test')")

		// READ
		rows := mustQuery(t, db, "SELECT name FROM roles WHERE name = 'crud_test_role'")
		if len(rows) != 1 || rows[0]["NAME"] != "crud_test_role" {
			t.Fatalf("READ after INSERT: %v", rows)
		}

		// UPDATE
		mustExec(t, db, "UPDATE roles SET description = 'Updated CRUD' WHERE name = 'crud_test_role'")

		// READ again
		rows = mustQuery(t, db, "SELECT description FROM roles WHERE name = 'crud_test_role'")
		if len(rows) != 1 || rows[0]["DESCRIPTION"] != "Updated CRUD" {
			t.Fatalf("READ after UPDATE: %v", rows)
		}

		// DELETE
		mustExec(t, db, "DELETE FROM roles WHERE name = 'crud_test_role'")

		// READ should return 0 rows
		rows = mustQuery(t, db, "SELECT * FROM roles WHERE name = 'crud_test_role'")
		if len(rows) != 0 {
			t.Errorf("READ after DELETE: expected 0 rows, got %d", len(rows))
		}
	})
}
