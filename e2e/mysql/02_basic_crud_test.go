//go:build e2e_mysql

package e2e_mysql

import (
	"fmt"
	"testing"
)

func TestBasicSelect(t *testing.T) {
	db := connect(t)

	t.Run("select_all_from_small_table", func(t *testing.T) {
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
		rows := mustQuery(t, db, "SELECT username FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["username"] != "user_1" {
			t.Errorf("unexpected username: %v", rows[0]["username"])
		}
	})

	t.Run("select_with_where_uuid_pk", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT name FROM products WHERE slug = 'product-1'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 product, got %d", len(rows))
		}
		if rows[0]["name"] != "Product 1" {
			t.Errorf("unexpected name: %v", rows[0]["name"])
		}
	})

	t.Run("select_limit_n", func(t *testing.T) {
		assertQueryRowCount(t, db, 5, "SELECT * FROM users LIMIT 5")
	})

	t.Run("select_with_limit_offset", func(t *testing.T) {
		assertQueryRowCount(t, db, 10,
			"SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5")
	})

	t.Run("select_with_order_by_asc", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT id FROM users ORDER BY id ASC LIMIT 3")
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		id0 := rows[0]["id"].(int64)
		id1 := rows[1]["id"].(int64)
		if id0 >= id1 {
			t.Errorf("not ascending: %d >= %d", id0, id1)
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
		rows := mustQuery(t, db, "SELECT * FROM settings WHERE value IS NULL")
		_ = rows
	})
}

func TestBasicInsert(t *testing.T) {
	db := connect(t)

	t.Run("insert_auto_increment_pk", func(t *testing.T) {
		res := mustExecResult(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_new_user', 'e2e@test.com', 'E2E User', 1)")
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatalf("LastInsertId: %v", err)
		}
		if id <= 100 {
			t.Errorf("shadow ID %d should be > 100 (offset range)", id)
		}
		t.Logf("New user ID (shadow offset): %d", id)
	})

	t.Run("insert_uuid_pk", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO products (id, name, slug, price) VALUES (UUID(), 'E2E Product', 'e2e-product', 42.00)")
	})

	t.Run("insert_no_pk_table", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.test', 'true')")
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
		name, ok := rows[0]["display_name"].(string)
		if !ok || name != "Updated User 10" {
			t.Errorf("expected 'Updated User 10', got %v", rows[0]["display_name"])
		}
	})

	t.Run("prod_unchanged_after_update", func(t *testing.T) {
		direct := connectDirect(t)
		rows := mustQuery(t, direct, "SELECT display_name FROM users WHERE id = 10")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row from prod")
		}
		name, _ := rows[0]["display_name"].(string)
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
		// Insert additional users so that shadow inserts exceed the single
		// delete from TestBasicDelete, making the merged count > 100.
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_merged_1', 'merged1@test.com', 'Merged 1', 1)")
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_merged_2', 'merged2@test.com', 'Merged 2', 1)")

		count := queryScalar[int64](t, db, "SELECT COUNT(*) FROM users")
		// Should be prod (100) + shadow inserts (3 total) - deletes (1) = 102.
		if count <= 100 {
			t.Errorf("expected more than 100 users after inserts, got %d", count)
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
		// INSERT into shadow.
		res := mustExecResult(t, db,
			"INSERT INTO orders (user_id, status, total_amount) VALUES (1, 'draft', 55.00)")
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatalf("INSERT LastInsertId: %v", err)
		}
		t.Logf("Inserted order ID: %d", id)

		// UPDATE on shadow (row exists there from INSERT above).
		mustExec(t, db, fmt.Sprintf("UPDATE orders SET status = 'shipped' WHERE id = %d", id))

		// DELETE on shadow.
		mustExec(t, db, fmt.Sprintf("DELETE FROM orders WHERE id = %d", id))

		// Verify prod never received the shadow-only writes.
		direct := connectDirect(t)
		rows := mustQuery(t, direct, fmt.Sprintf("SELECT * FROM orders WHERE id = %d", id))
		if len(rows) != 0 {
			t.Errorf("prod should not have shadow-inserted order (id=%d), got %d rows", id, len(rows))
		}
	})
}
