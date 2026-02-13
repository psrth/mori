//go:build e2e_mssql

package e2e_mssql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBasicSelect(t *testing.T) {
	db := connect(t)

	t.Run("select_all_from_small_table", func(t *testing.T) {
		// roles has 10 seed rows + 1 from lifecycle test.
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

	t.Run("select_top_n", func(t *testing.T) {
		assertQueryRowCount(t, db, 5, "SELECT TOP 5 * FROM users")
	})

	t.Run("select_with_offset_fetch", func(t *testing.T) {
		assertQueryRowCount(t, db, 10,
			"SELECT * FROM users ORDER BY id OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")
	})

	t.Run("select_with_order_by_asc", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT TOP 3 id FROM users ORDER BY id ASC")
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
		// display_name is populated for all seed users, but we can check for a NULL pattern.
		rows := mustQuery(t, db, "SELECT * FROM settings WHERE value IS NULL")
		// Settings all have values, so 0 rows is fine. Just ensure no error.
		_ = rows
	})
}

func TestBasicInsert(t *testing.T) {
	db := connect(t)

	t.Run("insert_identity_pk", func(t *testing.T) {
		// MSSQL: use OUTPUT INSERTED.id to get the new ID.
		var id int64
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := db.QueryRowContext(ctx,
			"INSERT INTO users (username, email, display_name, is_active) OUTPUT INSERTED.id VALUES ('e2e_new_user', 'e2e@test.com', 'E2E User', 1)",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT OUTPUT: %v", err)
		}
		// Shadow offset should be well above 100 (prod max).
		if id <= 100 {
			t.Errorf("shadow ID %d should be > 100 (offset range)", id)
		}
		t.Logf("New user ID (shadow offset): %d", id)
	})

	t.Run("insert_uuid_pk", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO products (name, slug, price) VALUES ('E2E Product', 'e2e-product', 42.00)")
	})

	t.Run("insert_no_pk_table", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO settings ([key], value) VALUES ('e2e.test', 'true')")
	})

	t.Run("insert_with_output_inserted", func(t *testing.T) {
		rows := mustQuery(t, db,
			"INSERT INTO users (username, email, display_name, is_active) OUTPUT INSERTED.* VALUES ('e2e_output_user', 'e2e_output@test.com', 'Output User', 1)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row from OUTPUT INSERTED.*, got %d", len(rows))
		}
		if rows[0]["username"] != "e2e_output_user" {
			t.Errorf("returned username = %v, want 'e2e_output_user'", rows[0]["username"])
		}
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
		// users table has inserts from TestBasicInsert.
		count := queryScalar[int64](t, db, "SELECT COUNT(*) FROM users")
		// Should be prod (100) + shadow inserts - deletes.
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("full_cycle", func(t *testing.T) {
		// INSERT
		var id int64
		err := db.QueryRowContext(ctx,
			"INSERT INTO orders (user_id, status, total_amount) OUTPUT INSERTED.id VALUES (1, 'draft', 55.00)",
		).Scan(&id)
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		t.Logf("Inserted order ID: %d", id)

		// READ
		rows := mustQuery(t, db, fmt.Sprintf("SELECT status FROM orders WHERE id = %d", id))
		if len(rows) != 1 || rows[0]["status"] != "draft" {
			t.Fatalf("READ after INSERT: %v", rows)
		}

		// UPDATE
		mustExec(t, db, fmt.Sprintf("UPDATE orders SET status = 'shipped' WHERE id = %d", id))

		// READ again
		rows = mustQuery(t, db, fmt.Sprintf("SELECT status FROM orders WHERE id = %d", id))
		if len(rows) != 1 || rows[0]["status"] != "shipped" {
			t.Fatalf("READ after UPDATE: %v", rows)
		}

		// DELETE
		mustExec(t, db, fmt.Sprintf("DELETE FROM orders WHERE id = %d", id))

		// READ should return 0 rows
		rows = mustQuery(t, db, fmt.Sprintf("SELECT * FROM orders WHERE id = %d", id))
		if len(rows) != 0 {
			t.Errorf("READ after DELETE: expected 0 rows, got %d", len(rows))
		}
	})
}
