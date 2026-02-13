//go:build e2e_mssql

package e2e_mssql

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNoPKTable(t *testing.T) {
	db := connect(t)

	t.Run("select_from_settings", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM settings")
		if len(rows) < 20 {
			t.Errorf("expected at least 20 settings rows, got %d", len(rows))
		}
	})

	t.Run("insert_into_settings", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO settings ([key], value) VALUES ('e2e.edge', 'test')")
	})

	t.Run("select_settings_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM settings WHERE [key] = 'e2e.edge'")
		if len(rows) < 1 {
			t.Errorf("expected to find inserted setting, got %d rows", len(rows))
		}
	})

	t.Run("update_settings_by_key", func(t *testing.T) {
		mustExec(t, db,
			"UPDATE settings SET value = 'updated' WHERE [key] = 'e2e.edge'")
		rows := mustQuery(t, db, "SELECT value FROM settings WHERE [key] = 'e2e.edge'")
		if len(rows) < 1 {
			t.Errorf("expected to find updated setting, got %d rows", len(rows))
		}
	})
}

func TestUUIDPK(t *testing.T) {
	db := connect(t)

	t.Run("select_products_by_slug", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT id, name, price FROM products WHERE slug = 'product-50'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 product, got %d", len(rows))
		}
	})

	t.Run("insert_uuid_product", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO products (name, slug, price) VALUES ('Edge UUID Product', 'edge-uuid-product', 19.99)")
	})

	t.Run("select_uuid_product_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM products WHERE slug = 'edge-uuid-product'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("update_uuid_product", func(t *testing.T) {
		mustExec(t, db,
			"UPDATE products SET price = 29.99 WHERE slug = 'edge-uuid-product'")
		rows := mustQuery(t, db, "SELECT price FROM products WHERE slug = 'edge-uuid-product'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("delete_uuid_product", func(t *testing.T) {
		mustExec(t, db, "DELETE FROM products WHERE slug = 'edge-uuid-product'")
		rows := mustQuery(t, db, "SELECT * FROM products WHERE slug = 'edge-uuid-product'")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows after delete, got %d", len(rows))
		}
	})
}

func TestCompositePK(t *testing.T) {
	db := connect(t)

	t.Run("select_user_roles", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM user_roles WHERE user_id = 1")
		if len(rows) < 1 {
			t.Errorf("expected user_roles rows, got %d", len(rows))
		}
	})

	t.Run("insert_composite_pk", func(t *testing.T) {
		// Use a user_id/role_id combo that does not already exist.
		mustExec(t, db, "INSERT INTO user_roles (user_id, role_id) VALUES (99, 10)")
	})

	t.Run("select_composite_pk_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM user_roles WHERE user_id = 99 AND role_id = 10")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("delete_composite_pk", func(t *testing.T) {
		mustExec(t, db, "DELETE FROM user_roles WHERE user_id = 99 AND role_id = 10")
		rows := mustQuery(t, db, "SELECT * FROM user_roles WHERE user_id = 99 AND role_id = 10")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows after delete, got %d", len(rows))
		}
	})
}

func TestNullHandling(t *testing.T) {
	db := connect(t)

	t.Run("insert_with_nulls", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_null_user', 'null@test.com', NULL, 1)")
	})

	t.Run("select_where_null", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE display_name IS NULL AND username = 'e2e_null_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 null-display-name user, got %d", len(rows))
		}
	})

	t.Run("update_set_to_null", func(t *testing.T) {
		mustExec(t, db, "UPDATE users SET display_name = NULL WHERE id = 3")
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["display_name"] != nil {
			t.Errorf("expected NULL, got %v", rows[0]["display_name"])
		}
	})

	t.Run("update_null_to_value", func(t *testing.T) {
		mustExec(t, db, "UPDATE users SET display_name = 'Restored Name' WHERE id = 3")
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 3")
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
	db := connect(t)

	t.Run("insert_10kb_nvarchar_max", func(t *testing.T) {
		bigText := strings.Repeat("A", 10*1024)
		mustExec(t, db,
			"INSERT INTO settings ([key], value) VALUES ('e2e.bigtext', @p1)", bigText)
	})

	t.Run("read_10kb_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT value FROM settings WHERE [key] = 'e2e.bigtext'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		body, ok := rows[0]["value"].(string)
		if !ok || len(body) != 10*1024 {
			t.Errorf("expected 10KB body, got %d bytes", len(body))
		}
	})
}

func TestEmptyResults(t *testing.T) {
	db := connect(t)

	t.Run("select_impossible_where", func(t *testing.T) {
		assertQueryRowCount(t, db, 0, "SELECT * FROM users WHERE 1 = 0")
	})

	t.Run("select_nonexistent_id", func(t *testing.T) {
		assertQueryRowCount(t, db, 0, "SELECT * FROM users WHERE id = 999999")
	})
}

func TestConcurrentConnections(t *testing.T) {
	t.Run("two_connections_see_each_others_inserts", func(t *testing.T) {
		db1 := connect(t)
		db2 := connect(t)

		// Insert via db1.
		mustExec(t, db1, "INSERT INTO settings ([key], value) VALUES ('e2e.concurrent1', 'from_db1')")

		// Should be visible via db2.
		rows := mustQuery(t, db2, "SELECT * FROM settings WHERE [key] = 'e2e.concurrent1'")
		if len(rows) != 1 {
			t.Errorf("db2 should see db1's insert, got %d rows", len(rows))
		}
	})

	t.Run("parallel_inserts", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make([]error, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				db, err := sql.Open("sqlserver", proxyDSN)
				if err != nil {
					errors[idx] = err
					return
				}
				defer db.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, errors[idx] = db.ExecContext(ctx,
					"INSERT INTO settings ([key], value) VALUES (@p1, @p2)",
					"e2e.parallel_"+strings.Repeat("x", idx), "parallel_test")
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

func TestSpecialCharacters(t *testing.T) {
	db := connect(t)

	t.Run("single_quotes", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_obriens', 'obrien@test.com', @p1, 1)", "O'Brien")

		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE username = 'e2e_obriens'")
		if len(rows) != 1 || rows[0]["display_name"] != "O'Brien" {
			t.Errorf("unexpected: %v", rows)
		}
	})

	t.Run("unicode", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_unicode', 'unicode@test.com', @p1, 1)", "Test: hello world")

		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE username = 'e2e_unicode'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("newlines_in_nvarchar_max", func(t *testing.T) {
		body := "Line 1\nLine 2\nLine 3"
		mustExec(t, db,
			"INSERT INTO settings ([key], value) VALUES ('e2e.newlines', @p1)", body)

		rows := mustQuery(t, db, "SELECT value FROM settings WHERE [key] = 'e2e.newlines'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		got, ok := rows[0]["value"].(string)
		if !ok || got != body {
			t.Errorf("body roundtrip failed: got %q, want %q", got, body)
		}
	})
}

func TestBracketQuoting(t *testing.T) {
	db := connect(t)

	t.Run("select_with_bracket_quoted_column", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT [key], value FROM settings WHERE [key] = 'site.name'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("insert_with_bracket_quoted_column", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO settings ([key], value) VALUES ('e2e.bracket', 'test')")
		rows := mustQuery(t, db, "SELECT [key] FROM settings WHERE [key] = 'e2e.bracket'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})
}
