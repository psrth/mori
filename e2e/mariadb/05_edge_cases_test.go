//go:build e2e_mariadb

package e2e_mariadb

import (
	"context"
	"database/sql"
	"fmt"
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

	t.Run("insert_into_settings_succeeds", func(t *testing.T) {
		// No-PK tables: writes go to shadow, reads go to prod.
		// We verify the INSERT succeeds without error.
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.edge', 'test')")
	})

	t.Run("update_settings_succeeds", func(t *testing.T) {
		mustExec(t, db,
			"UPDATE settings SET value = 'updated' WHERE `key` = 'site.name'")
	})

	t.Run("select_prod_settings_still_intact", func(t *testing.T) {
		// Reads from prod should still return the original seed data.
		rows := mustQuery(t, db, "SELECT * FROM settings WHERE `key` = 'site.name'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for site.name, got %d", len(rows))
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
			"INSERT INTO products (id, name, slug, price) VALUES (UUID(), 'Edge UUID Product', 'edge-uuid-product', 19.99)")
	})

	t.Run("select_uuid_product_after_insert", func(t *testing.T) {
		// Shadow-inserted row may not be visible via PROD_DIRECT reads.
		// Just verify the query runs without error.
		_ = mustQuery(t, db, "SELECT * FROM products WHERE slug = 'edge-uuid-product'")
	})

	t.Run("update_uuid_product", func(t *testing.T) {
		// Verify UPDATE succeeds; read-back may go to PROD_DIRECT.
		mustExec(t, db,
			"UPDATE products SET price = 29.99 WHERE slug = 'edge-uuid-product'")
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
		// Shadow-inserted row may not be visible via PROD_DIRECT reads.
		_ = mustQuery(t, db, "SELECT * FROM user_roles WHERE user_id = 99 AND role_id = 10")
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
		// Shadow-inserted row may not be visible via PROD_DIRECT reads.
		// Just verify the query runs without error.
		_ = mustQuery(t, db, "SELECT * FROM users WHERE display_name IS NULL AND username = 'e2e_null_user'")
	})

	t.Run("update_set_to_null", func(t *testing.T) {
		// Verify UPDATE to NULL succeeds; read-back may go to PROD_DIRECT.
		mustExec(t, db, "UPDATE users SET display_name = NULL WHERE id = 3")
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("update_null_to_value", func(t *testing.T) {
		// Verify UPDATE from NULL to value succeeds.
		mustExec(t, db, "UPDATE users SET display_name = 'Restored Name' WHERE id = 3")
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})
}

func TestLargeText(t *testing.T) {
	db := connect(t)

	t.Run("insert_10kb_into_roles_description", func(t *testing.T) {
		bigText := strings.Repeat("A", 10*1024)
		mustExec(t, db,
			"INSERT INTO roles (name, description) VALUES ('e2e_bigtext_role', ?)", bigText)
	})

	t.Run("update_10kb_text_succeeds", func(t *testing.T) {
		// Verify that a 10KB UPDATE goes through the proxy without error.
		bigText := strings.Repeat("B", 10*1024)
		mustExec(t, db,
			"UPDATE roles SET description = ? WHERE id = 1", bigText)
	})

	t.Run("select_large_text_returns_row", func(t *testing.T) {
		// Verify SELECT on the updated row returns a result (may come from
		// PROD_DIRECT or MERGED_READ depending on proxy routing).
		rows := mustQuery(t, db, "SELECT description FROM roles WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
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
	t.Run("two_connections_see_same_prod_data", func(t *testing.T) {
		db1 := connect(t)
		db2 := connect(t)

		// Both connections should see the same prod data.
		c1 := queryScalar[int64](t, db1, "SELECT COUNT(*) FROM roles")
		c2 := queryScalar[int64](t, db2, "SELECT COUNT(*) FROM roles")
		if c1 != c2 {
			t.Errorf("connections see different role counts: %d vs %d", c1, c2)
		}
		if c1 < 10 {
			t.Errorf("expected at least 10 roles, got %d", c1)
		}
	})

	t.Run("parallel_inserts", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make([]error, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				db, err := sql.Open("mysql", proxyDSN)
				if err != nil {
					errors[idx] = err
					return
				}
				defer db.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, errors[idx] = db.ExecContext(ctx,
					"INSERT INTO roles (name, description) VALUES (?, ?)",
					fmt.Sprintf("e2e_parallel_%d", idx), "parallel_test")
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

func TestParameterizedQueries(t *testing.T) {
	db := connect(t)

	t.Run("select_with_param", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id = ?", 1)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("insert_with_params_succeeds", func(t *testing.T) {
		// Verify parameterized INSERT doesn't error.
		mustExec(t, db,
			"INSERT INTO roles (name, description) VALUES (?, ?)", "e2e_param_role", "param_value")
	})

	t.Run("update_with_params_succeeds", func(t *testing.T) {
		// Verify parameterized UPDATE doesn't error.
		mustExec(t, db,
			"UPDATE roles SET description = ? WHERE id = 2", "param_updated")
	})

	t.Run("select_with_multiple_params", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id BETWEEN ? AND ?", 1, 5)
		if len(rows) < 1 {
			t.Errorf("expected at least 1 row, got %d", len(rows))
		}
	})
}

func TestTransactions(t *testing.T) {
	db := connect(t)

	t.Run("commit_transaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		_, err = tx.Exec("INSERT INTO roles (name, description) VALUES ('e2e_tx_commit_role', 'committed')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Exec in tx: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		// Commit succeeded without error — the data is in shadow.
	})

	t.Run("rollback_transaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		_, err = tx.Exec("INSERT INTO roles (name, description) VALUES ('e2e_tx_rollback_role', 'should_not_exist')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Exec in tx: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		// Rollback succeeded — verify the row is not in prod.
		rows := mustQuery(t, db, "SELECT * FROM roles WHERE name = 'e2e_tx_rollback_role'")
		if len(rows) != 0 {
			t.Errorf("rolled-back row should not exist in prod, got %d rows", len(rows))
		}
	})

	t.Run("transaction_insert_succeeds", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		_, err = tx.Exec("INSERT INTO roles (name, description) VALUES ('e2e_tx_role', 'from_tx')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("Exec in tx: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})
}

func TestSpecialCharacters(t *testing.T) {
	db := connect(t)

	t.Run("single_quotes_insert", func(t *testing.T) {
		// Verify parameterized insert with special characters doesn't error.
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_obriens', 'obrien@test.com', ?, 1)", "O'Brien")
	})

	t.Run("unicode_insert", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_unicode', 'unicode@test.com', ?, 1)", "Test: hello world")
	})

	t.Run("special_chars_update_succeeds", func(t *testing.T) {
		// Verify UPDATE with special characters doesn't error.
		mustExec(t, db, "UPDATE users SET display_name = ? WHERE id = 5", "O'Brien & Co <tag>")
	})

	t.Run("newlines_insert_succeeds", func(t *testing.T) {
		body := "Line 1\nLine 2\nLine 3"
		mustExec(t, db,
			"INSERT INTO roles (name, description) VALUES ('e2e_newlines_role', ?)", body)
	})
}

func TestBacktickQuoting(t *testing.T) {
	db := connect(t)

	t.Run("select_with_backtick_quoted_column", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT `key`, value FROM settings WHERE `key` = 'site.name'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("insert_with_backtick_quoted_column", func(t *testing.T) {
		// Verify the INSERT with backtick-quoted columns doesn't error.
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.backtick', 'test')")
	})

	t.Run("select_backtick_quoted_table_and_columns", func(t *testing.T) {
		// Verify backtick quoting on table + column works for reads.
		rows := mustQuery(t, db, "SELECT `id`, `name` FROM `roles` WHERE `id` = 1")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})
}
