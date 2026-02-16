//go:build e2e_mysql

package e2e_mysql

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
			"INSERT INTO settings (`key`, value) VALUES ('e2e.edge', 'test')")
	})

	t.Run("select_settings_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM settings WHERE `key` = 'e2e.edge'")
		if len(rows) != 1 {
			t.Errorf("expected 1 settings row, got %d", len(rows))
		}
	})

	t.Run("update_settings_by_key", func(t *testing.T) {
		// Update a prod-seeded key (exists in both prod and shadow).
		mustExec(t, db,
			"UPDATE settings SET value = 'updated' WHERE `key` = 'site.name'")
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
		rows := mustQuery(t, db, "SELECT * FROM products WHERE slug = 'edge-uuid-product'")
		if len(rows) != 1 {
			t.Errorf("expected 1 product, got %d", len(rows))
		}
	})

	t.Run("update_uuid_product", func(t *testing.T) {
		// Update the shadow-inserted product. Write goes to shadow which has the row.
		mustExec(t, db,
			"UPDATE products SET price = 29.99 WHERE slug = 'edge-uuid-product'")
	})

	t.Run("delete_uuid_product", func(t *testing.T) {
		mustExec(t, db, "DELETE FROM products WHERE slug = 'edge-uuid-product'")
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
		mustExec(t, db, "INSERT INTO user_roles (user_id, role_id) VALUES (99, 10)")
	})

	t.Run("select_composite_pk_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM user_roles WHERE user_id = 99 AND role_id = 10")
		if len(rows) != 1 {
			t.Errorf("expected 1 user_role row, got %d", len(rows))
		}
	})

	t.Run("delete_composite_pk", func(t *testing.T) {
		mustExec(t, db, "DELETE FROM user_roles WHERE user_id = 99 AND role_id = 10")
	})
}

func TestNullHandling(t *testing.T) {
	db := connect(t)

	t.Run("insert_with_nulls", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_null_user', 'null@test.com', NULL, 1)")
	})

	t.Run("select_where_null", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE username = 'e2e_null_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row for null user, got %d", len(rows))
		}
	})

	t.Run("update_set_to_null", func(t *testing.T) {
		mustExec(t, db, "UPDATE users SET display_name = NULL WHERE id = 3")
	})

	t.Run("update_null_to_value", func(t *testing.T) {
		mustExec(t, db, "UPDATE users SET display_name = 'Restored Name' WHERE id = 3")
	})
}

func TestLargeText(t *testing.T) {
	db := connect(t)

	t.Run("insert_10kb_text", func(t *testing.T) {
		bigText := strings.Repeat("A", 10*1024)
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.bigtext', ?)", bigText)
	})

	t.Run("read_10kb_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT value FROM settings WHERE `key` = 'e2e.bigtext'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		val, ok := rows[0]["value"].(string)
		if !ok {
			val2, ok2 := rows[0]["value"].([]byte)
			if !ok2 {
				t.Fatalf("unexpected type for value: %T", rows[0]["value"])
			}
			val = string(val2)
		}
		if len(val) != 10*1024 {
			t.Errorf("expected 10240 bytes, got %d", len(val))
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
	t.Run("two_connections_independent_reads", func(t *testing.T) {
		db1 := connect(t)
		db2 := connect(t)

		// Both connections read from prod seed data.
		v1 := queryScalar[int64](t, db1, "SELECT COUNT(*) FROM users")
		v2 := queryScalar[int64](t, db2, "SELECT COUNT(*) FROM users")
		if v1 != v2 {
			t.Errorf("concurrent connections see different counts: %d vs %d", v1, v2)
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
					"INSERT INTO settings (`key`, value) VALUES (?, ?)",
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

func TestParameterizedQueries(t *testing.T) {
	db := connect(t)

	t.Run("parameterized_select", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id = ?", 1)
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("parameterized_insert", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES (?, ?)",
			"e2e.param_test", "parameterized_value")
	})

	t.Run("parameterized_update", func(t *testing.T) {
		// Update a prod-seeded key.
		mustExec(t, db,
			"UPDATE settings SET value = ? WHERE `key` = ?",
			"updated_param_value", "site.name")
	})
}

func TestTransactions(t *testing.T) {
	db := connect(t)

	t.Run("commit_transaction", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.tx_commit', 'committed')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in tx: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})

	t.Run("rollback_transaction", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}

		_, err = tx.ExecContext(ctx,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.tx_rollback', 'should_not_exist')")
		if err != nil {
			tx.Rollback()
			t.Fatalf("INSERT in tx: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
	})
}

func TestSpecialCharacters(t *testing.T) {
	db := connect(t)

	t.Run("single_quotes", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_obriens', 'obrien@test.com', ?, 1)", "O'Brien")
	})

	t.Run("unicode", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_unicode', 'unicode@test.com', ?, 1)", "Test: hello world")
	})

	t.Run("newlines_in_text", func(t *testing.T) {
		body := "Line 1\nLine 2\nLine 3"
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.newlines', ?)", body)
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
		mustExec(t, db,
			"INSERT INTO settings (`key`, value) VALUES ('e2e.backtick', 'test')")
	})
}
