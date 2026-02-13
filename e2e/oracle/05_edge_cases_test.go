//go:build e2e_oracle

package e2e_oracle

import (
	"database/sql"
	"strings"
	"sync"
	"testing"
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
		mustExec(t, db, `INSERT INTO settings ("KEY", value) VALUES ('e2e.edge', 'test')`)
	})

	t.Run("select_settings_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, `SELECT * FROM settings WHERE "KEY" = 'e2e.edge'`)
		if len(rows) < 1 {
			t.Errorf("expected to find inserted setting, got %d rows", len(rows))
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
		// Insert a role assignment that may not exist yet.
		mustExec(t, db, "INSERT INTO user_roles (user_id, role_id) VALUES (99, 9)")
	})

	t.Run("select_composite_pk_after_insert", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM user_roles WHERE user_id = 99 AND role_id = 9")
		if len(rows) < 1 {
			t.Errorf("expected inserted user_role row, got %d", len(rows))
		}
	})
}

func TestNullHandling(t *testing.T) {
	db := connect(t)

	t.Run("insert_with_null", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name) VALUES ('e2e_null_user', 'null@test.com', NULL)")
	})

	t.Run("select_where_null", func(t *testing.T) {
		rows := mustQuery(t, db,
			"SELECT * FROM users WHERE display_name IS NULL AND username = 'e2e_null_user'")
		if len(rows) != 1 {
			t.Errorf("expected 1 null-display-name user, got %d", len(rows))
		}
	})

	t.Run("update_to_null", func(t *testing.T) {
		mustExec(t, db, "UPDATE users SET display_name = NULL WHERE id = 3")
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["DISPLAY_NAME"] != nil {
			t.Errorf("expected NULL, got %v", rows[0]["DISPLAY_NAME"])
		}
	})

	t.Run("update_from_null", func(t *testing.T) {
		mustExec(t, db, "UPDATE users SET display_name = 'Restored Name' WHERE id = 3")
		rows := mustQuery(t, db, "SELECT display_name FROM users WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["DISPLAY_NAME"].(string)
		if !ok || name != "Restored Name" {
			t.Errorf("expected 'Restored Name', got %v", rows[0]["DISPLAY_NAME"])
		}
	})
}

func TestLargeText(t *testing.T) {
	db := connect(t)

	t.Run("insert_10kb_varchar2", func(t *testing.T) {
		bigText := strings.Repeat("A", 3900) // VARCHAR2(4000) limit in Oracle
		mustExec(t, db,
			`INSERT INTO settings ("KEY", value) VALUES ('e2e.bigtext', :1)`, bigText)
	})

	t.Run("read_large_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, db, `SELECT value FROM settings WHERE "KEY" = 'e2e.bigtext'`)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		val, ok := rows[0]["VALUE"].(string)
		if !ok || len(val) != 3900 {
			t.Errorf("expected 3900-char value, got %d chars", len(val))
		}
	})
}

func TestConcurrentConnections(t *testing.T) {
	t.Run("two_connections_see_each_others_inserts", func(t *testing.T) {
		db1 := connect(t)
		db2 := connect(t)

		// Insert via db1.
		mustExec(t, db1,
			"INSERT INTO roles (name, description) VALUES ('concurrent-role-1', 'concurrent test')")

		// Should be visible via db2.
		rows := mustQuery(t, db2, "SELECT * FROM roles WHERE name = 'concurrent-role-1'")
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
				db, err := sql.Open("oracle", proxyDSN)
				if err != nil {
					errors[idx] = err
					return
				}
				defer db.Close()
				_, errors[idx] = db.Exec(
					`INSERT INTO settings ("KEY", value) VALUES (:1, :2)`,
					"parallel_"+strings.Repeat("x", idx), "value")
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

func TestEmptyResults(t *testing.T) {
	db := connect(t)

	t.Run("select_impossible_where", func(t *testing.T) {
		assertQueryRowCount(t, db, 0, "SELECT * FROM users WHERE 1 = 0")
	})

	t.Run("select_nonexistent_id", func(t *testing.T) {
		assertQueryRowCount(t, db, 0, "SELECT * FROM users WHERE id = 999999")
	})
}
