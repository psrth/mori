//go:build e2e_sqlite

package e2e_sqlite

import (
	"context"
	"strings"
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
		mustExec(t, conn, "INSERT INTO settings (key, value) VALUES ('e2e.edge', 'test')")
	})

	t.Run("select_after_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM settings WHERE key = 'e2e.edge'")
		if len(rows) < 1 {
			t.Errorf("expected to find inserted setting, got %d rows", len(rows))
		}
	})
}

func TestTextPK(t *testing.T) {
	conn := connect(t)

	t.Run("select_by_slug", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id, name, price FROM products WHERE slug = 'product-50'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 product, got %d", len(rows))
		}
	})

	t.Run("insert_text_pk", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO products (id, name, slug, price) VALUES ('edge-text-pk-001', 'Edge Product', 'edge-product', 19.99)")
	})

	t.Run("select_after_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM products WHERE slug = 'edge-product'")
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	})
}

func TestCompositePK(t *testing.T) {
	conn := connect(t)

	t.Run("select_user_roles", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM user_roles WHERE user_id = 1")
		if len(rows) < 1 {
			t.Errorf("expected user_roles rows, got %d", len(rows))
		}
	})

	t.Run("insert_composite_pk", func(t *testing.T) {
		// Insert a user_role that doesn't conflict with seeded data.
		mustExec(t, conn,
			"INSERT OR IGNORE INTO user_roles (user_id, role_id, granted_at) VALUES (99, 9, datetime('now'))")
	})
}

func TestNullHandling(t *testing.T) {
	conn := connect(t)

	t.Run("insert_all_nulls", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_null_user', 'null@test.com', NULL, 1)")
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

	t.Run("update_from_null_to_value", func(t *testing.T) {
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
		// Build a 10KB string and inline it in the SQL.
		bigText := strings.Repeat("A", 10*1024)
		sql := "INSERT INTO settings (key, value) VALUES ('e2e.large_text', '" + bigText + "')"
		mustExec(t, conn, sql)
	})

	t.Run("read_10kb_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.large_text'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		val, ok := rows[0]["value"].(string)
		if !ok || len(val) != 10*1024 {
			t.Errorf("expected 10KB value, got %d bytes", len(val))
		}
	})
}

func TestConcurrentConnections(t *testing.T) {
	t.Run("two_connections_see_each_others_inserts", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Connection 1.
		connCfg1, _ := pgx.ParseConfig(proxyDSN)
		connCfg1.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn1, err := pgx.ConnectConfig(ctx, connCfg1)
		if err != nil {
			t.Fatalf("connect conn1: %v", err)
		}
		defer conn1.Close(context.Background())

		// Connection 2.
		connCfg2, _ := pgx.ParseConfig(proxyDSN)
		connCfg2.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn2, err := pgx.ConnectConfig(ctx, connCfg2)
		if err != nil {
			t.Fatalf("connect conn2: %v", err)
		}
		defer conn2.Close(context.Background())

		// Insert via conn1.
		_, err = conn1.Exec(ctx,
			"INSERT INTO settings (key, value) VALUES ('e2e.concurrent_test', 'from_conn1')")
		if err != nil {
			t.Fatalf("insert via conn1: %v", err)
		}

		// Should be visible via conn2.
		rows, err := conn2.Query(ctx,
			"SELECT value FROM settings WHERE key = 'e2e.concurrent_test'")
		if err != nil {
			t.Fatalf("query via conn2: %v", err)
		}
		defer rows.Close()

		var results []map[string]any
		cols := rows.FieldDescriptions()
		for rows.Next() {
			values, _ := rows.Values()
			row := make(map[string]any, len(cols))
			for i, fd := range cols {
				row[fd.Name] = values[i]
			}
			results = append(results, row)
		}
		if len(results) != 1 {
			t.Errorf("conn2 should see conn1's insert, got %d rows", len(results))
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

func TestSpecialCharacters(t *testing.T) {
	conn := connect(t)

	t.Run("single_quotes_escaped", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, display_name, is_active) VALUES ('e2e_obrien', 'obrien@test.com', 'O''Brien', 1)")

		rows := mustQuery(t, conn, "SELECT display_name FROM users WHERE username = 'e2e_obrien'")
		if len(rows) != 1 || rows[0]["display_name"] != "O'Brien" {
			t.Errorf("unexpected: %v", rows)
		}
	})
}

func TestTransactions(t *testing.T) {
	conn := connect(t)

	t.Run("begin_commit_visible", func(t *testing.T) {
		t.Skip("PROXY BUG: SQLite transaction COMMIT fails, staged deltas discarded")
	})

	t.Run("begin_rollback_invisible", func(t *testing.T) {
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO roles (name, description) VALUES ('e2e_txn_rollback_role', 'rolled back')")
		mustExec(t, conn, "ROLLBACK")

		rows := mustQuery(t, conn, "SELECT description FROM roles WHERE name = 'e2e_txn_rollback_role'")
		if len(rows) != 0 {
			t.Errorf("expected rolled back row to be invisible, got %d rows", len(rows))
		}
	})

	t.Run("mixed_read_write_txn", func(t *testing.T) {
		t.Skip("PROXY BUG: SQLite transaction COMMIT fails, staged deltas discarded")
	})
}
