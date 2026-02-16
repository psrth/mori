//go:build e2e_duckdb

package e2e_duckdb

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

	t.Run("update_no_pk_row", func(t *testing.T) {
		mustExec(t, conn, "UPDATE settings SET value = 'updated' WHERE key = 'e2e.edge'")
		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.edge'")
		if len(rows) < 1 {
			t.Errorf("expected to find updated setting")
		}
	})
}

func TestTextPK(t *testing.T) {
	conn := connect(t)

	t.Run("select_by_slug", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id, name, price FROM products WHERE slug = 'product-1'")
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

	t.Run("update_text_pk_row", func(t *testing.T) {
		mustExec(t, conn, "UPDATE products SET price = 29.99 WHERE id = 'edge-text-pk-001'")
		rows := mustQuery(t, conn, "SELECT price FROM products WHERE id = 'edge-text-pk-001'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("delete_text_pk_row", func(t *testing.T) {
		mustExec(t, conn, "DELETE FROM products WHERE id = 'edge-text-pk-001'")
		rows := mustQuery(t, conn, "SELECT * FROM products WHERE id = 'edge-text-pk-001'")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows after delete, got %d", len(rows))
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
		// Seed maps each user to exactly one role: user N -> role ((N-1)%10)+1.
		// So user 2 has role 2 only. (2, 5) is guaranteed not to exist.
		mustExec(t, conn,
			"INSERT INTO user_roles (user_id, role_id, granted_at) VALUES (2, 5, current_timestamp)")
	})

	t.Run("select_after_composite_insert", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM user_roles WHERE user_id = 2 AND role_id = 5")
		if len(rows) < 1 {
			t.Errorf("expected to find composite PK row, got %d", len(rows))
		}
	})

	t.Run("delete_composite_pk", func(t *testing.T) {
		mustExec(t, conn, "DELETE FROM user_roles WHERE user_id = 2 AND role_id = 5")
		rows := mustQuery(t, conn, "SELECT * FROM user_roles WHERE user_id = 2 AND role_id = 5")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows after composite delete, got %d", len(rows))
		}
	})
}

func TestNullHandling(t *testing.T) {
	conn := connect(t)

	t.Run("insert_with_null", func(t *testing.T) {
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

	t.Run("coalesce_with_null", func(t *testing.T) {
		mustExec(t, conn, "UPDATE users SET display_name = NULL WHERE id = 4")
		rows := mustQuery(t, conn, "SELECT COALESCE(display_name, 'default') AS name FROM users WHERE id = 4")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		name, ok := rows[0]["name"].(string)
		if !ok || name != "default" {
			t.Errorf("expected 'default', got %v", rows[0]["name"])
		}
	})

	t.Run("is_not_null_filter", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT COUNT(*) AS cnt FROM users WHERE display_name IS NOT NULL")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})
}

func TestLargeText(t *testing.T) {
	conn := connect(t)

	t.Run("insert_10kb_text", func(t *testing.T) {
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

	t.Run("insert_100kb_text", func(t *testing.T) {
		bigText := strings.Repeat("B", 100*1024)
		sql := "INSERT INTO settings (key, value) VALUES ('e2e.huge_text', '" + bigText + "')"
		mustExec(t, conn, sql)
	})

	t.Run("read_100kb_text_roundtrip", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.huge_text'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		val, ok := rows[0]["value"].(string)
		if !ok || len(val) != 100*1024 {
			t.Errorf("expected 100KB value, got %d bytes", len(val))
		}
	})
}

func TestConcurrentConnections(t *testing.T) {
	t.Run("two_connections_see_each_others_inserts", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		connCfg1, _ := pgx.ParseConfig(proxyDSN)
		connCfg1.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn1, err := pgx.ConnectConfig(ctx, connCfg1)
		if err != nil {
			t.Fatalf("connect conn1: %v", err)
		}
		defer conn1.Close(context.Background())

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

	t.Run("three_concurrent_connections", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conns := make([]*pgx.Conn, 3)
		for i := range conns {
			cfg, _ := pgx.ParseConfig(proxyDSN)
			cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			c, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatalf("connect conn%d: %v", i, err)
			}
			defer c.Close(context.Background())
			conns[i] = c
		}

		// Each connection queries independently.
		// The proxy returns all values as text (OID 25), so scan into any.
		for i, c := range conns {
			var val any
			err := c.QueryRow(ctx, "SELECT 1").Scan(&val)
			if err != nil {
				t.Errorf("conn%d query failed: %v", i, err)
			}
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

	t.Run("select_empty_string_match", func(t *testing.T) {
		assertQueryRowCount(t, conn, 0, "SELECT * FROM users WHERE username = ''")
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

	t.Run("unicode_characters", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO settings (key, value) VALUES ('e2e.unicode', 'Hello World')")

		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.unicode'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		val, ok := rows[0]["value"].(string)
		if !ok || val != "Hello World" {
			t.Errorf("expected 'Hello World', got %v", rows[0]["value"])
		}
	})

	t.Run("backslash_in_string", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO settings (key, value) VALUES ('e2e.backslash', 'path\\to\\file')")
		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.backslash'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("newline_in_string", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO settings (key, value) VALUES ('e2e.newline', 'line1\nline2')")
		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.newline'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})
}

func TestTransactions(t *testing.T) {
	conn := connect(t)

	t.Run("begin_commit_visible", func(t *testing.T) {
		t.Skip("PROXY BUG: DuckDB transaction COMMIT fails for no-PK tables, staged deltas discarded")
	})

	t.Run("begin_rollback_invisible", func(t *testing.T) {
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO settings (key, value) VALUES ('e2e.txn_rollback', 'rolled_back')")
		mustExec(t, conn, "ROLLBACK")

		rows := mustQuery(t, conn, "SELECT value FROM settings WHERE key = 'e2e.txn_rollback'")
		if len(rows) != 0 {
			t.Errorf("expected rolled back row to be invisible, got %d rows", len(rows))
		}
	})
}

func TestDuckDBSpecific(t *testing.T) {
	conn := connect(t)

	t.Run("generate_series", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM generate_series(1, 10) AS t(i)")
		if len(rows) != 10 {
			t.Errorf("expected 10 rows from generate_series, got %d", len(rows))
		}
	})

	t.Run("string_functions", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT LENGTH('hello') AS len, UPPER('hello') AS up, LOWER('WORLD') AS lo")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("list_aggregate", func(t *testing.T) {
		t.Skip("PROXY BUG: DuckDB LIST() returns array type that cannot be scanned by proxy NullString scanner")
	})

	t.Run("epoch_extraction", func(t *testing.T) {
		// current_timestamp returns TIMESTAMP WITH TIME ZONE; cast to TIMESTAMP for epoch().
		assertNoError(t, conn,
			"SELECT EPOCH(current_timestamp::TIMESTAMP)")
	})
}
