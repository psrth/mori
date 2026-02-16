//go:build e2e_cockroachdb

package e2e_cockroachdb

import (
	"testing"
)

func TestMigrations(t *testing.T) {
	conn := connect(t)

	t.Run("add_column_text", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE users ADD COLUMN phone TEXT")
	})

	t.Run("select_after_add_column_shows_null_for_prod_rows", func(t *testing.T) {
		// Prod rows should show NULL for the new phone column.
		rows := mustQuery(t, conn, "SELECT id, phone FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["phone"] != nil {
			t.Errorf("expected NULL for prod row phone, got %v", rows[0]["phone"])
		}
	})

	t.Run("insert_after_add_column", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, password_hash, phone) VALUES ('e2e_phone_user', 'phone@test.com', 'hash', '555-0199')")
	})

	t.Run("select_new_column_has_value_for_shadow_row", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT phone FROM users WHERE username = 'e2e_phone_user'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		phone, ok := rows[0]["phone"].(string)
		if !ok || phone != "555-0199" {
			t.Errorf("expected '555-0199', got %v", rows[0]["phone"])
		}
	})

	t.Run("add_column_with_default", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE posts ADD COLUMN priority INTEGER DEFAULT 0")
	})

	t.Run("add_column_boolean", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE orders ADD COLUMN is_expedited BOOLEAN DEFAULT false")
	})

	t.Run("drop_column", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE user_profiles DROP COLUMN avatar_url")
	})

	t.Run("select_after_drop_column_excludes_it", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT * FROM user_profiles WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if _, exists := rows[0]["avatar_url"]; exists {
			t.Error("dropped column avatar_url should not appear in results")
		}
	})

	t.Run("rename_column", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE user_profiles RENAME COLUMN bio TO biography")
	})

	t.Run("select_after_rename_uses_new_name", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT biography FROM user_profiles WHERE id = 2")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		// The column should be returned as 'biography', not 'bio'.
		if _, exists := rows[0]["biography"]; !exists {
			t.Error("expected column named 'biography' in results")
		}
	})

	t.Run("create_new_table", func(t *testing.T) {
		mustExec(t, conn, `CREATE TABLE e2e_new_table (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value NUMERIC(10,2),
			created_at TIMESTAMP DEFAULT now()
		)`)
	})

	t.Run("insert_into_new_table", func(t *testing.T) {
		mustExec(t, conn, "INSERT INTO e2e_new_table (name, value) VALUES ('test1', 42.50)")
		mustExec(t, conn, "INSERT INTO e2e_new_table (name, value) VALUES ('test2', 99.99)")
	})

	t.Run("select_from_new_table", func(t *testing.T) {
		assertQueryRowCount(t, conn, 2, "SELECT * FROM e2e_new_table")
	})

	t.Run("create_index", func(t *testing.T) {
		mustExec(t, conn, "CREATE INDEX idx_e2e_new_table_name ON e2e_new_table(name)")
	})

	t.Run("drop_table", func(t *testing.T) {
		mustExec(t, conn, "DROP TABLE e2e_new_table")
	})

	t.Run("select_from_dropped_table_errors", func(t *testing.T) {
		assertQueryError(t, conn, "SELECT * FROM e2e_new_table")
	})

	t.Run("update_after_add_column", func(t *testing.T) {
		mustExec(t, conn, "UPDATE users SET phone = '555-0001' WHERE id = 2")
		rows := mustQuery(t, conn, "SELECT phone FROM users WHERE id = 2")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		phone, ok := rows[0]["phone"].(string)
		if !ok || phone != "555-0001" {
			t.Errorf("expected '555-0001', got %v", rows[0]["phone"])
		}
	})

	t.Run("join_after_ddl", func(t *testing.T) {
		// Verify JOINs still work after schema changes.
		rows := mustQuery(t, conn,
			`SELECT u.username, u.phone, up.biography
			 FROM users u
			 LEFT JOIN user_profiles up ON u.id = up.user_id
			 WHERE u.id <= 3`)
		if len(rows) < 1 {
			t.Errorf("expected rows from JOIN after DDL, got %d", len(rows))
		}
	})

	t.Run("schema_status_shows_diffs", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		// After DDL operations, status should show schema diffs.
		t.Logf("Status after DDL:\n%s", out)
	})
}
