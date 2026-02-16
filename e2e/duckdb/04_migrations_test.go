//go:build e2e_duckdb

package e2e_duckdb

import (
	"testing"
)

func TestMigrations(t *testing.T) {
	conn := connect(t)

	t.Run("add_column", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE users ADD COLUMN phone VARCHAR")
	})

	t.Run("select_after_add_column", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT id, phone FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["phone"] != nil {
			t.Errorf("expected NULL for prod row phone, got %v", rows[0]["phone"])
		}
	})

	t.Run("insert_with_new_column", func(t *testing.T) {
		mustExec(t, conn,
			"INSERT INTO users (username, email, display_name, is_active, phone) VALUES ('e2e_phone_user', 'phone@test.com', 'Phone User', 1, '555-0199')")
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
		mustExec(t, conn, "ALTER TABLE orders ADD COLUMN is_expedited INTEGER DEFAULT 0")
	})

	t.Run("add_column_varchar_with_default", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE orders ADD COLUMN priority VARCHAR DEFAULT 'normal'")
	})

	t.Run("create_new_table", func(t *testing.T) {
		mustExec(t, conn, `CREATE TABLE e2e_new_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR NOT NULL,
			value DOUBLE,
			created_at TIMESTAMP DEFAULT current_timestamp
		)`)
	})

	t.Run("insert_into_new_table", func(t *testing.T) {
		mustExec(t, conn, "INSERT INTO e2e_new_table (id, name, value) VALUES (1, 'test1', 42.50)")
		mustExec(t, conn, "INSERT INTO e2e_new_table (id, name, value) VALUES (2, 'test2', 99.99)")
	})

	t.Run("select_from_new_table", func(t *testing.T) {
		assertQueryRowCount(t, conn, 2, "SELECT * FROM e2e_new_table")
	})

	t.Run("update_new_table", func(t *testing.T) {
		mustExec(t, conn, "UPDATE e2e_new_table SET value = 50.00 WHERE id = 1")
		rows := mustQuery(t, conn, "SELECT value FROM e2e_new_table WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("delete_from_new_table", func(t *testing.T) {
		mustExec(t, conn, "DELETE FROM e2e_new_table WHERE id = 2")
		assertQueryRowCount(t, conn, 1, "SELECT * FROM e2e_new_table")
	})

	t.Run("drop_table", func(t *testing.T) {
		mustExec(t, conn, "DROP TABLE e2e_new_table")
	})

	t.Run("select_from_dropped_table_errors", func(t *testing.T) {
		assertQueryError(t, conn, "SELECT * FROM e2e_new_table")
	})

	t.Run("rename_column", func(t *testing.T) {
		mustExec(t, conn, "ALTER TABLE users RENAME COLUMN phone TO phone_number")
	})

	t.Run("select_renamed_column", func(t *testing.T) {
		rows := mustQuery(t, conn, "SELECT phone_number FROM users WHERE username = 'e2e_phone_user'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		phone, ok := rows[0]["phone_number"].(string)
		if !ok || phone != "555-0199" {
			t.Errorf("expected '555-0199', got %v", rows[0]["phone_number"])
		}
	})

	t.Run("create_and_drop_sequence_table", func(t *testing.T) {
		mustExec(t, conn, `CREATE TABLE e2e_temp_table (
			id INTEGER PRIMARY KEY,
			data VARCHAR
		)`)
		mustExec(t, conn, "INSERT INTO e2e_temp_table (id, data) VALUES (1, 'hello')")
		assertQueryRowCount(t, conn, 1, "SELECT * FROM e2e_temp_table")
		mustExec(t, conn, "DROP TABLE e2e_temp_table")
		assertQueryError(t, conn, "SELECT * FROM e2e_temp_table")
	})
}
