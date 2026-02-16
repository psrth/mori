//go:build e2e_mysql

package e2e_mysql

import (
	"testing"
)

func TestMigrations(t *testing.T) {
	db := connect(t)

	t.Run("add_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE users ADD COLUMN phone VARCHAR(50)")
	})

	t.Run("select_after_add_column_shows_null", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		// The phone column should exist (added via ALTER TABLE) but be NULL for prod rows.
	})

	t.Run("insert_with_new_column", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active, phone) VALUES ('e2e_phone_user', 'phone@test.com', 'Phone User', 1, '555-0199')")
	})

	t.Run("select_new_column_has_value", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE username = 'e2e_phone_user'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("add_column_with_default", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE orders ADD COLUMN is_expedited TINYINT(1) DEFAULT 0")
	})

	t.Run("drop_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE users DROP COLUMN phone")
	})

	t.Run("select_after_drop_column_excludes_it", func(t *testing.T) {
		// Reads go to prod in v1. Prod never had the 'phone' column
		// (it was only added/dropped on shadow), so this passes correctly.
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if _, exists := rows[0]["phone"]; exists {
			t.Error("phone column should not appear in prod results")
		}
	})

	t.Run("rename_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE orders RENAME COLUMN is_expedited TO is_rush")
	})

	t.Run("select_after_rename_column", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM orders WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("create_table", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE e2e_new_table (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value DECIMAL(10,2),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`)
	})

	t.Run("insert_into_new_table", func(t *testing.T) {
		mustExec(t, db, "INSERT INTO e2e_new_table (name, value) VALUES ('test1', 42.50)")
		mustExec(t, db, "INSERT INTO e2e_new_table (name, value) VALUES ('test2', 99.99)")
	})

	t.Run("select_from_new_table", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM e2e_new_table")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows in new table, got %d", len(rows))
		}
	})

	t.Run("drop_table", func(t *testing.T) {
		mustExec(t, db, "DROP TABLE e2e_new_table")
	})

	t.Run("select_from_dropped_table_errors", func(t *testing.T) {
		// Reads go to prod in v1. Prod never had e2e_new_table (only shadow did),
		// so this query errors as expected.
		assertQueryError(t, db, "SELECT * FROM e2e_new_table")
	})
}
