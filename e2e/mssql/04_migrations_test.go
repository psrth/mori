//go:build e2e_mssql

package e2e_mssql

import (
	"testing"
)

func TestMigrations(t *testing.T) {
	db := connect(t)

	t.Run("add_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE users ADD phone NVARCHAR(50)")
	})

	t.Run("select_after_add_column_shows_null", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT id, phone FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["phone"] != nil {
			t.Errorf("expected NULL for prod row phone, got %v", rows[0]["phone"])
		}
	})

	t.Run("insert_with_new_column", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, display_name, is_active, phone) VALUES ('e2e_phone_user', 'phone@test.com', 'Phone User', 1, '555-0199')")
	})

	t.Run("select_new_column_has_value", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT phone FROM users WHERE username = 'e2e_phone_user'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		phone, ok := rows[0]["phone"].(string)
		if !ok || phone != "555-0199" {
			t.Errorf("expected '555-0199', got %v", rows[0]["phone"])
		}
	})

	t.Run("add_column_with_default", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE orders ADD is_expedited BIT DEFAULT 0")
	})

	t.Run("drop_column", func(t *testing.T) {
		// Drop the phone column we just added.
		mustExec(t, db, "ALTER TABLE users DROP COLUMN phone")
	})

	t.Run("select_after_drop_column_excludes_it", func(t *testing.T) {
		rows := mustQuery(t, db, "SELECT * FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if _, exists := rows[0]["phone"]; exists {
			t.Error("dropped column phone should not appear in results")
		}
	})

	t.Run("create_table", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE e2e_new_table (
			id INT IDENTITY(1,1) PRIMARY KEY,
			name NVARCHAR(255) NOT NULL,
			value DECIMAL(10,2),
			created_at DATETIME2 DEFAULT GETDATE()
		)`)
	})

	t.Run("insert_into_new_table", func(t *testing.T) {
		mustExec(t, db, "INSERT INTO e2e_new_table (name, value) VALUES ('test1', 42.50)")
		mustExec(t, db, "INSERT INTO e2e_new_table (name, value) VALUES ('test2', 99.99)")
	})

	t.Run("select_from_new_table", func(t *testing.T) {
		assertQueryRowCount(t, db, 2, "SELECT * FROM e2e_new_table")
	})

	t.Run("drop_table", func(t *testing.T) {
		mustExec(t, db, "DROP TABLE e2e_new_table")
	})

	t.Run("select_from_dropped_table_errors", func(t *testing.T) {
		assertQueryError(t, db, "SELECT * FROM e2e_new_table")
	})
}
