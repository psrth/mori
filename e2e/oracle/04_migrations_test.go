//go:build e2e_oracle

package e2e_oracle

import (
	"testing"
)

func TestMigrations(t *testing.T) {
	db := connect(t)

	t.Run("add_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE users ADD (phone VARCHAR2(50))")
	})

	t.Run("select_after_add_column", func(t *testing.T) {
		// Prod rows should show NULL for the new phone column.
		rows := mustQuery(t, db, "SELECT id, phone FROM users WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["PHONE"] != nil {
			t.Errorf("expected NULL for prod row phone, got %v", rows[0]["PHONE"])
		}
	})

	t.Run("insert_with_new_column", func(t *testing.T) {
		mustExec(t, db,
			"INSERT INTO users (username, email, phone) VALUES ('e2e_phone_user', 'phone@test.com', '555-0199')")
		rows := mustQuery(t, db, "SELECT phone FROM users WHERE username = 'e2e_phone_user'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		phone, ok := rows[0]["PHONE"].(string)
		if !ok || phone != "555-0199" {
			t.Errorf("expected '555-0199', got %v", rows[0]["PHONE"])
		}
	})

	t.Run("add_column_with_default", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE orders ADD (is_expedited NUMBER(1) DEFAULT 0)")
	})

	t.Run("drop_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE users DROP COLUMN phone")
	})

	t.Run("create_table", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE e2e_new_table (
			id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name VARCHAR2(255) NOT NULL,
			value NUMBER(10,2),
			created_at TIMESTAMP DEFAULT SYSTIMESTAMP
		)`)
	})

	t.Run("insert_into_new_table", func(t *testing.T) {
		mustExec(t, db, "INSERT INTO e2e_new_table (name, value) VALUES ('test1', 42.50)")
		mustExec(t, db, "INSERT INTO e2e_new_table (name, value) VALUES ('test2', 99.99)")
		assertQueryRowCount(t, db, 2, "SELECT * FROM e2e_new_table")
	})

	t.Run("drop_table", func(t *testing.T) {
		mustExec(t, db, "DROP TABLE e2e_new_table")
		assertQueryError(t, db, "SELECT * FROM e2e_new_table")
	})
}
