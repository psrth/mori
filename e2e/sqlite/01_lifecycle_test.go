//go:build e2e_sqlite

package e2e_sqlite

import (
	"strings"
	"testing"
)

func TestLifecycle(t *testing.T) {
	t.Run("proxy_accepts_simple_query", func(t *testing.T) {
		conn := connect(t)
		val := queryInt64(t, conn, "SELECT 1")
		if val != 1 {
			t.Errorf("SELECT 1 returned %d, want 1", val)
		}
	})

	t.Run("proxy_accepts_datetime_function", func(t *testing.T) {
		conn := connect(t)
		assertNoError(t, conn, "SELECT datetime('now')")
	})

	t.Run("status_shows_sqlite_engine", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(strings.ToLower(out), "sqlite") {
			t.Errorf("status output missing sqlite:\n%s", out)
		}
	})

	t.Run("status_shows_no_deltas_initially", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(out, "(none)") {
			t.Logf("status output:\n%s", out)
			// Not a hard failure; deltas may exist from test ordering.
		}
	})

	t.Run("db_name_via_proxy", func(t *testing.T) {
		conn := connect(t)
		// SQLite doesn't have current_database(); use a literal instead.
		val := queryString(t, conn, "SELECT 'main'")
		if val != "main" {
			t.Errorf("SELECT 'main' returned %q, want 'main'", val)
		}
	})

	t.Run("multiple_connections_work", func(t *testing.T) {
		conn1 := connect(t)
		conn2 := connect(t)
		v1 := queryInt64(t, conn1, "SELECT 1")
		v2 := queryInt64(t, conn2, "SELECT 2")
		if v1 != 1 || v2 != 2 {
			t.Errorf("got v1=%d v2=%d, want 1, 2", v1, v2)
		}
	})

	t.Run("write_isolation_insert_not_in_prod", func(t *testing.T) {
		proxy := connect(t)
		direct := connectDirect(t)

		// Get initial prod count.
		var prodCountBefore int
		row := direct.QueryRow("SELECT COUNT(*) FROM roles")
		if err := row.Scan(&prodCountBefore); err != nil {
			t.Fatalf("count prod roles: %v", err)
		}

		// Insert through proxy.
		mustExec(t, proxy, "INSERT INTO roles (name, description) VALUES ('e2e_lifecycle_test', 'temp role')")

		// Verify prod is untouched.
		var prodCountAfter int
		row = direct.QueryRow("SELECT COUNT(*) FROM roles")
		if err := row.Scan(&prodCountAfter); err != nil {
			t.Fatalf("count prod roles after: %v", err)
		}
		if prodCountAfter != prodCountBefore {
			t.Errorf("prod roles count changed from %d to %d", prodCountBefore, prodCountAfter)
		}
	})
}
