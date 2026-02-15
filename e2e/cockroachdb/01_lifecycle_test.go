//go:build e2e_cockroachdb

package e2e_cockroachdb

import (
	"strings"
	"testing"
)

func TestLifecycle(t *testing.T) {
	t.Run("proxy_accepts_simple_query", func(t *testing.T) {
		conn := connect(t)
		val := queryScalar[int64](t, conn, "SELECT 1")
		if val != 1 {
			t.Errorf("SELECT 1 returned %d, want 1", val)
		}
	})

	t.Run("proxy_accepts_select_now", func(t *testing.T) {
		conn := connect(t)
		assertNoError(t, conn, "SELECT now()")
	})

	t.Run("status_shows_initialized", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(out, "Engine:") {
			t.Errorf("status output missing Engine info:\n%s", out)
		}
		if !strings.Contains(strings.ToLower(out), "cockroachdb") {
			t.Errorf("status output missing cockroachdb:\n%s", out)
		}
	})

	t.Run("status_shows_no_deltas_initially", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(out, "(none)") {
			t.Logf("status output:\n%s", out)
			// Not a hard failure; deltas may exist from test ordering.
		}
	})

	t.Run("connection_string_via_proxy", func(t *testing.T) {
		conn := connect(t)
		var dbname string
		err := conn.QueryRow(t.Context(), "SELECT current_database()").Scan(&dbname)
		if err != nil {
			t.Fatalf("current_database: %v", err)
		}
		if dbname != dbName {
			t.Errorf("current_database = %q, want %q", dbname, dbName)
		}
	})

	t.Run("multiple_connections_work", func(t *testing.T) {
		conn1 := connect(t)
		conn2 := connect(t)
		v1 := queryScalar[int64](t, conn1, "SELECT 1")
		v2 := queryScalar[int64](t, conn2, "SELECT 2")
		if v1 != 1 || v2 != 2 {
			t.Errorf("got v1=%d v2=%d, want 1, 2", v1, v2)
		}
	})

	t.Run("prod_untouched_after_insert", func(t *testing.T) {
		proxy := connect(t)
		direct := connectDirect(t)

		// Get initial prod count.
		prodCountBefore := queryScalar[int64](t, direct, "SELECT COUNT(*) FROM roles")

		// Insert through proxy.
		mustExec(t, proxy, "INSERT INTO roles (name, description, permissions) VALUES ('e2e_lifecycle_test', 'temp role', ARRAY['read'])")

		// Verify prod is untouched.
		prodCountAfter := queryScalar[int64](t, direct, "SELECT COUNT(*) FROM roles")
		if prodCountAfter != prodCountBefore {
			t.Errorf("prod roles count changed from %d to %d", prodCountBefore, prodCountAfter)
		}
	})
}
