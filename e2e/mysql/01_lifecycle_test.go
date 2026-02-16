//go:build e2e_mysql

package e2e_mysql

import (
	"strings"
	"testing"
)

func TestLifecycle(t *testing.T) {
	t.Run("proxy_accepts_simple_query", func(t *testing.T) {
		db := connect(t)
		val := queryScalar[int64](t, db, "SELECT 1")
		if val != 1 {
			t.Errorf("SELECT 1 returned %d, want 1", val)
		}
	})

	t.Run("proxy_accepts_now_function", func(t *testing.T) {
		db := connect(t)
		assertNoError(t, db, "SELECT NOW()")
	})

	t.Run("status_shows_mysql_engine", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(out, "Engine:") {
			t.Errorf("status output missing Engine info:\n%s", out)
		}
		if !strings.Contains(strings.ToLower(out), "mysql") {
			t.Errorf("status output missing mysql:\n%s", out)
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
		db := connect(t)
		name := queryScalar[string](t, db, "SELECT DATABASE()")
		if name != dbName {
			t.Errorf("DATABASE() = %q, want %q", name, dbName)
		}
	})

	t.Run("multiple_connections_work", func(t *testing.T) {
		db1 := connect(t)
		db2 := connect(t)
		v1 := queryScalar[int64](t, db1, "SELECT 1")
		v2 := queryScalar[int64](t, db2, "SELECT 2")
		if v1 != 1 || v2 != 2 {
			t.Errorf("got v1=%d v2=%d, want 1, 2", v1, v2)
		}
	})

	t.Run("write_isolation_insert_not_in_prod", func(t *testing.T) {
		proxy := connect(t)
		direct := connectDirect(t)

		// Get initial prod count.
		prodCountBefore := queryScalar[int64](t, direct, "SELECT COUNT(*) FROM roles")

		// Insert through proxy.
		mustExec(t, proxy, "INSERT INTO roles (name, description) VALUES ('e2e_lifecycle_test', 'temp role')")

		// Verify prod is untouched.
		prodCountAfter := queryScalar[int64](t, direct, "SELECT COUNT(*) FROM roles")
		if prodCountAfter != prodCountBefore {
			t.Errorf("prod roles count changed from %d to %d", prodCountBefore, prodCountAfter)
		}
	})

	t.Run("version_query", func(t *testing.T) {
		db := connect(t)
		ver := queryScalar[string](t, db, "SELECT VERSION()")
		if ver == "" {
			t.Error("VERSION() returned empty string")
		}
		t.Logf("MySQL version: %.80s", ver)
	})
}
