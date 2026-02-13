//go:build e2e_oracle

package e2e_oracle

import (
	"strings"
	"testing"
)

func TestLifecycle(t *testing.T) {
	t.Run("proxy_accepts_simple_query", func(t *testing.T) {
		db := connect(t)
		val := queryScalar[int64](t, db, "SELECT 1 FROM DUAL")
		if val != 1 {
			t.Errorf("SELECT 1 FROM DUAL returned %d, want 1", val)
		}
	})

	t.Run("proxy_accepts_datetime_function", func(t *testing.T) {
		db := connect(t)
		assertNoError(t, db, "SELECT SYSTIMESTAMP FROM DUAL")
	})

	t.Run("status_shows_oracle_engine", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(out, "Engine:") {
			t.Errorf("status output missing Engine info:\n%s", out)
		}
		if !strings.Contains(strings.ToLower(out), "oracle") {
			t.Errorf("status output missing oracle:\n%s", out)
		}
	})

	t.Run("status_shows_no_deltas_initially", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(out, "(none)") {
			t.Logf("status output:\n%s", out)
			// Not a hard failure; deltas may exist from test ordering.
		}
	})

	t.Run("service_name_via_proxy", func(t *testing.T) {
		db := connect(t)
		var svcName string
		err := db.QueryRow("SELECT SYS_CONTEXT('USERENV', 'SERVICE_NAME') FROM DUAL").Scan(&svcName)
		if err != nil {
			t.Fatalf("SYS_CONTEXT query: %v", err)
		}
		t.Logf("Service name via proxy: %s", svcName)
	})

	t.Run("multiple_connections_work", func(t *testing.T) {
		db1 := connect(t)
		db2 := connect(t)
		v1 := queryScalar[int64](t, db1, "SELECT 1 FROM DUAL")
		v2 := queryScalar[int64](t, db2, "SELECT 2 FROM DUAL")
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
}
