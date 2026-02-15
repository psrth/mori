//go:build e2e_redis

package e2e_redis

import (
	"testing"
)

func TestWriteGuard(t *testing.T) {
	t.Run("set_never_reaches_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:guard_set"
		redisCmd(t, conn, r, "SET", key, "should_not_be_in_prod")

		if keyExistsDirect(t, key) {
			t.Error("SET through proxy should not create key in prod")
		}
	})

	t.Run("del_never_removes_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		// user:3 is "Charlie" in prod.
		redisCmd(t, conn, r, "DEL", "user:3")

		// Prod should still have it.
		val := getDirectValue(t, "user:3")
		if val != "Charlie" {
			t.Errorf("DEL through proxy removed key from prod: got %q", val)
		}
	})

	t.Run("hset_never_reaches_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:guard_hash"
		redisCmd(t, conn, r, "HSET", key, "f1", "v1")

		if keyExistsDirect(t, key) {
			t.Error("HSET through proxy should not create key in prod")
		}
	})

	t.Run("rpush_never_reaches_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:guard_list"
		redisCmd(t, conn, r, "RPUSH", key, "a", "b", "c")

		if keyExistsDirect(t, key) {
			t.Error("RPUSH through proxy should not create key in prod")
		}
	})

	t.Run("sadd_never_reaches_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:guard_set_type"
		redisCmd(t, conn, r, "SADD", key, "x", "y")

		if keyExistsDirect(t, key) {
			t.Error("SADD through proxy should not create key in prod")
		}
	})

	t.Run("zadd_never_reaches_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:guard_zset"
		redisCmd(t, conn, r, "ZADD", key, "1", "a", "2", "b")

		if keyExistsDirect(t, key) {
			t.Error("ZADD through proxy should not create key in prod")
		}
	})

	t.Run("incr_never_changes_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		// counter:hits is "100" in prod.
		redisCmd(t, conn, r, "INCR", "counter:hits")
		redisCmd(t, conn, r, "INCR", "counter:hits")
		redisCmd(t, conn, r, "INCR", "counter:hits")

		val := getDirectValue(t, "counter:hits")
		if val != "100" {
			t.Errorf("INCR through proxy changed prod counter to %q, expected '100'", val)
		}
	})

	t.Run("mset_never_reaches_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		k1 := "e2e:guard_mset_a"
		k2 := "e2e:guard_mset_b"
		redisCmd(t, conn, r, "MSET", k1, "v1", k2, "v2")

		if keyExistsDirect(t, k1) {
			t.Error("MSET key a should not exist in prod")
		}
		if keyExistsDirect(t, k2) {
			t.Error("MSET key b should not exist in prod")
		}
	})

	t.Run("rename_never_affects_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		// Create a key in shadow first.
		redisCmd(t, conn, r, "SET", "e2e:guard_rename_src", "rval")
		redisCmd(t, conn, r, "RENAME", "e2e:guard_rename_src", "e2e:guard_rename_dst")

		if keyExistsDirect(t, "e2e:guard_rename_dst") {
			t.Error("RENAME dest should not exist in prod")
		}
	})

	t.Run("reads_pass_through_to_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		// Reads should work directly from prod.
		resp := redisCmd(t, conn, r, "GET", "session:abc")
		assertStringReply(t, resp, "user:1")

		resp = redisCmd(t, conn, r, "GET", "session:def")
		assertStringReply(t, resp, "user:2")
	})

	t.Run("prod_data_untouched_after_all_tests", func(t *testing.T) {
		// Verify all seeded prod keys are still intact.
		checks := map[string]string{
			"user:1":       "Alice",
			"user:2":       "Bob",
			"user:3":       "Charlie",
			"session:abc":  "user:1",
			"session:def":  "user:2",
			"counter:hits": "100",
		}
		for key, expected := range checks {
			val := getDirectValue(t, key)
			if val != expected {
				t.Errorf("prod key %q = %q, want %q", key, val, expected)
			}
		}
	})
}

func TestFlushBlocked(t *testing.T) {
	t.Run("flushdb_blocked_or_safe", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "FLUSHDB")
		// FLUSHDB should either be blocked (error) or go to shadow only.
		// Either way, prod should be untouched.
		t.Logf("FLUSHDB response: type=%c str=%q", resp.Type, resp.Str)

		// Verify prod still has data.
		val := getDirectValue(t, "user:1")
		if val != "Alice" {
			t.Errorf("FLUSHDB affected prod: user:1 = %q, want 'Alice'", val)
		}
	})

	t.Run("flushall_blocked_or_safe", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "FLUSHALL")
		t.Logf("FLUSHALL response: type=%c str=%q", resp.Type, resp.Str)

		// Verify prod still has data.
		val := getDirectValue(t, "user:1")
		if val != "Alice" {
			t.Errorf("FLUSHALL affected prod: user:1 = %q, want 'Alice'", val)
		}
	})
}
