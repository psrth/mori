//go:build e2e_redis

package e2e_redis

import (
	"fmt"
	"testing"
)

func TestBasicGet(t *testing.T) {
	t.Run("get_existing_key_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "GET", "user:1")
		assertStringReply(t, resp, "Alice")
	})

	t.Run("get_nonexistent_key", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "GET", "nonexistent:key")
		assertNullReply(t, resp)
	})

	t.Run("get_multiple_keys_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp1 := redisCmd(t, conn, r, "GET", "user:1")
		resp2 := redisCmd(t, conn, r, "GET", "user:2")
		resp3 := redisCmd(t, conn, r, "GET", "user:3")
		assertStringReply(t, resp1, "Alice")
		assertStringReply(t, resp2, "Bob")
		assertStringReply(t, resp3, "Charlie")
	})
}

func TestBasicSet(t *testing.T) {
	t.Run("set_new_key_returns_ok", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "SET", "e2e:write1", "hello")
		assertOKReply(t, resp)
	})

	t.Run("set_visible_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		// Write through proxy.
		redisCmd(t, conn, r, "SET", "e2e:write2", "world")
		// Read back through proxy (merged read should return shadow value).
		resp := redisCmd(t, conn, r, "GET", "e2e:write2")
		assertStringReply(t, resp, "world")
	})

	t.Run("set_does_not_reach_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:write_guard_test1"
		redisCmd(t, conn, r, "SET", key, "shadow_only")

		// Check prod directly - key should NOT exist.
		if keyExistsDirect(t, key) {
			t.Error("key should not exist in prod after SET through proxy")
		}
	})

	t.Run("set_overwrite_prod_key_in_shadow", func(t *testing.T) {
		conn, r := connectProxy(t)
		// user:1 has "Alice" in prod.
		redisCmd(t, conn, r, "SET", "user:1", "Alice-Modified")

		// Read through proxy should return the shadow value.
		resp := redisCmd(t, conn, r, "GET", "user:1")
		assertStringReply(t, resp, "Alice-Modified")

		// Prod should still have the original value.
		prodVal := getDirectValue(t, "user:1")
		if prodVal != "Alice" {
			t.Errorf("prod value changed to %q, expected 'Alice'", prodVal)
		}
	})
}

func TestBasicDelete(t *testing.T) {
	t.Run("del_returns_count", func(t *testing.T) {
		conn, r := connectProxy(t)
		// Set a key first, then delete it.
		redisCmd(t, conn, r, "SET", "e2e:deltest1", "todelete")
		resp := redisCmd(t, conn, r, "DEL", "e2e:deltest1")
		assertIntReply(t, resp, 1)
	})

	t.Run("del_does_not_affect_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		// Delete a prod key through the proxy.
		redisCmd(t, conn, r, "DEL", "user:2")

		// Prod should still have the key.
		prodVal := getDirectValue(t, "user:2")
		if prodVal != "Bob" {
			t.Errorf("prod value for user:2 changed to %q, expected 'Bob'", prodVal)
		}
	})
}

func TestBasicIncr(t *testing.T) {
	t.Run("incr_existing_counter", func(t *testing.T) {
		conn, r := connectProxy(t)
		// counter:hits starts at 100 in prod.
		resp := redisCmd(t, conn, r, "INCR", "counter:hits")
		if resp.Type != ':' {
			t.Errorf("expected integer reply, got type %c", resp.Type)
		}
	})

	t.Run("incr_does_not_change_prod", func(t *testing.T) {
		// Prod counter should still be 100.
		prodVal := getDirectValue(t, "counter:hits")
		if prodVal != "100" {
			t.Errorf("prod counter changed to %q, expected '100'", prodVal)
		}
	})

	t.Run("incrby_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "INCRBY", "e2e:counter1", "5")
		resp := redisCmd(t, conn, r, "GET", "e2e:counter1")
		assertStringReply(t, resp, "5")
	})
}

func TestExists(t *testing.T) {
	t.Run("exists_prod_key", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "EXISTS", "user:1")
		assertIntReply(t, resp, 1)
	})

	t.Run("exists_nonexistent_key", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "EXISTS", "nonexistent:key:12345")
		assertIntReply(t, resp, 0)
	})
}

func TestMget(t *testing.T) {
	t.Run("mget_prod_keys", func(t *testing.T) {
		conn, r := connectProxy(t)
		// Use user:3, session:abc, session:def which are not modified by earlier tests
		// (user:1 is overwritten in TestBasicSet, user:2 is deleted in TestBasicDelete).
		resp := redisCmd(t, conn, r, "MGET", "user:3", "session:abc", "session:def")
		assertArrayLen(t, resp, 3)
		if resp.Array[0].Str != "Charlie" {
			t.Errorf("MGET[0] = %q, want 'Charlie'", resp.Array[0].Str)
		}
		if resp.Array[1].Str != "user:1" {
			t.Errorf("MGET[1] = %q, want 'user:1'", resp.Array[1].Str)
		}
		if resp.Array[2].Str != "user:2" {
			t.Errorf("MGET[2] = %q, want 'user:2'", resp.Array[2].Str)
		}
	})

	t.Run("mget_with_missing_key", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "MGET", "user:1", "nonexistent:xyz", "user:3")
		assertArrayLen(t, resp, 3)
		if !resp.Array[1].IsNull {
			t.Errorf("expected null for missing key, got %+v", resp.Array[1])
		}
	})
}

func TestCRUDCycle(t *testing.T) {
	t.Run("full_set_get_update_get_del_get", func(t *testing.T) {
		conn, r := connectProxy(t)
		key := "e2e:crud_cycle"

		// SET
		resp := redisCmd(t, conn, r, "SET", key, "initial")
		assertOKReply(t, resp)

		// GET - verify set
		resp = redisCmd(t, conn, r, "GET", key)
		assertStringReply(t, resp, "initial")

		// UPDATE (overwrite)
		resp = redisCmd(t, conn, r, "SET", key, "updated")
		assertOKReply(t, resp)

		// GET - verify update
		resp = redisCmd(t, conn, r, "GET", key)
		assertStringReply(t, resp, "updated")

		// DEL
		resp = redisCmd(t, conn, r, "DEL", key)
		assertIntReply(t, resp, 1)

		// GET - verify deleted
		resp = redisCmd(t, conn, r, "GET", key)
		assertNullReply(t, resp)
	})
}

func TestTTL(t *testing.T) {
	t.Run("set_with_ex", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "SET", "e2e:ttltest", "expiring", "EX", "3600")
		assertOKReply(t, resp)
	})

	t.Run("ttl_returns_positive", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "SET", "e2e:ttltest2", "expiring", "EX", "3600")
		resp := redisCmd(t, conn, r, "TTL", "e2e:ttltest2")
		if resp.Type != ':' || resp.Int <= 0 {
			t.Errorf("expected positive TTL, got type=%c int=%d", resp.Type, resp.Int)
		}
	})
}

func TestAppend(t *testing.T) {
	t.Run("append_to_new_key", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "APPEND", "e2e:appendtest", "hello")
		if resp.Type != ':' {
			t.Errorf("expected integer reply, got type %c", resp.Type)
		}
	})

	t.Run("append_visible_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "SET", "e2e:appendtest2", "hello")
		redisCmd(t, conn, r, "APPEND", "e2e:appendtest2", " world")
		resp := redisCmd(t, conn, r, "GET", "e2e:appendtest2")
		assertStringReply(t, resp, "hello world")
	})
}

func TestMset(t *testing.T) {
	t.Run("mset_multiple_keys", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "MSET", "e2e:mset1", "val1", "e2e:mset2", "val2")
		assertOKReply(t, resp)

		// Verify both keys.
		resp = redisCmd(t, conn, r, "GET", "e2e:mset1")
		assertStringReply(t, resp, "val1")
		resp = redisCmd(t, conn, r, "GET", "e2e:mset2")
		assertStringReply(t, resp, "val2")
	})

	t.Run("mset_does_not_reach_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		k1 := fmt.Sprintf("e2e:mset_guard_%d", 1)
		k2 := fmt.Sprintf("e2e:mset_guard_%d", 2)
		redisCmd(t, conn, r, "MSET", k1, "v1", k2, "v2")

		if keyExistsDirect(t, k1) {
			t.Error("MSET key should not exist in prod")
		}
		if keyExistsDirect(t, k2) {
			t.Error("MSET key should not exist in prod")
		}
	})
}
