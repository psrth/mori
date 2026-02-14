//go:build e2e_redis

package e2e_redis

import (
	"testing"
)

func TestHash(t *testing.T) {
	t.Run("hget_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "HGET", "profile:1", "name")
		assertStringReply(t, resp, "Alice")
	})

	t.Run("hgetall_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "HGETALL", "profile:1")
		// HGETALL returns flat array: [field1, val1, field2, val2, ...]
		if resp.Type != '*' {
			t.Fatalf("expected array reply, got type %c", resp.Type)
		}
		if len(resp.Array) < 4 {
			t.Errorf("expected at least 4 elements (2 fields), got %d", len(resp.Array))
		}
	})

	t.Run("hset_goes_to_shadow", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "HSET", "e2e:hash1", "field1", "value1", "field2", "value2")
		if resp.Type != ':' {
			t.Errorf("expected integer reply for HSET, got type %c", resp.Type)
		}

		// Read back.
		resp = redisCmd(t, conn, r, "HGET", "e2e:hash1", "field1")
		assertStringReply(t, resp, "value1")

		// Should not exist in prod.
		if keyExistsDirect(t, "e2e:hash1") {
			t.Error("HSET key should not exist in prod")
		}
	})

	t.Run("hdel_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "HSET", "e2e:hash_del", "f1", "v1", "f2", "v2")
		resp := redisCmd(t, conn, r, "HDEL", "e2e:hash_del", "f1")
		assertIntReply(t, resp, 1)
	})

	t.Run("hexists_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "HEXISTS", "profile:1", "name")
		assertIntReply(t, resp, 1)
	})

	t.Run("hlen_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "HLEN", "profile:1")
		if resp.Type != ':' || resp.Int < 2 {
			t.Errorf("expected HLEN >= 2, got type=%c int=%d", resp.Type, resp.Int)
		}
	})

	t.Run("hmset_does_not_reach_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "HSET", "e2e:hmset1", "a", "1", "b", "2", "c", "3")
		if keyExistsDirect(t, "e2e:hmset1") {
			t.Error("HSET (multi) key should not exist in prod")
		}
	})
}

func TestList(t *testing.T) {
	t.Run("lrange_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "LRANGE", "queue:tasks", "0", "-1")
		assertArrayLen(t, resp, 3)
		if resp.Array[0].Str != "task1" {
			t.Errorf("LRANGE[0] = %q, want 'task1'", resp.Array[0].Str)
		}
	})

	t.Run("llen_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "LLEN", "queue:tasks")
		assertIntReply(t, resp, 3)
	})

	t.Run("rpush_goes_to_shadow", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "RPUSH", "e2e:list1", "a", "b", "c")
		if resp.Type != ':' {
			t.Errorf("expected integer reply for RPUSH, got type %c", resp.Type)
		}

		// Read back.
		resp = redisCmd(t, conn, r, "LRANGE", "e2e:list1", "0", "-1")
		assertArrayLen(t, resp, 3)

		// Not in prod.
		if keyExistsDirect(t, "e2e:list1") {
			t.Error("RPUSH key should not exist in prod")
		}
	})

	t.Run("lpush_goes_to_shadow", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "LPUSH", "e2e:list2", "x", "y")
		resp := redisCmd(t, conn, r, "LLEN", "e2e:list2")
		assertIntReply(t, resp, 2)
	})

	t.Run("lpop_rpop_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "RPUSH", "e2e:list_pop", "1", "2", "3")
		resp := redisCmd(t, conn, r, "LPOP", "e2e:list_pop")
		assertStringReply(t, resp, "1")
		resp = redisCmd(t, conn, r, "RPOP", "e2e:list_pop")
		assertStringReply(t, resp, "3")
	})

	t.Run("lindex_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "LINDEX", "queue:tasks", "0")
		assertStringReply(t, resp, "task1")
	})
}

func TestSet(t *testing.T) {
	t.Run("smembers_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "SMEMBERS", "tags:post1")
		assertArrayLen(t, resp, 3)
	})

	t.Run("scard_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "SCARD", "tags:post1")
		assertIntReply(t, resp, 3)
	})

	t.Run("sismember_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "SISMEMBER", "tags:post1", "redis")
		assertIntReply(t, resp, 1)
		resp = redisCmd(t, conn, r, "SISMEMBER", "tags:post1", "nonexistent")
		assertIntReply(t, resp, 0)
	})

	t.Run("sadd_goes_to_shadow", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "SADD", "e2e:set1", "a", "b", "c")
		if resp.Type != ':' {
			t.Errorf("expected integer reply for SADD, got type %c", resp.Type)
		}

		resp = redisCmd(t, conn, r, "SCARD", "e2e:set1")
		assertIntReply(t, resp, 3)

		if keyExistsDirect(t, "e2e:set1") {
			t.Error("SADD key should not exist in prod")
		}
	})

	t.Run("srem_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "SADD", "e2e:set_rem", "x", "y", "z")
		resp := redisCmd(t, conn, r, "SREM", "e2e:set_rem", "x")
		assertIntReply(t, resp, 1)
		resp = redisCmd(t, conn, r, "SCARD", "e2e:set_rem")
		assertIntReply(t, resp, 2)
	})
}

func TestSortedSet(t *testing.T) {
	t.Run("zrange_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ZRANGE", "leaderboard:game1", "0", "-1")
		assertArrayLen(t, resp, 3)
		// Sorted by score ascending: player1(100), player3(150), player2(200).
		if resp.Array[0].Str != "player1" {
			t.Errorf("ZRANGE[0] = %q, want 'player1'", resp.Array[0].Str)
		}
	})

	t.Run("zcard_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ZCARD", "leaderboard:game1")
		assertIntReply(t, resp, 3)
	})

	t.Run("zscore_from_prod", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ZSCORE", "leaderboard:game1", "player2")
		// ZSCORE returns a bulk string "200".
		assertStringReply(t, resp, "200")
	})

	t.Run("zadd_goes_to_shadow", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ZADD", "e2e:zset1", "10", "a", "20", "b", "30", "c")
		if resp.Type != ':' {
			t.Errorf("expected integer reply for ZADD, got type %c", resp.Type)
		}

		resp = redisCmd(t, conn, r, "ZCARD", "e2e:zset1")
		assertIntReply(t, resp, 3)

		if keyExistsDirect(t, "e2e:zset1") {
			t.Error("ZADD key should not exist in prod")
		}
	})

	t.Run("zrem_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		redisCmd(t, conn, r, "ZADD", "e2e:zset_rem", "1", "x", "2", "y", "3", "z")
		resp := redisCmd(t, conn, r, "ZREM", "e2e:zset_rem", "x")
		assertIntReply(t, resp, 1)
		resp = redisCmd(t, conn, r, "ZCARD", "e2e:zset_rem")
		assertIntReply(t, resp, 2)
	})

	t.Run("zrangebyscore_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ZRANGEBYSCORE", "leaderboard:game1", "100", "200")
		if resp.Type != '*' {
			t.Errorf("expected array reply, got type %c", resp.Type)
		}
		if len(resp.Array) != 3 {
			t.Errorf("expected 3 members in score range, got %d", len(resp.Array))
		}
	})

	t.Run("zrank_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ZRANK", "leaderboard:game1", "player1")
		assertIntReply(t, resp, 0) // player1 has lowest score, rank 0.
	})
}

func TestType(t *testing.T) {
	t.Run("type_string", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "TYPE", "user:1")
		assertStringReply(t, resp, "string")
	})

	t.Run("type_hash", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "TYPE", "profile:1")
		assertStringReply(t, resp, "hash")
	})

	t.Run("type_list", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "TYPE", "queue:tasks")
		assertStringReply(t, resp, "list")
	})

	t.Run("type_set", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "TYPE", "tags:post1")
		assertStringReply(t, resp, "set")
	})

	t.Run("type_zset", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "TYPE", "leaderboard:game1")
		assertStringReply(t, resp, "zset")
	})

	t.Run("type_nonexistent", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "TYPE", "nonexistent:abc")
		assertStringReply(t, resp, "none")
	})
}
