package classify

import (
	"testing"

	"github.com/mori-dev/mori/internal/core"
)

func TestClassify(t *testing.T) {
	c := New(nil)

	tests := []struct {
		name       string
		query      string
		wantOp     core.OpType
		wantSub    core.SubType
		wantTables []string
	}{
		// Reads
		{"GET", "GET user:1", core.OpRead, core.SubSelect, []string{"user"}},
		{"MGET", "MGET user:1 user:2 session:abc", core.OpRead, core.SubSelect, []string{"user", "session"}},
		{"HGET", "HGET user:1 name", core.OpRead, core.SubSelect, []string{"user"}},
		{"HGETALL", "HGETALL user:1", core.OpRead, core.SubSelect, []string{"user"}},
		{"LRANGE", "LRANGE mylist 0 -1", core.OpRead, core.SubSelect, []string{"mylist"}},
		{"SMEMBERS", "SMEMBERS myset", core.OpRead, core.SubSelect, []string{"myset"}},
		{"ZRANGE", "ZRANGE leaderboard 0 10", core.OpRead, core.SubSelect, []string{"leaderboard"}},
		{"EXISTS", "EXISTS user:1 user:2", core.OpRead, core.SubSelect, []string{"user"}},
		{"TTL", "TTL session:abc", core.OpRead, core.SubSelect, []string{"session"}},
		{"TYPE", "TYPE mykey", core.OpRead, core.SubSelect, []string{"mykey"}},
		{"KEYS no key pos", "KEYS *", core.OpRead, core.SubSelect, nil},
		{"SCAN", "SCAN 0 MATCH user:* COUNT 100", core.OpRead, core.SubSelect, nil},
		{"DBSIZE", "DBSIZE", core.OpRead, core.SubSelect, nil},

		// Writes (insert — new keys)
		{"SET", "SET user:1 hello", core.OpWrite, core.SubInsert, []string{"user"}},
		{"SETNX", "SETNX lock:task1 1", core.OpWrite, core.SubInsert, []string{"lock"}},
		{"MSET", "MSET user:1 val1 session:2 val2", core.OpWrite, core.SubInsert, []string{"user", "session"}},

		// Writes (update — hydrate before write)
		{"INCR", "INCR counter:hits", core.OpWrite, core.SubUpdate, []string{"counter"}},
		{"DECR", "DECR counter:hits", core.OpWrite, core.SubUpdate, []string{"counter"}},
		{"INCRBY", "INCRBY counter:hits 5", core.OpWrite, core.SubUpdate, []string{"counter"}},
		{"DECRBY", "DECRBY counter:hits 3", core.OpWrite, core.SubUpdate, []string{"counter"}},
		{"INCRBYFLOAT", "INCRBYFLOAT price:item 1.5", core.OpWrite, core.SubUpdate, []string{"price"}},
		{"APPEND", "APPEND log:entry data", core.OpWrite, core.SubUpdate, []string{"log"}},
		{"SETRANGE", "SETRANGE buf:1 5 hello", core.OpWrite, core.SubUpdate, []string{"buf"}},
		{"GETSET", "GETSET key:1 newval", core.OpWrite, core.SubUpdate, []string{"key"}},
		{"HSET", "HSET user:1 name Alice", core.OpWrite, core.SubUpdate, []string{"user"}},
		{"HSETNX", "HSETNX user:1 email a@b.c", core.OpWrite, core.SubUpdate, []string{"user"}},
		{"HMSET", "HMSET user:1 name Alice age 30", core.OpWrite, core.SubUpdate, []string{"user"}},
		{"HINCRBY", "HINCRBY user:1 age 1", core.OpWrite, core.SubUpdate, []string{"user"}},
		{"HINCRBYFLOAT", "HINCRBYFLOAT user:1 score 1.5", core.OpWrite, core.SubUpdate, []string{"user"}},
		{"LPUSH", "LPUSH queue:tasks task1", core.OpWrite, core.SubUpdate, []string{"queue"}},
		{"RPUSH", "RPUSH queue:tasks task2", core.OpWrite, core.SubUpdate, []string{"queue"}},
		{"LPUSHX", "LPUSHX queue:tasks task1", core.OpWrite, core.SubUpdate, []string{"queue"}},
		{"RPUSHX", "RPUSHX queue:tasks task2", core.OpWrite, core.SubUpdate, []string{"queue"}},
		{"LSET", "LSET queue:tasks 0 newtask", core.OpWrite, core.SubUpdate, []string{"queue"}},
		{"LINSERT", "LINSERT queue:tasks BEFORE task1 task0", core.OpWrite, core.SubUpdate, []string{"queue"}},
		{"SADD", "SADD tags:post1 redis", core.OpWrite, core.SubUpdate, []string{"tags"}},
		{"ZADD", "ZADD leaderboard 100 player1", core.OpWrite, core.SubUpdate, []string{"leaderboard"}},
		{"ZINCRBY", "ZINCRBY leaderboard 5 player1", core.OpWrite, core.SubUpdate, []string{"leaderboard"}},
		{"XADD", "XADD stream:events * key val", core.OpWrite, core.SubUpdate, []string{"stream"}},
		{"RENAME", "RENAME old:key new:key", core.OpWrite, core.SubUpdate, []string{"old", "new"}},
		{"EXPIRE", "EXPIRE session:abc 3600", core.OpWrite, core.SubUpdate, []string{"session"}},
		{"PERSIST", "PERSIST session:abc", core.OpWrite, core.SubUpdate, []string{"session"}},

		// Writes (delete — tombstone tracking)
		{"DEL single", "DEL user:1", core.OpWrite, core.SubDelete, []string{"user"}},
		{"DEL multi", "DEL user:1 session:abc", core.OpWrite, core.SubDelete, []string{"user", "session"}},
		{"UNLINK", "UNLINK temp:key1", core.OpWrite, core.SubDelete, []string{"temp"}},
		{"HDEL", "HDEL user:1 field1", core.OpWrite, core.SubDelete, []string{"user"}},
		{"SREM", "SREM myset member1", core.OpWrite, core.SubDelete, []string{"myset"}},
		{"SPOP", "SPOP myset", core.OpWrite, core.SubDelete, []string{"myset"}},
		{"ZREM", "ZREM leaderboard player1", core.OpWrite, core.SubDelete, []string{"leaderboard"}},
		{"ZPOPMIN", "ZPOPMIN leaderboard", core.OpWrite, core.SubDelete, []string{"leaderboard"}},
		{"ZPOPMAX", "ZPOPMAX leaderboard", core.OpWrite, core.SubDelete, []string{"leaderboard"}},
		{"LREM", "LREM mylist 0 val", core.OpWrite, core.SubDelete, []string{"mylist"}},
		{"LPOP", "LPOP mylist", core.OpWrite, core.SubDelete, []string{"mylist"}},
		{"RPOP", "RPOP mylist", core.OpWrite, core.SubDelete, []string{"mylist"}},
		{"XDEL", "XDEL stream:events 1-1", core.OpWrite, core.SubDelete, []string{"stream"}},
		{"GETDEL", "GETDEL key:1", core.OpWrite, core.SubDelete, []string{"key"}},

		// Writes (truncate)
		{"LTRIM", "LTRIM mylist 0 99", core.OpWrite, core.SubTruncate, []string{"mylist"}},
		{"XTRIM", "XTRIM stream:events MAXLEN 1000", core.OpWrite, core.SubTruncate, []string{"stream"}},

		// DDL
		{"FLUSHDB", "FLUSHDB", core.OpDDL, core.SubOther, nil},
		{"FLUSHALL", "FLUSHALL", core.OpDDL, core.SubOther, nil},
		{"CONFIG SET", "CONFIG SET maxmemory 100mb", core.OpDDL, core.SubOther, nil},

		// Transactions
		{"MULTI", "MULTI", core.OpTransaction, core.SubBegin, nil},
		{"EXEC", "EXEC", core.OpTransaction, core.SubCommit, nil},
		{"DISCARD", "DISCARD", core.OpTransaction, core.SubRollback, nil},

		// Meta
		{"PING", "PING", core.OpOther, core.SubOther, nil},
		{"INFO", "INFO", core.OpOther, core.SubOther, nil},
		{"SELECT", "SELECT 1", core.OpOther, core.SubOther, nil},
		{"AUTH", "AUTH mypassword", core.OpOther, core.SubOther, nil},
		{"CONFIG GET", "CONFIG GET maxmemory", core.OpOther, core.SubOther, nil},

		// Edge cases
		{"empty", "", core.OpOther, core.SubOther, nil},
		{"unknown cmd", "FOOBAR arg1", core.OpOther, core.SubOther, nil},
		{"case insensitive", "get User:1", core.OpRead, core.SubSelect, []string{"User"}},
		{"key without prefix", "GET simplekey", core.OpRead, core.SubSelect, []string{"simplekey"}},

		// SORT with STORE
		{"SORT read", "SORT mylist LIMIT 0 10", core.OpRead, core.SubSelect, []string{"mylist"}},
		{"SORT write", "SORT mylist STORE result:sorted", core.OpWrite, core.SubInsert, []string{"mylist", "result"}},

		// Pub/Sub
		{"SUBSCRIBE", "SUBSCRIBE channel1", core.OpOther, core.SubOther, nil},
		{"PSUBSCRIBE", "PSUBSCRIBE chan:*", core.OpOther, core.SubOther, nil},
		{"PUBLISH", "PUBLISH channel1 hello", core.OpWrite, core.SubNotify, nil},

		// Lua scripting
		{"EVAL", `EVAL "return 1" 2 user:1 session:2 arg1`, core.OpWrite, core.SubInsert, []string{"user", "session"}},
		{"EVALSHA", "EVALSHA abc123 1 counter:hits arg1", core.OpWrite, core.SubInsert, []string{"counter"}},
		{"EVAL no keys", `EVAL "return 1" 0`, core.OpWrite, core.SubInsert, nil},
		{"SCRIPT", "SCRIPT LOAD return 1", core.OpOther, core.SubOther, nil},

		// HyperLogLog
		{"PFADD", "PFADD hll:visits user1", core.OpWrite, core.SubUpdate, []string{"hll"}},
		{"PFCOUNT", "PFCOUNT hll:visits hll:visits2", core.OpRead, core.SubSelect, []string{"hll"}},
		{"PFMERGE", "PFMERGE hll:dest hll:src1 hll:src2", core.OpWrite, core.SubInsert, []string{"hll"}},

		// Bitmap
		{"SETBIT", "SETBIT bitmap:flags 7 1", core.OpWrite, core.SubUpdate, []string{"bitmap"}},
		{"GETBIT", "GETBIT bitmap:flags 7", core.OpRead, core.SubSelect, []string{"bitmap"}},
		{"BITCOUNT", "BITCOUNT bitmap:flags", core.OpRead, core.SubSelect, []string{"bitmap"}},
		{"BITPOS", "BITPOS bitmap:flags 1", core.OpRead, core.SubSelect, []string{"bitmap"}},
		{"BITFIELD", "BITFIELD counter:bf SET u8 0 100", core.OpWrite, core.SubUpdate, []string{"counter"}},

		// Geospatial
		{"GEOADD", "GEOADD geo:places 13.361 38.115 Palermo", core.OpWrite, core.SubUpdate, []string{"geo"}},
		{"GEODIST", "GEODIST geo:places Palermo Catania", core.OpRead, core.SubSelect, []string{"geo"}},
		{"GEOHASH", "GEOHASH geo:places Palermo", core.OpRead, core.SubSelect, []string{"geo"}},
		{"GEOPOS", "GEOPOS geo:places Palermo", core.OpRead, core.SubSelect, []string{"geo"}},
		{"GEOSEARCH", "GEOSEARCH geo:places FROMMEMBER Palermo BYRADIUS 100 km", core.OpRead, core.SubSelect, []string{"geo"}},
		{"GEOSEARCHSTORE", "GEOSEARCHSTORE dst:geo geo:places FROMMEMBER Palermo BYRADIUS 100 km", core.OpWrite, core.SubInsert, []string{"dst"}},

		// Redis 7.0+
		{"GETEX", "GETEX session:abc EX 3600", core.OpWrite, core.SubUpdate, []string{"session"}},
		{"GETDEL", "GETDEL key:1", core.OpWrite, core.SubDelete, []string{"key"}},
		{"EXPIRETIME", "EXPIRETIME session:abc", core.OpRead, core.SubSelect, []string{"session"}},
		{"PEXPIRETIME", "PEXPIRETIME session:abc", core.OpRead, core.SubSelect, []string{"session"}},

		// EVALRO/EVALSHA_RO — key extraction
		{"EVALRO", `EVALRO "return redis.call('GET',KEYS[1])" 1 user:1`, core.OpRead, core.SubSelect, []string{"user"}},
		{"EVALSHA_RO", "EVALSHA_RO abc123 2 user:1 session:2", core.OpRead, core.SubSelect, []string{"user", "session"}},

		// RENAME includes both source and destination prefixes
		{"RENAMENX", "RENAMENX old:key new:key", core.OpWrite, core.SubUpdate, []string{"old", "new"}},

		// New commands (Redis 6.2+/7.0+)
		{"XREADGROUP", "XREADGROUP GROUP mygroup consumer1 COUNT 10 STREAMS stream:1 >", core.OpWrite, core.SubUpdate, nil},
		{"XAUTOCLAIM", "XAUTOCLAIM stream:1 mygroup consumer1 0", core.OpWrite, core.SubUpdate, []string{"stream"}},
		{"ZDIFF", "ZDIFF 2 zset:1 zset:2", core.OpRead, core.SubSelect, nil},
		{"ZDIFFSTORE", "ZDIFFSTORE dest:zset 2 zset:1 zset:2", core.OpWrite, core.SubInsert, []string{"dest"}},
		{"ZRANGESTORE", "ZRANGESTORE dest:zset src:zset 0 10", core.OpWrite, core.SubInsert, []string{"dest"}},

		// Dangerous commands — blocked
		{"SHUTDOWN", "SHUTDOWN NOSAVE", core.OpOther, core.SubNotSupported, nil},
		{"REPLICAOF", "REPLICAOF NO ONE", core.OpOther, core.SubNotSupported, nil},
		{"DEBUG", "DEBUG SLEEP 0", core.OpOther, core.SubNotSupported, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.query)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
			if len(cl.Tables) != len(tt.wantTables) {
				t.Errorf("Tables = %v, want %v", cl.Tables, tt.wantTables)
			} else {
				for i, table := range cl.Tables {
					if table != tt.wantTables[i] {
						t.Errorf("Tables[%d] = %q, want %q", i, table, tt.wantTables[i])
					}
				}
			}
		})
	}
}

func TestKeyPrefix(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"user:123", "user"},
		{"session:abc:def", "session"},
		{"simplekey", "simplekey"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := KeyPrefix(tt.key); got != tt.want {
			t.Errorf("KeyPrefix(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestIsWriteCommand(t *testing.T) {
	writes := []string{"SET", "DEL", "HSET", "LPUSH", "SADD", "ZADD", "FLUSHDB", "set", "del", "EVAL", "EVALSHA", "PFADD", "SETBIT", "BITFIELD", "GEOADD", "GEOSEARCHSTORE", "GETEX", "GETDEL"}
	for _, cmd := range writes {
		if !IsWriteCommand(cmd) {
			t.Errorf("IsWriteCommand(%q) = false, want true", cmd)
		}
	}
	reads := []string{"GET", "HGET", "LRANGE", "SMEMBERS", "ZRANGE", "PING", "INFO", "get"}
	for _, cmd := range reads {
		if IsWriteCommand(cmd) {
			t.Errorf("IsWriteCommand(%q) = true, want false", cmd)
		}
	}
}

func TestIsPubSubSubscribe(t *testing.T) {
	subs := []string{"SUBSCRIBE", "PSUBSCRIBE", "subscribe", "psubscribe"}
	for _, cmd := range subs {
		if !IsPubSubSubscribe(cmd) {
			t.Errorf("IsPubSubSubscribe(%q) = false, want true", cmd)
		}
	}
	nonSubs := []string{"UNSUBSCRIBE", "PUBLISH", "GET", "SET"}
	for _, cmd := range nonSubs {
		if IsPubSubSubscribe(cmd) {
			t.Errorf("IsPubSubSubscribe(%q) = true, want false", cmd)
		}
	}
}

func TestIsPubSubUnsubscribe(t *testing.T) {
	unsubs := []string{"UNSUBSCRIBE", "PUNSUBSCRIBE", "unsubscribe"}
	for _, cmd := range unsubs {
		if !IsPubSubUnsubscribe(cmd) {
			t.Errorf("IsPubSubUnsubscribe(%q) = false, want true", cmd)
		}
	}
	nonUnsubs := []string{"SUBSCRIBE", "PUBLISH", "GET"}
	for _, cmd := range nonUnsubs {
		if IsPubSubUnsubscribe(cmd) {
			t.Errorf("IsPubSubUnsubscribe(%q) = true, want false", cmd)
		}
	}
}

func TestIsEvalCommand(t *testing.T) {
	evals := []string{"EVAL", "EVALSHA", "EVALRO", "EVALSHA_RO", "eval", "evalsha", "evalro", "evalsha_ro"}
	for _, cmd := range evals {
		if !IsEvalCommand(cmd) {
			t.Errorf("IsEvalCommand(%q) = false, want true", cmd)
		}
	}
	nonEvals := []string{"GET", "SET", "SCRIPT", "FCALL"}
	for _, cmd := range nonEvals {
		if IsEvalCommand(cmd) {
			t.Errorf("IsEvalCommand(%q) = true, want false", cmd)
		}
	}
}

func TestExtractEvalKeys(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"two keys", []string{"return 1", "2", "user:1", "session:2", "arg1"}, []string{"user:1", "session:2"}},
		{"one key", []string{"return 1", "1", "counter:hits", "arg1"}, []string{"counter:hits"}},
		{"zero keys", []string{"return 1", "0"}, nil},
		{"no numkeys", []string{"return 1"}, nil},
		{"empty args", []string{}, nil},
		{"invalid numkeys", []string{"return 1", "abc"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractEvalKeys(tt.args)
			if len(got) != len(tt.want) {
				t.Errorf("ExtractEvalKeys() = %v, want %v", got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("ExtractEvalKeys()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
