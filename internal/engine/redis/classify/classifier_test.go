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

		// Writes
		{"SET", "SET user:1 hello", core.OpWrite, core.SubInsert, []string{"user"}},
		{"SETNX", "SETNX lock:task1 1", core.OpWrite, core.SubInsert, []string{"lock"}},
		{"MSET", "MSET user:1 val1 session:2 val2", core.OpWrite, core.SubInsert, []string{"user", "session"}},
		{"INCR", "INCR counter:hits", core.OpWrite, core.SubInsert, []string{"counter"}},
		{"HSET", "HSET user:1 name Alice", core.OpWrite, core.SubInsert, []string{"user"}},
		{"LPUSH", "LPUSH queue:tasks task1", core.OpWrite, core.SubInsert, []string{"queue"}},
		{"SADD", "SADD tags:post1 redis", core.OpWrite, core.SubInsert, []string{"tags"}},
		{"ZADD", "ZADD leaderboard 100 player1", core.OpWrite, core.SubInsert, []string{"leaderboard"}},
		{"EXPIRE", "EXPIRE session:abc 3600", core.OpWrite, core.SubInsert, []string{"session"}},

		// Deletes
		{"DEL single", "DEL user:1", core.OpWrite, core.SubDelete, []string{"user"}},
		{"DEL multi", "DEL user:1 session:abc", core.OpWrite, core.SubDelete, []string{"user", "session"}},
		{"UNLINK", "UNLINK temp:key1", core.OpWrite, core.SubDelete, []string{"temp"}},

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
		{"SORT write", "SORT mylist STORE result:sorted", core.OpWrite, core.SubInsert, []string{"mylist"}},
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
	writes := []string{"SET", "DEL", "HSET", "LPUSH", "SADD", "ZADD", "FLUSHDB", "set", "del"}
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
