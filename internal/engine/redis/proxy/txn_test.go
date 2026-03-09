package proxy

import (
	"testing"

	"github.com/mori-dev/mori/internal/core"
)

func TestExtractWriteKeys(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"SET single key", []string{"SET", "user:1", "value"}, []string{"user:1"}},
		{"MSET two keys", []string{"MSET", "k1", "v1", "k2", "v2"}, []string{"k1", "k2"}},
		{"MSETNX three keys", []string{"MSETNX", "a", "1", "b", "2", "c", "3"}, []string{"a", "b", "c"}},
		{"RENAME both keys", []string{"RENAME", "old:key", "new:key"}, []string{"old:key", "new:key"}},
		{"RENAMENX both keys", []string{"RENAMENX", "old:key", "new:key"}, []string{"old:key", "new:key"}},
		{"RPOPLPUSH both keys", []string{"RPOPLPUSH", "src:list", "dst:list"}, []string{"src:list", "dst:list"}},
		{"LMOVE both keys", []string{"LMOVE", "src:list", "dst:list", "LEFT", "RIGHT"}, []string{"src:list", "dst:list"}},
		{"SMOVE both keys", []string{"SMOVE", "src:set", "dst:set", "member"}, []string{"src:set", "dst:set"}},
		{"COPY dest only", []string{"COPY", "src:key", "dst:key"}, []string{"dst:key"}},
		{"SDIFFSTORE dest", []string{"SDIFFSTORE", "dst:set", "s1", "s2"}, []string{"dst:set"}},
		{"SINTERSTORE dest", []string{"SINTERSTORE", "dst:set", "s1", "s2"}, []string{"dst:set"}},
		{"SUNIONSTORE dest", []string{"SUNIONSTORE", "dst:set", "s1", "s2"}, []string{"dst:set"}},
		{"ZUNIONSTORE dest", []string{"ZUNIONSTORE", "dst:zset", "2", "s1", "s2"}, []string{"dst:zset"}},
		{"ZINTERSTORE dest", []string{"ZINTERSTORE", "dst:zset", "2", "s1", "s2"}, []string{"dst:zset"}},
		{"BITOP dest", []string{"BITOP", "AND", "dst:key", "s1", "s2"}, []string{"dst:key"}},
		{"SORT with STORE", []string{"SORT", "mylist", "STORE", "result:list"}, []string{"mylist", "result:list"}},
		{"GEOSEARCHSTORE", []string{"GEOSEARCHSTORE", "dst:geo", "src:geo", "FROMMEMBER", "m", "BYRADIUS", "10", "km"}, []string{"dst:geo"}},
		{"DEL single", []string{"DEL", "user:1"}, []string{"user:1"}},
		{"INCR", []string{"INCR", "counter:hits"}, []string{"counter:hits"}},
		{"empty args", []string{"SET"}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractWriteKeys(tt.args)
			if len(got) != len(tt.want) {
				t.Fatalf("extractWriteKeys(%v) = %v, want %v", tt.args, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractWriteKeys(%v)[%d] = %q, want %q", tt.args, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestExtractHydrationKeys(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"INCR single key", []string{"INCR", "counter:1"}, []string{"counter:1"}},
		{"RENAME both keys", []string{"RENAME", "old:key", "new:key"}, []string{"old:key", "new:key"}},
		{"RPOPLPUSH both", []string{"RPOPLPUSH", "src", "dst"}, []string{"src", "dst"}},
		{"LMOVE both", []string{"LMOVE", "src", "dst", "LEFT", "RIGHT"}, []string{"src", "dst"}},
		{"SMOVE both", []string{"SMOVE", "src", "dst", "member"}, []string{"src", "dst"}},
		{"SDIFFSTORE all keys", []string{"SDIFFSTORE", "dst", "s1", "s2"}, []string{"dst", "s1", "s2"}},
		{"SINTERSTORE all keys", []string{"SINTERSTORE", "dst", "s1", "s2", "s3"}, []string{"dst", "s1", "s2", "s3"}},
		{"SUNIONSTORE all keys", []string{"SUNIONSTORE", "dst", "s1", "s2"}, []string{"dst", "s1", "s2"}},
		{"ZUNIONSTORE all keys", []string{"ZUNIONSTORE", "dst", "2", "s1", "s2"}, []string{"dst", "2", "s1", "s2"}},
		{"BITOP source keys", []string{"BITOP", "AND", "dst", "s1", "s2"}, []string{"s1", "s2"}},
		{"SORT source", []string{"SORT", "mylist"}, []string{"mylist"}},
		{"empty args", []string{"SET"}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractHydrationKeys(tt.args)
			if len(got) != len(tt.want) {
				t.Fatalf("extractHydrationKeys(%v) = %v, want %v", tt.args, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractHydrationKeys(%v)[%d] = %q, want %q", tt.args, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestTombstoneResponseForCommand(t *testing.T) {
	tests := []struct {
		cmd      string
		wantType byte // first byte of RESP response
	}{
		// Null bulk string commands.
		{"GET", '$'},
		{"GETRANGE", '$'},
		{"HGET", '$'},
		{"LINDEX", '$'},
		{"ZSCORE", '$'},
		{"DUMP", '$'},

		// Empty array commands.
		{"HGETALL", '*'},
		{"HMGET", '*'},
		{"LRANGE", '*'},
		{"SMEMBERS", '*'},
		{"ZRANGE", '*'},
		{"XRANGE", '*'},
		{"HKEYS", '*'},
		{"HVALS", '*'},

		// Integer 0 commands.
		{"LLEN", ':'},
		{"SCARD", ':'},
		{"ZCARD", ':'},
		{"XLEN", ':'},
		{"STRLEN", ':'},
		{"HLEN", ':'},
		{"EXISTS", ':'},
		{"SISMEMBER", ':'},
		{"HEXISTS", ':'},

		// TTL/PTTL return -2.
		{"TTL", ':'},
		{"PTTL", ':'},

		// TYPE returns simple string "none".
		{"TYPE", '+'},
	}

	for _, tt := range tests {
		t.Run(tt.cmd, func(t *testing.T) {
			resp := tombstoneResponseForCommand(tt.cmd)
			if len(resp) == 0 {
				t.Fatal("got empty response")
			}
			if resp[0] != tt.wantType {
				t.Errorf("tombstoneResponseForCommand(%q) type = %c, want %c", tt.cmd, resp[0], tt.wantType)
			}

			// Verify specific values for TTL/PTTL (-2) and integer commands (0).
			respStr := string(resp)
			switch tt.cmd {
			case "TTL", "PTTL":
				if respStr != ":-2\r\n" {
					t.Errorf("tombstoneResponseForCommand(%q) = %q, want \":-2\\r\\n\"", tt.cmd, respStr)
				}
			case "LLEN", "SCARD", "ZCARD", "XLEN", "STRLEN", "HLEN", "EXISTS", "SISMEMBER", "HEXISTS":
				if respStr != ":0\r\n" {
					t.Errorf("tombstoneResponseForCommand(%q) = %q, want \":0\\r\\n\"", tt.cmd, respStr)
				}
			case "TYPE":
				if respStr != "+none\r\n" {
					t.Errorf("tombstoneResponseForCommand(%q) = %q, want \"+none\\r\\n\"", tt.cmd, respStr)
				}
			}
		})
	}
}

func TestRedisTxnState(t *testing.T) {
	// Test that redisTxnState tracks write keys correctly.
	txn := redisTxnState{
		inMulti:   true,
		writeKeys: make(map[string]bool),
	}

	// Simulate queueing a SET and an INCR command.
	txn.writeKeys["user:1"] = true
	txn.writeKeys["counter:hits"] = true

	if !txn.writeKeys["user:1"] {
		t.Error("expected user:1 in writeKeys")
	}
	if !txn.writeKeys["counter:hits"] {
		t.Error("expected counter:hits in writeKeys")
	}
	if txn.writeKeys["missing:key"] {
		t.Error("expected missing:key NOT in writeKeys")
	}
}

func TestBuildScanResponse(t *testing.T) {
	// Test that buildScanResponse produces valid RESP.
	resp := buildScanResponse("42", []string{"key:1", "key:2"})
	if len(resp) == 0 {
		t.Fatal("got empty response")
	}
	// Should start with array marker.
	if resp[0] != '*' {
		t.Errorf("expected array marker, got %c", resp[0])
	}
	// Verify it contains the cursor and keys.
	respStr := string(resp)
	if !contains(respStr, "42") {
		t.Error("response should contain cursor '42'")
	}
	if !contains(respStr, "key:1") || !contains(respStr, "key:2") {
		t.Error("response should contain keys")
	}
}

func TestScanPhaseFlag(t *testing.T) {
	// Verify the phase flag is the high bit.
	if scanPhaseFlag != (1 << 63) {
		t.Fatalf("scanPhaseFlag = %d, want %d", scanPhaseFlag, uint64(1)<<63)
	}

	// Phase 0: cursor without flag.
	cursor := uint64(42)
	if (cursor & scanPhaseFlag) != 0 {
		t.Error("cursor 42 should be Phase 0")
	}

	// Phase 1: cursor with flag.
	cursor = 42 | scanPhaseFlag
	if (cursor & scanPhaseFlag) == 0 {
		t.Error("cursor with flag should be Phase 1")
	}

	// Extract inner cursor from Phase 1.
	inner := cursor &^ scanPhaseFlag
	if inner != 42 {
		t.Errorf("inner cursor = %d, want 42", inner)
	}
}

func TestIsMultiKeyRead(t *testing.T) {
	multiKey := []string{"MGET", "EXISTS", "SDIFF", "SINTER", "SUNION"}
	for _, cmd := range multiKey {
		if !isMultiKeyRead(cmd) {
			t.Errorf("isMultiKeyRead(%q) = false, want true", cmd)
		}
	}
	singleKey := []string{"GET", "SET", "HGET", "LRANGE"}
	for _, cmd := range singleKey {
		if isMultiKeyRead(cmd) {
			t.Errorf("isMultiKeyRead(%q) = true, want false", cmd)
		}
	}
}

// TestClassifySubTypeRouting verifies that the reclassified SubTypes produce
// the correct routing strategies via the core router.
func TestClassifySubTypeRouting(t *testing.T) {
	tests := []struct {
		subType  core.SubType
		strategy core.RoutingStrategy
	}{
		{core.SubInsert, core.StrategyShadowWrite},
		{core.SubUpdate, core.StrategyHydrateAndWrite},
		{core.SubDelete, core.StrategyShadowDelete},
		{core.SubTruncate, core.StrategyTruncate},
	}

	router := core.NewRouter(nil, nil, nil)
	for _, tt := range tests {
		t.Run(tt.subType.String(), func(t *testing.T) {
			cl := &core.Classification{
				OpType:  core.OpWrite,
				SubType: tt.subType,
				Tables:  []string{"test"},
			}
			got := router.Route(cl)
			if got != tt.strategy {
				t.Errorf("Route(SubType=%v) = %v, want %v", tt.subType, got, tt.strategy)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsHelper(s, sub))
}

func containsHelper(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
