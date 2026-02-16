package proxy

import (
	"bufio"
	"bytes"
	"testing"
)

func TestReadWriteSimpleString(t *testing.T) {
	input := "+OK\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '+' || v.Str != "OK" {
		t.Errorf("got type=%c str=%q, want type=+ str=OK", v.Type, v.Str)
	}
	if got := string(v.Bytes()); got != input {
		t.Errorf("Bytes() = %q, want %q", got, input)
	}
}

func TestReadWriteError(t *testing.T) {
	input := "-ERR unknown command\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '-' || v.Str != "ERR unknown command" {
		t.Errorf("got type=%c str=%q", v.Type, v.Str)
	}
	if got := string(v.Bytes()); got != input {
		t.Errorf("Bytes() = %q, want %q", got, input)
	}
}

func TestReadWriteInteger(t *testing.T) {
	input := ":42\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != ':' || v.Int != 42 {
		t.Errorf("got type=%c int=%d, want type=: int=42", v.Type, v.Int)
	}
	if got := string(v.Bytes()); got != input {
		t.Errorf("Bytes() = %q, want %q", got, input)
	}
}

func TestReadWriteBulkString(t *testing.T) {
	input := "$6\r\nfoobar\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '$' || v.Str != "foobar" || v.IsNull {
		t.Errorf("got type=%c str=%q null=%v", v.Type, v.Str, v.IsNull)
	}
	if got := string(v.Bytes()); got != input {
		t.Errorf("Bytes() = %q, want %q", got, input)
	}
}

func TestReadWriteNullBulkString(t *testing.T) {
	input := "$-1\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '$' || !v.IsNull {
		t.Errorf("got type=%c null=%v, want null bulk string", v.Type, v.IsNull)
	}
	if got := string(v.Bytes()); got != input {
		t.Errorf("Bytes() = %q, want %q", got, input)
	}
}

func TestReadWriteArray(t *testing.T) {
	input := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 2 {
		t.Fatalf("got type=%c len=%d", v.Type, len(v.Array))
	}
	if v.Array[0].Str != "foo" || v.Array[1].Str != "bar" {
		t.Errorf("got [%q, %q], want [foo, bar]", v.Array[0].Str, v.Array[1].Str)
	}
	if got := string(v.Bytes()); got != input {
		t.Errorf("Bytes() = %q, want %q", got, input)
	}
}

func TestReadNullArray(t *testing.T) {
	input := "*-1\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '*' || !v.IsNull {
		t.Errorf("got type=%c null=%v, want null array", v.Type, v.IsNull)
	}
}

func TestReadInlineCommand(t *testing.T) {
	input := "PING\r\n"
	r := bufio.NewReader(bytes.NewReader([]byte(input)))
	v, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("ReadRESPValue: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 1 {
		t.Fatalf("got type=%c len=%d", v.Type, len(v.Array))
	}
	if v.Array[0].Str != "PING" {
		t.Errorf("got %q, want PING", v.Array[0].Str)
	}
}

func TestParseCommand(t *testing.T) {
	v := BuildCommandArray("SET", "mykey", "myvalue")
	cmd, args, err := ParseCommand(v)
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	if cmd != "SET" {
		t.Errorf("cmd = %q, want SET", cmd)
	}
	if len(args) != 2 || args[0] != "mykey" || args[1] != "myvalue" {
		t.Errorf("args = %v, want [mykey myvalue]", args)
	}
}

func TestCommandToInline(t *testing.T) {
	v := BuildCommandArray("GET", "user:1")
	got := CommandToInline(v)
	if got != "GET user:1" {
		t.Errorf("CommandToInline = %q, want %q", got, "GET user:1")
	}
}

func TestBuildHelpers(t *testing.T) {
	// Error reply
	e := BuildErrorReply("ERR test")
	if e.Type != '-' || e.Str != "ERR test" {
		t.Errorf("BuildErrorReply: type=%c str=%q", e.Type, e.Str)
	}

	// Simple string
	s := BuildSimpleString("OK")
	if s.Type != '+' || s.Str != "OK" {
		t.Errorf("BuildSimpleString: type=%c str=%q", s.Type, s.Str)
	}

	// Null bulk string
	n := BuildNullBulkString()
	if n.Type != '$' || !n.IsNull {
		t.Errorf("BuildNullBulkString: type=%c null=%v", n.Type, n.IsNull)
	}

	// Integer
	i := BuildInteger(99)
	if i.Type != ':' || i.Int != 99 {
		t.Errorf("BuildInteger: type=%c int=%d", i.Type, i.Int)
	}
}

func TestRoundTrip(t *testing.T) {
	// Build a complex command, serialize, parse back.
	original := BuildCommandArray("MSET", "key1", "val1", "key2", "val2")
	raw := original.Bytes()

	r := bufio.NewReader(bytes.NewReader(raw))
	parsed, err := ReadRESPValue(r)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if len(parsed.Array) != 5 {
		t.Fatalf("got %d elements, want 5", len(parsed.Array))
	}
	for i, want := range []string{"MSET", "key1", "val1", "key2", "val2"} {
		if parsed.Array[i].Str != want {
			t.Errorf("Array[%d].Str = %q, want %q", i, parsed.Array[i].Str, want)
		}
	}
}

func TestParseScanResponse(t *testing.T) {
	t.Run("valid response", func(t *testing.T) {
		resp := &RESPValue{
			Type: '*',
			Array: []RESPValue{
				{Type: '$', Str: "42"},
				{Type: '*', Array: []RESPValue{
					{Type: '$', Str: "user:1"},
					{Type: '$', Str: "user:2"},
					{Type: '$', Str: "session:abc"},
				}},
			},
		}
		cursor, keys := parseScanResponse(resp)
		if cursor != "42" {
			t.Errorf("cursor = %q, want %q", cursor, "42")
		}
		if len(keys) != 3 {
			t.Fatalf("keys len = %d, want 3", len(keys))
		}
		if keys[0] != "user:1" || keys[1] != "user:2" || keys[2] != "session:abc" {
			t.Errorf("keys = %v", keys)
		}
	})

	t.Run("cursor zero with empty keys", func(t *testing.T) {
		resp := &RESPValue{
			Type: '*',
			Array: []RESPValue{
				{Type: '$', Str: "0"},
				{Type: '*', Array: []RESPValue{}},
			},
		}
		cursor, keys := parseScanResponse(resp)
		if cursor != "0" {
			t.Errorf("cursor = %q, want %q", cursor, "0")
		}
		if len(keys) != 0 {
			t.Errorf("keys len = %d, want 0", len(keys))
		}
	})

	t.Run("nil response", func(t *testing.T) {
		cursor, keys := parseScanResponse(nil)
		if cursor != "0" {
			t.Errorf("cursor = %q, want %q", cursor, "0")
		}
		if keys != nil {
			t.Errorf("keys = %v, want nil", keys)
		}
	})
}
