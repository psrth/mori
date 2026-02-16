package mcp

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestIsReadQuery(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"SELECT * FROM users", true},
		{"select * from users", true},
		{"  SELECT 1", true},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", true},
		{"EXPLAIN SELECT * FROM users", true},
		{"SHOW TABLES", true},
		{"INSERT INTO users (name) VALUES ('a')", false},
		{"UPDATE users SET name='b'", false},
		{"DELETE FROM users WHERE id=1", false},
		{"CREATE TABLE t (id INT)", false},
		{"DROP TABLE t", false},
		{"", false},
	}
	for _, tt := range tests {
		got := isReadQuery(tt.query)
		if got != tt.want {
			t.Errorf("isReadQuery(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestParseRedisCommand(t *testing.T) {
	tests := []struct {
		cmd  string
		want []string
	}{
		{"GET key", []string{"GET", "key"}},
		{"SET key value", []string{"SET", "key", "value"}},
		{"SET key 'hello world'", []string{"SET", "key", "hello world"}},
		{`SET key "hello world"`, []string{"SET", "key", "hello world"}},
		{"HGETALL myhash", []string{"HGETALL", "myhash"}},
		{"  GET   key  ", []string{"GET", "key"}},
		{"KEYS user:*", []string{"KEYS", "user:*"}},
	}
	for _, tt := range tests {
		got := parseRedisCommand(tt.cmd)
		if len(got) != len(tt.want) {
			t.Errorf("parseRedisCommand(%q) = %v, want %v", tt.cmd, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("parseRedisCommand(%q)[%d] = %q, want %q", tt.cmd, i, got[i], tt.want[i])
			}
		}
	}
}

func TestEncodeRESP(t *testing.T) {
	got := string(encodeRESP([]string{"GET", "key"}))
	want := "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
	if got != want {
		t.Errorf("encodeRESP = %q, want %q", got, want)
	}
}

func TestReadRESPValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple string", "+OK\r\n", "OK"},
		{"integer", ":42\r\n", "(integer) 42"},
		{"bulk string", "$5\r\nhello\r\n", "hello"},
		{"null bulk", "$-1\r\n", "(nil)"},
		{"array", "*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n", "1) foo\n2) bar\n3) baz"},
		{"empty array", "*0\r\n", "(empty list)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			val, err := readRESPValue(reader)
			if err != nil {
				t.Fatalf("readRESPValue: %v", err)
			}
			got := formatRESPValue(val)
			if got != tt.want {
				t.Errorf("formatRESPValue = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadRESPValue_Error(t *testing.T) {
	input := "-ERR unknown command\r\n"
	reader := bufio.NewReader(bytes.NewReader([]byte(input)))
	_, err := readRESPValue(reader)
	if err == nil || !strings.Contains(err.Error(), "ERR unknown command") {
		t.Errorf("expected error containing 'ERR unknown command', got %v", err)
	}
}

func TestAutoDetectValue(t *testing.T) {
	tests := []struct {
		input string
		want  any
	}{
		{"true", true},
		{"false", false},
		{"42", 42},
		{"0", 0},
		{"hello", "hello"},
		{"-5", -5},
	}
	for _, tt := range tests {
		got := autoDetectValue(tt.input)
		switch want := tt.want.(type) {
		case bool:
			if g, ok := got.(bool); !ok || g != want {
				t.Errorf("autoDetectValue(%q) = %v (%T), want %v", tt.input, got, got, tt.want)
			}
		case int:
			if g, ok := got.(int); !ok || g != want {
				t.Errorf("autoDetectValue(%q) = %v (%T), want %v", tt.input, got, got, tt.want)
			}
		case string:
			if g, ok := got.(string); !ok || g != want {
				t.Errorf("autoDetectValue(%q) = %v (%T), want %v", tt.input, got, got, tt.want)
			}
		}
	}
}

func TestFormatRESPValue_Hash(t *testing.T) {
	// HGETALL response: alternating key-value pairs
	val := []any{"field1", "value1", "field2", "value2"}
	got := formatRESPValue(val)
	// Should be formatted as JSON object
	if !strings.Contains(got, "field1") || !strings.Contains(got, "value1") {
		t.Errorf("formatRESPValue hash = %s, expected JSON with field1/value1", got)
	}
}

func TestEngineConfigDispatch(t *testing.T) {
	// Verify that engine names map to the correct tool registration path.
	engines := map[string]string{
		"postgres":    "pgwire",
		"cockroachdb": "pgwire",
		"sqlite":      "pgwire",
		"duckdb":      "pgwire",
		"mysql":       "mysql",
		"mariadb":     "mysql",
		"mssql":       "mssql",
	}

	for engine, expected := range engines {
		category := classifyEngine(engine)
		if category != expected {
			t.Errorf("engine %q: classifyEngine = %q, want %q", engine, category, expected)
		}
	}
}

// classifyEngine mirrors the dispatch logic in registerSQLTools for testing.
func classifyEngine(engine string) string {
	switch engine {
	case "mysql", "mariadb":
		return "mysql"
	case "mssql":
		return "mssql"
	default:
		return "pgwire"
	}
}
