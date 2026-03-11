//go:build e2e_redis

package e2e_redis

import (
	"bufio"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/psrth/mori/internal/engine/redis/proxy"
)

// connectProxy returns a TCP connection to the mori RESP proxy.
func connectProxy(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, bufio.NewReader(conn)
}

// connectDirect returns a TCP connection directly to the prod Redis.
func connectDirect(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", prodAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("connect to prod Redis: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, bufio.NewReader(conn)
}

// redisCmd sends a Redis command and returns the RESP response.
func redisCmd(t *testing.T, conn net.Conn, r *bufio.Reader, args ...string) *proxy.RESPValue {
	t.Helper()
	cmd := proxy.BuildCommandArray(args...)
	if _, err := conn.Write(cmd.Bytes()); err != nil {
		t.Fatalf("write command %v: %v", args, err)
	}
	resp, err := proxy.ReadRESPValue(r)
	if err != nil {
		t.Fatalf("read response for %v: %v", args, err)
	}
	return resp
}

// assertStringReply asserts that the RESP value is a string (simple or bulk) with the expected value.
func assertStringReply(t *testing.T, resp *proxy.RESPValue, expected string) {
	t.Helper()
	if resp.Type == '+' || resp.Type == '$' {
		if resp.Str != expected {
			t.Errorf("got %q, want %q", resp.Str, expected)
		}
		return
	}
	t.Errorf("expected string reply, got type %c value %+v", resp.Type, resp)
}

// assertIntReply asserts that the RESP value is an integer with the expected value.
func assertIntReply(t *testing.T, resp *proxy.RESPValue, expected int64) {
	t.Helper()
	if resp.Type != ':' {
		t.Errorf("expected integer reply, got type %c", resp.Type)
		return
	}
	if resp.Int != expected {
		t.Errorf("got %d, want %d", resp.Int, expected)
	}
}

// assertNullReply asserts that the RESP value is null.
func assertNullReply(t *testing.T, resp *proxy.RESPValue) {
	t.Helper()
	if !resp.IsNull {
		t.Errorf("expected null reply, got type %c value %+v", resp.Type, resp)
	}
}

// assertOKReply asserts that the reply is "+OK".
func assertOKReply(t *testing.T, resp *proxy.RESPValue) {
	t.Helper()
	if resp.Type != '+' || resp.Str != "OK" {
		t.Errorf("expected +OK, got type %c value %q", resp.Type, resp.Str)
	}
}

// assertErrorReply asserts that the reply is an error.
func assertErrorReply(t *testing.T, resp *proxy.RESPValue) {
	t.Helper()
	if resp.Type != '-' {
		t.Errorf("expected error reply, got type %c value %+v", resp.Type, resp)
	}
}

// assertArrayLen asserts that the RESP value is an array with the expected length.
func assertArrayLen(t *testing.T, resp *proxy.RESPValue, expected int) {
	t.Helper()
	if resp.Type != '*' {
		t.Errorf("expected array reply, got type %c", resp.Type)
		return
	}
	if len(resp.Array) != expected {
		t.Errorf("got array length %d, want %d", len(resp.Array), expected)
	}
}

// getDirectValue gets a value directly from prod Redis.
func getDirectValue(t *testing.T, key string) string {
	t.Helper()
	conn, r := connectDirect(t)
	defer conn.Close()
	resp := redisCmd(t, conn, r, "GET", key)
	if resp.IsNull {
		return ""
	}
	return resp.Str
}

// setDirectValue sets a value directly on prod Redis.
func setDirectValue(t *testing.T, key, value string) {
	t.Helper()
	conn, r := connectDirect(t)
	defer conn.Close()
	redisCmd(t, conn, r, "SET", key, value)
}

// keyExistsDirect checks if a key exists directly on prod Redis.
func keyExistsDirect(t *testing.T, key string) bool {
	t.Helper()
	conn, r := connectDirect(t)
	defer conn.Close()
	resp := redisCmd(t, conn, r, "EXISTS", key)
	return resp.Type == ':' && resp.Int == 1
}

// runMoriCommand executes a mori CLI command.
func runMoriCommand(t *testing.T, args ...string) string {
	t.Helper()
	proc := newMoriCommand(t, args...)
	out, err := proc.CombinedOutput()
	if err != nil {
		t.Logf("mori %v failed: %v\nOutput: %s", args, err, string(out))
	}
	return string(out)
}

func newMoriCommand(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(moriBin, args...)
	cmd.Dir = projectDir
	return cmd
}
