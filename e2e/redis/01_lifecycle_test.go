//go:build e2e_redis

package e2e_redis

import (
	"strings"
	"testing"
)

func TestLifecycle(t *testing.T) {
	t.Run("ping_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "PING")
		assertStringReply(t, resp, "PONG")
	})

	t.Run("echo_through_proxy", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "ECHO", "hello-mori")
		assertStringReply(t, resp, "hello-mori")
	})

	t.Run("multiple_connections", func(t *testing.T) {
		conn1, r1 := connectProxy(t)
		conn2, r2 := connectProxy(t)

		resp1 := redisCmd(t, conn1, r1, "PING")
		resp2 := redisCmd(t, conn2, r2, "PING")

		assertStringReply(t, resp1, "PONG")
		assertStringReply(t, resp2, "PONG")
	})

	t.Run("sequential_commands_on_same_conn", func(t *testing.T) {
		conn, r := connectProxy(t)
		for i := 0; i < 10; i++ {
			resp := redisCmd(t, conn, r, "PING")
			assertStringReply(t, resp, "PONG")
		}
	})

	t.Run("status_shows_redis_engine", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		lower := strings.ToLower(out)
		if !strings.Contains(lower, "redis") {
			t.Errorf("status output missing redis:\n%s", out)
		}
	})

	t.Run("dbsize_returns_integer", func(t *testing.T) {
		conn, r := connectProxy(t)
		resp := redisCmd(t, conn, r, "DBSIZE")
		if resp.Type != ':' {
			t.Errorf("expected integer reply for DBSIZE, got type %c", resp.Type)
		}
	})
}
