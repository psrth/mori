//go:build e2e_redis

package e2e_redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/redis/proxy"
)

// Global test state.
var (
	proxyAddr     string // "127.0.0.1:PORT" for the mori RESP proxy
	prodAddr      string // "127.0.0.1:PORT" for the prod Redis
	proxyPort     = 19006
	prodPort      = 19007
	projectDir    string
	moriBin       string
	moriProcess   *exec.Cmd
	repoRoot      string
	prodContainer string // Docker container ID for prod Redis
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Create temp project directory.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-redis-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to create temp dir: %v\n", err)
		return 1
	}
	defer os.RemoveAll(projectDir)

	// 2. Build mori binary.
	fmt.Println("SETUP: Building mori binary...")
	moriBin = filepath.Join(projectDir, "mori")
	if err := buildMori(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to build mori: %v\n", err)
		return 1
	}

	// 3. Start prod Redis container.
	fmt.Println("SETUP: Starting prod Redis container...")
	if err := startProdRedis(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start prod Redis: %v\n", err)
		return 1
	}
	defer stopProdRedis()

	// 4. Wait for prod Redis to be ready.
	fmt.Println("SETUP: Waiting for prod Redis...")
	prodAddr = fmt.Sprintf("127.0.0.1:%d", prodPort)
	if err := waitForRedis(prodAddr, 30*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: prod Redis not ready: %v\n", err)
		return 1
	}

	// 5. Seed prod Redis.
	fmt.Println("SETUP: Seeding prod Redis...")
	if err := seedProdRedis(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed prod Redis: %v\n", err)
		return 1
	}

	// 6. Write mori.yaml.
	fmt.Println("SETUP: Writing mori.yaml...")
	if err := writeMoriYAML(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write mori.yaml: %v\n", err)
		return 1
	}

	// 7. Start mori proxy.
	proxyAddr = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 8. Wait for proxy to accept connections.
	fmt.Println("SETUP: Waiting for proxy to be ready...")
	if err := waitForRedis(proxyAddr, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: proxy not ready: %v\n", err)
		return 1
	}

	fmt.Println("SETUP: Ready! Running tests...")
	fmt.Println()
	return m.Run()
}

func buildMori(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", moriBin, "./cmd/mori")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func startProdRedis(ctx context.Context) error {
	portMapping := fmt.Sprintf("127.0.0.1:%d:6379", prodPort)
	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", "mori-e2e-prod-redis",
		"-p", portMapping,
		"redis:7",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run: %s", strings.TrimSpace(string(out)))
	}
	prodContainer = strings.TrimSpace(string(out))
	return nil
}

func stopProdRedis() {
	if prodContainer != "" {
		exec.Command("docker", "rm", "-f", prodContainer).Run()
	}
}

func waitForRedis(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// Send PING.
		r := bufio.NewReader(conn)
		cmd := proxy.BuildCommandArray("PING")
		conn.Write(cmd.Bytes())
		resp, err := proxy.ReadRESPValue(r)
		conn.Close()
		if err == nil && resp.Type == '+' && resp.Str == "PONG" {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for Redis at %s", addr)
}

func seedProdRedis() error {
	conn, err := net.DialTimeout("tcp", prodAddr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	commands := [][]string{
		// String keys.
		{"SET", "user:1", "Alice"},
		{"SET", "user:2", "Bob"},
		{"SET", "user:3", "Charlie"},
		{"SET", "session:abc", "user:1"},
		{"SET", "session:def", "user:2"},
		{"SET", "counter:hits", "100"},

		// Hash keys.
		{"HSET", "profile:1", "name", "Alice", "email", "alice@example.com"},
		{"HSET", "profile:2", "name", "Bob", "email", "bob@example.com"},

		// List keys.
		{"RPUSH", "queue:tasks", "task1", "task2", "task3"},

		// Set keys.
		{"SADD", "tags:post1", "redis", "database", "nosql"},
		{"SADD", "tags:post2", "go", "programming"},

		// Sorted set keys.
		{"ZADD", "leaderboard:game1", "100", "player1", "200", "player2", "150", "player3"},
	}

	for _, args := range commands {
		cmd := proxy.BuildCommandArray(args...)
		if _, err := conn.Write(cmd.Bytes()); err != nil {
			return fmt.Errorf("write %s: %w", args[0], err)
		}
		if _, err := proxy.ReadRESPValue(r); err != nil {
			return fmt.Errorf("read response for %s: %w", args[0], err)
		}
	}

	return nil
}

func writeMoriYAML() error {
	connStr := fmt.Sprintf("redis://127.0.0.1:%d/0", prodPort)
	cfg := config.NewProjectConfig()
	cfg.AddConnection("e2e", &config.Connection{
		Engine:   "redis",
		Provider: "direct",
		Host:     "127.0.0.1",
		Extra:    map[string]string{"connection_string": connStr},
	})
	return config.WriteProjectConfig(projectDir, cfg)
}

func startMoriProxy(ctx context.Context) error {
	moriProcess = exec.CommandContext(ctx, moriBin, "start", "e2e",
		"--port", fmt.Sprintf("%d", proxyPort),
		"--verbose",
	)
	moriProcess.Dir = projectDir
	moriProcess.Stdout = os.Stdout
	moriProcess.Stderr = os.Stderr
	return moriProcess.Start()
}

func stopMoriProxy() {
	if moriProcess != nil && moriProcess.Process != nil {
		moriProcess.Process.Kill()
		moriProcess.Wait()
	}
}
