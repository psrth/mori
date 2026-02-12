//go:build e2e

package e2e

import (
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

	"github.com/jackc/pgx/v5"
)

// Global test state — set by TestMain, used by all tests.
var (
	prodDSN       string
	proxyDSN      string
	proxyPort     = 19002
	prodPort      = 15432
	mcpPort       = 19000
	dbName        = "mori_e2e_test"
	dbUser        = "postgres"
	dbPass        = "testpass"
	containerName = "mori-e2e-prod"
	projectDir    string // Temp directory used as mori project root
	moriBin       string // Path to the compiled mori binary
	moriProcess   *exec.Cmd
	repoRoot      string // Path to the mori source repo
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	// Determine the repo root (parent of e2e/).
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filename))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Create a temp project directory for .mori/.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-*")
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

	// 3. Start Prod PostgreSQL container.
	fmt.Println("SETUP: Starting Prod PostgreSQL container...")
	if err := startProdContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start Prod container: %v\n", err)
		return 1
	}
	defer stopProdContainer()

	// 4. Wait for Prod to accept connections.
	prodDSN = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", dbUser, dbPass, prodPort, dbName)
	proxyDSN = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", dbUser, dbPass, proxyPort, dbName)

	fmt.Println("SETUP: Waiting for Prod to be ready...")
	if err := waitForPostgres(ctx, prodDSN, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: Prod not ready: %v\n", err)
		return 1
	}

	// 5. Seed the Prod database.
	fmt.Println("SETUP: Seeding Prod database...")
	if err := seedProdDatabase(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed Prod: %v\n", err)
		return 1
	}

	// 6. Run mori init.
	fmt.Println("SETUP: Running mori init...")
	if err := runMoriInit(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori init failed: %v\n", err)
		return 1
	}

	// 7. Start mori proxy in background.
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 8. Wait for proxy to accept connections.
	fmt.Println("SETUP: Waiting for proxy to be ready...")
	if err := waitForPostgres(ctx, proxyDSN, 30*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: proxy not ready: %v\n", err)
		return 1
	}

	fmt.Println("SETUP: Ready! Running tests...")
	fmt.Println()
	return m.Run()
}

// buildMori compiles the mori binary from source.
func buildMori(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", moriBin, "./cmd/mori")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// startProdContainer launches a Docker PostgreSQL container to serve as Prod.
func startProdContainer(ctx context.Context) error {
	// Remove any existing container with the same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"-e", "POSTGRES_PASSWORD="+dbPass,
		"-e", "POSTGRES_DB="+dbName,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
		"-p", fmt.Sprintf("127.0.0.1:%d:5432", prodPort),
		"postgres:16",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// stopProdContainer removes the Docker Prod container.
func stopProdContainer() {
	exec.Command("docker", "rm", "-f", containerName).Run()
}

// waitForPostgres polls until a connection succeeds or timeout.
func waitForPostgres(ctx context.Context, dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		connCtx, connCancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := pgx.Connect(connCtx, dsn)
		if err == nil {
			conn.Close(connCtx)
			connCancel()
			return nil
		}
		connCancel()
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for postgres at %s", dsn)
}

// seedProdDatabase executes the seed.sql file against the Prod container.
func seedProdDatabase(ctx context.Context) error {
	seedPath := filepath.Join(repoRoot, "e2e", "seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		return fmt.Errorf("reading seed.sql: %w", err)
	}

	conn, err := pgx.Connect(ctx, prodDSN)
	if err != nil {
		return fmt.Errorf("connecting to Prod: %w", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, string(seedSQL))
	if err != nil {
		return fmt.Errorf("executing seed.sql: %w", err)
	}
	return nil
}

// runMoriInit runs `mori init --from <prodDSN>` from the project directory.
func runMoriInit(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, moriBin, "init", "--from", prodDSN)
	cmd.Dir = projectDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// startMoriProxy starts the mori proxy in the background.
func startMoriProxy(ctx context.Context) error {
	moriProcess = exec.CommandContext(ctx, moriBin, "start",
		"--port", fmt.Sprintf("%d", proxyPort),
		"--verbose",
		"--mcp",
		"--mcp-port", fmt.Sprintf("%d", mcpPort),
	)
	moriProcess.Dir = projectDir
	moriProcess.Stdout = os.Stdout
	moriProcess.Stderr = os.Stderr
	return moriProcess.Start()
}

// stopMoriProxy gracefully stops the mori proxy.
func stopMoriProxy() {
	if moriProcess != nil && moriProcess.Process != nil {
		moriProcess.Process.Kill()
		moriProcess.Wait()
	}
}

// restartMoriProxy stops and restarts the proxy (for lifecycle tests).
func restartMoriProxy(t *testing.T) {
	t.Helper()

	// Stop via mori stop.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, moriBin, "stop")
	cmd.Dir = projectDir
	if out, err := cmd.CombinedOutput(); err != nil {
		// If stop fails, force kill.
		if moriProcess != nil && moriProcess.Process != nil {
			moriProcess.Process.Kill()
			moriProcess.Wait()
		}
		t.Logf("mori stop output: %s (err: %v)", string(out), err)
	}

	// Wait for port to be free.
	time.Sleep(1 * time.Second)
	waitForPortFree(proxyPort, 10*time.Second)

	// Start again.
	moriProcess = exec.CommandContext(context.Background(), moriBin, "start",
		"--port", fmt.Sprintf("%d", proxyPort),
		"--verbose",
		"--mcp",
		"--mcp-port", fmt.Sprintf("%d", mcpPort),
	)
	moriProcess.Dir = projectDir
	moriProcess.Stdout = os.Stdout
	moriProcess.Stderr = os.Stderr
	if err := moriProcess.Start(); err != nil {
		t.Fatalf("failed to restart mori: %v", err)
	}

	// Wait for proxy to accept connections.
	if err := waitForPostgres(ctx, proxyDSN, 30*time.Second); err != nil {
		t.Fatalf("proxy not ready after restart: %v", err)
	}
}

// waitForPortFree waits until a TCP port is no longer in use.
func waitForPortFree(port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err != nil {
			return // Port is free.
		}
		conn.Close()
		time.Sleep(300 * time.Millisecond)
	}
}

// runMoriCommand executes a mori CLI command from the project directory
// and returns its combined output.
func runMoriCommand(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, moriBin, args...)
	cmd.Dir = projectDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("mori %s failed: %v\nOutput: %s", strings.Join(args, " "), err, string(out))
	}
	return string(out)
}
