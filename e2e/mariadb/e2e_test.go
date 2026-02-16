//go:build e2e_mariadb

package e2e_mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mori-dev/mori/internal/core/config"
)

// Global test state -- set by TestMain, used by all tests.
var (
	prodDSN       string
	proxyDSN      string
	proxyPort     = 19022
	prodPort      = 15436
	mcpPort       = 19024
	dbName        = "mori_test"
	dbUser        = "root"
	dbPass        = "moripass"
	containerName = "mori-e2e-mariadb-prod"
	projectDir    string
	moriBin       string
	moriProcess   *exec.Cmd
	repoRoot      string
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	// Determine the repo root (parent of e2e/).
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Create a temp project directory.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-mariadb-*")
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

	// 3. Start MariaDB Docker container.
	fmt.Println("SETUP: Starting MariaDB container...")
	if err := startProdContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start MariaDB container: %v\n", err)
		return 1
	}
	defer stopProdContainer()

	// 4. Wait for MariaDB to accept connections.
	prodDSN = fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s?multiStatements=true", dbUser, dbPass, prodPort, dbName)
	proxyDSN = fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s?interpolateParams=true", dbUser, dbPass, proxyPort, dbName)

	fmt.Println("SETUP: Waiting for MariaDB to be ready...")
	if err := waitForMariaDB(prodDSN, 90*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: MariaDB not ready: %v\n", err)
		return 1
	}

	// 5. Seed the database.
	fmt.Println("SETUP: Seeding database...")
	if err := seedDatabase(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed database: %v\n", err)
		return 1
	}

	// 6. Write mori.yaml.
	fmt.Println("SETUP: Writing mori.yaml...")
	cfg := config.NewProjectConfig()
	cfg.AddConnection("e2e", &config.Connection{
		Engine:   "mariadb",
		Provider: "direct",
		Host:     "127.0.0.1",
		Port:     prodPort,
		User:     dbUser,
		Password: dbPass,
		Database: dbName,
	})
	if err := config.WriteProjectConfig(projectDir, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write mori.yaml: %v\n", err)
		return 1
	}

	// 7. Start mori proxy.
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 8. Wait for proxy to accept MySQL connections.
	fmt.Println("SETUP: Waiting for proxy to be ready (may take a few minutes for Shadow init)...")
	if err := waitForMariaDB(proxyDSN, 360*time.Second); err != nil {
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

// startProdContainer launches the MariaDB Docker container.
func startProdContainer(ctx context.Context) error {
	// Remove any existing container with the same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"-e", "MARIADB_ROOT_PASSWORD="+dbPass,
		"-e", "MARIADB_DATABASE="+dbName,
		"-p", fmt.Sprintf("127.0.0.1:%d:3306", prodPort),
		"mariadb:11",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// stopProdContainer removes the Docker container.
func stopProdContainer() {
	exec.Command("docker", "rm", "-f", containerName).Run()
}

// waitForMariaDB polls until a connection succeeds or timeout.
func waitForMariaDB(dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			err = db.Ping()
			db.Close()
			if err == nil {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for MariaDB at %s", dsn)
}

// seedDatabase reads seed.sql and executes it against the E2E database.
// MariaDB seed uses stored procedures with DELIMITER, so we use the mysql
// client inside the container to execute it.
func seedDatabase(ctx context.Context) error {
	seedPath := filepath.Join(repoRoot, "e2e", "mariadb", "seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		return fmt.Errorf("reading seed.sql: %w", err)
	}

	// Use docker exec to run the seed via the mariadb client, which supports
	// DELIMITER and stored procedures.
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", containerName,
		"mariadb", "-uroot", "-p"+dbPass, dbName)
	cmd.Stdin = strings.NewReader(string(seedSQL))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// startMoriProxy starts the mori proxy in the background.
func startMoriProxy(ctx context.Context) error {
	moriProcess = exec.CommandContext(ctx, moriBin, "start", "e2e",
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
