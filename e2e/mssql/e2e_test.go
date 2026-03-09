//go:build e2e_mssql

package e2e_mssql

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
	"syscall"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/mori-dev/mori/internal/core/config"
)

// Global test state -- set by TestMain, used by all tests.
var (
	prodDSN       string
	proxyDSN      string
	proxyPort     = 19003
	prodPort      = 15433
	mcpPort       = 19025
	dbName        = "mori_e2e_mssql"
	dbUser        = "sa"
	dbPass        = "Mori_P@ss1"
	containerName = "mori-e2e-mssql-prod"
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
	projectDir, err = os.MkdirTemp("", "mori-e2e-mssql-*")
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

	// 3. Start MSSQL Docker container.
	fmt.Println("SETUP: Starting MSSQL container...")
	if err := startProdContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start MSSQL container: %v\n", err)
		return 1
	}
	defer stopProdContainer()

	// 4. Wait for MSSQL to accept connections on master.
	masterDSN := fmt.Sprintf("sqlserver://%s:%s@127.0.0.1:%d?database=master", dbUser, dbPass, prodPort)
	fmt.Println("SETUP: Waiting for MSSQL to be ready...")
	if err := waitForMSSQL(masterDSN, 90*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: MSSQL not ready: %v\n", err)
		return 1
	}

	// 5. Create the E2E database.
	fmt.Println("SETUP: Creating database...")
	if err := createDatabase(ctx, masterDSN); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to create database: %v\n", err)
		return 1
	}

	// 6. Seed the database.
	prodDSN = fmt.Sprintf("sqlserver://%s:%s@127.0.0.1:%d?database=%s", dbUser, dbPass, prodPort, dbName)
	proxyDSN = fmt.Sprintf("sqlserver://%s:%s@127.0.0.1:%d?database=%s", dbUser, dbPass, proxyPort, dbName)

	fmt.Println("SETUP: Seeding database...")
	if err := seedDatabase(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed database: %v\n", err)
		return 1
	}

	// 7. Write mori.yaml.
	fmt.Println("SETUP: Writing mori.yaml...")
	cfg := config.NewProjectConfig()
	cfg.AddConnection("e2e", &config.Connection{
		Engine:   "mssql",
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

	// 8. Start mori proxy (triggers engine Init on first run).
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 9. Wait for proxy to accept TDS connections.
	// Allow extra time because engine Init creates a Shadow container
	// and MSSQL under emulation (ARM) is slow to start.
	fmt.Println("SETUP: Waiting for proxy to be ready (may take a few minutes for Shadow init)...")
	if err := waitForMSSQL(proxyDSN, 360*time.Second); err != nil {
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

// startProdContainer launches the MSSQL Docker container.
func startProdContainer(ctx context.Context) error {
	// Remove any existing container with the same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"-e", "ACCEPT_EULA=Y",
		"-e", "MSSQL_SA_PASSWORD="+dbPass,
		"--memory=2g",
		"-p", fmt.Sprintf("127.0.0.1:%d:1433", prodPort),
		"mcr.microsoft.com/mssql/server:2022-latest",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// stopProdContainer removes the Docker container.
func stopProdContainer() {
	exec.Command("docker", "rm", "-f", containerName).Run()
}

// waitForMSSQL polls until a connection succeeds or timeout.
func waitForMSSQL(dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("sqlserver", dsn)
		if err == nil {
			err = db.Ping()
			db.Close()
			if err == nil {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for MSSQL at %s", dsn)
}

// createDatabase creates the E2E database on the MSSQL instance.
func createDatabase(ctx context.Context, masterDSN string) error {
	db, err := sql.Open("sqlserver", masterDSN)
	if err != nil {
		return fmt.Errorf("connecting to master: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		return fmt.Errorf("CREATE DATABASE: %w", err)
	}
	return nil
}

// seedDatabase reads seed.sql and executes it against the E2E database.
// T-SQL uses GO as a batch separator; we split on it and execute each batch.
func seedDatabase(ctx context.Context) error {
	seedPath := filepath.Join(repoRoot, "e2e", "mssql", "seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		return fmt.Errorf("reading seed.sql: %w", err)
	}

	db, err := sql.Open("sqlserver", prodDSN)
	if err != nil {
		return fmt.Errorf("connecting to prod: %w", err)
	}
	defer db.Close()

	// Split on GO batch separator (case-insensitive, must be on its own line).
	batches := splitBatches(string(seedSQL))
	for i, batch := range batches {
		batch = strings.TrimSpace(batch)
		if batch == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, batch); err != nil {
			return fmt.Errorf("batch %d: %w\nSQL: %s", i+1, err, truncSQL(batch))
		}
	}
	return nil
}

// splitBatches splits a T-SQL script on GO batch separators.
func splitBatches(script string) []string {
	var batches []string
	var current strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.EqualFold(trimmed, "GO") {
			batches = append(batches, current.String())
			current.Reset()
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		batches = append(batches, s)
	}
	return batches
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
	if moriProcess == nil || moriProcess.Process == nil {
		return
	}
	moriProcess.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- moriProcess.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		moriProcess.Process.Kill()
		<-done
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
