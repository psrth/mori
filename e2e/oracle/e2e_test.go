//go:build e2e_oracle

package e2e_oracle

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

	"github.com/mori-dev/mori/internal/core/config"
	_ "github.com/sijms/go-ora/v2"
)

// Global test state — set by TestMain, used by all tests.
var (
	prodDSN       string
	proxyDSN      string
	proxyPort     = 19004
	prodPort      = 15521
	dbUser        = "system"
	dbPass        = "mori"
	serviceName   = "XEPDB1"
	containerName = "mori-e2e-oracle-prod"
	projectDir    string // Temp directory used as mori project root.
	moriBin       string // Path to the compiled mori binary.
	moriProcess   *exec.Cmd
	repoRoot      string // Path to the mori source repo.
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	// Determine the repo root (parent of e2e/oracle/).
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// 1. Create a temp project directory.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-oracle-*")
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

	// 3. Start Oracle Docker container.
	fmt.Println("SETUP: Starting Oracle XE container...")
	if err := startProdContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start Oracle container: %v\n", err)
		return 1
	}
	defer stopProdContainer()

	// 4. Wait for Oracle to accept connections (Oracle XE is slow to start).
	prodDSN = fmt.Sprintf("oracle://%s:%s@127.0.0.1:%d/%s", dbUser, dbPass, prodPort, serviceName)
	proxyDSN = fmt.Sprintf("oracle://%s:%s@127.0.0.1:%d/%s", dbUser, dbPass, proxyPort, serviceName)

	fmt.Println("SETUP: Waiting for Oracle to be ready (this may take a while)...")
	if err := waitForOracle(prodDSN, 300*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: Oracle not ready: %v\n", err)
		return 1
	}

	// 5. Seed the database.
	fmt.Println("SETUP: Seeding Oracle database...")
	if err := seedDatabase(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed database: %v\n", err)
		return 1
	}

	// 6. Write mori.yaml programmatically.
	fmt.Println("SETUP: Writing mori.yaml...")
	if err := writeMoriYAML(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write mori.yaml: %v\n", err)
		return 1
	}

	// 7. Start mori proxy (will trigger engine Init on first run).
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 8. Wait for proxy to accept connections.
	// Allow extra time because engine Init creates a Shadow container
	// and Oracle under emulation (ARM) is slow to start.
	fmt.Println("SETUP: Waiting for proxy to be ready (may take a few minutes for Shadow init)...")
	if err := waitForOracle(proxyDSN, 540*time.Second); err != nil {
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

// startProdContainer launches an Oracle XE Docker container to serve as Prod.
func startProdContainer(ctx context.Context) error {
	// Remove any existing container with the same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"-e", "ORACLE_PASSWORD="+dbPass,
		"-p", fmt.Sprintf("127.0.0.1:%d:1521", prodPort),
		"gvenzl/oracle-xe:21-slim",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// stopProdContainer removes the Docker Oracle container.
func stopProdContainer() {
	exec.Command("docker", "rm", "-f", containerName).Run()
}

// waitForOracle polls until an Oracle connection succeeds or timeout.
func waitForOracle(dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("oracle", dsn)
		if err == nil {
			err = db.Ping()
			db.Close()
			if err == nil {
				return nil
			}
		}
		time.Sleep(2 * time.Second) // Oracle is slow, wait longer between attempts.
	}
	return fmt.Errorf("timeout waiting for Oracle at %s", dsn)
}

// seedDatabase executes the seed.sql file against the Prod Oracle database.
func seedDatabase(ctx context.Context) error {
	seedPath := filepath.Join(repoRoot, "e2e", "oracle", "seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		return fmt.Errorf("reading seed.sql: %w", err)
	}

	db, err := sql.Open("oracle", prodDSN)
	if err != nil {
		return fmt.Errorf("connecting to Oracle: %w", err)
	}
	defer db.Close()

	// Split on lines containing only "/" (the PL/SQL block terminator).
	blocks := splitOracleBlocks(string(seedSQL))
	for i, block := range blocks {
		block = strings.TrimSpace(block)
		if block == "" || block == "COMMIT" {
			continue
		}
		// Remove trailing semicolons for standalone statements (not PL/SQL blocks).
		// Check if the block contains BEGIN/DECLARE (not just starts with, since
		// comments may precede the PL/SQL keyword).
		upperBlock := strings.ToUpper(block)
		isPLSQL := strings.Contains(upperBlock, "BEGIN") || strings.Contains(upperBlock, "DECLARE")
		if !isPLSQL {
			block = strings.TrimRight(block, ";")
			block = strings.TrimSpace(block)
		}
		if _, err := db.ExecContext(ctx, block); err != nil {
			return fmt.Errorf("executing block %d: %w\nSQL: %s", i, err, truncSQL(block))
		}
	}
	return nil
}

// splitOracleBlocks splits Oracle SQL on lines containing only "/".
func splitOracleBlocks(sql string) []string {
	var blocks []string
	var current strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "/" {
			if current.Len() > 0 {
				blocks = append(blocks, current.String())
				current.Reset()
			}
			continue
		}
		if current.Len() > 0 {
			current.WriteByte('\n')
		}
		current.WriteString(line)
	}
	if current.Len() > 0 {
		blocks = append(blocks, current.String())
	}
	return blocks
}

// writeMoriYAML writes a mori.yaml file for the Oracle connection.
func writeMoriYAML() error {
	cfg := config.NewProjectConfig()
	cfg.AddConnection("e2e", &config.Connection{
		Engine:   "oracle",
		Provider: "direct",
		Host:     "127.0.0.1",
		Port:     prodPort,
		User:     dbUser,
		Password: dbPass,
		Extra:    map[string]string{"service_name": serviceName},
	})
	return config.WriteProjectConfig(projectDir, cfg)
}

// startMoriProxy starts the mori proxy in the background.
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
