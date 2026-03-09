//go:build e2e_sqlite

package e2e_sqlite

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

	"github.com/jackc/pgx/v5"
	"github.com/mori-dev/mori/internal/core/config"
	_ "modernc.org/sqlite"
)

// Global test state set by TestMain, used by all tests.
var (
	proxyDSN   string
	prodDBPath string // Absolute path to the prod SQLite file.
	proxyPort  = 19005
	projectDir string // Temp directory used as mori project root.
	moriBin    string // Path to the compiled mori binary.
	moriProcess *exec.Cmd
	repoRoot   string // Path to the mori source repo.
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	// Determine the repo root (parent of e2e/sqlite/).
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Create a temp project directory.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-sqlite-*")
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

	// 3. Create prod SQLite database file.
	fmt.Println("SETUP: Creating prod SQLite database...")
	prodDBPath = filepath.Join(projectDir, "prod.db")
	if err := createAndSeedProdDB(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to create/seed prod DB: %v\n", err)
		return 1
	}

	// 4. Write mori.yaml programmatically.
	fmt.Println("SETUP: Writing mori.yaml...")
	if err := writeMoriYAML(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write mori.yaml: %v\n", err)
		return 1
	}

	// 5. Start mori proxy (will trigger engine Init on first run).
	proxyDSN = fmt.Sprintf("postgres://any:any@127.0.0.1:%d/main?sslmode=disable", proxyPort)

	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 6. Wait for proxy to accept connections.
	fmt.Println("SETUP: Waiting for proxy to be ready...")
	if err := waitForProxy(ctx, proxyDSN, 60*time.Second); err != nil {
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

// createAndSeedProdDB creates the prod SQLite file, applies the DDL schema,
// and inserts seed data using Go loops.
func createAndSeedProdDB(ctx context.Context) error {
	db, err := sql.Open("sqlite", prodDBPath)
	if err != nil {
		return fmt.Errorf("open prod DB: %w", err)
	}
	defer db.Close()

	// Apply DDL from seed.sql.
	seedPath := filepath.Join(repoRoot, "e2e", "sqlite", "seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		return fmt.Errorf("reading seed.sql: %w", err)
	}
	if _, err := db.ExecContext(ctx, string(seedSQL)); err != nil {
		return fmt.Errorf("executing seed.sql: %w", err)
	}

	// Seed data via Go loops.
	if err := seedData(db); err != nil {
		return fmt.Errorf("seeding data: %w", err)
	}

	return nil
}

func seedData(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	// 100 users
	for i := 1; i <= 100; i++ {
		isActive := 1
		if i%10 == 0 {
			isActive = 0
		}
		_, err := tx.Exec(
			"INSERT INTO users (username, email, display_name, is_active) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("user_%d", i),
			fmt.Sprintf("user_%d@example.com", i),
			fmt.Sprintf("User Number %d", i),
			isActive,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert user %d: %w", i, err)
		}
	}

	// 10 roles
	roleNames := []string{"superadmin", "admin", "editor", "viewer", "moderator",
		"billing", "support", "developer", "analyst", "guest"}
	for _, name := range roleNames {
		_, err := tx.Exec("INSERT INTO roles (name, description) VALUES (?, ?)",
			name, fmt.Sprintf("Role: %s", name))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert role %s: %w", name, err)
		}
	}

	// 150 user_roles
	for i := 1; i <= 150; i++ {
		_, err := tx.Exec(
			"INSERT OR IGNORE INTO user_roles (user_id, role_id, granted_at) VALUES (?, ?, datetime('now'))",
			((i-1)%100)+1, ((i-1)%10)+1,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert user_role %d: %w", i, err)
		}
	}

	// 200 orders
	statuses := []string{"pending", "processing", "shipped", "completed", "cancelled"}
	for i := 1; i <= 200; i++ {
		_, err := tx.Exec(
			"INSERT INTO orders (user_id, status, total_amount) VALUES (?, ?, ?)",
			((i-1)%100)+1, statuses[i%5], 20.00+float64(i)*3.75,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert order %d: %w", i, err)
		}
	}

	// 50 products (text PK)
	for i := 1; i <= 50; i++ {
		_, err := tx.Exec(
			"INSERT INTO products (id, name, slug, price) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("prod-%08d-0000-0000-0000-%012d", i, i),
			fmt.Sprintf("Product %d", i),
			fmt.Sprintf("product-%d", i),
			10.00+float64(i)*1.50,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert product %d: %w", i, err)
		}
	}

	// 20 settings (no PK)
	settingsList := [][2]string{
		{"site.name", "Mori Test App"}, {"site.url", "https://test.example.com"},
		{"site.description", "E2E test application"}, {"mail.smtp_host", "smtp.example.com"},
		{"mail.smtp_port", "587"}, {"mail.from", "noreply@example.com"},
		{"auth.session_ttl", "3600"}, {"auth.max_attempts", "5"},
		{"auth.lockout_duration", "300"}, {"ui.theme", "dark"},
		{"ui.language", "en"}, {"ui.items_per_page", "25"},
		{"cache.ttl", "600"}, {"cache.driver", "redis"},
		{"log.level", "info"}, {"log.format", "json"},
		{"feature.dark_mode", "true"}, {"feature.beta_users", "false"},
		{"rate_limit.requests", "100"}, {"rate_limit.window", "60"},
	}
	for _, s := range settingsList {
		_, err := tx.Exec("INSERT INTO settings (key, value) VALUES (?, ?)", s[0], s[1])
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert setting %s: %w", s[0], err)
		}
	}

	return tx.Commit()
}

// writeMoriYAML writes a mori.yaml file for the SQLite connection.
func writeMoriYAML() error {
	cfg := config.NewProjectConfig()
	cfg.AddConnection("e2e", &config.Connection{
		Engine:   "sqlite",
		Provider: "direct",
		Extra:    map[string]string{"path": prodDBPath},
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

// waitForProxy polls until the pgwire proxy accepts connections.
func waitForProxy(ctx context.Context, dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		connCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		connCfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			cancel()
			return fmt.Errorf("parse proxy DSN: %w", err)
		}
		connCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(connCtx, connCfg)
		if err == nil {
			conn.Close(connCtx)
			cancel()
			return nil
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for proxy at %s", dsn)
}

// waitForPortFree waits until a TCP port is no longer in use.
func waitForPortFree(port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err != nil {
			return
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
