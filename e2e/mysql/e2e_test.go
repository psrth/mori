//go:build e2e_mysql

package e2e_mysql

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

	_ "github.com/go-sql-driver/mysql"
	"github.com/psrth/mori/internal/core/config"
)

// Global test state -- set by TestMain, used by all tests.
var (
	prodDSN       string
	proxyDSN      string
	proxyPort     = 19021
	prodPort      = 15435
	mcpPort       = 19023
	dbName        = "mori_test"
	dbUser        = "root"
	dbPass        = "moripass"
	containerName = "mori-e2e-mysql-prod"
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
	projectDir, err = os.MkdirTemp("", "mori-e2e-mysql-*")
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

	// 3. Start MySQL Docker container.
	fmt.Println("SETUP: Starting MySQL container...")
	if err := startProdContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start MySQL container: %v\n", err)
		return 1
	}
	defer stopProdContainer()

	// 4. Wait for MySQL to accept connections.
	prodDSN = fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s?multiStatements=true", dbUser, dbPass, prodPort, dbName)
	proxyDSN = fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s?interpolateParams=true", dbUser, dbPass, proxyPort, dbName)

	fmt.Println("SETUP: Waiting for MySQL to be ready...")
	if err := waitForMySQL(prodDSN, 90*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: MySQL not ready: %v\n", err)
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
		Engine:   "mysql",
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

	// 7. Start mori proxy (triggers engine Init on first run).
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 8. Wait for proxy to accept MySQL connections.
	// Allow extra time because engine Init creates a Shadow container.
	// The 5s sleep lets the shadow MySQL process stabilize after Init
	// applies the schema and before the proxy opens its first connection.
	fmt.Println("SETUP: Waiting for proxy to be ready (may take a few minutes for Shadow init)...")
	time.Sleep(5 * time.Second)

	if err := waitForMySQL(proxyDSN, 360*time.Second); err != nil {
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

// startProdContainer launches the MySQL Docker container.
func startProdContainer(ctx context.Context) error {
	// Remove any existing container with the same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"-e", "MYSQL_ROOT_PASSWORD="+dbPass,
		"-e", "MYSQL_DATABASE="+dbName,
		"-p", fmt.Sprintf("127.0.0.1:%d:3306", prodPort),
		"mysql:8.0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// stopProdContainer removes the Docker container.
func stopProdContainer() {
	exec.Command("docker", "rm", "-f", containerName).Run()
}

// waitForMySQL polls until a connection succeeds or timeout.
func waitForMySQL(dsn string, timeout time.Duration) error {
	// Add per-connection timeouts to the DSN to prevent hanging.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	timedDSN := dsn + sep + "timeout=5s&readTimeout=5s&writeTimeout=5s"

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", timedDSN)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(ctx)
		cancel()
		db.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for MySQL at %s (last error: %v)", dsn, lastErr)
}

// seedDatabase reads seed.sql and also programmatically generates loop-based data.
func seedDatabase(ctx context.Context) error {
	db, err := sql.Open("mysql", prodDSN)
	if err != nil {
		return fmt.Errorf("connecting to prod: %w", err)
	}
	defer db.Close()

	// Execute seed.sql (DDL + static inserts).
	seedPath := filepath.Join(repoRoot, "e2e", "mysql", "seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		return fmt.Errorf("reading seed.sql: %w", err)
	}

	// Split on semicolons and execute each statement.
	stmts := splitStatements(string(seedSQL))
	for i, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("statement %d: %w\nSQL: %s", i+1, err, truncSQL(stmt))
		}
	}

	// Programmatic seed: users (100 rows).
	for i := 1; i <= 100; i++ {
		isActive := 1
		if i%10 == 0 {
			isActive = 0
		}
		_, err := db.ExecContext(ctx,
			"INSERT INTO users (username, email, display_name, is_active) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("user_%d", i),
			fmt.Sprintf("user_%d@example.com", i),
			fmt.Sprintf("User Number %d", i),
			isActive,
		)
		if err != nil {
			return fmt.Errorf("seeding user %d: %w", i, err)
		}
	}

	// Programmatic seed: user_roles (~150 rows).
	for j := 1; j <= 150; j++ {
		userID := ((j - 1) % 100) + 1
		roleID := ((j - 1) % 10) + 1
		// INSERT IGNORE to skip duplicate composite PKs.
		_, err := db.ExecContext(ctx,
			"INSERT IGNORE INTO user_roles (user_id, role_id) VALUES (?, ?)",
			userID, roleID,
		)
		if err != nil {
			return fmt.Errorf("seeding user_role %d: %w", j, err)
		}
	}

	// Programmatic seed: orders (200 rows).
	statuses := []string{"pending", "shipped", "processing", "cancelled", "completed"}
	for k := 1; k <= 200; k++ {
		status := statuses[k%5]
		amount := 20.00 + float64(k)*3.75
		_, err := db.ExecContext(ctx,
			"INSERT INTO orders (user_id, status, total_amount) VALUES (?, ?, ?)",
			((k-1)%100)+1, status, amount,
		)
		if err != nil {
			return fmt.Errorf("seeding order %d: %w", k, err)
		}
	}

	// Programmatic seed: products (50 rows with UUID).
	for m := 1; m <= 50; m++ {
		price := 10.00 + float64(m)*1.50
		_, err := db.ExecContext(ctx,
			"INSERT INTO products (id, name, slug, price) VALUES (UUID(), ?, ?, ?)",
			fmt.Sprintf("Product %d", m),
			fmt.Sprintf("product-%d", m),
			price,
		)
		if err != nil {
			return fmt.Errorf("seeding product %d: %w", m, err)
		}
	}

	return nil
}

// splitStatements splits a SQL script on semicolons.
// It handles simple cases; doesn't account for strings containing semicolons.
func splitStatements(script string) []string {
	var stmts []string
	var current strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		// Skip comment-only lines.
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			stmts = append(stmts, current.String())
			current.Reset()
		}
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		stmts = append(stmts, s)
	}
	return stmts
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
