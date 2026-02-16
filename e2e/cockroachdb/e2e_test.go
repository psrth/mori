//go:build e2e_cockroachdb

package e2e_cockroachdb

import (
	"context"
	"encoding/json"
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
	prodDSN          string
	proxyDSN         string
	proxyPort        = 19020
	prodPort         = 15434
	mcpPort          = 19018
	dbName           = "defaultdb"
	dbUser           = "root"
	containerName    = "mori-e2e-crdb-prod"
	shadowContainer  = "mori-e2e-crdb-shadow"
	shadowPort       = 15440
	projectDir       string // Temp directory used as mori project root
	moriBin          string // Path to the compiled mori binary
	moriProcess      *exec.Cmd
	repoRoot         string // Path to the mori source repo
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	// Determine the repo root (parent of e2e/cockroachdb/).
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Create a temp project directory for .mori/.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-crdb-*")
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

	// 3. Start Prod CockroachDB container.
	fmt.Println("SETUP: Starting Prod CockroachDB container...")
	if err := startProdContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start Prod container: %v\n", err)
		return 1
	}
	defer stopContainer(containerName)

	// 4. Wait for Prod to accept connections.
	prodDSN = fmt.Sprintf("postgres://%s@127.0.0.1:%d/%s?sslmode=disable", dbUser, prodPort, dbName)
	proxyDSN = fmt.Sprintf("postgres://%s@127.0.0.1:%d/%s?sslmode=disable", dbUser, proxyPort, dbName)

	fmt.Println("SETUP: Waiting for CockroachDB to be ready...")
	if err := waitForDB(ctx, prodDSN, 90*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: Prod not ready: %v\n", err)
		return 1
	}

	// 5. Seed the Prod database.
	fmt.Println("SETUP: Seeding Prod database...")
	if err := seedProdDatabase(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed Prod: %v\n", err)
		return 1
	}

	// 6. Start Shadow PostgreSQL container manually.
	// (pg_dump doesn't work against CockroachDB, so we set up the shadow ourselves.)
	fmt.Println("SETUP: Starting Shadow PostgreSQL container...")
	if err := startShadowContainer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start Shadow container: %v\n", err)
		return 1
	}
	defer stopContainer(shadowContainer)

	shadowDSN := fmt.Sprintf("postgres://postgres:mori@127.0.0.1:%d/%s?sslmode=disable", shadowPort, dbName)
	fmt.Println("SETUP: Waiting for Shadow to be ready...")
	if err := waitForDB(ctx, shadowDSN, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: Shadow not ready: %v\n", err)
		return 1
	}

	// 7. Apply schema (tables only, no data) to Shadow.
	fmt.Println("SETUP: Applying schema to Shadow...")
	if err := applyShadowSchema(ctx, shadowDSN); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to apply schema to Shadow: %v\n", err)
		return 1
	}

	// 8. Write mori.yaml and .mori/ config files.
	fmt.Println("SETUP: Writing mori config...")
	if err := writeMoriConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write mori.yaml: %v\n", err)
		return 1
	}
	if err := writeRuntimeConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write runtime config: %v\n", err)
		return 1
	}

	// 9. Start mori proxy in background.
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 10. Wait for proxy to accept connections.
	fmt.Println("SETUP: Waiting for proxy to be ready...")
	if err := waitForDB(ctx, proxyDSN, 120*time.Second); err != nil {
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

// startProdContainer launches a Docker CockroachDB container to serve as Prod.
func startProdContainer(ctx context.Context) error {
	exec.CommandContext(ctx, "docker", "rm", "-f", containerName).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", containerName,
		"-p", fmt.Sprintf("127.0.0.1:%d:26257", prodPort),
		"cockroachdb/cockroach:latest",
		"start-single-node", "--insecure",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// startShadowContainer launches a PostgreSQL container to serve as the Shadow.
func startShadowContainer(ctx context.Context) error {
	exec.CommandContext(ctx, "docker", "rm", "-f", shadowContainer).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", shadowContainer,
		"-e", "POSTGRES_PASSWORD=mori",
		"-e", "POSTGRES_DB="+dbName,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
		"-p", fmt.Sprintf("127.0.0.1:%d:5432", shadowPort),
		"postgres:16",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// stopContainer removes a Docker container by name.
func stopContainer(name string) {
	exec.Command("docker", "rm", "-f", name).Run()
}

// waitForDB polls until a pgx connection succeeds or timeout.
func waitForDB(ctx context.Context, dsn string, timeout time.Duration) error {
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
	return fmt.Errorf("timeout waiting for database at %s", dsn)
}

// seedProdDatabase executes the seed.sql file against the Prod container.
func seedProdDatabase(ctx context.Context) error {
	seedPath := filepath.Join(repoRoot, "e2e", "cockroachdb", "seed.sql")
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

// applyShadowSchema applies the table schema (no data) to the Shadow PostgreSQL.
// We use the same seed.sql DDL but skip the INSERT/seed data sections.
func applyShadowSchema(ctx context.Context, shadowDSN string) error {
	conn, err := pgx.Connect(ctx, shadowDSN)
	if err != nil {
		return fmt.Errorf("connecting to Shadow: %w", err)
	}
	defer conn.Close(ctx)

	// Create schema-only DDL adapted for PostgreSQL shadow.
	// FK constraints are OMITTED so that inserts referencing prod-only IDs don't fail.
	// Use gen_random_uuid() which is available in PG 13+ without extensions.
	schemaSQL := `
CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    username      TEXT NOT NULL UNIQUE,
    email         TEXT NOT NULL UNIQUE,
    display_name  TEXT,
    password_hash TEXT NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT true,
    login_count   INTEGER NOT NULL DEFAULT 0,
    metadata      JSONB DEFAULT '{}',
    tags          TEXT[] DEFAULT '{}',
    last_login_ip TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT now(),
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE user_profiles (
    id         BIGSERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL UNIQUE,
    bio        TEXT,
    avatar_url TEXT,
    location   TEXT,
    website    TEXT,
    birth_date DATE,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE organizations (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    slug        TEXT NOT NULL UNIQUE,
    description TEXT,
    website     TEXT,
    is_verified BOOLEAN NOT NULL DEFAULT false,
    settings    JSONB DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE org_members (
    org_id    INTEGER NOT NULL,
    user_id   INTEGER NOT NULL,
    role      TEXT NOT NULL DEFAULT 'member',
    joined_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, user_id)
);

CREATE TABLE roles (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    permissions TEXT[] DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE user_roles (
    user_id    INTEGER NOT NULL,
    role_id    INTEGER NOT NULL,
    granted_at TIMESTAMP NOT NULL DEFAULT now(),
    granted_by INTEGER,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    description TEXT,
    price       NUMERIC(10,2) NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    stock       INTEGER NOT NULL DEFAULT 0,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    attributes  JSONB DEFAULT '{}',
    tags        TEXT[] DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE categories (
    id        BIGSERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    slug      TEXT NOT NULL UNIQUE,
    parent_id INTEGER,
    depth     INTEGER NOT NULL DEFAULT 0,
    path      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'USD',
    shipping_addr   JSONB,
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL,
    product_id  UUID NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 1,
    unit_price  NUMERIC(10,2) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       BIGINT NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    currency       TEXT NOT NULL DEFAULT 'USD',
    method         TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    transaction_id TEXT UNIQUE,
    metadata       JSONB DEFAULT '{}',
    created_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE posts (
    id           BIGSERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    title        TEXT NOT NULL,
    body         TEXT NOT NULL,
    slug         TEXT NOT NULL UNIQUE,
    status       TEXT NOT NULL DEFAULT 'draft',
    view_count   BIGINT NOT NULL DEFAULT 0,
    metadata     JSONB DEFAULT '{}',
    published_at TIMESTAMP,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE comments (
    id         BIGSERIAL PRIMARY KEY,
    post_id    INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    parent_id  INTEGER,
    body       TEXT NOT NULL,
    is_edited  BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE tags (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE
);

CREATE TABLE post_tags (
    post_id INTEGER NOT NULL,
    tag_id  INTEGER NOT NULL,
    PRIMARY KEY (post_id, tag_id)
);

CREATE TABLE audit_log (
    id         BIGSERIAL PRIMARY KEY,
    user_id    INTEGER,
    action     TEXT NOT NULL,
    resource   TEXT NOT NULL,
    resource_id TEXT,
    details    JSONB DEFAULT '{}',
    ip_address TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE settings (
    key        TEXT NOT NULL,
    value      TEXT,
    category   TEXT NOT NULL DEFAULT 'general',
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE sessions (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    INTEGER NOT NULL,
    token      TEXT NOT NULL UNIQUE,
    user_agent TEXT,
    ip_address TEXT,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
`
	// Set sequence offsets so shadow IDs don't collide with prod.
	// CockroachDB SERIAL uses unique_rowid() which produces huge IDs,
	// but the prod data uses explicit sequential IDs 1..N.
	offsetSQL := `
SELECT setval('users_id_seq', 100000);
SELECT setval('user_profiles_id_seq', 100000);
SELECT setval('organizations_id_seq', 100000);
SELECT setval('roles_id_seq', 100000);
SELECT setval('categories_id_seq', 100000);
SELECT setval('orders_id_seq', 100000);
SELECT setval('order_items_id_seq', 100000);
SELECT setval('posts_id_seq', 100000);
SELECT setval('comments_id_seq', 100000);
SELECT setval('tags_id_seq', 100000);
SELECT setval('audit_log_id_seq', 200000);
`

	_, err = conn.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("applying schema DDL: %w", err)
	}

	_, err = conn.Exec(ctx, offsetSQL)
	if err != nil {
		return fmt.Errorf("setting sequence offsets: %w", err)
	}
	return nil
}

// writeMoriConfig writes a mori.yaml for CockroachDB directly.
func writeMoriConfig() error {
	yaml := fmt.Sprintf(`version: 1
connections:
    default:
        engine: cockroachdb
        host: 127.0.0.1
        port: %d
        user: %s
        password: ""
        database: %s
        ssl_mode: disable
`, prodPort, dbUser, dbName)
	return os.WriteFile(filepath.Join(projectDir, "mori.yaml"), []byte(yaml), 0600)
}

// writeRuntimeConfig creates .mori/config.json, tables.json, and sequences.json
// so that mori start skips the init flow (which would try pg_dump on CockroachDB).
func writeRuntimeConfig() error {
	moriDir := filepath.Join(projectDir, ".mori")
	if err := os.MkdirAll(filepath.Join(moriDir, "log"), 0755); err != nil {
		return err
	}

	// config.json
	cfg := map[string]any{
		"prod_connection":   prodDSN,
		"shadow_port":       shadowPort,
		"shadow_container":  shadowContainer,
		"shadow_image":      "postgres:16",
		"engine":            "cockroachdb",
		"engine_version":    "13.0",
		"proxy_port":        proxyPort,
		"extensions":        []string{},
		"initialized_at":    time.Now().Format(time.RFC3339),
		"active_connection": "default",
	}
	cfgData, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(filepath.Join(moriDir, "config.json"), cfgData, 0644); err != nil {
		return err
	}

	// tables.json — PK metadata for each table
	tables := map[string]map[string]any{
		"users":          {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"user_profiles":  {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"organizations":  {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"org_members":    {"pk_columns": []string{"org_id", "user_id"}, "pk_type": "composite"},
		"roles":          {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"user_roles":     {"pk_columns": []string{"user_id", "role_id"}, "pk_type": "composite"},
		"products":       {"pk_columns": []string{"id"}, "pk_type": "uuid"},
		"categories":     {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"orders":         {"pk_columns": []string{"id"}, "pk_type": "bigserial"},
		"order_items":    {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"payments":       {"pk_columns": []string{"id"}, "pk_type": "uuid"},
		"posts":          {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"comments":       {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"tags":           {"pk_columns": []string{"id"}, "pk_type": "serial"},
		"post_tags":      {"pk_columns": []string{"post_id", "tag_id"}, "pk_type": "composite"},
		"audit_log":      {"pk_columns": []string{"id"}, "pk_type": "bigserial"},
		"settings":       {"pk_columns": []string{}, "pk_type": "none"},
		"sessions":       {"pk_columns": []string{"id"}, "pk_type": "uuid"},
	}
	tablesData, _ := json.MarshalIndent(tables, "", "  ")
	if err := os.WriteFile(filepath.Join(moriDir, "tables.json"), tablesData, 0644); err != nil {
		return err
	}

	// sequences.json — offset data so shadow IDs don't collide with prod
	sequences := map[string]map[string]any{
		"users":          {"column": "id", "type": "serial", "prod_max": 500, "shadow_start": 100000},
		"user_profiles":  {"column": "id", "type": "serial", "prod_max": 500, "shadow_start": 100000},
		"organizations":  {"column": "id", "type": "serial", "prod_max": 50, "shadow_start": 100000},
		"roles":          {"column": "id", "type": "serial", "prod_max": 10, "shadow_start": 100000},
		"categories":     {"column": "id", "type": "serial", "prod_max": 30, "shadow_start": 100000},
		"orders":         {"column": "id", "type": "bigserial", "prod_max": 2000, "shadow_start": 100000},
		"order_items":    {"column": "id", "type": "serial", "prod_max": 5000, "shadow_start": 100000},
		"posts":          {"column": "id", "type": "serial", "prod_max": 1000, "shadow_start": 100000},
		"comments":       {"column": "id", "type": "serial", "prod_max": 3000, "shadow_start": 100000},
		"tags":           {"column": "id", "type": "serial", "prod_max": 50, "shadow_start": 100000},
		"audit_log":      {"column": "id", "type": "bigserial", "prod_max": 50000, "shadow_start": 200000},
	}
	seqData, _ := json.MarshalIndent(sequences, "", "  ")
	if err := os.WriteFile(filepath.Join(moriDir, "sequences.json"), seqData, 0644); err != nil {
		return err
	}

	return nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, moriBin, "stop")
	cmd.Dir = projectDir
	if out, err := cmd.CombinedOutput(); err != nil {
		if moriProcess != nil && moriProcess.Process != nil {
			moriProcess.Process.Kill()
			moriProcess.Wait()
		}
		t.Logf("mori stop output: %s (err: %v)", string(out), err)
	}

	time.Sleep(1 * time.Second)
	waitForPortFree(proxyPort, 10*time.Second)

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

	if err := waitForDB(ctx, proxyDSN, 30*time.Second); err != nil {
		t.Fatalf("proxy not ready after restart: %v", err)
	}
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
