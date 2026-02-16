package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/mysql/connstr"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
	"github.com/mori-dev/mori/internal/engine/mysql/shadow"
)

// InitOptions holds the options for initializing a MySQL Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
	ConnName    string
	Image       string // Docker image override (default: "mysql:8.0")
	EngineName  string // Engine name for config (default: "mysql")
}

// InitResult holds the result of a successful initialization.
type InitResult struct {
	Config    *config.Config
	Container *shadow.ContainerInfo
	Dump      *schema.DumpResult
}

// Init performs the complete Mori initialization sequence for MySQL.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the production connection string.
	dsn, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Connect to Prod.
	var prodDB *sql.DB
	if err := ui.Spinner("Connecting to production MySQL database...", func() error {
		var e error
		prodDB, e = sql.Open("mysql", dsn.GoDSN())
		if e != nil {
			return e
		}
		return prodDB.PingContext(ctx)
	}); err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodDB.Close()

	// 3. Detect MySQL version.
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect MySQL version: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Connected — MySQL %s", versionStr))

	// 4. Docker image — match production version, allow override.
	imageName := opts.Image
	if imageName == "" {
		imageName = matchDockerImage(versionStr, opts.EngineName)
	}

	// 5. Set up Docker container.
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, err
	}
	defer mgr.Close()

	if err := ui.Spinner(fmt.Sprintf("Pulling Docker image %s...", imageName), func() error {
		return mgr.Pull(ctx, imageName)
	}); err != nil {
		return nil, err
	}

	var containerInfo *shadow.ContainerInfo
	if err := ui.Spinner("Creating Shadow container...", func() error {
		var e error
		containerInfo, e = mgr.Create(ctx, shadow.ContainerConfig{
			Image:    imageName,
			DBName:   dsn.DBName,
			Password: "mori",
		})
		return e
	}); err != nil {
		return nil, fmt.Errorf("failed to create Shadow container: %w", err)
	}

	var initErr error
	defer func() {
		if initErr != nil {
			ui.StepWarn("Cleaning up Shadow container...")
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		}
	}()

	ui.StepDone(fmt.Sprintf("Shadow container ready on port %d", containerInfo.HostPort))

	// 6. Dump production schema.
	var schemaSQL string
	if err := ui.Spinner("Dumping production schema...", func() error {
		var e error
		schemaSQL, e = schema.DumpSchema(ctx, dsn, imageName)
		return e
	}); err != nil {
		initErr = err
		return nil, fmt.Errorf("schema dump failed: %w", err)
	}
	schemaSQL = schema.StripForeignKeys(schemaSQL)

	// 7. Detect table metadata.
	tables, err := schema.DetectTableMetadata(ctx, prodDB, dsn.DBName)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}
	ui.StepDone(fmt.Sprintf("%d tables dumped", len(tables)))

	// 8. Detect auto_increment offsets.
	offsets, err := schema.DetectAutoIncrementOffsets(ctx, prodDB, tables)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("auto_increment offset detection failed: %w", err)
	}

	// 9. Apply schema to Shadow.
	shadowDSN := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	shadowDB, err := sql.Open("mysql", shadowDSN)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to connect to Shadow: %w", err)
	}
	defer shadowDB.Close()

	// Wait for the Shadow database to accept TCP connections from the host.
	// mysqladmin ping (used by WaitReady) may succeed before the TCP listener
	// is fully ready for external connections.
	{
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			if err := shadowDB.PingContext(pingCtx); err == nil {
				pingCancel()
				break
			}
			pingCancel()
			time.Sleep(500 * time.Millisecond)
		}
	}

	if err := schema.ApplySchema(ctx, shadowDB, schemaSQL); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}

	// 10. Apply auto_increment offsets to Shadow.
	if err := schema.ApplyAutoIncrementOffsets(ctx, shadowDB, offsets); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply auto_increment offsets: %w", err)
	}

	// 11. Persist configuration.
	cfg := &config.Config{
		ProdConnection:  dsn.GoDSN(),
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     imageName,
		Engine:          engineName(opts.EngineName),
		EngineVersion:   versionStr,
		ProxyPort:       9002,
		InitializedAt:   time.Now(),
	}

	if err := config.WriteConnConfig(opts.ProjectRoot, opts.ConnName, cfg); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	moriDir := config.ConnDir(opts.ProjectRoot, opts.ConnName)
	if err := schema.WriteTables(moriDir, tables); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write tables: %w", err)
	}

	return &InitResult{
		Config:    cfg,
		Container: containerInfo,
		Dump: &schema.DumpResult{
			SchemaSQL: schemaSQL,
			Tables:    tables,
		},
	}, nil
}

// engineName returns the engine name to use in config, defaulting to "mysql".
func engineName(override string) string {
	if override != "" {
		return override
	}
	return "mysql"
}

// matchDockerImage selects a Docker image tag that matches the production
// MySQL/MariaDB version. For example, "8.0.35-0ubuntu0.22.04.1" -> "mysql:8.0"
// and "10.11.6-MariaDB" -> "mariadb:10.11".
func matchDockerImage(versionStr, engine string) string {
	versionStr = strings.TrimSpace(versionStr)
	lower := strings.ToLower(versionStr)

	// Detect MariaDB from version string (e.g., "10.11.6-MariaDB-1:10.11.6+maria~ubu2204").
	if strings.Contains(lower, "mariadb") || engine == "mariadb" {
		major, minor := parseMajorMinor(versionStr)
		if major > 0 {
			return fmt.Sprintf("mariadb:%d.%d", major, minor)
		}
		return "mariadb:11"
	}

	// MySQL: parse major.minor.
	major, minor := parseMajorMinor(versionStr)
	if major > 0 {
		return fmt.Sprintf("mysql:%d.%d", major, minor)
	}
	return "mysql:8.0"
}

// parseMajorMinor extracts the major and minor version numbers from a version string.
func parseMajorMinor(version string) (int, int) {
	// Strip leading non-digit characters.
	start := 0
	for start < len(version) && (version[start] < '0' || version[start] > '9') {
		start++
	}
	version = version[start:]

	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return 0, 0
	}

	major := parseDigits(parts[0])
	minor := parseDigits(parts[1])
	return major, minor
}

// parseDigits extracts a leading integer from a string.
func parseDigits(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}
