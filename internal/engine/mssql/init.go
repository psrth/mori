package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/mssql/connstr"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/engine/mssql/shadow"
)

// InitOptions holds the options for initializing an MSSQL Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
	ConnName    string
}

// InitResult holds the result of a successful initialization.
type InitResult struct {
	Config    *config.Config
	Container *shadow.ContainerInfo
	Dump      *schema.DumpResult
}

// Init performs the complete Mori initialization sequence for MSSQL.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the production connection string.
	dsn, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Connect to Prod.
	var prodDB *sql.DB
	if err := ui.Spinner("Connecting to production MSSQL database...", func() error {
		var e error
		prodDB, e = sql.Open("sqlserver", dsn.GoDSN())
		if e != nil {
			return e
		}
		return prodDB.PingContext(ctx)
	}); err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodDB.Close()

	// 3. Detect MSSQL version.
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect MSSQL version: %w", err)
	}
	shortVersion := extractShortVersion(versionStr)
	ui.StepDone(fmt.Sprintf("Connected — MSSQL %s", shortVersion))

	// 4. Docker image — match Prod version for Shadow.
	imageName := versionToImage(versionStr)

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
			Password: "Mori_P@ss1",
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

	// 6. Dump production schema (via SQL queries, not a dump tool).
	var schemaSQL string
	if err := ui.Spinner("Dumping production schema...", func() error {
		var e error
		schemaSQL, e = schema.DumpSchema(ctx, prodDB, dsn.DBName)
		return e
	}); err != nil {
		initErr = err
		return nil, fmt.Errorf("schema dump failed: %w", err)
	}
	schemaSQL = schema.StripForeignKeys(schemaSQL)

	// 7. Detect table metadata.
	tables, err := schema.DetectTableMetadata(ctx, prodDB)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}
	ui.StepDone(fmt.Sprintf("%d tables dumped", len(tables)))

	// 8. Detect identity offsets.
	offsets, err := schema.DetectIdentityOffsets(ctx, prodDB, tables)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("identity offset detection failed: %w", err)
	}

	// 9. Apply schema to Shadow.
	shadowDSNStr := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	shadowDB, err := sql.Open("sqlserver", shadowDSNStr)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to connect to Shadow: %w", err)
	}
	defer shadowDB.Close()

	if err := schema.ApplySchema(ctx, shadowDB, schemaSQL); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}

	// 10. Apply identity offsets to Shadow.
	if err := schema.ApplyIdentityOffsets(ctx, shadowDB, offsets); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply identity offsets: %w", err)
	}

	// 11. Persist configuration.
	cfg := &config.Config{
		ProdConnection:  dsn.GoDSN(),
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     imageName,
		Engine:          "mssql",
		EngineVersion:   shortVersion,
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

// versionToImage maps the @@VERSION output to the appropriate Docker image tag.
// Supported versions: 2017, 2019, 2022. Falls back to 2022-latest.
func versionToImage(versionStr string) string {
	const baseImage = "mcr.microsoft.com/mssql/server"

	upper := strings.ToUpper(versionStr)

	switch {
	case strings.Contains(upper, "2017"):
		return baseImage + ":2017-latest"
	case strings.Contains(upper, "2019"):
		return baseImage + ":2019-latest"
	case strings.Contains(upper, "2022"):
		return baseImage + ":2022-latest"
	default:
		// Default to 2022 for unknown versions.
		return baseImage + ":2022-latest"
	}
}

// extractShortVersion extracts a concise version string from @@VERSION output.
func extractShortVersion(fullVersion string) string {
	// @@VERSION returns something like:
	// "Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4120.1 (X64) ..."
	// We want just "2022 (16.0.4120.1)" or similar.
	lines := splitLines(fullVersion)
	if len(lines) > 0 {
		line := lines[0]
		if len(line) > 80 {
			line = line[:80]
		}
		return line
	}
	if len(fullVersion) > 80 {
		return fullVersion[:80]
	}
	return fullVersion
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
