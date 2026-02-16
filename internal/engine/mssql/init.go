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
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/engine/mssql/shadow"
)

// InitOptions holds the options for initializing an MSSQL Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
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
	fmt.Println("Connecting to production MSSQL database...")
	prodDB, err := sql.Open("sqlserver", dsn.GoDSN())
	if err != nil {
		return nil, fmt.Errorf("cannot open connection to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodDB.Close()

	if err := prodDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}

	// 3. Detect MSSQL version.
	fmt.Println("Detecting MSSQL version...")
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect MSSQL version: %w", err)
	}
	// Extract a shorter version string.
	shortVersion := extractShortVersion(versionStr)
	fmt.Printf("  MSSQL %s detected\n", shortVersion)

	// 4. Docker image — match Prod version for Shadow.
	imageName := versionToImage(versionStr)

	// 5. Set up Docker container.
	fmt.Printf("Pulling Docker image %s...\n", imageName)
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, err
	}
	defer mgr.Close()

	if err := mgr.Pull(ctx, imageName); err != nil {
		return nil, err
	}

	fmt.Println("Creating Shadow container...")
	containerInfo, err := mgr.Create(ctx, shadow.ContainerConfig{
		Image:    imageName,
		DBName:   dsn.DBName,
		Password: "Mori_P@ss1",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Shadow container: %w", err)
	}

	var initErr error
	defer func() {
		if initErr != nil {
			fmt.Println("Cleaning up Shadow container due to error...")
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		}
	}()

	fmt.Printf("  Shadow container %s running on port %d\n", containerInfo.ContainerName, containerInfo.HostPort)

	// 6. Dump production schema (via SQL queries, not a dump tool).
	fmt.Println("Dumping production schema...")
	schemaSQL, err := schema.DumpSchema(ctx, prodDB, dsn.DBName)
	if err != nil {
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
	fmt.Printf("  %d tables\n", len(tables))

	// 8. Detect identity offsets.
	offsets, err := schema.DetectIdentityOffsets(ctx, prodDB, tables)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("identity offset detection failed: %w", err)
	}

	// 9. Apply schema to Shadow.
	shadowDSNStr := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	fmt.Println("Applying schema to Shadow...")
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
	fmt.Println("Persisting configuration...")
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

	if err := config.WriteConfig(opts.ProjectRoot, cfg); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	moriDir := config.MoriDirPath(opts.ProjectRoot)
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
