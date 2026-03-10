package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mori-dev/mori/internal/core/config"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/shadow"
	"github.com/mori-dev/mori/internal/registry"
	"github.com/mori-dev/mori/internal/ui"
)

// CRDBInit performs the Mori initialization sequence for CockroachDB.
// It replaces the PostgreSQL Init pipeline with CockroachDB-specific logic:
//   - Uses SHOW CREATE ALL TABLES instead of pg_dump (which is incompatible)
//   - Detects unique_rowid() SERIAL columns instead of nextval()
//   - Skips extension detection (CockroachDB has no extensions)
//   - Writes engine: "cockroachdb" to config
//   - Uses the PG-compat version reported by CockroachDB for the shadow image
func CRDBInit(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the production connection string (using CockroachDB default port).
	dsn, err := connstr.ParseWithDefaultPort(opts.ProdConnStr, crdbDefaultPort)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Connect to Prod.
	var prodConn *pgx.Conn
	if err := ui.Spinner("Connecting to CockroachDB...", func() error {
		var e error
		prodConn, e = pgx.Connect(ctx, dsn.ConnString())
		return e
	}); err != nil {
		return nil, fmt.Errorf("cannot connect to CockroachDB at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodConn.Close(ctx)

	// 3. Detect CockroachDB's PG-compat version for the shadow image.
	// CockroachDB reports a PostgreSQL-compatible version (e.g., "13.0.0").
	var versionStr string
	if err := prodConn.QueryRow(ctx, "SHOW server_version").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect CockroachDB version: %w", err)
	}
	version, err := schema.ParseVersion(versionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CockroachDB PG-compat version: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Connected — CockroachDB (PG-compat %s)", version.Full))

	// 4. Determine Docker image — use PG compat version for shadow.
	imageName := version.ImageTag
	if opts.ImageOverride != "" {
		imageName = opts.ImageOverride
	}

	// 5. Set up Shadow Docker container (PostgreSQL — schema-compatible shadow).
	mgr, err := shadow.NewManager()
	if err != nil {
		return nil, err
	}
	defer mgr.Close()

	if err := ui.Spinner(fmt.Sprintf("Checking Docker image %s...", imageName), func() error {
		if mgr.ImageExists(ctx, imageName) {
			return nil
		}
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

	// Cleanup on failure.
	var initErr error
	defer func() {
		if initErr != nil {
			ui.StepWarn("Cleaning up Shadow container...")
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		}
	}()

	ui.StepDone(fmt.Sprintf("Shadow container ready on port %d", containerInfo.HostPort))

	// 6. Dump production schema using SHOW CREATE ALL TABLES.
	var dumpResult *schema.DumpResult
	if err := ui.Spinner("Dumping CockroachDB schema...", func() error {
		var e error
		dumpResult, e = schema.CRDBFullDump(ctx, prodConn)
		return e
	}); err != nil {
		initErr = err
		return nil, err
	}
	ui.StepDone(fmt.Sprintf("%d tables dumped (CockroachDB — no extensions)", len(dumpResult.Tables)))

	// 7. Apply schema to Shadow (PostgreSQL).
	// No extensions to install for CockroachDB.
	shadowConnStr := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	if err := ui.Spinner("Applying schema to Shadow...", func() error {
		return schema.ApplyToShadow(ctx, shadowConnStr, dumpResult, &schema.ExtInstallOptions{
			ContainerID: containerInfo.ContainerID,
			PGMajor:     version.Major,
		})
	}); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}

	// 8. Persist configuration with engine: "cockroachdb".
	cfg := &config.Config{
		ProdConnection:  dsn.ConnString(),
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     imageName,
		Engine:          string(registry.CockroachDB),
		EngineVersion:   version.Full,
		ProxyPort:       9002,
		Extensions:      nil, // CockroachDB has no extensions
		InitializedAt:   time.Now(),
	}

	if err := config.WriteConnConfig(opts.ProjectRoot, opts.ConnName, cfg); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	moriDir := config.ConnDir(opts.ProjectRoot, opts.ConnName)
	if err := schema.WriteSequences(moriDir, dumpResult.Sequences); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write sequences: %w", err)
	}
	if err := schema.WriteTables(moriDir, dumpResult.Tables); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write tables: %w", err)
	}

	// Populate schema registry with FK metadata.
	if len(dumpResult.ForeignKeys) > 0 {
		reg := coreSchema.NewRegistry()
		for _, fk := range dumpResult.ForeignKeys {
			reg.RecordForeignKey(fk.ChildTable, fk)
		}
		if err := coreSchema.WriteRegistry(moriDir, reg); err != nil {
			initErr = err
			return nil, fmt.Errorf("failed to write schema registry: %w", err)
		}
	}

	return &InitResult{
		Config:    cfg,
		Container: containerInfo,
		Dump:      dumpResult,
	}, nil
}
