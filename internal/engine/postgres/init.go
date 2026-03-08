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
	"github.com/mori-dev/mori/internal/ui"
)

// InitOptions holds the options for initializing a Mori project.
type InitOptions struct {
	ProdConnStr   string // Required: production connection string
	ImageOverride string // Optional: Docker image override
	ProjectRoot   string // Required: path to the project root directory
	ConnName      string // Required: connection name for per-connection state
}

// InitResult holds the result of a successful initialization.
type InitResult struct {
	Config    *config.Config
	Container *shadow.ContainerInfo
	Dump      *schema.DumpResult
}

// Init performs the complete Mori initialization sequence.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the production connection string
	dsn, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Connect to Prod
	var prodConn *pgx.Conn
	if err := ui.Spinner("Connecting to production database...", func() error {
		var e error
		prodConn, e = pgx.Connect(ctx, dsn.ConnString())
		return e
	}); err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodConn.Close(ctx)

	// 3. Detect PostgreSQL version
	var versionStr string
	if err := prodConn.QueryRow(ctx, "SHOW server_version").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect PostgreSQL version: %w", err)
	}
	version, err := schema.ParseVersion(versionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL version: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Connected — PostgreSQL %s", version.Full))

	// 4. Determine Docker image
	imageName := version.ImageTag
	if opts.ImageOverride != "" {
		imageName = opts.ImageOverride
	}

	// 5. Set up Docker container
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

	// Cleanup on failure: remove container if any subsequent step fails
	var initErr error
	defer func() {
		if initErr != nil {
			ui.StepWarn("Cleaning up Shadow container...")
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		}
	}()

	ui.StepDone(fmt.Sprintf("Shadow container ready on port %d", containerInfo.HostPort))

	// 6. Dump production schema
	var dumpResult *schema.DumpResult
	if err := ui.Spinner("Dumping production schema...", func() error {
		var e error
		dumpResult, e = schema.FullDump(ctx, prodConn, dsn, imageName)
		return e
	}); err != nil {
		initErr = err
		return nil, err
	}
	ui.StepDone(fmt.Sprintf("%d tables, %d extensions dumped", len(dumpResult.Tables), len(dumpResult.Extensions)))

	// 7. Apply schema to Shadow
	shadowConnStr := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	extOpts := &schema.ExtInstallOptions{
		ContainerID: containerInfo.ContainerID,
		PGMajor:     version.Major,
	}
	if err := ui.Spinner("Applying schema to Shadow...", func() error {
		return schema.ApplyToShadow(ctx, shadowConnStr, dumpResult, extOpts)
	}); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}

	// 8. Persist configuration
	cfg := &config.Config{
		ProdConnection:  dsn.ConnString(),
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     imageName,
		Engine:          "postgres",
		EngineVersion:   version.Full,
		ProxyPort:       9002,
		Extensions:      extensionNames(dumpResult.Extensions),
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

	// Populate schema registry with FK metadata from the initial dump so
	// that the proxy-layer FK enforcer can validate referential integrity
	// from the very first session.
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

func extensionNames(exts []schema.Extension) []string {
	names := make([]string, len(exts))
	for i, e := range exts {
		names[i] = e.Name
	}
	return names
}
