package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/shadow"
)

// InitOptions holds the options for initializing a Mori project.
type InitOptions struct {
	ProdConnStr   string // Required: production connection string
	ImageOverride string // Optional: Docker image override
	ProjectRoot   string // Required: path to the project root directory
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
	fmt.Println("Connecting to production database...")
	prodConn, err := pgx.Connect(ctx, dsn.ConnString())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodConn.Close(ctx)

	// 3. Detect PostgreSQL version
	fmt.Println("Detecting PostgreSQL version...")
	var versionStr string
	if err := prodConn.QueryRow(ctx, "SHOW server_version").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect PostgreSQL version: %w", err)
	}
	version, err := schema.ParseVersion(versionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL version: %w", err)
	}
	fmt.Printf("  PostgreSQL %s detected\n", version.Full)

	// 4. Determine Docker image
	imageName := version.ImageTag
	if opts.ImageOverride != "" {
		imageName = opts.ImageOverride
	}

	// 5. Set up Docker container
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
		Password: "mori",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Shadow container: %w", err)
	}

	// Cleanup on failure: remove container if any subsequent step fails
	var initErr error
	defer func() {
		if initErr != nil {
			fmt.Println("Cleaning up Shadow container due to error...")
			mgr.StopAndRemove(ctx, containerInfo.ContainerID)
		}
	}()

	fmt.Printf("  Shadow container %s running on port %d\n", containerInfo.ContainerName, containerInfo.HostPort)

	// 6. Dump production schema
	fmt.Println("Dumping production schema...")
	dumpResult, err := schema.FullDump(ctx, prodConn, dsn)
	if err != nil {
		initErr = err
		return nil, err
	}
	fmt.Printf("  %d tables, %d extensions, %d sequences to offset\n",
		len(dumpResult.Tables), len(dumpResult.Extensions), len(dumpResult.Sequences))

	// 7. Apply schema to Shadow
	shadowConnStr := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	fmt.Println("Applying schema to Shadow...")
	if err := schema.ApplyToShadow(ctx, shadowConnStr, dumpResult); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}

	// 8. Persist configuration
	fmt.Println("Persisting configuration...")
	cfg := &config.Config{
		ProdConnection:  dsn.Redacted(),
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     imageName,
		Engine:          "postgres",
		EngineVersion:   version.Full,
		ProxyPort:       5432,
		Extensions:      extensionNames(dumpResult.Extensions),
		InitializedAt:   time.Now(),
	}

	if err := config.WriteConfig(opts.ProjectRoot, cfg); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	moriDir := config.MoriDirPath(opts.ProjectRoot)
	if err := schema.WriteSequences(moriDir, dumpResult.Sequences); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write sequences: %w", err)
	}
	if err := schema.WriteTables(moriDir, dumpResult.Tables); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to write tables: %w", err)
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
