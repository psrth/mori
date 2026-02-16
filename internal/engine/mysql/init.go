package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/mysql/connstr"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
	"github.com/mori-dev/mori/internal/engine/mysql/shadow"
)

// InitOptions holds the options for initializing a MySQL Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
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
	fmt.Println("Connecting to production MySQL database...")
	prodDB, err := sql.Open("mysql", dsn.GoDSN())
	if err != nil {
		return nil, fmt.Errorf("cannot open connection to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodDB.Close()

	if err := prodDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}

	// 3. Detect MySQL version.
	fmt.Println("Detecting MySQL version...")
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect MySQL version: %w", err)
	}
	fmt.Printf("  MySQL %s detected\n", versionStr)

	// 4. Docker image — default to mysql:8.0, allow override for MariaDB.
	imageName := opts.Image
	if imageName == "" {
		imageName = "mysql:8.0"
	}

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
		Password: "mori",
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

	// 6. Dump production schema.
	fmt.Println("Dumping production schema...")
	schemaSQL, err := schema.DumpSchema(ctx, dsn, imageName)
	if err != nil {
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
	fmt.Printf("  %d tables\n", len(tables))

	// 8. Detect auto_increment offsets.
	offsets, err := schema.DetectAutoIncrementOffsets(ctx, prodDB, tables)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("auto_increment offset detection failed: %w", err)
	}

	// 9. Apply schema to Shadow.
	shadowDSN := connstr.ShadowDSN(containerInfo.HostPort, dsn.DBName)
	fmt.Println("Applying schema to Shadow...")
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
	fmt.Println("Persisting configuration...")
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

// engineName returns the engine name to use in config, defaulting to "mysql".
func engineName(override string) string {
	if override != "" {
		return override
	}
	return "mysql"
}
