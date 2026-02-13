package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/oracle/connstr"
	"github.com/mori-dev/mori/internal/engine/oracle/schema"
	"github.com/mori-dev/mori/internal/engine/oracle/shadow"
)

// InitOptions holds the options for initializing an Oracle Mori project.
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

// Init performs the complete Mori initialization sequence for Oracle.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the production connection string.
	dsn, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Connect to Prod.
	fmt.Println("Connecting to production Oracle database...")
	prodDB, err := sql.Open("oracle", dsn.GoOraDSN())
	if err != nil {
		return nil, fmt.Errorf("cannot open connection to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}
	defer prodDB.Close()

	if err := prodDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("cannot connect to production database at %s:%d: %w", dsn.Host, dsn.Port, err)
	}

	// 3. Detect Oracle version.
	fmt.Println("Detecting Oracle version...")
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1").Scan(&versionStr); err != nil {
		// Fall back if V$VERSION is not accessible.
		versionStr = "Oracle (version unknown)"
	}
	fmt.Printf("  %s detected\n", versionStr)

	// 4. Docker image — use gvenzl/oracle-xe:21-slim for Shadow.
	imageName := "gvenzl/oracle-xe:21-slim"

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
		Image:       imageName,
		ServiceName: dsn.ServiceName,
		Password:    "mori",
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

	// 6. Dump production schema using SQL-based DDL extraction.
	fmt.Println("Dumping production schema...")
	schemaOwner := dsn.User
	schemaSQL, err := schema.DumpSchema(ctx, prodDB, schemaOwner)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("schema dump failed: %w", err)
	}
	schemaSQL = schema.StripForeignKeys(schemaSQL)

	// 7. Detect table metadata.
	tables, err := schema.DetectTableMetadata(ctx, prodDB, schemaOwner)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}
	fmt.Printf("  %d tables\n", len(tables))

	// 8. Detect sequence offsets.
	offsets, err := schema.DetectSequenceOffsets(ctx, prodDB, schemaOwner, tables)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("sequence offset detection failed: %w", err)
	}

	// 9. Apply schema to Shadow.
	shadowDSN := connstr.ShadowDSN(containerInfo.HostPort, "XEPDB1")
	fmt.Println("Applying schema to Shadow...")
	shadowDB, err := sql.Open("oracle", shadowDSN)
	if err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to connect to Shadow: %w", err)
	}
	defer shadowDB.Close()

	if err := schema.ApplySchema(ctx, shadowDB, schemaSQL); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}

	// 10. Apply sequence offsets to Shadow.
	if err := schema.ApplySequenceOffsets(ctx, shadowDB, schemaOwner, offsets); err != nil {
		initErr = err
		return nil, fmt.Errorf("failed to apply sequence offsets: %w", err)
	}

	// 11. Persist configuration.
	fmt.Println("Persisting configuration...")
	cfg := &config.Config{
		ProdConnection:  dsn.GoOraDSN(),
		ShadowPort:      containerInfo.HostPort,
		ShadowContainer: containerInfo.ContainerName,
		ShadowImage:     imageName,
		Engine:          "oracle",
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
