package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/sqlite/connstr"
	"github.com/mori-dev/mori/internal/engine/sqlite/schema"
	"github.com/mori-dev/mori/internal/engine/sqlite/shadow"
)

// InitOptions holds the options for initializing a SQLite Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
}

// InitResult holds the result of a successful initialization.
type InitResult struct {
	Config *config.Config
	Tables map[string]schema.TableMeta
}

// Init performs the complete Mori initialization sequence for SQLite.
func Init(ctx context.Context, opts InitOptions) (*InitResult, error) {
	// 1. Parse the connection string.
	info, err := connstr.Parse(opts.ProdConnStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	// 2. Validate the file exists.
	if err := info.Validate(); err != nil {
		return nil, fmt.Errorf("production database not found: %w", err)
	}

	// 3. Open and verify the prod database.
	fmt.Println("Connecting to production SQLite database...")
	prodDB, err := sql.Open("sqlite", info.DSN())
	if err != nil {
		return nil, fmt.Errorf("cannot open production database %q: %w", info.FilePath, err)
	}
	defer prodDB.Close()

	if err := prodDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("cannot connect to production database %q: %w", info.FilePath, err)
	}

	// 4. Detect SQLite version.
	fmt.Println("Detecting SQLite version...")
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect SQLite version: %w", err)
	}
	fmt.Printf("  SQLite %s detected\n", versionStr)

	// 5. Create .mori directory.
	moriDir := config.MoriDirPath(opts.ProjectRoot)
	if err := config.InitDir(opts.ProjectRoot); err != nil {
		return nil, fmt.Errorf("failed to create .mori directory: %w", err)
	}

	// 6. Copy prod database to shadow.
	fmt.Println("Creating Shadow database (file copy)...")
	shadowPath, err := shadow.CreateShadow(info.FilePath, moriDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create shadow database: %w", err)
	}
	fmt.Printf("  Shadow: %s\n", shadowPath)

	// 7. Detect table metadata from prod.
	fmt.Println("Detecting table metadata...")
	tables, err := schema.DetectTableMetadata(ctx, prodDB)
	if err != nil {
		shadow.RemoveShadow(moriDir)
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}
	fmt.Printf("  %d tables\n", len(tables))

	// 8. Detect auto-increment offsets.
	offsets, err := schema.DetectAutoIncrementOffsets(ctx, prodDB, tables)
	if err != nil {
		shadow.RemoveShadow(moriDir)
		return nil, fmt.Errorf("auto-increment offset detection failed: %w", err)
	}

	// 9. Apply auto-increment offsets to shadow.
	if len(offsets) > 0 {
		fmt.Println("Applying auto-increment offsets to Shadow...")
		shadowDB, err := sql.Open("sqlite", shadowPath)
		if err != nil {
			shadow.RemoveShadow(moriDir)
			return nil, fmt.Errorf("failed to open shadow database: %w", err)
		}
		defer shadowDB.Close()

		if err := schema.ApplyAutoIncrementOffsets(ctx, shadowDB, offsets); err != nil {
			shadow.RemoveShadow(moriDir)
			return nil, fmt.Errorf("failed to apply auto-increment offsets: %w", err)
		}
	}

	// 10. Persist configuration.
	fmt.Println("Persisting configuration...")
	cfg := &config.Config{
		ProdConnection:  info.FilePath,
		ShadowPort:      0,  // Embedded — no port.
		ShadowContainer: "", // No container.
		ShadowImage:     "", // No image.
		Engine:          "sqlite",
		EngineVersion:   versionStr,
		ProxyPort:       9002,
		InitializedAt:   time.Now(),
	}

	if err := config.WriteConfig(opts.ProjectRoot, cfg); err != nil {
		shadow.RemoveShadow(moriDir)
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	if err := schema.WriteTables(moriDir, tables); err != nil {
		shadow.RemoveShadow(moriDir)
		return nil, fmt.Errorf("failed to write tables: %w", err)
	}

	return &InitResult{
		Config: cfg,
		Tables: tables,
	}, nil
}
