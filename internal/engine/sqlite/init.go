package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/engine/sqlite/connstr"
	"github.com/psrth/mori/internal/ui"
	"github.com/psrth/mori/internal/engine/sqlite/schema"
	"github.com/psrth/mori/internal/engine/sqlite/shadow"
)

// InitOptions holds the options for initializing a SQLite Mori project.
type InitOptions struct {
	ProdConnStr string
	ProjectRoot string
	ConnName    string
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
	var prodDB *sql.DB
	if err := ui.Spinner("Connecting to production SQLite database...", func() error {
		var e error
		prodDB, e = sql.Open("sqlite", info.DSN())
		if e != nil {
			return e
		}
		return prodDB.PingContext(ctx)
	}); err != nil {
		return nil, fmt.Errorf("cannot open production database %q: %w", info.FilePath, err)
	}
	defer prodDB.Close()

	// 4. Detect SQLite version.
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect SQLite version: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Connected — SQLite %s", versionStr))

	// 5. Create .mori directory.
	moriDir := config.ConnDir(opts.ProjectRoot, opts.ConnName)
	if err := config.InitConnDir(opts.ProjectRoot, opts.ConnName); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	// 6. Copy prod database to shadow.
	var shadowPath string
	if err := ui.Spinner("Creating Shadow database...", func() error {
		var e error
		shadowPath, e = shadow.CreateShadow(info.FilePath, moriDir)
		return e
	}); err != nil {
		return nil, fmt.Errorf("failed to create shadow database: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Shadow database created: %s", shadowPath))

	// 7. Detect table metadata from prod.
	tables, err := schema.DetectTableMetadata(ctx, prodDB)
	if err != nil {
		shadow.RemoveShadow(moriDir)
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}
	ui.StepDone(fmt.Sprintf("%d tables detected", len(tables)))

	// 8. Detect auto-increment offsets.
	offsets, err := schema.DetectAutoIncrementOffsets(ctx, prodDB, tables)
	if err != nil {
		shadow.RemoveShadow(moriDir)
		return nil, fmt.Errorf("auto-increment offset detection failed: %w", err)
	}

	// 9. Apply auto-increment offsets to shadow.
	if len(offsets) > 0 {
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

	if err := config.WriteConnConfig(opts.ProjectRoot, opts.ConnName, cfg); err != nil {
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
