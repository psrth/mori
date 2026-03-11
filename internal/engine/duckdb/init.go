package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/engine/duckdb/connstr"
	"github.com/psrth/mori/internal/engine/duckdb/schema"
	"github.com/psrth/mori/internal/engine/duckdb/shadow"
	"github.com/psrth/mori/internal/ui"
)

// InitOptions holds the options for initializing a DuckDB Mori project.
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

// Init performs the complete Mori initialization sequence for DuckDB.
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
	if err := ui.Spinner("Connecting to production DuckDB database...", func() error {
		var e error
		prodDB, e = sql.Open("duckdb", info.DSN())
		if e != nil {
			return e
		}
		return prodDB.PingContext(ctx)
	}); err != nil {
		return nil, fmt.Errorf("cannot open production database %q: %w", info.FilePath, err)
	}
	defer prodDB.Close()

	// 4. Detect DuckDB version.
	var versionStr string
	if err := prodDB.QueryRowContext(ctx, "SELECT version()").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect DuckDB version: %w", err)
	}
	ui.StepDone(fmt.Sprintf("Connected — DuckDB %s", versionStr))

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

	// 7b. Detect generated columns.
	tables, _ = schema.DetectGeneratedColumns(ctx, prodDB, tables)

	// 8. Persist configuration.
	// Note: DuckDB shadow is a full file copy (not schema-only), so the
	// shadow's internal auto-increment sequences already have the correct
	// state from prod. No sequence offset manipulation is needed.
	cfg := &config.Config{
		ProdConnection:  info.FilePath,
		ShadowPort:      0,  // Embedded — no port.
		ShadowContainer: "", // No container.
		ShadowImage:     "", // No image.
		Engine:          "duckdb",
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
