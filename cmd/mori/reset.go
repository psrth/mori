package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset [connection-name]",
	Short: "Reset all local state",
	Long: `Wipe all local mutations and restore a clean-slate view of production.

This truncates all Shadow database tables, clears the Delta Map, Tombstone
Set, and Schema Registry. After reset, the application sees only production
data as if starting fresh.

Use --hard to also re-sync the schema from production (useful when the
production schema has changed since initialization).`,
	Args: cobra.MaximumNArgs(1),
	RunE: runReset,
}

func init() {
	resetCmd.Flags().Bool("hard", false, "Re-sync schema from production")
}

func runReset(cmd *cobra.Command, args []string) error {
	hard, _ := cmd.Flags().GetBool("hard")
	ctx := cmd.Context()

	// 1. Find project root, resolve connection.
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	connName, err := resolveInitializedConnection(projectRoot, args)
	if err != nil {
		return err
	}

	// 2. Refuse if proxy is running.
	pidPath := config.ConnPidFilePath(projectRoot, connName)
	if pid, running := isProxyRunning(pidPath); running {
		return fmt.Errorf("mori proxy is running (PID %d) — stop it first with 'mori stop %s'", pid, connName)
	}

	cfg, err := config.ReadConnConfig(projectRoot, connName)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	connDir := config.ConnDir(projectRoot, connName)

	// 3. Parse prod DSN for dbname.
	dsn, err := connstr.Parse(cfg.ProdConnection)
	if err != nil {
		return fmt.Errorf("invalid prod connection in config: %w", err)
	}

	// 4. Connect to Shadow and truncate tables.
	shadowConnStr := connstr.ShadowDSN(cfg.ShadowPort, dsn.DBName)

	var shadowConn *pgx.Conn
	connErr := ui.Spinner("Connecting to Shadow database...", func() error {
		var e error
		shadowConn, e = pgx.Connect(ctx, shadowConnStr)
		return e
	})
	if connErr != nil {
		return fmt.Errorf("cannot connect to Shadow (is the container running?): %w", connErr)
	}
	defer shadowConn.Close(ctx)
	ui.StepDone("Connected")

	tables, err := schema.ReadTables(connDir)
	if err != nil {
		ui.StepWarn("Could not read table metadata, skipping truncation.")
		tables = nil
	}

	if hard {
		// --hard: drop all tables, re-dump schema from prod.
		_ = ui.Spinner("Dropping Shadow tables...", func() error {
			if tables != nil {
				for tableName := range tables {
					shadowConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(tableName)))
				}
			}
			return nil
		})
		shadowConn.Close(ctx)

		var dumpResult *schema.DumpResult
		syncErr := ui.Spinner("Re-syncing schema from production...", func() error {
			prodConn, err := pgx.Connect(ctx, dsn.ConnString())
			if err != nil {
				return fmt.Errorf("cannot connect to production: %w", err)
			}
			defer prodConn.Close(ctx)

			var e error
			dumpResult, e = schema.FullDump(ctx, prodConn, dsn, cfg.ShadowImage)
			if e != nil {
				return fmt.Errorf("schema dump failed: %w", e)
			}

			extOpts := &schema.ExtInstallOptions{
				ContainerID: cfg.ShadowContainer,
			}
			if v, parseErr := schema.ParseVersion(cfg.EngineVersion); parseErr == nil {
				extOpts.PGMajor = v.Major
			}
			if e := schema.ApplyToShadow(ctx, shadowConnStr, dumpResult, extOpts); e != nil {
				return fmt.Errorf("failed to apply schema to Shadow: %w", e)
			}

			return nil
		})
		if syncErr != nil {
			return syncErr
		}

		// Persist updated metadata.
		if err := schema.WriteSequences(connDir, dumpResult.Sequences); err != nil {
			return fmt.Errorf("failed to write sequences: %w", err)
		}
		if err := schema.WriteTables(connDir, dumpResult.Tables); err != nil {
			return fmt.Errorf("failed to write tables: %w", err)
		}

		ui.StepDone(fmt.Sprintf("Re-synced %d tables, %d sequence offsets",
			len(dumpResult.Tables), len(dumpResult.Sequences)))
	} else {
		// Soft reset: truncate tables and reset sequences.
		if tables != nil {
			tableCount := len(tables)
			_ = ui.Spinner("Truncating Shadow tables...", func() error {
				for tableName := range tables {
					_, err := shadowConn.Exec(ctx,
						fmt.Sprintf("TRUNCATE TABLE %s CASCADE", quoteIdent(tableName)))
					if err != nil {
						fmt.Printf("  Warning: could not truncate %s: %v\n", tableName, err)
					}
				}
				return nil
			})
			ui.StepDone(fmt.Sprintf("Truncated %d %s", tableCount, pluralize(tableCount, "table", "tables")))
		}

		// Reset sequence offsets.
		if seqs, err := schema.ReadSequences(connDir); err == nil {
			if err := schema.ApplySequenceOffsets(ctx, shadowConnStr, seqs); err != nil {
				ui.StepWarn(fmt.Sprintf("Could not reset sequence offsets: %v", err))
			}
		}
	}

	// 5. Clear state files.
	_ = ui.Spinner("Clearing local state...", func() error {
		os.Remove(filepath.Join(connDir, delta.DeltaFile))
		os.Remove(filepath.Join(connDir, delta.TombstoneFile))
		os.Remove(filepath.Join(connDir, coreSchema.RegistryFile))
		os.Remove(config.ConnLogFilePath(projectRoot, connName))
		return nil
	})

	ui.StepDone("Reset complete — run 'mori start' to begin fresh.")
	return nil
}

// resolveInitializedConnection figures out which initialized connection to use.
func resolveInitializedConnection(projectRoot string, args []string) (string, error) {
	if len(args) == 1 {
		name := args[0]
		if !config.IsConnInitialized(projectRoot, name) {
			return "", fmt.Errorf("connection %q is not initialized — run 'mori start %s' first", name, name)
		}
		return name, nil
	}

	initialized := config.InitializedConnections(projectRoot)
	if len(initialized) == 0 {
		return "", fmt.Errorf("no initialized connections — run 'mori start' first")
	}
	if len(initialized) == 1 {
		return initialized[0], nil
	}

	return "", fmt.Errorf("multiple initialized connections (%v) — specify which one", initialized)
}
