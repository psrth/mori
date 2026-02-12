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
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset all local state",
	Long: `Wipe all local mutations and restore a clean-slate view of production.

This truncates all Shadow database tables, clears the Delta Map, Tombstone
Set, and Schema Registry. After reset, the application sees only production
data as if starting fresh.

Use --hard to also re-sync the schema from production (useful when the
production schema has changed since initialization).`,
	RunE: runReset,
}

func init() {
	resetCmd.Flags().Bool("hard", false, "Re-sync schema from production")
}

func runReset(cmd *cobra.Command, args []string) error {
	hard, _ := cmd.Flags().GetBool("hard")
	ctx := cmd.Context()

	// 1. Find project root, verify initialized.
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}
	if !config.IsInitialized(projectRoot) {
		return fmt.Errorf("mori is not initialized — run 'mori init --from <conn_string>' first")
	}

	// 2. Refuse if proxy is running.
	pidPath := config.PidFilePath(projectRoot)
	if pid, running := isProxyRunning(pidPath); running {
		return fmt.Errorf("mori proxy is running (PID %d) — stop it first with 'mori stop'", pid)
	}

	cfg, err := config.ReadConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	moriDir := config.MoriDirPath(projectRoot)

	// 3. Parse prod DSN for dbname.
	dsn, err := connstr.Parse(cfg.ProdConnection)
	if err != nil {
		return fmt.Errorf("invalid prod connection in config: %w", err)
	}

	// 4. Connect to Shadow and truncate tables.
	fmt.Println("Connecting to Shadow database...")
	shadowConnStr := connstr.ShadowDSN(cfg.ShadowPort, dsn.DBName)
	shadowConn, err := pgx.Connect(ctx, shadowConnStr)
	if err != nil {
		return fmt.Errorf("cannot connect to Shadow (is the container running?): %w", err)
	}
	defer shadowConn.Close(ctx)

	tables, err := schema.ReadTables(moriDir)
	if err != nil {
		fmt.Println("  Warning: could not read table metadata, skipping truncation.")
		tables = nil
	}

	if hard {
		// --hard: drop all tables, re-dump schema from prod.
		fmt.Println("Hard reset: dropping Shadow tables...")
		if tables != nil {
			for tableName := range tables {
				shadowConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(tableName)))
			}
		}
		shadowConn.Close(ctx)

		fmt.Println("Re-syncing schema from production...")
		prodConn, err := pgx.Connect(ctx, dsn.ConnString())
		if err != nil {
			return fmt.Errorf("cannot connect to production: %w", err)
		}
		defer prodConn.Close(ctx)

		dumpResult, err := schema.FullDump(ctx, prodConn, dsn, cfg.ShadowImage)
		if err != nil {
			return fmt.Errorf("schema dump failed: %w", err)
		}

		if err := schema.ApplyToShadow(ctx, shadowConnStr, dumpResult); err != nil {
			return fmt.Errorf("failed to apply schema to Shadow: %w", err)
		}

		// Persist updated metadata.
		if err := schema.WriteSequences(moriDir, dumpResult.Sequences); err != nil {
			return fmt.Errorf("failed to write sequences: %w", err)
		}
		if err := schema.WriteTables(moriDir, dumpResult.Tables); err != nil {
			return fmt.Errorf("failed to write tables: %w", err)
		}

		fmt.Printf("  Re-synced %d tables, %d sequence offsets\n",
			len(dumpResult.Tables), len(dumpResult.Sequences))
	} else {
		// Soft reset: truncate tables and reset sequences.
		if tables != nil {
			fmt.Println("Truncating Shadow tables...")
			for tableName := range tables {
				_, err := shadowConn.Exec(ctx,
					fmt.Sprintf("TRUNCATE TABLE %s CASCADE", quoteIdent(tableName)))
				if err != nil {
					fmt.Printf("  Warning: could not truncate %s: %v\n", tableName, err)
				}
			}
		}

		// Reset sequence offsets.
		if seqs, err := schema.ReadSequences(moriDir); err == nil {
			if err := schema.ApplySequenceOffsets(ctx, shadowConnStr, seqs); err != nil {
				fmt.Printf("  Warning: could not reset sequence offsets: %v\n", err)
			}
		}
	}

	// 5. Clear state files.
	fmt.Println("Clearing local state...")
	os.Remove(filepath.Join(moriDir, delta.DeltaFile))
	os.Remove(filepath.Join(moriDir, delta.TombstoneFile))
	os.Remove(filepath.Join(moriDir, coreSchema.RegistryFile))

	// 6. Clear log file.
	os.Remove(config.LogFilePath(projectRoot))

	fmt.Println("\nReset complete. Run 'mori start' to begin a fresh session.")
	return nil
}
