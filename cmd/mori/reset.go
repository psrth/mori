package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/engine/postgres/connstr"
	"github.com/psrth/mori/internal/engine/postgres/schema"
	"github.com/psrth/mori/internal/ui"
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

	// 4. Ensure Shadow container is running (it may have been stopped by 'mori stop').
	shadowStartedByUs := false
	if cfg.ShadowContainer != "" {
		wasRunning, ensureErr := ensureShadowRunningForReset(ctx, cfg.ShadowContainer, cfg.ShadowPort)
		if ensureErr != nil {
			return fmt.Errorf("failed to ensure Shadow container is running: %w", ensureErr)
		}
		shadowStartedByUs = !wasRunning
	}

	// 5. Connect to Shadow and truncate tables.
	shadowConnStr := connstr.ShadowDSN(cfg.ShadowPort, dsn.DBName)

	var shadowConn *pgx.Conn
	connErr := ui.Spinner("Connecting to Shadow database...", func() error {
		var e error
		shadowConn, e = pgx.Connect(ctx, shadowConnStr)
		return e
	})
	if connErr != nil {
		// If we started the container, stop it before returning.
		if shadowStartedByUs && cfg.ShadowContainer != "" {
			stopShadowContainer(context.Background(), cfg.ShadowContainer)
		}
		return fmt.Errorf("cannot connect to Shadow: %w", connErr)
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
		// Soft reset: revert DDL changes, truncate tables, and reset sequences.

		// Revert DDL changes recorded in the schema registry before clearing it.
		// Without this, shadow DB retains ALTER TABLE changes (added columns, etc.)
		// and subsequent sessions fail when re-applying the same DDL.
		registry, regErr := coreSchema.ReadRegistry(connDir)
		if regErr == nil {
			ddlCount := 0
			_ = ui.Spinner("Reverting DDL changes...", func() error {
				for _, tableName := range registry.Tables() {
					diff := registry.GetDiff(tableName)
					if diff == nil {
						continue
					}

					// Drop tables that were created in Shadow.
					if diff.IsNewTable {
						shadowConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(tableName)))
						ddlCount++
						continue
					}

					// Revert added columns.
					for _, col := range diff.Added {
						shadowConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s",
							quoteIdent(tableName), quoteIdent(col.Name)))
						ddlCount++
					}

					// Revert dropped columns.
					for _, colName := range diff.Dropped {
						// Re-add with TEXT type as a safe default; the original type
						// isn't tracked in the registry for drops.
						shadowConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT",
							quoteIdent(tableName), quoteIdent(colName)))
						ddlCount++
					}

					// Revert renamed columns (reverse: new_name → old_name).
					for oldName, newName := range diff.Renamed {
						shadowConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
							quoteIdent(tableName), quoteIdent(newName), quoteIdent(oldName)))
						ddlCount++
					}

					// Revert type changes.
					for colName, types := range diff.TypeChanged {
						oldType := types[0]
						if oldType != "" && oldType != "unknown" {
							shadowConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s",
								quoteIdent(tableName), quoteIdent(colName), oldType,
								quoteIdent(colName), oldType))
							ddlCount++
						}
					}
				}
				return nil
			})
			if ddlCount > 0 {
				ui.StepDone(fmt.Sprintf("Reverted %d DDL %s", ddlCount, pluralize(ddlCount, "change", "changes")))
			}
		}

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

	// Stop Shadow container if we started it for this reset.
	if shadowStartedByUs && cfg.ShadowContainer != "" {
		stopShadowContainer(context.Background(), cfg.ShadowContainer)
	}

	fmt.Println()
	ui.StepDone("Reset complete.")
	ui.Info("Next: run 'mori start' to begin fresh.")
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

// ensureShadowRunningForReset ensures the Shadow container is running and reports
// whether it was already running. This lets the caller stop the container after
// reset if it was started solely for this operation.
func ensureShadowRunningForReset(ctx context.Context, containerName string, shadowPort int) (wasRunning bool, err error) {
	if isShadowContainerRunning(ctx, containerName) {
		return true, nil
	}
	if err := ensureShadowRunning(ctx, containerName, shadowPort); err != nil {
		return false, err
	}
	return false, nil
}

// isShadowContainerRunning checks whether a Docker container is currently running.
func isShadowContainerRunning(ctx context.Context, containerName string) bool {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(checkCtx, "docker", "inspect", containerName,
		"--format", "{{.State.Running}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "true"
}
