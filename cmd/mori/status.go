package main

import (
	"fmt"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Display current Mori state",
	Long: `Show the current state of the Mori project: engine info, connection
details, delta row counts, tombstone counts, schema diffs, and sequence
offsets.

If Mori is not initialized, prints a message indicating so.`,
	RunE: runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	if !config.IsInitialized(projectRoot) {
		if config.HasProjectConfig(projectRoot) {
			fmt.Println("Mori has connections configured but no active session.")
			fmt.Println("Run 'mori start' to begin proxying.")
		} else {
			fmt.Println("Mori is not initialized.")
			fmt.Println("Run 'mori init' to get started.")
		}
		return nil
	}

	cfg, err := config.ReadConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	// Connection info.
	if cfg.ActiveConnection != "" {
		fmt.Printf("Connection:   %s\n", cfg.ActiveConnection)
	}
	fmt.Printf("Engine:       %s %s\n", cfg.Engine, cfg.EngineVersion)
	fmt.Printf("Prod:         %s (read-only)\n", cfg.RedactedProdConnection())
	fmt.Printf("Shadow:       localhost:%d\n", cfg.ShadowPort)

	// Proxy running state.
	pidPath := config.PidFilePath(projectRoot)
	if pid, running := isProxyRunning(pidPath); running {
		fmt.Printf("Proxy:        localhost:%d (running, PID %d)\n", cfg.ProxyPort, pid)
	} else {
		fmt.Printf("Proxy:        localhost:%d (stopped)\n", cfg.ProxyPort)
	}

	moriDir := config.MoriDirPath(projectRoot)

	// Delta Rows.
	if dm, err := delta.ReadDeltaMap(moriDir); err == nil {
		tables := dm.Tables()
		insertedTables := dm.InsertedTablesList()
		if len(tables) > 0 || len(insertedTables) > 0 {
			fmt.Println("\nDelta Rows:")
			shown := make(map[string]bool)
			for _, t := range tables {
				count := dm.CountForTable(t)
				fmt.Printf("  %-20s %d %s\n", t, count, pluralize(count, "row", "rows"))
				shown[t] = true
			}
			for _, t := range insertedTables {
				if !shown[t] {
					fmt.Printf("  %-20s (inserts)\n", t)
				}
			}
		} else {
			fmt.Println("\nDelta Rows:   (none)")
		}
	} else {
		fmt.Println("\nDelta Rows:   (none)")
	}

	// Tombstones.
	if ts, err := delta.ReadTombstoneSet(moriDir); err == nil {
		tables := ts.Tables()
		if len(tables) > 0 {
			fmt.Println("\nTombstones:")
			for _, t := range tables {
				count := ts.CountForTable(t)
				fmt.Printf("  %-20s %d %s\n", t, count, pluralize(count, "row", "rows"))
			}
		} else {
			fmt.Println("\nTombstones:   (none)")
		}
	} else {
		fmt.Println("\nTombstones:   (none)")
	}

	// Schema Diffs.
	if sr, err := coreSchema.ReadRegistry(moriDir); err == nil {
		tables := sr.Tables()
		if len(tables) > 0 {
			fmt.Println("\nSchema Diffs:")
			for _, t := range tables {
				diff := sr.GetDiff(t)
				fmt.Printf("  %-20s %s\n", t, formatSchemaDiff(diff))
			}
		} else {
			fmt.Println("\nSchema Diffs: (none)")
		}
	} else {
		fmt.Println("\nSchema Diffs: (none)")
	}

	// Sequence Offsets.
	if seqs, err := schema.ReadSequences(moriDir); err == nil && len(seqs) > 0 {
		fmt.Println("\nSequence Offsets:")
		for tableName, offset := range seqs {
			label := tableName + "." + offset.Column
			fmt.Printf("  %-20s start=%s (prod max: %s)\n",
				label,
				formatNumber(offset.ShadowStart),
				formatNumber(offset.ProdMax))
		}
	} else {
		fmt.Println("\nSequence Offsets: (none)")
	}

	return nil
}
