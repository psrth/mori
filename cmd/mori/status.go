package main

import (
	"fmt"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status [connection-name]",
	Short: "Display current Mori state",
	Long: `Show the current state of a Mori connection: engine info, connection
details, delta row counts, tombstone counts, schema diffs, and sequence
offsets.

If no connection name is given and only one is initialized, it is selected
automatically.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	initialized := config.InitializedConnections(projectRoot)
	if len(initialized) == 0 {
		if config.HasProjectConfig(projectRoot) {
			fmt.Println("Mori has connections configured but none initialized.")
			ui.Info("Run 'mori start' to begin proxying.")
		} else {
			fmt.Println("Mori is not initialized.")
			ui.Info("Run 'mori init' to get started.")
		}
		return nil
	}

	connName, err := resolveInitializedConnection(projectRoot, args)
	if err != nil {
		return err
	}

	cfg, err := config.ReadConnConfig(projectRoot, connName)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	connDir := config.ConnDir(projectRoot, connName)

	// Connection info box.
	pidPath := config.ConnPidFilePath(projectRoot, connName)
	var proxyStatus string
	if _, running := isProxyRunning(pidPath); running {
		proxyStatus = fmt.Sprintf("localhost:%d · %s", cfg.ProxyPort, ui.Green(ui.IconActive+" running"))
	} else {
		proxyStatus = fmt.Sprintf("localhost:%d · %s", cfg.ProxyPort, ui.Dim(ui.IconInactive+" stopped"))
	}

	const labelW = 6
	boxContent := ui.BoxLine("Engine", fmt.Sprintf("%s %s", cfg.Engine, cfg.EngineVersion), labelW) + "\n" +
		ui.BoxLine("Prod", cfg.RedactedProdConnection(), labelW) + "\n" +
		ui.BoxLine("Shadow", fmt.Sprintf("localhost:%d", cfg.ShadowPort), labelW) + "\n" +
		ui.BoxLine("Proxy", proxyStatus, labelW)
	fmt.Println(ui.Box(connName, boxContent))

	// Delta Rows.
	if dm, err := delta.ReadDeltaMap(connDir); err == nil {
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
			for t, c := range insertedTables {
				if !shown[t] {
					if c > 0 {
						fmt.Printf("  %-20s %d %s\n", t, c, pluralize(c, "insert", "inserts"))
					} else {
						fmt.Printf("  %-20s %s\n", t, ui.Dim("(inserts)"))
					}
				}
			}
		} else {
			fmt.Printf("Delta Rows: %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("Delta Rows: %s\n", ui.Dim("(none)"))
	}

	// Tombstones.
	if ts, err := delta.ReadTombstoneSet(connDir); err == nil {
		tables := ts.Tables()
		if len(tables) > 0 {
			fmt.Println("\nTombstones:")
			for _, t := range tables {
				count := ts.CountForTable(t)
				fmt.Printf("  %-20s %d %s\n", t, count, pluralize(count, "row", "rows"))
			}
		} else {
			fmt.Printf("Tombstones: %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("Tombstones: %s\n", ui.Dim("(none)"))
	}

	// Schema Diffs.
	if sr, err := coreSchema.ReadRegistry(connDir); err == nil {
		tables := sr.Tables()
		if len(tables) > 0 {
			fmt.Println("\nSchema Diffs:")
			for _, t := range tables {
				diff := sr.GetDiff(t)
				fmt.Printf("  %-20s %s\n", t, formatSchemaDiff(diff))
			}
		} else {
			fmt.Printf("Schema Diffs: %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("Schema Diffs: %s\n", ui.Dim("(none)"))
	}

	// Sequence Offsets.
	if seqs, err := schema.ReadSequences(connDir); err == nil && len(seqs) > 0 {
		fmt.Println("\nSequence Offsets:")
		for tableName, offset := range seqs {
			label := tableName + "." + offset.Column
			fmt.Printf("  %-20s start=%s (prod max: %s)\n",
				label,
				formatNumber(offset.ShadowStart),
				formatNumber(offset.ProdMax))
		}
	} else {
		fmt.Printf("Sequence Offsets: %s\n", ui.Dim("(none)"))
	}

	return nil
}
