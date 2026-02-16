package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect <table> [connection-name]",
	Short: "Show detailed state for a table",
	Long: `Display detailed state for a specific table: delta row count, tombstone
count, schema diffs (added/dropped/renamed columns), sequence offset,
and primary key information.

If multiple connections are initialized, specify which one as the second argument.`,
	Args: cobra.RangeArgs(1, 2),
	RunE: runInspect,
}

func runInspect(cmd *cobra.Command, args []string) error {
	table := args[0]

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	// Resolve connection: second arg or auto-detect.
	var connArgs []string
	if len(args) >= 2 {
		connArgs = args[1:]
	}
	connName, err := resolveInitializedConnection(projectRoot, connArgs)
	if err != nil {
		return err
	}

	connDir := config.ConnDir(projectRoot, connName)

	// Verify the table exists in metadata.
	tables, err := schema.ReadTables(connDir)
	if err != nil {
		return fmt.Errorf("failed to read table metadata: %w", err)
	}
	meta, ok := tables[table]
	if !ok {
		known := make([]string, 0, len(tables))
		for t := range tables {
			known = append(known, t)
		}
		sort.Strings(known)
		return fmt.Errorf("table %q not found in Mori metadata (known tables: %s)",
			table, strings.Join(known, ", "))
	}

	// Header.
	fmt.Printf("Table: %s\n", ui.Cyan(table))
	if len(meta.PKColumns) > 0 {
		fmt.Printf("  PK:         %s (%s)\n", strings.Join(meta.PKColumns, ", "), meta.PKType)
	} else {
		fmt.Printf("  PK:         %s\n", ui.Dim("(none)"))
	}

	// Delta rows.
	if dm, err := delta.ReadDeltaMap(connDir); err == nil {
		count := dm.CountForTable(table)
		pks := dm.DeltaPKs(table)
		hasInserts := dm.HasInserts(table)
		if count > 0 || hasInserts {
			line := fmt.Sprintf("  Deltas:     %s %d modified %s",
				ui.Yellow("~"),
				count, pluralize(count, "row", "rows"))
			if hasInserts {
				line += " + inserts"
			}
			if len(pks) > 0 {
				display := pks
				if len(display) > 10 {
					display = display[:10]
				}
				line += fmt.Sprintf(" (PKs: %s", strings.Join(display, ", "))
				if len(pks) > 10 {
					line += fmt.Sprintf(", ... +%d more", len(pks)-10)
				}
				line += ")"
			}
			fmt.Println(line)
		} else {
			fmt.Printf("  Deltas:     %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("  Deltas:     %s\n", ui.Dim("(none)"))
	}

	// Tombstones.
	if ts, err := delta.ReadTombstoneSet(connDir); err == nil {
		count := ts.CountForTable(table)
		pks := ts.TombstonedPKs(table)
		if count > 0 {
			display := pks
			if len(display) > 10 {
				display = display[:10]
			}
			line := fmt.Sprintf("  Tombstones: %s %d %s (PKs: %s",
				ui.Red("-"),
				count, pluralize(count, "row", "rows"), strings.Join(display, ", "))
			if len(pks) > 10 {
				line += fmt.Sprintf(", ... +%d more", len(pks)-10)
			}
			line += ")"
			fmt.Println(line)
		} else {
			fmt.Printf("  Tombstones: %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("  Tombstones: %s\n", ui.Dim("(none)"))
	}

	// Schema diffs.
	if sr, err := coreSchema.ReadRegistry(connDir); err == nil {
		if diff := sr.GetDiff(table); diff != nil {
			fmt.Println("  Schema Diffs:")
			for _, col := range diff.Added {
				fmt.Printf("    %s %s (%s)\n", ui.Green("+"), col.Name, col.Type)
			}
			for _, col := range diff.Dropped {
				fmt.Printf("    %s %s\n", ui.Red("-"), col)
			}
			for old, newName := range diff.Renamed {
				fmt.Printf("    %s %s %s %s\n", ui.Purple("~"), old, ui.IconArrow, newName)
			}
			for col, types := range diff.TypeChanged {
				fmt.Printf("    %s %s: %s %s %s\n", ui.Purple("~"), col, types[0], ui.IconArrow, types[1])
			}
		} else {
			fmt.Printf("  Schema Diffs: %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("  Schema Diffs: %s\n", ui.Dim("(none)"))
	}

	// Sequence offset.
	if seqs, err := schema.ReadSequences(connDir); err == nil {
		if offset, ok := seqs[table]; ok {
			fmt.Printf("  Sequence:   %s.%s start=%s (prod max: %s)\n",
				table, offset.Column,
				formatNumber(offset.ShadowStart),
				formatNumber(offset.ProdMax))
		} else {
			fmt.Printf("  Sequence:   %s\n", ui.Dim("(none)"))
		}
	} else {
		fmt.Printf("  Sequence:   %s\n", ui.Dim("(none)"))
	}

	return nil
}
