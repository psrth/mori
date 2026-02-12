package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect <table>",
	Short: "Show detailed state for a table",
	Long: `Display detailed state for a specific table: delta row count, tombstone
count, schema diffs (added/dropped/renamed columns), sequence offset,
and primary key information.`,
	Args: cobra.ExactArgs(1),
	RunE: runInspect,
}

func runInspect(cmd *cobra.Command, args []string) error {
	table := args[0]

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}
	if !config.IsInitialized(projectRoot) {
		return fmt.Errorf("mori is not initialized — run 'mori init --from <conn_string>' first")
	}

	moriDir := config.MoriDirPath(projectRoot)

	// Verify the table exists in metadata.
	tables, err := schema.ReadTables(moriDir)
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
	fmt.Printf("Table: %s\n", table)
	if len(meta.PKColumns) > 0 {
		fmt.Printf("  PK:         %s (%s)\n", strings.Join(meta.PKColumns, ", "), meta.PKType)
	} else {
		fmt.Println("  PK:         (none)")
	}

	// Delta rows.
	if dm, err := delta.ReadDeltaMap(moriDir); err == nil {
		count := dm.CountForTable(table)
		pks := dm.DeltaPKs(table)
		hasInserts := dm.HasInserts(table)
		if count > 0 || hasInserts {
			line := fmt.Sprintf("  Deltas:     %d modified %s", count, pluralize(count, "row", "rows"))
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
			fmt.Println("  Deltas:     (none)")
		}
	} else {
		fmt.Println("  Deltas:     (none)")
	}

	// Tombstones.
	if ts, err := delta.ReadTombstoneSet(moriDir); err == nil {
		count := ts.CountForTable(table)
		pks := ts.TombstonedPKs(table)
		if count > 0 {
			display := pks
			if len(display) > 10 {
				display = display[:10]
			}
			line := fmt.Sprintf("  Tombstones: %d %s (PKs: %s",
				count, pluralize(count, "row", "rows"), strings.Join(display, ", "))
			if len(pks) > 10 {
				line += fmt.Sprintf(", ... +%d more", len(pks)-10)
			}
			line += ")"
			fmt.Println(line)
		} else {
			fmt.Println("  Tombstones: (none)")
		}
	} else {
		fmt.Println("  Tombstones: (none)")
	}

	// Schema diffs.
	if sr, err := coreSchema.ReadRegistry(moriDir); err == nil {
		if diff := sr.GetDiff(table); diff != nil {
			fmt.Println("  Schema Diffs:")
			for _, col := range diff.Added {
				fmt.Printf("    + %s (%s)\n", col.Name, col.Type)
			}
			for _, col := range diff.Dropped {
				fmt.Printf("    - %s\n", col)
			}
			for old, newName := range diff.Renamed {
				fmt.Printf("    ~ %s -> %s\n", old, newName)
			}
			for col, types := range diff.TypeChanged {
				fmt.Printf("    ~ %s: %s -> %s\n", col, types[0], types[1])
			}
		} else {
			fmt.Println("  Schema Diffs: (none)")
		}
	} else {
		fmt.Println("  Schema Diffs: (none)")
	}

	// Sequence offset.
	if seqs, err := schema.ReadSequences(moriDir); err == nil {
		if offset, ok := seqs[table]; ok {
			fmt.Printf("  Sequence:   %s.%s start=%s (prod max: %s)\n",
				table, offset.Column,
				formatNumber(offset.ShadowStart),
				formatNumber(offset.ProdMax))
		} else {
			fmt.Println("  Sequence:   (none)")
		}
	} else {
		fmt.Println("  Sequence:   (none)")
	}

	return nil
}
