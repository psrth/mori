package tui

import (
	"fmt"
	"strings"

	"github.com/mori-dev/mori/internal/ui"
)

// RenderInspectInline renders compact inspect content for the inline panel.
// No box — the dashboard composes this into the split left panel.
func RenderInspectInline(innerW, innerH int, table string, snap Snapshot) string {
	var rows []string

	// PK info.
	if snap.Tables != nil {
		if meta, ok := snap.Tables[table]; ok && len(meta.PKColumns) > 0 {
			rows = append(rows, DimText.Render(fmt.Sprintf(" PK: %s (%s)", strings.Join(meta.PKColumns, ", "), meta.PKType)))
		}
	}

	// Deltas.
	if snap.DeltaMap != nil {
		count := snap.DeltaMap.CountForTable(table)
		hasInserts := snap.DeltaMap.HasInserts(table)
		if count > 0 || hasInserts {
			label := fmt.Sprintf(" ∆ %d %s", count, ui.Pluralize(count, "row", "rows"))
			if hasInserts {
				label += " + inserts"
			}
			rows = append(rows, DeltaStyle.Render(label))
		}
	}

	// Tombstones.
	if snap.Tombstones != nil {
		count := snap.Tombstones.CountForTable(table)
		if count > 0 {
			rows = append(rows, TombstoneStyle.Render(fmt.Sprintf(" ✗ %d %s", count, ui.Pluralize(count, "row", "rows"))))
		}
	}

	// Schema.
	if snap.SchemaReg != nil {
		if diff := snap.SchemaReg.GetDiff(table); diff != nil {
			for _, col := range diff.Added {
				rows = append(rows, SchemaStyle.Render(fmt.Sprintf(" + %s (%s)", col.Name, col.Type)))
			}
			for _, col := range diff.Dropped {
				rows = append(rows, SchemaStyle.Render(fmt.Sprintf(" - %s", col)))
			}
			for old, newName := range diff.Renamed {
				rows = append(rows, SchemaStyle.Render(fmt.Sprintf(" ~ %s → %s", old, newName)))
			}
			if diff.IsNewTable {
				rows = append(rows, SchemaStyle.Render(" (new table)"))
			}
		}
	}

	// Sequences.
	if snap.Sequences != nil {
		if offset, ok := snap.Sequences[table]; ok {
			rows = append(rows, DimText.Render(fmt.Sprintf(" seq: %s start=%s max=%s",
				offset.Column,
				ui.FormatNumber(offset.ShadowStart),
				ui.FormatNumber(offset.ProdMax))))
		}
	}

	if len(rows) == 0 {
		rows = append(rows, DimText.Render(" (no changes)"))
	}

	return strings.Join(rows, "\n")
}
