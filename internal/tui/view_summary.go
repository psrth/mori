package tui

import (
	"fmt"
	"strings"
	"time"
)

// RenderSummary renders the session summary content (no box — composed by dashboard).
func RenderSummary(innerW, innerH int, snap Snapshot, totalQueries int) string {
	const labelW = 12

	var rows []string

	rows = append(rows, fmt.Sprintf(" %-*s%d", labelW, "Queries", totalQueries))

	// Edits (deltas + inserts).
	editRows := 0
	editTables := 0
	if snap.DeltaMap != nil {
		seen := make(map[string]bool)
		for _, t := range snap.DeltaMap.Tables() {
			c := snap.DeltaMap.CountForTable(t)
			editRows += c
			if c > 0 {
				editTables++
				seen[t] = true
			}
		}
		for t, c := range snap.DeltaMap.InsertedTablesList() {
			editRows += c
			if !seen[t] {
				editTables++
			}
		}
	}
	editVal := fmt.Sprintf("%d rows / %d tables", editRows, editTables)
	rows = append(rows, EditStyle.Render(fmt.Sprintf(" %-*s%s", labelW, "~ Edits", editVal)))

	// Tombstones.
	tombRows := 0
	tombTables := 0
	if snap.Tombstones != nil {
		for _, t := range snap.Tombstones.Tables() {
			c := snap.Tombstones.CountForTable(t)
			tombRows += c
			if c > 0 {
				tombTables++
			}
		}
	}
	tombVal := fmt.Sprintf("%d rows / %d tables", tombRows, tombTables)
	rows = append(rows, DeleteStyle.Render(fmt.Sprintf(" %-*s%s", labelW, "- Deletes", tombVal)))

	// Schema.
	schemaTables := 0
	if snap.SchemaReg != nil {
		schemaTables = len(snap.SchemaReg.Tables())
	}
	schemaVal := fmt.Sprintf("%d tables", schemaTables)
	if schemaTables > 0 {
		rows = append(rows, SchemaStyle.Render(fmt.Sprintf(" %-*s%s", labelW, "Schema", schemaVal)))
	} else {
		rows = append(rows, DimText.Render(fmt.Sprintf(" %-*s%s", labelW, "Schema", schemaVal)))
	}

	// Uptime.
	if snap.Config != nil && !snap.Config.InitializedAt.IsZero() && snap.ProxyRunning {
		uptime := time.Since(snap.Config.InitializedAt).Round(time.Second)
		rows = append(rows, fmt.Sprintf(" %-*s%s", labelW, "Uptime", formatDuration(uptime)))
	}

	return strings.Join(rows, "\n")
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", m, s)
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh %dm %ds", h, m, s)
}
