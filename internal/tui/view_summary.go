package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/mori-dev/mori/internal/logging"
)

// RenderSummary renders the session summary content (no box — composed by dashboard).
func RenderSummary(innerW, innerH int, snap Snapshot, totalQueries int) string {
	const labelW = 12

	var rows []string

	rows = append(rows, fmt.Sprintf(" %-*s%d", labelW, "Queries", totalQueries))

	// Deltas.
	deltaRows := 0
	deltaTables := 0
	if snap.DeltaMap != nil {
		for _, t := range snap.DeltaMap.Tables() {
			c := snap.DeltaMap.CountForTable(t)
			deltaRows += c
			if c > 0 {
				deltaTables++
			}
		}
		for _, t := range snap.DeltaMap.InsertedTablesList() {
			if snap.DeltaMap.CountForTable(t) == 0 {
				deltaTables++
			}
		}
	}
	deltaVal := fmt.Sprintf("%d rows / %d tables", deltaRows, deltaTables)
	rows = append(rows, DeltaStyle.Render(fmt.Sprintf(" %-*s%s", labelW, "∆ Deltas", deltaVal)))

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
	rows = append(rows, TombstoneStyle.Render(fmt.Sprintf(" %-*s%s", labelW, "✗ Tombs", tombVal)))

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

// RenderRoutingChart renders a horizontal bar chart of routing strategy distribution.
func RenderRoutingChart(innerW, innerH int, entries []logging.LogEntry) string {
	type stratCount struct {
		name  string
		count int
	}

	counts := make(map[string]int)
	total := 0
	for _, e := range entries {
		if e.Strategy != "" {
			counts[e.Strategy]++
			total++
		}
	}

	if total == 0 {
		return DimText.Render(" (no routing data)")
	}

	var sorted []stratCount
	for name, count := range counts {
		sorted = append(sorted, stratCount{name, count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	if len(sorted) > innerH {
		sorted = sorted[:innerH]
	}

	const nameW = 18
	barMaxW := innerW - nameW - 8
	if barMaxW < 3 {
		barMaxW = 3
	}

	maxCount := sorted[0].count

	var rows []string
	for _, sc := range sorted {
		pct := float64(sc.count) / float64(total) * 100
		barLen := int(float64(sc.count) / float64(maxCount) * float64(barMaxW))
		if barLen < 1 && sc.count > 0 {
			barLen = 1
		}

		style := lipgloss.NewStyle().Foreground(ColorMuted)
		if c, ok := StrategyColor[sc.name]; ok {
			style = lipgloss.NewStyle().Foreground(c)
		}

		nameStr := style.Render(PadRight(sc.name, nameW))
		barStr := style.Render(strings.Repeat("█", barLen))
		barPad := strings.Repeat(" ", barMaxW-barLen)
		pctStr := DimText.Render(fmt.Sprintf("%3.0f%%", pct))

		rows = append(rows, " "+nameStr+" "+barStr+barPad+" "+pctStr)
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
