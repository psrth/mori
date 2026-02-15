package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/mori-dev/mori/internal/logging"
)

// RenderQueryStream renders the query stream content (no box — box added by dashboard).
func RenderQueryStream(innerW, innerH int, entries []logging.LogEntry) string {
	if len(entries) == 0 {
		return DimText.Render(" (waiting for queries)")
	}

	// Column widths for single-line format:
	// TIME(8)  STRATEGY(18)  DURATION(8)  SQL(rest)
	const tsW = 8
	const stratW = 18
	const durW = 8
	sqlW := innerW - tsW - stratW - durW - 4 // 4 for spacing
	if sqlW < 10 {
		sqlW = 10
	}

	// Show most recent entries first (reverse chronological).
	var rows []string
	end := len(entries)
	start := end - innerH
	if start < 0 {
		start = 0
	}
	for i := end - 1; i >= start; i-- {
		line := formatQueryLine(entries[i], tsW, stratW, durW, sqlW)
		rows = append(rows, line)
	}

	return strings.Join(rows, "\n")
}

func formatQueryLine(entry logging.LogEntry, tsW, stratW, durW, sqlW int) string {
	// Timestamp.
	ts := DimText.Render(PadRight(entry.Timestamp.Local().Format("15:04:05"), tsW))

	// Strategy with color.
	strategy := entry.Strategy
	if strategy == "" {
		strategy = strings.ToUpper(entry.Event)
	}
	strategyStyle := lipgloss.NewStyle().Foreground(ColorMuted)
	if c, ok := StrategyColor[strategy]; ok {
		strategyStyle = lipgloss.NewStyle().Foreground(c)
	}
	stratCol := strategyStyle.Render(PadRight(strategy, stratW))

	// Duration.
	var durStr string
	if entry.DurationMs > 0 {
		durStr = fmt.Sprintf("%.1fms", entry.DurationMs)
	}
	durCol := DimText.Render(PadRight(durStr, durW))

	// SQL.
	sql := entry.SQL
	if sql == "" {
		sql = entry.Detail
		if sql == "" {
			sql = entry.Event
		}
	}
	// Clean up whitespace in SQL.
	sql = strings.Join(strings.Fields(sql), " ")
	sqlCol := Ellipsis(sql, sqlW)

	return " " + ts + " " + stratCol + " " + durCol + " " + sqlCol
}
