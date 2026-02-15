package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/mori-dev/mori/internal/logging"
)

// DashboardLayout holds computed panel dimensions.
type DashboardLayout struct {
	Width  int
	Height int

	LeftW  int // left panel inner width (chars between │ and │)
	RightW int // right panel inner width

	ChartContentH int // rows for chart content (graph + legend)
	TablesH       int // rows for tables section content
	SessionH      int // rows for session section content (including divider)
	QueryH        int // rows for query panel content
}

// ComputeLayout calculates panel dimensions from terminal size.
func ComputeLayout(width, height int) DashboardLayout {
	l := DashboardLayout{Width: width, Height: height}

	// Overhead rows:
	//   top bar (2) + chart header (1) + bottom header (1) + bottom bar (1) = 5
	overhead := 5
	contentH := height - overhead
	if contentH < 10 {
		contentH = 10
	}

	// Chart: ~25% of content, min 6 (5 graph + 1 legend).
	l.ChartContentH = contentH * 25 / 100
	if l.ChartContentH < 6 {
		l.ChartContentH = 6
	}

	// Bottom section: everything else.
	bottomH := contentH - l.ChartContentH
	if bottomH < 4 {
		bottomH = 4
	}

	// Session: fixed 6 rows (1 divider + 5 content), but cap at half of bottom.
	l.SessionH = 6
	if l.SessionH > bottomH/2 {
		l.SessionH = bottomH / 2
	}

	l.TablesH = bottomH - l.SessionH

	// Query content height = tables + session (queries span the full right side).
	l.QueryH = bottomH

	// Left panel: ~35% capped [22, 40].
	l.LeftW = width * 35 / 100
	if l.LeftW > 40 {
		l.LeftW = 40
	}
	if l.LeftW < 22 {
		l.LeftW = 22
	}

	// Right: total = │ + leftW + │ + rightW + │ = width
	l.RightW = width - l.LeftW - 3
	if l.RightW < 10 {
		l.RightW = 10
	}

	return l
}

// RenderDashboard composes the full-screen dashboard view.
func RenderDashboard(
	layout DashboardLayout,
	snap Snapshot,
	tableList *TableListModel,
	logEntries []logging.LogEntry,
	totalQueries int,
	inspecting bool,
	inspectTable string,
	searchQuery string,
	searching bool,
	metricsP50, metricsP95, metricsP99, metricsQPS []float64,
) string {
	if layout.Width < 50 || layout.Height < 12 {
		msg := "Terminal too small.\nMinimum: 50x12"
		return lipgloss.Place(layout.Width, layout.Height, lipgloss.Center, lipgloss.Center, msg)
	}

	var rows []string

	// ═══════════════════════════════════════════════
	// 1. Top bar (2 lines)
	// ═══════════════════════════════════════════════
	topBar := RenderTopBar(layout.Width, snap)
	rows = append(rows, topBar)

	// ═══════════════════════════════════════════════
	// 2. Chart header: ├─ Query Latency ──┬─ Throughput (q/s) ──┤
	// ═══════════════════════════════════════════════
	rows = append(rows, buildPanelHeader("Query Latency", "Throughput (q/s)", layout.LeftW, layout.RightW))

	// ═══════════════════════════════════════════════
	// 3. Chart content rows
	// ═══════════════════════════════════════════════
	latencyChart := RenderLatencyChart(metricsP50, metricsP95, metricsP99, layout.LeftW, layout.ChartContentH)
	throughputChart := RenderThroughputChart(metricsQPS, layout.RightW, layout.ChartContentH)

	leftChartLines := padLines(strings.Split(latencyChart, "\n"), layout.ChartContentH)
	rightChartLines := padLines(strings.Split(throughputChart, "\n"), layout.ChartContentH)

	for i := 0; i < layout.ChartContentH; i++ {
		rows = append(rows, buildContentRow(leftChartLines[i], rightChartLines[i], layout.LeftW, layout.RightW))
	}

	// ═══════════════════════════════════════════════
	// 4. Bottom section header: ├─ Tables ──┼─ Queries ──┤
	// ═══════════════════════════════════════════════
	hotCount := 0
	for _, t := range tableList.Tables {
		if isHot(t, snap) {
			hotCount++
		}
	}
	tablesTitle := "Tables"
	if hotCount > 0 {
		tablesTitle = fmt.Sprintf("Tables (%d hot)", hotCount)
	}

	queriesPanelTitle := "Queries"
	if searching {
		queriesPanelTitle = "Queries ─ /" + searchQuery + "█"
	} else if searchQuery != "" {
		queriesPanelTitle = "Queries ─ /" + searchQuery
	}
	rows = append(rows, buildPanelHeader(tablesTitle, queriesPanelTitle, layout.LeftW, layout.RightW))

	// ═══════════════════════════════════════════════
	// 5. Bottom content: Tables+Session (left) | Queries (right)
	// ═══════════════════════════════════════════════

	// -- Left: tables content --
	var leftLines []string
	leftMidRow := -1 // row where session divider goes
	inspectMidRow := -1

	if inspecting && inspectTable != "" {
		// Split tables area between table list and inspect.
		tablesDisplayH := layout.TablesH / 2
		inspectDisplayH := layout.TablesH - tablesDisplayH - 1 // -1 for inspect mid-border

		tableContent := RenderTableList(layout.LeftW, tablesDisplayH, tableList, snap)
		leftLines = append(leftLines, padLines(strings.Split(tableContent, "\n"), tablesDisplayH)...)

		inspectMidRow = tablesDisplayH

		inspectContent := RenderInspectInline(layout.LeftW, inspectDisplayH, inspectTable, snap)
		leftLines = append(leftLines, padLines(strings.Split(inspectContent, "\n"), inspectDisplayH)...)
	} else {
		tableContent := RenderTableList(layout.LeftW, layout.TablesH, tableList, snap)
		leftLines = append(leftLines, padLines(strings.Split(tableContent, "\n"), layout.TablesH)...)
	}

	// Session divider row index (relative to bottom content start).
	leftMidRow = layout.TablesH

	// -- Left: session content --
	sessionContentH := layout.SessionH - 1 // -1 for the divider row
	if sessionContentH < 0 {
		sessionContentH = 0
	}
	sessionContent := RenderSummary(layout.LeftW, sessionContentH, snap, totalQueries)
	sessionLines := padLines(strings.Split(sessionContent, "\n"), sessionContentH)

	// -- Right: query content --
	filteredEntries := logEntries
	if searchQuery != "" {
		filteredEntries = filterEntries(logEntries, searchQuery)
	}
	queryContent := RenderQueryStream(layout.RightW, layout.QueryH, filteredEntries)
	rightQueryLines := padLines(strings.Split(queryContent, "\n"), layout.QueryH)

	// -- Compose bottom rows in lockstep --
	totalBottomRows := layout.TablesH + layout.SessionH
	leftIdx, sessionIdx, rightIdx := 0, 0, 0

	for row := 0; row < totalBottomRows; row++ {
		isInspectMid := (inspecting && inspectTable != "" && inspectMidRow >= 0 && row == inspectMidRow)
		isSessionMid := (row == leftMidRow)

		rightCell := ""
		if rightIdx < len(rightQueryLines) {
			rightCell = rightQueryLines[rightIdx]
			rightIdx++
		}

		if isInspectMid && isSessionMid {
			// Both dividers on same row (unlikely but handle).
			title := "inspect: " + Ellipsis(inspectTable, layout.LeftW-12)
			rows = append(rows, buildLeftMidBorder(title, rightCell, layout.LeftW, layout.RightW))
		} else if isInspectMid {
			title := "inspect: " + Ellipsis(inspectTable, layout.LeftW-12)
			rows = append(rows, buildLeftMidBorder(title, rightCell, layout.LeftW, layout.RightW))
		} else if isSessionMid {
			// Session divider: ├─ Session ──┤ on left, continue queries on right.
			rows = append(rows, buildLeftMidBorder("Session", rightCell, layout.LeftW, layout.RightW))
		} else if row < leftMidRow {
			// Tables area.
			leftCell := ""
			if leftIdx < len(leftLines) {
				leftCell = leftLines[leftIdx]
				leftIdx++
			}
			rows = append(rows, buildContentRow(leftCell, rightCell, layout.LeftW, layout.RightW))
		} else {
			// Session area.
			leftCell := ""
			if sessionIdx < len(sessionLines) {
				leftCell = sessionLines[sessionIdx]
				sessionIdx++
			}
			rows = append(rows, buildContentRow(leftCell, rightCell, layout.LeftW, layout.RightW))
		}
	}

	// ═══════════════════════════════════════════════
	// 6. Bottom bar
	// ═══════════════════════════════════════════════
	bottomBar := RenderBottomBar(layout.Width, searching)
	rows = append(rows, bottomBar)

	return strings.Join(rows, "\n")
}

// padLines pads or truncates a slice of strings to exactly n lines.
func padLines(lines []string, n int) []string {
	for len(lines) < n {
		lines = append(lines, "")
	}
	if len(lines) > n {
		lines = lines[:n]
	}
	return lines
}

// buildPanelHeader: ├─ LeftTitle ──┬─ RightTitle ──┤
func buildPanelHeader(leftTitle, rightTitle string, leftW, rightW int) string {
	leftRendered := " " + PanelTitle.Render(leftTitle) + " "
	leftTitleVisualW := lipgloss.Width(leftRendered)
	leftFill := leftW - 1 - leftTitleVisualW
	if leftFill < 0 {
		leftFill = 0
	}

	rightRendered := " " + PanelTitle.Render(rightTitle) + " "
	rightTitleVisualW := lipgloss.Width(rightRendered)
	rightFill := rightW - 1 - rightTitleVisualW
	if rightFill < 0 {
		rightFill = 0
	}

	return BorderStyle.Render("├─") + leftRendered +
		BorderStyle.Render(strings.Repeat("─", leftFill)+"┬─") + rightRendered +
		BorderStyle.Render(strings.Repeat("─", rightFill)+"┤")
}

// buildContentRow: │ left │ right │
func buildContentRow(leftContent, rightContent string, leftW, rightW int) string {
	pipe := BorderStyle.Render("│")

	leftPad := leftW - lipgloss.Width(leftContent)
	if leftPad < 0 {
		leftPad = 0
	}
	rightPad := rightW - lipgloss.Width(rightContent)
	if rightPad < 0 {
		rightPad = 0
	}

	return pipe + leftContent + strings.Repeat(" ", leftPad) + pipe +
		rightContent + strings.Repeat(" ", rightPad) + pipe
}

// buildLeftMidBorder: ├─ Title ──┤ rightContent │
func buildLeftMidBorder(title, rightContent string, leftW, rightW int) string {
	pipe := BorderStyle.Render("│")

	titleRendered := " " + PanelTitle.Render(title) + " "
	titleVisualW := lipgloss.Width(titleRendered)
	fill := leftW - 1 - titleVisualW
	if fill < 0 {
		fill = 0
	}

	rightPad := rightW - lipgloss.Width(rightContent)
	if rightPad < 0 {
		rightPad = 0
	}

	return BorderStyle.Render("├─") + titleRendered +
		BorderStyle.Render(strings.Repeat("─", fill)+"┤") +
		rightContent + strings.Repeat(" ", rightPad) + pipe
}

// filterEntries filters log entries by search query.
func filterEntries(entries []logging.LogEntry, query string) []logging.LogEntry {
	q := strings.ToLower(query)
	var filtered []logging.LogEntry
	for _, e := range entries {
		if strings.Contains(strings.ToLower(e.SQL), q) ||
			strings.Contains(strings.ToLower(e.Strategy), q) ||
			strings.Contains(strings.ToLower(e.Event), q) ||
			strings.Contains(strings.ToLower(strings.Join(e.Tables, ",")), q) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}
