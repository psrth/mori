package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/mori-dev/mori/internal/logging"
)

// DashboardLayout holds computed panel dimensions.
type DashboardLayout struct {
	Width        int
	Height       int
	LeftW        int // left panel inner width
	RightW       int // right panel inner width
	TopInnerH    int // content rows in top section
	BottomInnerH int // content rows in query section
}

// ComputeLayout calculates panel dimensions from terminal size.
func ComputeLayout(width, height int) DashboardLayout {
	l := DashboardLayout{Width: width, Height: height}

	// Content height: minus top bar (1) and bottom bar (1).
	contentH := height - 2
	if contentH < 10 {
		contentH = 10
	}

	// Top section: ~40% of content. Bottom: ~60%.
	topH := contentH * 2 / 5
	if topH < 8 {
		topH = 8
	}
	bottomH := contentH - topH

	// Inner heights: subtract 1 header row each (borders are shared).
	l.TopInnerH = topH - 1
	l.BottomInnerH = bottomH - 1

	// Left panel width.
	l.LeftW = width / 3
	if l.LeftW > 38 {
		l.LeftW = 38
	}
	if l.LeftW < 22 {
		l.LeftW = 22
	}

	// Right panel: total = 1(│) + leftW + 1(│) + rightW + 1(│) = width.
	l.RightW = width - l.LeftW - 3

	return l
}

// RenderDashboard composes the single-screen dashboard view.
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
) string {
	if layout.Width < 50 || layout.Height < 12 {
		msg := "Terminal too small.\nMinimum: 50x12"
		return lipgloss.Place(layout.Width, layout.Height, lipgloss.Center, lipgloss.Center, msg)
	}

	pipe := BorderStyle.Render("│")

	// --- Top bar ---
	topBar := RenderTopBar(layout.Width, snap)

	// --- Build left panel content ---
	var leftContent []string
	leftMidRow := -1 // row index for inspect mid-border (-1 = none)

	if inspecting && inspectTable != "" {
		tablesH := layout.TopInnerH / 2
		inspectH := layout.TopInnerH - tablesH - 1 // -1 for mid-border row

		tableContent := RenderTableList(layout.LeftW, tablesH, tableList, snap)
		leftContent = append(leftContent, padLines(strings.Split(tableContent, "\n"), tablesH)...)

		leftMidRow = tablesH

		inspectContent := RenderInspectInline(layout.LeftW, inspectH, inspectTable, snap)
		leftContent = append(leftContent, padLines(strings.Split(inspectContent, "\n"), inspectH)...)
	} else {
		tableContent := RenderTableList(layout.LeftW, layout.TopInnerH, tableList, snap)
		leftContent = padLines(strings.Split(tableContent, "\n"), layout.TopInnerH)
	}

	// --- Build right panel content ---
	var rightContent []string
	rightMidRow := -1

	sessionH := 5
	routingH := layout.TopInnerH - sessionH - 1
	if routingH < 3 {
		// Not enough room for routing chart — give all to session.
		sessionH = layout.TopInnerH
		routingH = 0
	}

	sessionContent := RenderSummary(layout.RightW, sessionH, snap, totalQueries)
	rightContent = append(rightContent, padLines(strings.Split(sessionContent, "\n"), sessionH)...)

	if routingH > 0 {
		rightMidRow = sessionH
		routingContent := RenderRoutingChart(layout.RightW, routingH, logEntries)
		rightContent = append(rightContent, padLines(strings.Split(routingContent, "\n"), routingH)...)
	}

	// --- Compose top section row by row ---
	var rows []string

	// Panel headers: ├─ Tables (N hot) ──┬─ Session ──┤
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
	rows = append(rows, buildPanelHeader(tablesTitle, "Session", layout.LeftW, layout.RightW))

	// Content rows — walk through both panels in lockstep.
	leftIdx, rightIdx := 0, 0
	for row := 0; row < layout.TopInnerH; row++ {
		isLeftMid := (row == leftMidRow)
		isRightMid := (row == rightMidRow)

		if isLeftMid && isRightMid {
			leftTitle := "inspect: " + Ellipsis(inspectTable, layout.LeftW-12)
			rows = append(rows, buildDoubleMidBorder(leftTitle, "Routing", layout.LeftW, layout.RightW))
		} else if isLeftMid {
			leftTitle := "inspect: " + Ellipsis(inspectTable, layout.LeftW-12)
			rightCell := ""
			if rightIdx < len(rightContent) {
				rightCell = rightContent[rightIdx]
				rightIdx++
			}
			rows = append(rows, buildLeftMidBorder(leftTitle, rightCell, layout.LeftW, layout.RightW))
		} else if isRightMid {
			leftCell := ""
			if leftIdx < len(leftContent) {
				leftCell = leftContent[leftIdx]
				leftIdx++
			}
			rows = append(rows, buildRightMidBorder(leftCell, "Routing", layout.LeftW, layout.RightW))
		} else {
			leftCell := ""
			if leftIdx < len(leftContent) {
				leftCell = leftContent[leftIdx]
				leftIdx++
			}
			rightCell := ""
			if rightIdx < len(rightContent) {
				rightCell = rightContent[rightIdx]
				rightIdx++
			}
			rows = append(rows, buildContentRow(leftCell, rightCell, layout.LeftW, layout.RightW))
		}
	}

	// --- Queries header: ├─ Queries ──┴──────┤ ---
	queriesTitle := "Queries"
	if searching {
		queriesTitle = "Queries ─ /" + searchQuery + "█"
	} else if searchQuery != "" {
		queriesTitle = "Queries ─ /" + searchQuery
	}
	rows = append(rows, buildQueriesHeader(queriesTitle, layout.Width, layout.LeftW))

	// --- Query content ---
	filteredEntries := logEntries
	if searchQuery != "" {
		filteredEntries = filterEntries(logEntries, searchQuery)
	}
	queryContent := RenderQueryStream(layout.Width-2, layout.BottomInnerH, filteredEntries)
	qLines := padLines(strings.Split(queryContent, "\n"), layout.BottomInnerH)
	for _, line := range qLines {
		lineW := lipgloss.Width(line)
		pad := layout.Width - 2 - lineW
		if pad < 0 {
			pad = 0
		}
		rows = append(rows, pipe+line+strings.Repeat(" ", pad)+pipe)
	}

	// --- Bottom bar ---
	bottomBar := RenderBottomBar(layout.Width, searching)

	all := append([]string{topBar}, rows...)
	all = append(all, bottomBar)
	return strings.Join(all, "\n")
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

// buildRightMidBorder: │ leftContent ├─ Routing ──┤
func buildRightMidBorder(leftContent, title string, leftW, rightW int) string {
	pipe := BorderStyle.Render("│")

	leftPad := leftW - lipgloss.Width(leftContent)
	if leftPad < 0 {
		leftPad = 0
	}

	titleRendered := " " + PanelTitle.Render(title) + " "
	titleVisualW := lipgloss.Width(titleRendered)
	fill := rightW - 1 - titleVisualW
	if fill < 0 {
		fill = 0
	}

	return pipe + leftContent + strings.Repeat(" ", leftPad) +
		BorderStyle.Render("├─") + titleRendered +
		BorderStyle.Render(strings.Repeat("─", fill)+"┤")
}

// buildLeftMidBorder: ├─ inspect ──┤ rightContent │
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

// buildDoubleMidBorder: ├─ left ──┼─ right ──┤
func buildDoubleMidBorder(leftTitle, rightTitle string, leftW, rightW int) string {
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
		BorderStyle.Render(strings.Repeat("─", leftFill)+"┼─") + rightRendered +
		BorderStyle.Render(strings.Repeat("─", rightFill)+"┤")
}

// buildQueriesHeader: ├─ Queries ──┴──────┤
func buildQueriesHeader(title string, totalW, leftW int) string {
	titleRendered := " " + PanelTitle.Render(title) + " "
	titleVisualW := lipgloss.Width(titleRendered)
	rightW := totalW - leftW - 3

	fillBefore := leftW - 1 - titleVisualW
	if fillBefore >= 0 {
		return BorderStyle.Render("├─") + titleRendered +
			BorderStyle.Render(strings.Repeat("─", fillBefore)+"┴"+strings.Repeat("─", rightW)+"┤")
	}

	// Title extends past divider — skip ┴.
	innerW := totalW - 2
	fill := innerW - 1 - titleVisualW
	if fill < 0 {
		fill = 0
	}
	return BorderStyle.Render("├─") + titleRendered +
		BorderStyle.Render(strings.Repeat("─", fill)+"┤")
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
