package ui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Box colors for CLI output.
var (
	ColorBorder = lipgloss.Color("#3B3B3B")

	borderStyle = lipgloss.NewStyle().Foreground(ColorBorder)
	panelTitle  = lipgloss.NewStyle().Bold(true).Foreground(ColorCyan)
)

// Box renders a bordered box with a title in the top border.
//
//	╭─ Title ────────────╮
//	│ content             │
//	╰─────────────────────╯
//
// Width is auto-calculated from content if 0.
func Box(title string, content string) string {
	lines := strings.Split(content, "\n")

	// Calculate width from content.
	maxW := lipgloss.Width(title) + 6 // "╭─ " + title + " ─╮" minimum
	for _, line := range lines {
		w := lipgloss.Width(line)
		if w+2 > maxW { // +2 for "│" padding on each side
			maxW = w + 2
		}
	}
	width := maxW + 2 // +2 for border chars

	innerW := width - 2

	// Top border: ╭─ Title ───...───╮
	var top strings.Builder
	top.WriteString(borderStyle.Render("╭─"))
	titleRendered := " " + panelTitle.Render(title) + " "
	top.WriteString(titleRendered)
	titleVisualW := 2 + lipgloss.Width(titleRendered)
	remaining := width - titleVisualW - 1
	if remaining > 0 {
		top.WriteString(borderStyle.Render(strings.Repeat("─", remaining) + "╮"))
	} else {
		top.WriteString(borderStyle.Render("╮"))
	}

	// Bottom border: ╰───...───╯
	bottom := borderStyle.Render("╰" + strings.Repeat("─", innerW) + "╯")

	// Content lines.
	pipe := borderStyle.Render("│")
	var rows []string
	rows = append(rows, top.String())
	for _, line := range lines {
		lineW := lipgloss.Width(line)
		pad := innerW - lineW
		if pad < 0 {
			pad = 0
		}
		rows = append(rows, pipe+line+strings.Repeat(" ", pad)+pipe)
	}
	rows = append(rows, bottom)

	return strings.Join(rows, "\n")
}

// BoxLine formats a key-value pair for use inside a Box.
func BoxLine(label, value string, labelWidth int) string {
	return "  " + StyleDim.Render(padRight(label, labelWidth)) + "  " + value
}

// padRight pads s with spaces to reach exactly targetW visible characters.
func padRight(s string, targetW int) string {
	w := lipgloss.Width(s)
	if w >= targetW {
		return s
	}
	return s + strings.Repeat(" ", targetW-w)
}
