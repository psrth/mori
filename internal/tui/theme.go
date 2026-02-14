package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Colors.
var (
	ColorAccent  = lipgloss.Color("#7C3AED") // purple
	ColorRunning = lipgloss.Color("#22C55E") // green
	ColorStopped = lipgloss.Color("#EF4444") // red
	ColorWarning = lipgloss.Color("#EAB308") // yellow
	ColorMuted   = lipgloss.Color("#6B7280") // gray
	ColorCyan    = lipgloss.Color("#06B6D4")
	ColorPurple  = lipgloss.Color("#A855F7")
	ColorWhite   = lipgloss.Color("#F9FAFB")
	ColorDim     = lipgloss.Color("#4B5563")
	ColorBorder  = lipgloss.Color("#555555")
)

// Strategy colors map routing strategies to colors.
var StrategyColor = map[string]lipgloss.Color{
	"PROD_DIRECT":       ColorMuted,
	"MERGED_READ":       ColorWarning,
	"JOIN_PATCH":        ColorCyan,
	"SHADOW_WRITE":      ColorRunning,
	"HYDRATE_AND_WRITE": ColorRunning,
	"SHADOW_DELETE":     ColorStopped,
	"SHADOW_DDL":        ColorPurple,
	"TRANSACTION":       ColorDim,
}

// Styles.
var (
	TopBarRunning = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorRunning)

	TopBarStopped = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorStopped)

	PanelTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorAccent)

	BottomBarStyle = lipgloss.NewStyle().
			Foreground(ColorMuted)

	ShortcutKey = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorAccent)

	DimText = lipgloss.NewStyle().
		Foreground(ColorMuted)

	DeltaStyle = lipgloss.NewStyle().
			Foreground(ColorWarning)

	TombstoneStyle = lipgloss.NewStyle().
			Foreground(ColorStopped)

	SchemaStyle = lipgloss.NewStyle().
			Foreground(ColorPurple)

	SelectedRow = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorWhite)

	BorderStyle = lipgloss.NewStyle().
			Foreground(ColorBorder)
)

// Box renders a btop-style bordered box with a title in the top border.
//
//	╭─ Title ────────────╮
//	│ content             │
//	╰─────────────────────╯
func Box(title string, content string, width, height int) string {
	innerW := width - 2 // subtract left+right border chars

	// Top border: ╭─ Title ───...───╮
	var top strings.Builder
	top.WriteString(BorderStyle.Render("╭─"))
	titleRendered := " " + PanelTitle.Render(title) + " "
	top.WriteString(titleRendered)
	titleVisualW := 2 + lipgloss.Width(titleRendered) // "╭─" + title
	remaining := width - titleVisualW - 1              // -1 for "╮"
	if remaining > 0 {
		top.WriteString(BorderStyle.Render(strings.Repeat("─", remaining) + "╮"))
	} else {
		top.WriteString(BorderStyle.Render("╮"))
	}

	// Bottom border: ╰───...───╯
	bottom := BorderStyle.Render("╰" + strings.Repeat("─", innerW) + "╯")

	// Content lines.
	lines := strings.Split(content, "\n")

	// Calculate inner height (total height - top border - bottom border).
	innerH := height - 2
	if innerH < 0 {
		innerH = 0
	}

	// Pad or truncate lines to fill inner height.
	for len(lines) < innerH {
		lines = append(lines, "")
	}
	if len(lines) > innerH {
		lines = lines[:innerH]
	}

	var rows []string
	rows = append(rows, top.String())
	pipe := BorderStyle.Render("│")
	for _, line := range lines {
		// Pad line to inner width.
		lineW := lipgloss.Width(line)
		pad := innerW - lineW
		if pad < 0 {
			pad = 0
			// Truncate if too wide — rough truncation.
			if len(line) > innerW {
				line = line[:innerW-1] + "…"
			}
		}
		rows = append(rows, pipe+line+strings.Repeat(" ", pad)+pipe)
	}
	rows = append(rows, bottom)

	return strings.Join(rows, "\n")
}

// HLine draws a horizontal line that connects left and right borders.
// Used between vertically stacked boxes: ├───...───┤
func HLine(width int) string {
	innerW := width - 2
	if innerW < 0 {
		innerW = 0
	}
	return BorderStyle.Render("├" + strings.Repeat("─", innerW) + "┤")
}

// Ellipsis truncates s to maxW visible characters, adding "…" if truncated.
func Ellipsis(s string, maxW int) string {
	if maxW <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= maxW {
		return s
	}
	// Rough byte-level truncation (works for ASCII, close enough for mixed).
	if len(s) > maxW-1 {
		return s[:maxW-1] + "…"
	}
	return s
}

// PadRight pads s with spaces to reach exactly targetW visible characters.
func PadRight(s string, targetW int) string {
	w := lipgloss.Width(s)
	if w >= targetW {
		return s
	}
	return s + strings.Repeat(" ", targetW-w)
}
