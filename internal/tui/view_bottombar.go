package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Shortcut pairs: key, label.
var mainShortcuts = [][2]string{
	{"i", "inspect"},
	{"/", "search"},
	{"?", "help"},
	{"q", "quit"},
}

// RenderBottomBar renders the bottom border with keyboard shortcuts embedded.
func RenderBottomBar(width int, searching bool) string {
	innerW := width - 2

	if searching {
		hint := DimText.Render("[esc]cancel [enter]confirm")
		hintW := lipgloss.Width(hint)
		fill := innerW - hintW - 2
		if fill < 0 {
			fill = 0
		}
		return BorderStyle.Render("╰─") + " " + hint + " " +
			BorderStyle.Render(strings.Repeat("─", fill)+"╯")
	}

	var parts []string
	for _, sc := range mainShortcuts {
		key := ShortcutKey.Render("[" + sc[0] + "]")
		label := BottomBarStyle.Render(sc[1])
		parts = append(parts, key+label)
	}

	content := strings.Join(parts, " ")
	contentW := lipgloss.Width(content)

	fill := innerW - contentW - 2
	if fill < 0 {
		fill = 0
	}

	return BorderStyle.Render("╰─") + " " + content + " " +
		BorderStyle.Render(strings.Repeat("─", fill)+"╯")
}
