package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var helpSections = []struct {
	title string
	keys  [][2]string
}{
	{
		title: "Navigation",
		keys: [][2]string{
			{"j / ↓", "Move down"},
			{"k / ↑", "Move up"},
			{"enter", "Inspect selected table"},
			{"esc", "Close inspect / cancel"},
		},
	},
	{
		title: "Dashboard",
		keys: [][2]string{
			{"i", "Toggle inspect panel"},
			{"/", "Search queries"},
			{"?", "Toggle this help"},
			{"q", "Quit"},
		},
	},
}

// RenderHelp renders the help overlay centered on screen.
func RenderHelp(width, height int) string {
	var rows []string

	for _, section := range helpSections {
		rows = append(rows, " "+PanelTitle.Render(section.title))
		for _, kv := range section.keys {
			key := ShortcutKey.Render(PadRight(kv[0], 12))
			rows = append(rows, "   "+key+kv[1])
		}
		rows = append(rows, "")
	}

	content := strings.Join(rows, "\n")

	boxW := 46
	if boxW > width-4 {
		boxW = width - 4
	}
	boxH := len(rows) + 3
	if boxH > height-2 {
		boxH = height - 2
	}

	helpBox := Box("Help", content, boxW, boxH)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, helpBox)
}
