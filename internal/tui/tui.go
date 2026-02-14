package tui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
)

// Run launches the TUI dashboard.
func Run(projectRoot string, initialTail int) error {
	model := NewModel(projectRoot, initialTail)
	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}
	return nil
}
