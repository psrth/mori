package tui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
)

// Run launches the TUI dashboard for a specific connection.
func Run(projectRoot, connName string, initialTail int) error {
	model := NewModel(projectRoot, connName, initialTail)
	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}
	return nil
}
