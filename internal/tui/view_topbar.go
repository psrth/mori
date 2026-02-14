package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// RenderTopBar renders the top status bar inside a box border line.
// Format: ╭─ mori ● running ─── postgres/direct ─── my-conn ─── localhost:19099 ─╮
func RenderTopBar(width int, snap Snapshot) string {
	innerW := width - 2

	// Status.
	var status string
	if snap.ProxyRunning {
		status = TopBarRunning.Render("● running")
	} else {
		status = TopBarStopped.Render("○ stopped")
	}
	left := lipgloss.NewStyle().Bold(true).Foreground(ColorWhite).Render("mori") + " " + status

	// Engine/provider.
	var center string
	if snap.Config != nil {
		engine := snap.Config.Engine
		provider := ""
		if snap.ProjConfig != nil && snap.Config.ActiveConnection != "" {
			if conn := snap.ProjConfig.GetConnection(snap.Config.ActiveConnection); conn != nil {
				provider = conn.Provider
			}
		}
		if provider != "" {
			center = fmt.Sprintf("%s/%s", engine, provider)
		} else if engine != "" {
			center = engine
		}
	}

	// Connection name.
	var connName string
	if snap.Config != nil && snap.Config.ActiveConnection != "" {
		connName = snap.Config.ActiveConnection
	}

	// Proxy address.
	var right string
	if snap.Config != nil && snap.Config.ProxyPort > 0 {
		right = fmt.Sprintf("localhost:%d", snap.Config.ProxyPort)
	}

	// Build: ╭─ left ─── center ─── conn ─── right ─╮
	parts := []string{left}
	if center != "" {
		parts = append(parts, center)
	}
	if connName != "" {
		parts = append(parts, connName)
	}
	if right != "" {
		parts = append(parts, right)
	}

	contentParts := strings.Join(parts, BorderStyle.Render(" ─── "))
	contentW := lipgloss.Width(contentParts)

	fill := innerW - contentW - 2
	if fill < 0 {
		fill = 0
	}

	return BorderStyle.Render("╭─") + " " + contentParts + " " +
		BorderStyle.Render(strings.Repeat("─", fill)+"╮")
}
