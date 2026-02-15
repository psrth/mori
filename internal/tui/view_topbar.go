package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// RenderTopBar renders a two-line top bar:
//
//	Line 1: ╭─ mori ● running ───────────╮
//	Line 2: │  DB  engine/provider  :port          PROXY  mori/conn  :port  │
func RenderTopBar(width int, snap Snapshot) string {
	innerW := width - 2

	// --- Line 1: status ---
	var status string
	if snap.ProxyRunning {
		status = TopBarRunning.Render("● running")
	} else {
		status = TopBarStopped.Render("○ stopped")
	}
	left := lipgloss.NewStyle().Bold(true).Foreground(ColorWhite).Render("mori") + " " + status
	leftW := lipgloss.Width(left)

	fill1 := innerW - leftW - 2 // -2 for the spaces around content
	if fill1 < 0 {
		fill1 = 0
	}

	line1 := BorderStyle.Render("╭─") + " " + left + " " +
		BorderStyle.Render(strings.Repeat("─", fill1)+"╮")

	// --- Line 2: DB + PROXY info ---
	pipe := BorderStyle.Render("│")

	// DB section.
	dbLabel := LabelBold.Render("DB")
	var dbInfo string
	if snap.Config != nil {
		engine := snap.Config.Engine
		provider := ""
		dbPort := 0
		if snap.ProjConfig != nil && snap.Config.ActiveConnection != "" {
			if conn := snap.ProjConfig.GetConnection(snap.Config.ActiveConnection); conn != nil {
				provider = conn.Provider
				dbPort = conn.Port
			}
		}
		if provider != "" {
			dbInfo = fmt.Sprintf("%s/%s", engine, provider)
		} else if engine != "" {
			dbInfo = engine
		}
		if dbPort > 0 {
			dbInfo += "  " + lipgloss.NewStyle().Foreground(ColorWhite).Render(fmt.Sprintf(":%d", dbPort))
		}
	}

	// PROXY section.
	proxyLabel := LabelBold.Render("PROXY")
	var proxyInfo string
	if snap.Config != nil {
		connName := snap.Config.ActiveConnection
		if connName != "" {
			proxyInfo = fmt.Sprintf("mori/%s", connName)
		} else {
			proxyInfo = "mori"
		}
		if snap.Config.ProxyPort > 0 {
			proxyInfo += "  " + lipgloss.NewStyle().Foreground(ColorWhite).Render(fmt.Sprintf(":%d", snap.Config.ProxyPort))
		}
	}

	// Compose line 2 content.
	var line2Content string
	if dbInfo != "" && proxyInfo != "" {
		// Space between DB and PROXY sections.
		dbPart := "  " + dbLabel + "  " + dbInfo
		proxyPart := proxyLabel + "  " + proxyInfo
		dbPartW := lipgloss.Width(dbPart)
		proxyPartW := lipgloss.Width(proxyPart)

		gap := innerW - dbPartW - proxyPartW - 2 // -2 for trailing pad
		if gap < 4 {
			gap = 4
		}
		line2Content = dbPart + strings.Repeat(" ", gap) + proxyPart
	} else if dbInfo != "" {
		line2Content = "  " + dbLabel + "  " + dbInfo
	} else if proxyInfo != "" {
		line2Content = "  " + proxyLabel + "  " + proxyInfo
	} else {
		line2Content = "  " + DimText.Render("(not configured)")
	}

	contentW := lipgloss.Width(line2Content)
	pad2 := innerW - contentW
	if pad2 < 0 {
		pad2 = 0
	}
	line2 := pipe + line2Content + strings.Repeat(" ", pad2) + pipe

	return line1 + "\n" + line2
}
