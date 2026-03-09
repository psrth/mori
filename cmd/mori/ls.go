package main

import (
	"fmt"
	"strings"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var lsCmd = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List all configured connections",
	Long:    `List all database connections defined in mori.yaml, along with their status.`,
	RunE:    runLs,
}

func runLs(cmd *cobra.Command, args []string) error {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	if !config.HasProjectConfig(projectRoot) {
		fmt.Println("No mori.yaml found. Run 'mori init' to add a connection.")
		return nil
	}

	projCfg, err := config.ReadProjectConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read mori.yaml: %w", err)
	}

	names := projCfg.ConnectionNames()
	if len(names) == 0 {
		fmt.Println("No connections in mori.yaml. Run 'mori init' to add one.")
		return nil
	}

	// Determine column widths.
	nameW, engineW, providerW, hostW := 4, 6, 8, 4 // minimum header widths
	for _, name := range names {
		conn := projCfg.GetConnection(name)
		if len(name) > nameW {
			nameW = len(name)
		}
		if len(conn.Engine) > engineW {
			engineW = len(conn.Engine)
		}
		if len(conn.Provider) > providerW {
			providerW = len(conn.Provider)
		}
		host := conn.Host
		if host == "" {
			host = "-"
		}
		if len(host) > hostW {
			hostW = len(host)
		}
	}

	// Print header.
	fmtStr := fmt.Sprintf("  %%-%ds  %%-%ds  %%-%ds  %%-%ds  %%s\n", nameW, engineW, providerW, hostW)
	fmt.Printf(fmtStr, "NAME", "ENGINE", "PROVIDER", "HOST", "STATUS")
	fmt.Printf("  %s\n", strings.Repeat("─", nameW+engineW+providerW+hostW+20))

	// Print rows.
	for _, name := range names {
		conn := projCfg.GetConnection(name)
		host := conn.Host
		if host == "" {
			host = "-"
		}
		status := styledConnectionStatus(name, conn, projectRoot)
		fmt.Printf(fmtStr, name, conn.Engine, conn.Provider, host, status)
	}

	return nil
}

// styledConnectionStatus returns a styled status string for a connection.
func styledConnectionStatus(name string, conn *config.Connection, projectRoot string) string {
	// Check if engine is supported.
	if !registry.IsEngineSupported(registry.EngineID(conn.Engine)) {
		return ui.Red(ui.IconFail + " unsupported")
	}

	// Check if this connection is initialized and proxy is running.
	if config.IsConnInitialized(projectRoot, name) {
		pidPath := config.ConnPidFilePath(projectRoot, name)
		if _, running := isProxyRunning(pidPath); running {
			return ui.Green(fmt.Sprintf("%s running", ui.IconActive))
		}
	}

	return ui.Dim(ui.IconInactive + " ready")
}
