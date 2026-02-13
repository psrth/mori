package main

import (
	"fmt"
	"strings"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
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

	// Read runtime config for active connection info.
	var runtimeCfg *config.Config
	if config.IsInitialized(projectRoot) {
		runtimeCfg, _ = config.ReadConfig(projectRoot)
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
		status := connectionStatus(name, conn, runtimeCfg, projectRoot)
		fmt.Printf(fmtStr, name, conn.Engine, conn.Provider, host, status)
	}

	return nil
}

// connectionStatus returns a human-readable status string for a connection.
func connectionStatus(name string, conn *config.Connection, runtimeCfg *config.Config, projectRoot string) string {
	// Check if engine is supported.
	if !registry.IsEngineSupported(registry.EngineID(conn.Engine)) {
		return "unsupported"
	}

	// Check if this is the active connection.
	if runtimeCfg == nil || runtimeCfg.ActiveConnection != name {
		return "ready"
	}

	// Check if proxy is running.
	pidPath := config.PidFilePath(projectRoot)
	if pid, running := isProxyRunning(pidPath); running {
		return fmt.Sprintf("running (PID %d)", pid)
	}

	return "stopped"
}
