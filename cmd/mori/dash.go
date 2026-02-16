package main

import (
	"fmt"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/tui"
	"github.com/spf13/cobra"
)

var dashCmd = &cobra.Command{
	Use:   "dash [connection-name]",
	Short: "Launch the interactive dashboard",
	Long: `Open a live TUI dashboard that monitors the running Mori proxy.
Shows table state, live query stream, and session statistics.
The proxy must be started separately with 'mori start'.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runDash,
}

func init() {
	dashCmd.Flags().IntP("tail", "n", 100, "Number of recent log entries to load on start")
}

func runDash(cmd *cobra.Command, args []string) error {
	tailN, _ := cmd.Flags().GetInt("tail")

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	connName, err := resolveInitializedConnection(projectRoot, args)
	if err != nil {
		if config.HasProjectConfig(projectRoot) {
			return fmt.Errorf("no initialized connections — run 'mori start' first")
		}
		return fmt.Errorf("mori is not initialized — run 'mori init' first")
	}

	return tui.Run(projectRoot, connName, tailN)
}
