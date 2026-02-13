package main

import (
	"fmt"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/spf13/cobra"
)

var rmCmd = &cobra.Command{
	Use:   "rm <connection-name>",
	Short: "Remove a connection from mori.yaml",
	Long:  `Remove a named database connection from mori.yaml. Refuses if the connection's proxy is currently running.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runRm,
}

func init() {
	rmCmd.Flags().BoolP("force", "f", false, "Skip confirmation prompt")
}

func runRm(cmd *cobra.Command, args []string) error {
	name := args[0]
	force, _ := cmd.Flags().GetBool("force")

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	if !config.HasProjectConfig(projectRoot) {
		return fmt.Errorf("no mori.yaml found")
	}

	projCfg, err := config.ReadProjectConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read mori.yaml: %w", err)
	}

	if projCfg.GetConnection(name) == nil {
		return fmt.Errorf("connection %q not found in mori.yaml", name)
	}

	// Check if this connection is active and proxy is running.
	if config.IsInitialized(projectRoot) {
		if runtimeCfg, err := config.ReadConfig(projectRoot); err == nil {
			if runtimeCfg.ActiveConnection == name {
				pidPath := config.PidFilePath(projectRoot)
				if _, running := isProxyRunning(pidPath); running {
					return fmt.Errorf("connection %q is currently running — run 'mori stop' first", name)
				}
			}
		}
	}

	// Confirm unless --force.
	if !force {
		fmt.Printf("Remove connection %q from mori.yaml? [y/N] ", name)
		var confirm string
		fmt.Scanln(&confirm)
		if confirm != "y" && confirm != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	projCfg.RemoveConnection(name)
	if err := config.WriteProjectConfig(projectRoot, projCfg); err != nil {
		return fmt.Errorf("failed to write mori.yaml: %w", err)
	}

	fmt.Printf("Connection %q removed from mori.yaml.\n", name)
	return nil
}
