package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/ui"
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

	// Check if this connection's proxy is running.
	pidPath := config.ConnPidFilePath(projectRoot, name)
	if _, running := isProxyRunning(pidPath); running {
		return fmt.Errorf("connection %q is currently running — run 'mori stop %s' first", name, name)
	}

	// Confirm unless --force.
	if !force {
		ui.StepWarn(fmt.Sprintf("Removing '%s' will delete its Shadow container and all local state.", name))
		fmt.Printf("Remove connection %q from mori.yaml? [y/N] ", name)
		var confirm string
		fmt.Scanln(&confirm)
		if confirm != "y" && confirm != "Y" {
			ui.Info("Cancelled.")
			return nil
		}
	}

	// Remove Shadow container and state directory if initialized.
	if config.IsConnInitialized(projectRoot, name) {
		if cfg, cfgErr := config.ReadConnConfig(projectRoot, name); cfgErr == nil && cfg.ShadowContainer != "" {
			_ = ui.Spinner("Removing Shadow container...", func() error {
				rmCtx, rmCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer rmCancel()
				rmCmd := exec.CommandContext(rmCtx, "docker", "rm", "-f", "-v", cfg.ShadowContainer)
				if out, err := rmCmd.CombinedOutput(); err != nil {
					log.Printf("Warning: could not remove Shadow container: %s", strings.TrimSpace(string(out)))
				}
				return nil
			})
		}
		connDir := config.ConnDir(projectRoot, name)
		if err := os.RemoveAll(connDir); err != nil {
			log.Printf("Warning: could not remove state directory %s: %v", connDir, err)
		}
	}

	projCfg.RemoveConnection(name)
	if err := config.WriteProjectConfig(projectRoot, projCfg); err != nil {
		return fmt.Errorf("failed to write mori.yaml: %w", err)
	}

	ui.StepDone(fmt.Sprintf("Connection %s removed.", ui.Cyan(name)))
	return nil
}
