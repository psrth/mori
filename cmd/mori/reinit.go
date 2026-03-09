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

var reinitCmd = &cobra.Command{
	Use:   "reinit [connection-name]",
	Short: "Drop all state and re-initialize from scratch",
	Long: `Destroy the Shadow container and all local state for a connection,
while preserving the connection config in mori.yaml.

The next 'mori start' will create a fresh Shadow container and re-dump
the production schema. This is useful when you want a completely clean
slate, or when the Shadow container has become corrupted.

Use --all to reinit every initialized connection.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runReinit,
}

func init() {
	reinitCmd.Flags().BoolP("force", "f", false, "Skip confirmation prompt")
	reinitCmd.Flags().Bool("all", false, "Reinit all initialized connections")
}

func runReinit(cmd *cobra.Command, args []string) error {
	force, _ := cmd.Flags().GetBool("force")
	all, _ := cmd.Flags().GetBool("all")

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	var connNames []string
	if all {
		connNames = config.InitializedConnections(projectRoot)
		if len(connNames) == 0 {
			return fmt.Errorf("no initialized connections found")
		}
	} else {
		connName, err := resolveInitializedConnection(projectRoot, args)
		if err != nil {
			return err
		}
		connNames = []string{connName}
	}

	// Check none are running.
	for _, name := range connNames {
		pidPath := config.ConnPidFilePath(projectRoot, name)
		if _, running := isProxyRunning(pidPath); running {
			return fmt.Errorf("connection %q is currently running — run 'mori stop %s' first", name, name)
		}
	}

	// Confirm.
	if !force {
		label := strings.Join(connNames, ", ")
		ui.StepWarn(fmt.Sprintf("This will destroy the Shadow container and all local state for '%s'.", label))
		ui.Info("The connection config in mori.yaml will be preserved.")
		fmt.Printf("Proceed? [y/N] ")
		var confirm string
		fmt.Scanln(&confirm)
		if confirm != "y" && confirm != "Y" {
			ui.Info("Cancelled.")
			return nil
		}
	}

	for _, name := range connNames {
		if err := reinitConnection(projectRoot, name); err != nil {
			return err
		}
	}

	return nil
}

func reinitConnection(projectRoot, connName string) error {
	connDir := config.ConnDir(projectRoot, connName)

	// Remove Shadow container if it exists.
	if cfg, err := config.ReadConnConfig(projectRoot, connName); err == nil && cfg.ShadowContainer != "" {
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

	// Remove state directory.
	if err := ui.Spinner("Clearing state directory...", func() error {
		return os.RemoveAll(connDir)
	}); err != nil {
		return fmt.Errorf("failed to remove state directory %s: %w", connDir, err)
	}

	fmt.Println()
	ui.StepDone(fmt.Sprintf("Connection %s re-initialized.", ui.Cyan(connName)))
	ui.Info(fmt.Sprintf("Next: run 'mori start %s' to set up a fresh Shadow.", connName))
	fmt.Println()

	return nil
}
