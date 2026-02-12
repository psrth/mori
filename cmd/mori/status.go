package main

import (
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Display current Mori state",
	Long: `Show the current state of the Mori project: engine info, connection
details, delta row counts, tombstone counts, schema diffs, and sequence
offsets.

If Mori is not initialized, prints a message indicating so.`,
	RunE: runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	if !config.IsInitialized(projectRoot) {
		fmt.Println("Mori is not initialized.")
		fmt.Println("Run 'mori init --from <connection_string>' to get started.")
		return nil
	}

	cfg, err := config.ReadConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	fmt.Printf("Engine:       %s %s\n", cfg.Engine, cfg.EngineVersion)
	fmt.Printf("Prod:         %s (read-only)\n", cfg.RedactedProdConnection())
	fmt.Printf("Shadow:       localhost:%d\n", cfg.ShadowPort)

	// Show proxy running state.
	pidPath := config.PidFilePath(projectRoot)
	if data, err := os.ReadFile(pidPath); err == nil {
		if pid, err := strconv.Atoi(string(data)); err == nil {
			if proc, err := os.FindProcess(pid); err == nil {
				if err := proc.Signal(syscall.Signal(0)); err == nil {
					fmt.Printf("Proxy:        localhost:%d (running, PID %d)\n", cfg.ProxyPort, pid)
				} else {
					fmt.Printf("Proxy:        localhost:%d (stopped)\n", cfg.ProxyPort)
				}
			} else {
				fmt.Printf("Proxy:        localhost:%d (stopped)\n", cfg.ProxyPort)
			}
		} else {
			fmt.Printf("Proxy:        localhost:%d (stopped)\n", cfg.ProxyPort)
		}
	} else {
		fmt.Printf("Proxy:        localhost:%d (stopped)\n", cfg.ProxyPort)
	}

	fmt.Printf("Initialized:  %s\n", cfg.InitializedAt.Format("2006-01-02 15:04:05"))

	return nil
}
