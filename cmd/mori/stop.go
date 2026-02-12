package main

import (
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the Mori proxy",
	Long: `Gracefully stop the Mori proxy. Active connections are drained,
state is persisted to disk, and the proxy process exits.

By default, the Shadow database container is also stopped. Use --keep-shadow
to leave it running for direct inspection.`,
	RunE: runStop,
}

func init() {
	stopCmd.Flags().Bool("keep-shadow", false, "Leave the Shadow database container running")
}

func runStop(cmd *cobra.Command, args []string) error {
	// --keep-shadow is a no-op in Phase 3 (shadow not started by proxy yet).
	// Wired up for forward compatibility.

	// 1. Find project root.
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}
	if !config.IsInitialized(projectRoot) {
		return fmt.Errorf("mori is not initialized")
	}

	// 2. Read PID file.
	pidPath := config.PidFilePath(projectRoot)
	data, err := os.ReadFile(pidPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("mori proxy is not running (no PID file found)")
		}
		return fmt.Errorf("failed to read PID file: %w", err)
	}

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		os.Remove(pidPath)
		return fmt.Errorf("corrupt PID file — removed")
	}

	// 3. Check if process is alive.
	process, err := os.FindProcess(pid)
	if err != nil {
		os.Remove(pidPath)
		return fmt.Errorf("no process found for PID %d — removed stale PID file", pid)
	}
	if err := process.Signal(syscall.Signal(0)); err != nil {
		os.Remove(pidPath)
		return fmt.Errorf("proxy process (PID %d) is not running — removed stale PID file", pid)
	}

	// 4. Send SIGTERM.
	fmt.Printf("Sending SIGTERM to Mori proxy (PID %d)...\n", pid)
	if err := process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to send SIGTERM to PID %d: %w", pid, err)
	}

	// 5. Wait for process to exit.
	fmt.Println("Waiting for proxy to stop...")
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("proxy did not stop within 15 seconds (PID %d may still be running)", pid)
		case <-ticker.C:
			if _, err := os.Stat(pidPath); os.IsNotExist(err) {
				fmt.Println("Mori proxy stopped.")
				return nil
			}
			if err := process.Signal(syscall.Signal(0)); err != nil {
				os.Remove(pidPath)
				fmt.Println("Mori proxy stopped.")
				return nil
			}
		}
	}
}
