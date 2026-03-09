package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop [connection-name]",
	Short: "Stop the Mori proxy",
	Long: `Gracefully stop the Mori proxy for a connection. Active connections are
drained, state is persisted to disk, and the proxy process exits.

If no connection name is given and only one connection is running, it is
stopped automatically. If multiple are running, an error lists them.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runStop,
}

func init() {
	stopCmd.Flags().Bool("keep-shadow", false, "Leave the Shadow database container running")
}

func runStop(cmd *cobra.Command, args []string) error {
	// 1. Find project root.
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	// 2. Resolve connection name.
	connName, err := resolveRunningConnection(projectRoot, args)
	if err != nil {
		return err
	}

	// 3. Read PID file.
	pidPath := config.ConnPidFilePath(projectRoot, connName)
	data, err := os.ReadFile(pidPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("connection %q is not running (no PID file found)", connName)
		}
		return fmt.Errorf("failed to read PID file: %w", err)
	}

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		os.Remove(pidPath)
		return fmt.Errorf("corrupt PID file — removed")
	}

	// 4. Check if process is alive.
	process, err := os.FindProcess(pid)
	if err != nil {
		os.Remove(pidPath)
		return fmt.Errorf("no process found for PID %d — removed stale PID file", pid)
	}
	if err := process.Signal(syscall.Signal(0)); err != nil {
		os.Remove(pidPath)
		return fmt.Errorf("proxy process (PID %d) is not running — removed stale PID file", pid)
	}

	// 5. Send SIGTERM and wait.
	stopMsg := fmt.Sprintf("Stopping proxy [%s]...", connName)
	stopErr := ui.Spinner(stopMsg, func() error {
		if err := process.Signal(syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to send SIGTERM to PID %d: %w", pid, err)
		}

		deadline := time.After(15 * time.Second)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-deadline:
				return fmt.Errorf("proxy did not stop within 15 seconds (PID %d may still be running)", pid)
			case <-ticker.C:
				if _, err := os.Stat(pidPath); os.IsNotExist(err) {
					return nil
				}
				if err := process.Signal(syscall.Signal(0)); err != nil {
					os.Remove(pidPath)
					return nil
				}
			}
		}
	})
	if stopErr != nil {
		return stopErr
	}

	ui.StepDone(fmt.Sprintf("Stopped proxy %s.", ui.Cyan(connName)))

	// 6. Stop Shadow container unless --keep-shadow.
	keepShadow, _ := cmd.Flags().GetBool("keep-shadow")
	if !keepShadow {
		if cfg, cfgErr := config.ReadConnConfig(projectRoot, connName); cfgErr == nil && cfg.ShadowContainer != "" {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer stopCancel()
			_ = ui.Spinner("Stopping Shadow container...", func() error {
				stopCmd := exec.CommandContext(stopCtx, "docker", "stop", "-t", "10", cfg.ShadowContainer)
				if out, err := stopCmd.CombinedOutput(); err != nil {
					log.Printf("Warning: could not stop Shadow container: %s", strings.TrimSpace(string(out)))
				}
				return nil
			})
		}
	} else {
		ui.Info("Shadow container left running (--keep-shadow).")
	}

	stopConnTunnelIfRunning(projectRoot, connName)
	return nil
}

// resolveRunningConnection figures out which connection to stop.
func resolveRunningConnection(projectRoot string, args []string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}

	// Find all running connections.
	var running []string
	for _, name := range config.InitializedConnections(projectRoot) {
		pidPath := config.ConnPidFilePath(projectRoot, name)
		if _, alive := isProxyRunning(pidPath); alive {
			running = append(running, name)
		}
	}

	if len(running) == 0 {
		return "", fmt.Errorf("no running connections found")
	}
	if len(running) == 1 {
		return running[0], nil
	}

	return "", fmt.Errorf("multiple connections running (%v) — specify which to stop: mori stop <name>", running)
}

// stopConnTunnelIfRunning kills the tunnel subprocess for a specific connection.
func stopConnTunnelIfRunning(projectRoot, connName string) {
	tunnelPidPath := config.ConnTunnelPidFilePath(projectRoot, connName)
	data, err := os.ReadFile(tunnelPidPath)
	if err != nil {
		return
	}
	defer os.Remove(tunnelPidPath)

	tunnelPid, err := strconv.Atoi(string(data))
	if err != nil {
		return
	}
	process, err := os.FindProcess(tunnelPid)
	if err != nil {
		return
	}
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return // already dead
	}
	_ = process.Signal(syscall.SIGTERM)
	time.Sleep(2 * time.Second)
	if err := process.Signal(syscall.Signal(0)); err == nil {
		_ = process.Kill()
	}
}
