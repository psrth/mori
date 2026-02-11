package main

import (
	"fmt"

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
	keepShadow, _ := cmd.Flags().GetBool("keep-shadow")

	fmt.Println("Stopping Mori proxy...")
	if keepShadow {
		fmt.Println("  Shadow container will be kept running.")
	}
	fmt.Println("\n[stub] Stop not yet implemented.")
	return nil
}
