package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset all local state",
	Long: `Wipe all local mutations and restore a clean-slate view of production.

This truncates all Shadow database tables, clears the Delta Map, Tombstone
Set, and Schema Registry. After reset, the application sees only production
data as if starting fresh.

Use --hard to also re-sync the schema from production (useful when the
production schema has changed since initialization).`,
	RunE: runReset,
}

func init() {
	resetCmd.Flags().Bool("hard", false, "Re-sync schema from production")
}

func runReset(cmd *cobra.Command, args []string) error {
	hard, _ := cmd.Flags().GetBool("hard")

	fmt.Println("Resetting Mori state...")
	if hard {
		fmt.Println("  Hard reset: re-syncing schema from production.")
	}
	fmt.Println("\n[stub] Reset not yet implemented.")
	return nil
}
