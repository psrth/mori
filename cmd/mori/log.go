package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Show proxy activity log",
	Long: `Stream or dump recent proxy activity: queries, routing decisions,
hydration events, and timing information. Useful for debugging and
understanding how Mori routes your application's queries.`,
	RunE: runLog,
}

func init() {
	logCmd.Flags().IntP("tail", "n", 50, "Number of recent log entries to show")
}

func runLog(cmd *cobra.Command, args []string) error {
	tail, _ := cmd.Flags().GetInt("tail")

	fmt.Printf("Showing last %d log entries...\n", tail)
	fmt.Println("\n[stub] Log not yet implemented.")
	return nil
}
