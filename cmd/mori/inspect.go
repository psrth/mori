package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect <table>",
	Short: "Show detailed state for a table",
	Long: `Display detailed state for a specific table: delta row count, tombstone
count, schema diffs (added/dropped/renamed columns), sequence offset,
and a sample of locally-modified rows.`,
	Args: cobra.ExactArgs(1),
	RunE: runInspect,
}

func runInspect(cmd *cobra.Command, args []string) error {
	table := args[0]

	fmt.Printf("Inspecting table: %s\n", table)
	fmt.Println("\n[stub] Inspect not yet implemented.")
	return nil
}
