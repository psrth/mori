package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Version is set at build time via ldflags.
var Version = "dev"

var rootCmd = &cobra.Command{
	Use:   "mori",
	Short: "Database virtualization layer",
	Long: `Mori is a transparent proxy that sits between your application and a
production database. It provides copy-on-write semantics: reads come from
production, writes are captured locally. Your application sees one unified
database. Production is never written to.

Break something, reset, start over.`,
	Version: Version,
}

func init() {
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(logCmd)
	rootCmd.AddCommand(inspectCmd)
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(rmCmd)
	rootCmd.AddCommand(configCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
