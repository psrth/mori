package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the Mori proxy",
	Long: `Start the Shadow database container (if not running) and the Mori proxy.
The proxy listens on localhost and accepts database connections from your
application. Reads are served from production, writes are captured locally.`,
	RunE: runStart,
}

func init() {
	startCmd.Flags().IntP("port", "p", 5432, "Port for the proxy to listen on")
	startCmd.Flags().Bool("verbose", false, "Log all intercepted queries and routing decisions")
}

func runStart(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	verbose, _ := cmd.Flags().GetBool("verbose")

	fmt.Printf("Starting Mori proxy on port %d...\n", port)
	if verbose {
		fmt.Println("  Verbose logging enabled.")
	}
	fmt.Println("\n[stub] Start not yet implemented.")
	return nil
}
