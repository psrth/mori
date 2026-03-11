package main

import (
	"os"

	"github.com/psrth/mori/internal/ui"
	"github.com/spf13/cobra"

	_ "github.com/psrth/mori/internal/auth/providers"
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
	Version:       Version,
	SilenceUsage:  true,
	SilenceErrors: true,
}

// Custom help template with grouped commands.
const helpTemplate = `{{.Long}}

Usage:
  {{.UseLine}}{{if .HasAvailableSubCommands}} [command]{{end}}

Setup:
  init          Add a database connection
  config        View project configuration

Lifecycle:
  start         Start the Mori proxy
  stop          Stop the Mori proxy
  reset         Reset all local state
  reinit        Drop all state and re-initialize

Inspection:
  status        Display current Mori state
  log           Show proxy activity log
  inspect       Show detailed state for a table
  dash          Launch the interactive dashboard

Management:
  ls            List all configured connections
  rm            Remove a connection

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

Use "{{.CommandPath}} [command] --help" for more about a command.
`

func init() {
	rootCmd.SetHelpTemplate(helpTemplate)
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
	rootCmd.AddCommand(reinitCmd)
	rootCmd.AddCommand(dashCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		ui.StepFail(err.Error())
		os.Exit(1)
	}
}
