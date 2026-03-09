package main

import (
	"fmt"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "View project configuration",
	Long:  `Pretty-print the contents of mori.yaml with passwords redacted.`,
	RunE:  runConfig,
}

func runConfig(cmd *cobra.Command, args []string) error {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	if !config.HasProjectConfig(projectRoot) {
		fmt.Println("No mori.yaml found. Run 'mori init' to create one.")
		return nil
	}

	projCfg, err := config.ReadProjectConfig(projectRoot)
	if err != nil {
		return fmt.Errorf("failed to read mori.yaml: %w", err)
	}

	fmt.Printf("%s (version %d)\n", ui.Bold("mori.yaml"), projCfg.Version)
	fmt.Printf("Location: %s\n\n", ui.Dim(config.ProjectConfigPath(projectRoot)))

	names := projCfg.ConnectionNames()
	if len(names) == 0 {
		fmt.Println("No connections configured.")
		return nil
	}

	for _, name := range names {
		conn := projCfg.GetConnection(name)
		fmt.Printf("  [%s]\n", ui.Cyan(name))
		fmt.Printf("    %s %s\n", ui.Dim("engine:  "), conn.Engine)
		fmt.Printf("    %s %s\n", ui.Dim("provider:"), conn.Provider)
		if conn.Host != "" {
			fmt.Printf("    %s %s\n", ui.Dim("host:    "), conn.Host)
		}
		if conn.Port != 0 {
			fmt.Printf("    %s %d\n", ui.Dim("port:    "), conn.Port)
		}
		if conn.User != "" {
			fmt.Printf("    %s %s\n", ui.Dim("user:    "), conn.User)
		}
		fmt.Printf("    %s %s\n", ui.Dim("password:"), conn.RedactedPassword())
		if conn.Database != "" {
			fmt.Printf("    %s %s\n", ui.Dim("database:"), conn.Database)
		}
		if conn.SSLMode != "" {
			fmt.Printf("    %s %s\n", ui.Dim("ssl_mode:"), conn.SSLMode)
		}
		for k, v := range conn.Extra {
			fmt.Printf("    %s %s\n", ui.Dim(k+":"), v)
		}
		fmt.Println()
	}

	return nil
}
