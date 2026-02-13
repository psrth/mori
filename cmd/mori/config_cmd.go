package main

import (
	"fmt"

	"github.com/mori-dev/mori/internal/core/config"
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

	fmt.Printf("mori.yaml (version %d)\n", projCfg.Version)
	fmt.Printf("Location: %s\n\n", config.ProjectConfigPath(projectRoot))

	names := projCfg.ConnectionNames()
	if len(names) == 0 {
		fmt.Println("No connections configured.")
		return nil
	}

	for _, name := range names {
		conn := projCfg.GetConnection(name)
		fmt.Printf("  [%s]\n", name)
		fmt.Printf("    engine:   %s\n", conn.Engine)
		fmt.Printf("    provider: %s\n", conn.Provider)
		if conn.Host != "" {
			fmt.Printf("    host:     %s\n", conn.Host)
		}
		if conn.Port != 0 {
			fmt.Printf("    port:     %d\n", conn.Port)
		}
		if conn.User != "" {
			fmt.Printf("    user:     %s\n", conn.User)
		}
		fmt.Printf("    password: %s\n", conn.RedactedPassword())
		if conn.Database != "" {
			fmt.Printf("    database: %s\n", conn.Database)
		}
		if conn.SSLMode != "" {
			fmt.Printf("    ssl_mode: %s\n", conn.SSLMode)
		}
		for k, v := range conn.Extra {
			fmt.Printf("    %s: %s\n", k, v)
		}
		fmt.Println()
	}

	return nil
}
