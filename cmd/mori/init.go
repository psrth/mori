package main

import (
	"fmt"
	"strings"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine/postgres"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a Mori project",
	Long: `Connect to a production database, dump its schema, spin up a local Shadow
database container, apply the schema, offset sequences, and create the .mori/
configuration directory.

The Shadow database is a schema-only clone of production with zero rows.
All local mutations will be stored here.`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringP("from", "f", "", "Production database connection string (required)")
	initCmd.Flags().String("image", "", "Docker image for the Shadow database (default: auto-detect from Prod version)")
	initCmd.MarkFlagRequired("from")
}

func runInit(cmd *cobra.Command, args []string) error {
	from, _ := cmd.Flags().GetString("from")
	image, _ := cmd.Flags().GetString("image")

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	if config.IsInitialized(projectRoot) {
		return fmt.Errorf("mori is already initialized in %s — run 'mori reset --hard' to re-initialize", projectRoot)
	}

	result, err := postgres.Init(cmd.Context(), postgres.InitOptions{
		ProdConnStr:   from,
		ImageOverride: image,
		ProjectRoot:   projectRoot,
	})
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Mori initialized successfully!")
	fmt.Printf("  Engine:     PostgreSQL %s\n", result.Config.EngineVersion)
	fmt.Printf("  Shadow:     localhost:%d (container: %s)\n", result.Container.HostPort, result.Container.ContainerName)
	fmt.Printf("  Image:      %s\n", result.Config.ShadowImage)
	fmt.Printf("  Tables:     %d\n", len(result.Dump.Tables))
	fmt.Printf("  Sequences:  %d offset\n", len(result.Dump.Sequences))
	if len(result.Config.Extensions) > 0 {
		fmt.Printf("  Extensions: %s\n", strings.Join(result.Config.Extensions, ", "))
	}
	fmt.Println()
	fmt.Println("Next: run 'mori start' to begin proxying.")

	return nil
}
