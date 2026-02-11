package main

import (
	"fmt"

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

	fmt.Printf("Initializing Mori project...\n")
	fmt.Printf("  Prod: %s\n", from)
	if image != "" {
		fmt.Printf("  Image: %s\n", image)
	}
	fmt.Println("\n[stub] Init not yet implemented.")
	return nil
}
