package main

import (
	"fmt"
	"strings"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/engine"
	"github.com/mori-dev/mori/internal/registry"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"

	// Register engine implementations via side-effect imports.
	_ "github.com/mori-dev/mori/internal/engine/postgres"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Add a database connection",
	Long: `Interactively configure a new database connection and save it to mori.yaml.

Select an engine, a provider, fill in connection details, and give it a name.
The connection is saved locally — no containers or connections are created yet.
Run 'mori start <name>' to begin proxying.

For scripting, use --from with a connection string:
  mori init --from "postgres://user:pass@host:5432/db" --name my-db`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().StringP("from", "f", "", "Connection string (non-interactive)")
	initCmd.Flags().String("name", "", "Connection name (used with --from)")
	initCmd.Flags().String("image", "", "Docker image for Shadow container (overrides auto-detected version)")
}

func runInit(cmd *cobra.Command, args []string) error {
	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	// Load existing config if present, so we can append.
	var existing *config.ProjectConfig
	if config.HasProjectConfig(projectRoot) {
		existing, err = config.ReadProjectConfig(projectRoot)
		if err != nil {
			return fmt.Errorf("failed to read existing mori.yaml: %w", err)
		}
	}

	from, _ := cmd.Flags().GetString("from")
	name, _ := cmd.Flags().GetString("name")
	image, _ := cmd.Flags().GetString("image")

	// Non-interactive mode: --from flag provided.
	if from != "" {
		return runNonInteractiveInit(projectRoot, existing, from, name, image)
	}

	// Interactive mode (default).
	return runInteractiveInit(projectRoot, existing, image)
}

// runNonInteractiveInit parses a connection string and saves it to mori.yaml.
// Requires --name when used non-interactively.
func runNonInteractiveInit(projectRoot string, existing *config.ProjectConfig, connStr, name, image string) error {
	if name == "" {
		return fmt.Errorf("--name is required when using --from")
	}

	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid connection name %q: must be 1-40 lowercase alphanumeric chars or hyphens", name)
	}

	if existing != nil && existing.GetConnection(name) != nil {
		return fmt.Errorf("connection %q already exists in mori.yaml", name)
	}

	// Parse the connection string via the engine interface.
	// The --from flag assumes postgres for connection string parsing.
	eng, err := engine.Lookup(registry.Postgres)
	if err != nil {
		return fmt.Errorf("postgres engine not available: %w", err)
	}
	connInfo, err := eng.ParseConnStr(connStr)
	if err != nil {
		return fmt.Errorf("invalid connection string: %w", err)
	}

	conn := &config.Connection{
		Engine:   "postgres",
		Provider: "direct",
		Host:     connInfo.Host,
		Port:     connInfo.Port,
		User:     connInfo.User,
		Password: connInfo.Password,
		Database: connInfo.DBName,
		SSLMode:  connInfo.SSLMode,
	}
	if image != "" {
		conn.Extra = map[string]string{"image": image}
	}

	if existing == nil {
		existing = config.NewProjectConfig()
	}
	existing.AddConnection(name, conn)

	if err := config.WriteProjectConfig(projectRoot, existing); err != nil {
		return fmt.Errorf("failed to write mori.yaml: %w", err)
	}

	fmt.Println()
	ui.StepDone(fmt.Sprintf("Connection %s saved to mori.yaml", ui.Cyan(name)))

	const labelW = 8
	boxLines := []string{
		ui.BoxLine("Engine", "PostgreSQL", labelW),
		ui.BoxLine("Provider", "Direct / Self-Hosted", labelW),
		ui.BoxLine("Host", fmt.Sprintf("%s:%d", connInfo.Host, connInfo.Port), labelW),
		ui.BoxLine("Database", connInfo.DBName, labelW),
	}
	fmt.Println(ui.Box(name, strings.Join(boxLines, "\n")))

	fmt.Println()
	ui.Info(fmt.Sprintf("Next: run 'mori start %s' to begin proxying.", name))
	fmt.Println()

	return nil
}
