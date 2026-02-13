package main

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/charmbracelet/huh"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

// nameRe validates connection names: lowercase alphanumeric + hyphens, 1-40 chars.
var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// runInteractiveInit walks the user through engine → provider → fields → name
// and writes the result to mori.yaml.
func runInteractiveInit(projectRoot string, existingCfg *config.ProjectConfig) error {
	// ── Step 1: Select engine ────────────────────────────────────
	engineOptions := buildEngineOptions()
	var engineID string

	err := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select database engine").
				Options(engineOptions...).
				Value(&engineID),
		),
	).Run()
	if err != nil {
		return err
	}

	engine, ok := registry.EngineByID(registry.EngineID(engineID))
	if !ok {
		return fmt.Errorf("unknown engine: %s", engineID)
	}

	if !engine.Supported {
		fmt.Printf("\n  %s is not yet supported — config will be saved for future use.\n\n", engine.DisplayName)
	}

	// ── Step 2: Select provider ──────────────────────────────────
	providerOptions := buildProviderOptions(registry.EngineID(engineID))
	var providerID string

	err = huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select provider").
				Options(providerOptions...).
				Value(&providerID),
		),
	).Run()
	if err != nil {
		return err
	}

	// ── Step 3: Collect connection fields ────────────────────────
	authProvider := auth.Lookup(registry.ProviderID(providerID))
	fields := authProvider.Fields(registry.EngineID(engineID))
	if fields == nil {
		fields = registry.FieldsWithProviderDefaults(
			registry.EngineID(engineID),
			registry.ProviderID(providerID),
		)
	}

	// Allocate string pointers for each field so huh can bind to them.
	fieldPtrs := make([]*string, len(fields))
	for i, f := range fields {
		v := f.Default
		fieldPtrs[i] = &v
	}

	fieldGroup := buildFieldInputs(fields, fieldPtrs)
	err = huh.NewForm(fieldGroup).Run()
	if err != nil {
		return err
	}

	// Collect values back into a map.
	values := make(map[string]string, len(fields))
	for i, f := range fields {
		values[f.Key] = *fieldPtrs[i]
	}

	// ── Step 4: Connection name ──────────────────────────────────
	var connName string
	err = huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Connection name").
				Description("A short identifier for this connection (lowercase, hyphens ok)").
				Placeholder("my-prod-db").
				Value(&connName).
				Validate(func(s string) error {
					if !nameRe.MatchString(s) {
						return fmt.Errorf("must be 1-40 lowercase alphanumeric chars or hyphens, starting with a letter/digit")
					}
					if existingCfg != nil {
						if existingCfg.GetConnection(s) != nil {
							return fmt.Errorf("connection %q already exists in mori.yaml", s)
						}
					}
					return nil
				}),
		),
	).Run()
	if err != nil {
		return err
	}

	// ── Step 5: Build and save connection ────────────────────────
	conn := buildConnection(engineID, providerID, fields, values)

	if existingCfg == nil {
		existingCfg = config.NewProjectConfig()
	}
	existingCfg.AddConnection(connName, conn)

	if err := config.WriteProjectConfig(projectRoot, existingCfg); err != nil {
		return fmt.Errorf("failed to write mori.yaml: %w", err)
	}

	// ── Summary ──────────────────────────────────────────────────
	fmt.Println()
	fmt.Printf("  Connection %q saved to mori.yaml\n", connName)
	fmt.Printf("    Engine:   %s\n", engine.DisplayName)
	if p, ok := registry.ProviderByID(registry.ProviderID(providerID)); ok {
		fmt.Printf("    Provider: %s\n", p.DisplayName)
	}
	if conn.Host != "" {
		fmt.Printf("    Host:     %s\n", conn.Host)
	}
	if conn.Database != "" {
		fmt.Printf("    Database: %s\n", conn.Database)
	}
	fmt.Println()
	if engine.Supported {
		fmt.Printf("  Next: run 'mori start %s' to begin proxying.\n", connName)
	} else {
		fmt.Printf("  Note: %s support is coming soon. Config saved for future use.\n", engine.DisplayName)
	}
	fmt.Println()

	return nil
}

// buildEngineOptions creates huh select options grouped by tier.
func buildEngineOptions() []huh.Option[string] {
	allEngines := registry.AllEngines()
	var options []huh.Option[string]

	var currentTier registry.EngineTier
	for _, e := range allEngines {
		if e.Tier != currentTier {
			currentTier = e.Tier
		}
		label := e.DisplayName
		if !e.Supported {
			label += " (coming soon)"
		}
		options = append(options, huh.NewOption(label, string(e.ID)))
	}
	return options
}

// buildProviderOptions creates huh select options for providers compatible
// with the given engine, grouped by tier.
func buildProviderOptions(engineID registry.EngineID) []huh.Option[string] {
	compatible := registry.ProvidersForEngine(engineID)
	var options []huh.Option[string]

	var currentTier registry.ProviderTier
	for _, p := range compatible {
		if p.Tier != currentTier {
			currentTier = p.Tier
		}
		options = append(options, huh.NewOption(p.DisplayName, string(p.ID)))
	}
	return options
}

// buildFieldInputs creates a huh group with text inputs for each connection field.
// fieldPtrs must be pre-allocated string pointers, one per field.
func buildFieldInputs(fields []registry.ConnectionField, fieldPtrs []*string) *huh.Group {
	var huhFields []huh.Field

	for i, f := range fields {
		input := huh.NewInput().
			Title(f.Label).
			Placeholder(f.Placeholder).
			Value(fieldPtrs[i])

		if f.Sensitive {
			input = input.EchoMode(huh.EchoModePassword)
		}
		if f.Validate != nil {
			input = input.Validate(f.Validate)
		}

		huhFields = append(huhFields, input)
	}

	return huh.NewGroup(huhFields...)
}

// buildConnection creates a Connection from the collected field values.
func buildConnection(engineID, providerID string, fields []registry.ConnectionField, values map[string]string) *config.Connection {
	conn := &config.Connection{
		Engine:   engineID,
		Provider: providerID,
	}

	// Standard fields map directly to struct fields.
	standardKeys := map[string]bool{
		"host": true, "port": true, "user": true,
		"password": true, "database": true, "ssl_mode": true,
	}

	extras := make(map[string]string)
	for _, f := range fields {
		v := values[f.Key]
		if v == "" {
			continue
		}
		switch f.Key {
		case "host":
			conn.Host = v
		case "port":
			if n, err := strconv.Atoi(v); err == nil {
				conn.Port = n
			}
		case "user":
			conn.User = v
		case "password":
			conn.Password = v
		case "database":
			conn.Database = v
		case "ssl_mode", "ssl":
			conn.SSLMode = v
		default:
			if !standardKeys[f.Key] {
				extras[f.Key] = v
			}
		}
	}

	if len(extras) > 0 {
		conn.Extra = extras
	}

	return conn
}
