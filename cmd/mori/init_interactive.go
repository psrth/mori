package main

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/charmbracelet/huh"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
	"github.com/psrth/mori/internal/tunnel"
	"github.com/psrth/mori/internal/ui"

	// Register tunnel implementations via side-effect imports.
	_ "github.com/psrth/mori/internal/tunnel/tunnels"
)

// nameRe validates connection names: lowercase alphanumeric + hyphens, 1-40 chars.
var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// runInteractiveInit walks the user through name → engine → provider → paste-or-form → fields → tunnel → save
// and writes the result to mori.yaml.
func runInteractiveInit(projectRoot string, existingCfg *config.ProjectConfig, imageOverride string) error {
	var (
		connName   string
		engineID   string
		providerID string
		tunnelCfg  *config.TunnelConfig
		values     map[string]string
	)

	const maxStep = 5
	step := 0

	for step <= maxStep {
		var err error

		switch step {
		case 0:
			err = stepName(existingCfg, &connName)
		case 1:
			err = stepEngine(&engineID)
		case 2:
			err = stepProvider(engineID, &providerID)
		case 3:
			err = stepFields(engineID, providerID, &values)
		case 4:
			err = stepTunnel(engineID, providerID, &tunnelCfg)
		case 5:
			// Save — no user interaction, just build and write.
			return saveConnection(projectRoot, existingCfg, connName, engineID, providerID, values, tunnelCfg, imageOverride)
		}

		if errors.Is(err, huh.ErrUserAborted) {
			if step == 0 {
				return err
			}
			step--
			continue
		}
		if err != nil {
			return err
		}
		step++
	}

	return nil
}

// ── Step 0: Connection name ─────────────────────────────────────────────

func stepName(existingCfg *config.ProjectConfig, connName *string) error {
	return huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Connection name").
				Inline(true).
				Prompt("> ").
				Placeholder("my-prod-db (lowercase, hyphens ok)").
				Value(connName).
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
}

// ── Step 1: Select engine ───────────────────────────────────────────────

func stepEngine(engineID *string) error {
	options := buildEngineOptions()
	return huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select database engine").
				Options(options...).
				Value(engineID),
		),
	).Run()
}

// ── Step 2: Select provider ─────────────────────────────────────────────

func stepProvider(engineID string, providerID *string) error {
	options := buildProviderOptions(registry.EngineID(engineID))
	return huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select provider").
				Options(options...).
				Value(providerID),
		),
	).Run()
}

// ── Step 3: Connection fields (paste or form) ───────────────────────────

func stepFields(engineID, providerID string, values *map[string]string) error {
	// Determine the fields for this engine+provider combination.
	authProvider := auth.Lookup(registry.ProviderID(providerID))
	fields := authProvider.Fields(registry.EngineID(engineID))
	if fields == nil {
		fields = registry.FieldsWithProviderDefaults(
			registry.EngineID(engineID),
			registry.ProviderID(providerID),
		)
	}

	// Check if this engine has a host field (embedded DBs like SQLite/DuckDB don't).
	hasPasteable := engineSupportsPaste(registry.EngineID(engineID))

	if hasPasteable {
		var connectMethod string
		err := huh.NewForm(
			huh.NewGroup(
				huh.NewSelect[string]().
					Title("How would you like to connect?").
					Options(
						huh.NewOption("Paste a connection string", "paste"),
						huh.NewOption("Fill in connection details", "form"),
					).
					Value(&connectMethod),
			),
		).Run()
		if err != nil {
			return err
		}

		if connectMethod == "paste" {
			return stepPasteConnString(engineID, values)
		}
	}

	return stepFormFields(fields, values)
}

// engineSupportsPaste returns true if the engine uses host-based connection strings
// (i.e. not embedded databases like SQLite, DuckDB, Firestore, DynamoDB).
func engineSupportsPaste(engineID registry.EngineID) bool {
	switch engineID {
	case registry.SQLite, registry.DuckDB, registry.Firestore, registry.DynamoDB:
		return false
	default:
		return true
	}
}

// stepPasteConnString handles the "paste a connection string" flow.
func stepPasteConnString(engineID string, values *map[string]string) error {
	var connStr string
	err := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Connection string").
				Inline(true).
				Prompt("> ").
				Placeholder(connStringPlaceholder(registry.EngineID(engineID))).
				Value(&connStr).
				Validate(func(s string) error {
					if strings.TrimSpace(s) == "" {
						return fmt.Errorf("connection string is required")
					}
					return nil
				}),
		),
	).Run()
	if err != nil {
		return err
	}

	// Parse the connection string into field values.
	parsed, parseErr := parseConnString(registry.EngineID(engineID), connStr)
	if parseErr != nil {
		return fmt.Errorf("could not parse connection string: %w", parseErr)
	}

	*values = parsed
	return nil
}

// connStringPlaceholder returns a sample connection string for the given engine.
func connStringPlaceholder(engineID registry.EngineID) string {
	switch engineID {
	case registry.MySQL, registry.MariaDB:
		return "mysql://user:pass@host:3306/database"
	case registry.MSSQL:
		return "sqlserver://user:pass@host:1433?database=mydb"
	case registry.Redis:
		return "redis://:password@host:6379/0"
	case registry.MongoDB:
		return "mongodb://user:pass@host:27017/database"
	default:
		return "postgres://user:pass@host:5432/database?sslmode=require"
	}
}

// parseConnString extracts standard connection fields from a URL-format connection string.
func parseConnString(engineID registry.EngineID, raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	vals := make(map[string]string)

	// Handle MySQL DSN format: user:pass@tcp(host:port)/database
	if (engineID == registry.MySQL || engineID == registry.MariaDB) && strings.Contains(raw, "@tcp(") {
		return parseMySQLDSN(raw)
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	if u.Hostname() != "" {
		vals["host"] = u.Hostname()
	}
	if u.Port() != "" {
		vals["port"] = u.Port()
	}
	if u.User != nil {
		vals["user"] = u.User.Username()
		if p, ok := u.User.Password(); ok {
			vals["password"] = p
		}
	}

	// Database from path (strip leading /).
	db := strings.TrimPrefix(u.Path, "/")
	if db != "" {
		vals["database"] = db
	}

	// Query parameters.
	for key, qvals := range u.Query() {
		if len(qvals) == 0 {
			continue
		}
		switch key {
		case "sslmode":
			vals["ssl_mode"] = qvals[0]
		case "database":
			vals["database"] = qvals[0]
		case "authSource":
			vals["auth_source"] = qvals[0]
		}
	}

	return vals, nil
}

// parseMySQLDSN parses the Go MySQL driver format: user:pass@tcp(host:port)/database
func parseMySQLDSN(raw string) (map[string]string, error) {
	vals := make(map[string]string)

	// Split on @tcp(
	parts := strings.SplitN(raw, "@tcp(", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("unexpected MySQL DSN format")
	}

	// User:pass
	userPass := parts[0]
	if idx := strings.IndexByte(userPass, ':'); idx >= 0 {
		vals["user"] = userPass[:idx]
		vals["password"] = userPass[idx+1:]
	} else {
		vals["user"] = userPass
	}

	// host:port)/database
	rest := parts[1]
	closeParen := strings.IndexByte(rest, ')')
	if closeParen < 0 {
		return nil, fmt.Errorf("unexpected MySQL DSN format")
	}
	hostPort := rest[:closeParen]
	if idx := strings.LastIndexByte(hostPort, ':'); idx >= 0 {
		vals["host"] = hostPort[:idx]
		vals["port"] = hostPort[idx+1:]
	} else {
		vals["host"] = hostPort
	}

	dbPart := rest[closeParen+1:]
	dbPart = strings.TrimPrefix(dbPart, "/")
	if dbPart != "" {
		vals["database"] = dbPart
	}

	return vals, nil
}

// stepFormFields runs the multi-field form for manual entry.
func stepFormFields(fields []registry.ConnectionField, values *map[string]string) error {
	fieldPtrs := make([]*string, len(fields))
	for i, f := range fields {
		v := f.Default
		fieldPtrs[i] = &v
	}

	group := buildFieldInputs(fields, fieldPtrs)
	err := huh.NewForm(group).Run()
	if err != nil {
		return err
	}

	v := make(map[string]string, len(fields))
	for i, f := range fields {
		v[f.Key] = *fieldPtrs[i]
	}
	*values = v
	return nil
}

// ── Step 4: Tunnel ──────────────────────────────────────────────────────

func stepTunnel(engineID, providerID string, tunnelCfg **config.TunnelConfig) error {
	connectivityOpts := tunnel.ConnectivityOptionsFor(
		registry.EngineID(engineID),
		registry.ProviderID(providerID),
	)

	if len(connectivityOpts) <= 1 {
		return nil // no tunnel options
	}

	var selectedTunnelType string
	var tunnelOptions []huh.Option[string]
	for _, opt := range connectivityOpts {
		tunnelOptions = append(tunnelOptions, huh.NewOption(opt.Label, opt.TunnelType))
	}

	err := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("How do you connect?").
				Options(tunnelOptions...).
				Value(&selectedTunnelType),
		),
	).Run()
	if err != nil {
		return err
	}

	if selectedTunnelType == "" || selectedTunnelType == "none" {
		return nil
	}

	t, ok := tunnel.Lookup(selectedTunnelType)
	if !ok {
		return fmt.Errorf("unknown tunnel type: %s", selectedTunnelType)
	}

	tunnelFields := t.Fields()
	tunnelPtrs := make([]*string, len(tunnelFields))
	for i, f := range tunnelFields {
		v := f.Default
		tunnelPtrs[i] = &v
	}

	tunnelGroup := buildTunnelFieldInputs(tunnelFields, tunnelPtrs)
	err = huh.NewForm(tunnelGroup).Run()
	if err != nil {
		return err
	}

	params := make(map[string]string)
	for i, f := range tunnelFields {
		if *tunnelPtrs[i] != "" {
			params[f.Key] = *tunnelPtrs[i]
		}
	}

	*tunnelCfg = &config.TunnelConfig{
		Type:   selectedTunnelType,
		Params: params,
	}
	return nil
}

// ── Save ────────────────────────────────────────────────────────────────

func saveConnection(projectRoot string, existingCfg *config.ProjectConfig,
	connName, engineID, providerID string,
	values map[string]string,
	tunnelCfg *config.TunnelConfig, imageOverride string) error {

	conn := buildConnection(engineID, providerID, values)
	conn.Tunnel = tunnelCfg
	if imageOverride != "" {
		if conn.Extra == nil {
			conn.Extra = make(map[string]string)
		}
		conn.Extra["image"] = imageOverride
	}

	if existingCfg == nil {
		existingCfg = config.NewProjectConfig()
	}
	existingCfg.AddConnection(connName, conn)

	if err := config.WriteProjectConfig(projectRoot, existingCfg); err != nil {
		return fmt.Errorf("failed to write mori.yaml: %w", err)
	}

	// ── Summary ──────────────────────────────────────────────────
	engine, _ := registry.EngineByID(registry.EngineID(engineID))

	fmt.Println()
	ui.StepDone(fmt.Sprintf("Connection %s saved to mori.yaml", ui.Cyan(connName)))

	const labelW = 8
	var boxLines []string
	boxLines = append(boxLines, ui.BoxLine("Engine", engine.DisplayName, labelW))
	if p, ok := registry.ProviderByID(registry.ProviderID(providerID)); ok {
		boxLines = append(boxLines, ui.BoxLine("Provider", p.DisplayName, labelW))
	}
	if conn.Host != "" {
		boxLines = append(boxLines, ui.BoxLine("Host", conn.Host, labelW))
	}
	if conn.Database != "" {
		boxLines = append(boxLines, ui.BoxLine("Database", conn.Database, labelW))
	}
	if conn.Tunnel != nil {
		if t, ok := tunnel.Lookup(conn.Tunnel.Type); ok {
			boxLines = append(boxLines, ui.BoxLine("Tunnel", t.DisplayName(), labelW))
		}
	}

	fmt.Println(ui.Box(connName, strings.Join(boxLines, "\n")))

	if engine.Supported {
		fmt.Println()
		ui.Info(fmt.Sprintf("Next: run 'mori start %s' to begin proxying.", connName))
	} else {
		fmt.Println()
		ui.Info(fmt.Sprintf("%s is not yet supported — config saved for future use.", engine.DisplayName))
	}
	fmt.Println()

	return nil
}

// ── Helpers ─────────────────────────────────────────────────────────────

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

// buildFieldInputs creates a huh group with inline text inputs for each connection field.
// fieldPtrs must be pre-allocated string pointers, one per field.
func buildFieldInputs(fields []registry.ConnectionField, fieldPtrs []*string) *huh.Group {
	var huhFields []huh.Field

	for i, f := range fields {
		// Build placeholder: merge help text into it for non-required fields.
		placeholder := f.Placeholder
		if !f.Required {
			if f.Default != "" {
				placeholder = fmt.Sprintf("optional · defaults to %s", f.Default)
			} else if placeholder == "" {
				placeholder = "optional"
			}
		}

		title := f.Label
		if !f.Required {
			title += ui.Dim(" (optional)")
		}
		input := huh.NewInput().
			Title(title).
			Inline(true).
			Prompt("> ").
			Placeholder(placeholder).
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

// buildTunnelFieldInputs creates a huh group with inline text inputs for each tunnel field.
func buildTunnelFieldInputs(fields []tunnel.Field, fieldPtrs []*string) *huh.Group {
	var huhFields []huh.Field

	for i, f := range fields {
		input := huh.NewInput().
			Title(f.Label).
			Inline(true).
			Prompt("> ").
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
func buildConnection(engineID, providerID string, values map[string]string) *config.Connection {
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
	for key, v := range values {
		if v == "" {
			continue
		}
		switch key {
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
			if !standardKeys[key] {
				extras[key] = v
			}
		}
	}

	if len(extras) > 0 {
		conn.Extra = extras
	}

	return conn
}
