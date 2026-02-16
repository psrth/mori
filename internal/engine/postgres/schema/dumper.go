package schema

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
)

// Extension represents a PostgreSQL extension installed on Prod.
type Extension struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// SequenceOffset holds the offset information for a single table's sequence.
type SequenceOffset struct {
	Column      string `json:"column"`
	Type        string `json:"type"`
	ProdMax     int64  `json:"prod_max"`
	ShadowStart int64  `json:"shadow_start"`
}

// TableMeta holds primary key metadata for a table.
type TableMeta struct {
	PKColumns []string `json:"pk_columns"`
	PKType    string   `json:"pk_type"` // "serial", "bigserial", "uuid", "composite", "none"
}

// DumpResult holds the complete result of the schema dump and analysis.
type DumpResult struct {
	SchemaSQL  string
	Extensions []Extension
	Sequences  map[string]SequenceOffset
	Tables     map[string]TableMeta
}

// DetectExtensions queries Prod for all installed extensions.
func DetectExtensions(ctx context.Context, conn *pgx.Conn) ([]Extension, error) {
	rows, err := conn.Query(ctx,
		"SELECT extname, extversion FROM pg_extension WHERE extname != 'plpgsql'")
	if err != nil {
		return nil, fmt.Errorf("failed to query extensions: %w", err)
	}
	defer rows.Close()

	var exts []Extension
	for rows.Next() {
		var e Extension
		if err := rows.Scan(&e.Name, &e.Version); err != nil {
			return nil, fmt.Errorf("failed to scan extension: %w", err)
		}
		exts = append(exts, e)
	}
	return exts, rows.Err()
}

// DumpSchema runs pg_dump --schema-only against the Prod database.
// It runs pg_dump inside a Docker container using the given image to guarantee
// version parity with the production server.
func DumpSchema(ctx context.Context, dsn *connstr.ProdDSN, image string) (string, error) {
	args := []string{
		"run", "--rm",
		"-e", "PGPASSWORD=" + dsn.Password,
		"--add-host", "host.docker.internal:host-gateway",
		image,
		"pg_dump",
	}
	args = append(args, dsn.PgDumpDockerArgs()...)

	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("pg_dump failed: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("pg_dump failed: %w", err)
	}
	return string(out), nil
}

// fkRegex matches ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES statements.
// This handles both single-line and multi-line FK definitions from pg_dump output.
var fkRegex = regexp.MustCompile(`(?im)^ALTER TABLE\s+(?:ONLY\s+)?[\w."]+\s+ADD\s+CONSTRAINT\s+[\w."]+\s+FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+[\w."]+\s*\([^)]+\)[^;]*;\s*\n?`)

// StripForeignKeys removes all FK constraint definitions from a schema dump.
func StripForeignKeys(schemaSQL string) string {
	return fkRegex.ReplaceAllString(schemaSQL, "")
}

// psqlMetaRegex matches psql metacommands (lines starting with \) that appear
// in pg_dump output (e.g., \restrict, \unrestrict, \connect). These are not
// valid SQL and must be stripped before executing via pgx.
var psqlMetaRegex = regexp.MustCompile(`(?m)^\\[a-zA-Z].*\n?`)

// StripPsqlMeta removes psql metacommands from a schema dump.
func StripPsqlMeta(schemaSQL string) string {
	return psqlMetaRegex.ReplaceAllString(schemaSQL, "")
}

// ExtInstallOptions provides container context for auto-installing extensions.
type ExtInstallOptions struct {
	ContainerID string // Docker container ID or name for docker exec
	PGMajor     int    // PostgreSQL major version (e.g. 16) for apt package names
}

// InstallExtensions connects to Shadow and installs each extension.
// If CREATE EXTENSION fails and container info is provided, it attempts to
// auto-install the extension package via apt-get inside the container.
func InstallExtensions(ctx context.Context, shadowConnStr string, exts []Extension, opts *ExtInstallOptions) error {
	if len(exts) == 0 {
		return nil
	}

	conn, err := pgx.Connect(ctx, shadowConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to Shadow: %w", err)
	}
	defer conn.Close(ctx)

	for _, ext := range exts {
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %q", ext.Name))
		if err != nil {
			if opts != nil && opts.ContainerID != "" {
				fmt.Printf("  Extension %q not available, attempting auto-install...\n", ext.Name)
				if installErr := aptInstallExtension(ctx, opts.ContainerID, opts.PGMajor, ext.Name); installErr != nil {
					return fmt.Errorf("extension %q: CREATE EXTENSION failed and auto-install failed — re-run with --image <image-with-extension>: %w (install error: %v)", ext.Name, err, installErr)
				}
				// Retry CREATE EXTENSION after apt install.
				_, retryErr := conn.Exec(ctx, fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %q", ext.Name))
				if retryErr != nil {
					return fmt.Errorf("extension %q: installed package but CREATE EXTENSION still failed — re-run with --image <image-with-extension>: %w", ext.Name, retryErr)
				}
				fmt.Printf("  Extension %q installed successfully\n", ext.Name)
				continue
			}
			return fmt.Errorf("Prod uses extension %q but Shadow image doesn't have it — re-run with --image <image-with-extension>: %w", ext.Name, err)
		}
	}
	return nil
}

// aptInstallExtension runs apt-get inside the Shadow container to install a
// PostgreSQL extension package. Official Postgres Docker images ship with PGDG
// apt repos pre-configured, so packages like postgresql-16-pgvector are available.
func aptInstallExtension(ctx context.Context, containerID string, pgMajor int, extName string) error {
	// The standard PGDG package naming convention is postgresql-<major>-<extension>.
	pkgName := fmt.Sprintf("postgresql-%d-%s", pgMajor, extName)

	// Run apt-get update + install inside the container.
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID,
		"sh", "-c", fmt.Sprintf("apt-get update -qq && apt-get install -y -qq %s", pkgName))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("apt-get install %s failed: %s", pkgName, strings.TrimSpace(string(out)))
	}
	return nil
}

// ApplySchema connects to Shadow and executes the filtered schema SQL.
func ApplySchema(ctx context.Context, shadowConnStr string, schemaSQL string) error {
	conn, err := pgx.Connect(ctx, shadowConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to Shadow: %w", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to apply schema to Shadow: %w", err)
	}
	return nil
}

// DetectTableMetadata queries Prod for all user tables and their PK info.
func DetectTableMetadata(ctx context.Context, conn *pgx.Conn) (map[string]TableMeta, error) {
	// Get all public tables
	tableRows, err := conn.Query(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer tableRows.Close()

	var tableNames []string
	for tableRows.Next() {
		var name string
		if err := tableRows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
	}
	if err := tableRows.Err(); err != nil {
		return nil, err
	}

	tables := make(map[string]TableMeta)
	for _, tableName := range tableNames {
		meta, err := detectTablePK(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		tables[tableName] = meta
	}
	return tables, nil
}

func detectTablePK(ctx context.Context, conn *pgx.Conn, tableName string) (TableMeta, error) {
	// Query PK columns for this table
	rows, err := conn.Query(ctx, `
		SELECT kcu.column_name, c.data_type, c.column_default
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON kcu.constraint_name = tc.constraint_name
			AND kcu.table_schema = tc.table_schema
		JOIN information_schema.columns c
			ON c.table_name = kcu.table_name
			AND c.column_name = kcu.column_name
			AND c.table_schema = kcu.table_schema
		WHERE tc.table_name = $1
			AND tc.table_schema = 'public'
			AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position`, tableName)
	if err != nil {
		return TableMeta{}, fmt.Errorf("failed to query PK for table %q: %w", tableName, err)
	}
	defer rows.Close()

	var pkColumns []string
	var pkTypes []string
	var defaults []string
	for rows.Next() {
		var col, dataType string
		var colDefault *string
		if err := rows.Scan(&col, &dataType, &colDefault); err != nil {
			return TableMeta{}, fmt.Errorf("failed to scan PK column for table %q: %w", tableName, err)
		}
		pkColumns = append(pkColumns, col)
		pkTypes = append(pkTypes, dataType)
		d := ""
		if colDefault != nil {
			d = *colDefault
		}
		defaults = append(defaults, d)
	}
	if err := rows.Err(); err != nil {
		return TableMeta{}, err
	}

	if len(pkColumns) == 0 {
		return TableMeta{PKType: "none"}, nil
	}

	pkType := classifyPKType(pkTypes, defaults)
	if len(pkColumns) > 1 {
		pkType = "composite"
	}

	return TableMeta{
		PKColumns: pkColumns,
		PKType:    pkType,
	}, nil
}

func classifyPKType(dataTypes []string, defaults []string) string {
	if len(dataTypes) != 1 {
		return "composite"
	}
	dt := strings.ToLower(dataTypes[0])
	def := ""
	if len(defaults) > 0 {
		def = strings.ToLower(defaults[0])
	}

	// Check if it's a UUID
	if dt == "uuid" {
		return "uuid"
	}

	// Check sequence-based types by looking at the default
	if strings.Contains(def, "nextval") {
		if dt == "bigint" {
			return "bigserial"
		}
		if dt == "smallint" {
			return "smallserial"
		}
		return "serial"
	}

	// Identity columns
	if dt == "integer" || dt == "bigint" || dt == "smallint" {
		// Check if it's an identity column
		if dt == "bigint" {
			return "bigserial"
		}
		return "serial"
	}

	return dt
}

// DetectSequenceOffsets queries Prod for the current max PK value per table
// and computes Shadow sequence start values.
func DetectSequenceOffsets(ctx context.Context, conn *pgx.Conn, tables map[string]TableMeta) (map[string]SequenceOffset, error) {
	offsets := make(map[string]SequenceOffset)

	for tableName, meta := range tables {
		// Only offset serial/bigserial single-column PKs
		if meta.PKType == "none" || meta.PKType == "uuid" || meta.PKType == "composite" {
			continue
		}
		if len(meta.PKColumns) != 1 {
			continue
		}

		pkCol := meta.PKColumns[0]
		var maxVal *int64
		query := fmt.Sprintf("SELECT MAX(%s) FROM %s",
			quoteIdent(pkCol), quoteIdent(tableName))
		if err := conn.QueryRow(ctx, query).Scan(&maxVal); err != nil {
			return nil, fmt.Errorf("failed to get max PK for table %q: %w", tableName, err)
		}

		prodMax := int64(0)
		if maxVal != nil {
			prodMax = *maxVal
		}

		shadowStart := computeOffset(prodMax)
		offsets[tableName] = SequenceOffset{
			Column:      pkCol,
			Type:        meta.PKType,
			ProdMax:     prodMax,
			ShadowStart: shadowStart,
		}
	}
	return offsets, nil
}

// computeOffset calculates max(prod_max * 10, prod_max + 10_000_000).
func computeOffset(prodMax int64) int64 {
	a := prodMax * 10
	b := prodMax + 10_000_000
	if a > b {
		return a
	}
	return b
}

// ApplySequenceOffsets connects to Shadow and sets sequence start values.
func ApplySequenceOffsets(ctx context.Context, shadowConnStr string, offsets map[string]SequenceOffset) error {
	if len(offsets) == 0 {
		return nil
	}

	conn, err := pgx.Connect(ctx, shadowConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to Shadow: %w", err)
	}
	defer conn.Close(ctx)

	for tableName, offset := range offsets {
		// Find the sequence name using pg_get_serial_sequence
		var seqName *string
		err := conn.QueryRow(ctx,
			"SELECT pg_get_serial_sequence($1, $2)",
			tableName, offset.Column).Scan(&seqName)
		if err != nil {
			return fmt.Errorf("failed to get sequence for %s.%s: %w", tableName, offset.Column, err)
		}
		if seqName == nil {
			// No sequence found — might be an identity column, try ALTER TABLE
			_, err = conn.Exec(ctx, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s RESTART WITH %d",
				quoteIdent(tableName), quoteIdent(offset.Column), offset.ShadowStart))
			if err != nil {
				// Not critical — skip this table
				continue
			}
			continue
		}

		_, err = conn.Exec(ctx, fmt.Sprintf(
			"ALTER SEQUENCE %s RESTART WITH %d", *seqName, offset.ShadowStart))
		if err != nil {
			return fmt.Errorf("failed to offset sequence %s: %w", *seqName, err)
		}
	}
	return nil
}

// FullDump performs the complete schema dump pipeline from Prod.
// The image parameter specifies which Docker image to use for pg_dump.
func FullDump(ctx context.Context, conn *pgx.Conn, dsn *connstr.ProdDSN, image string) (*DumpResult, error) {
	exts, err := DetectExtensions(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("extension detection failed: %w", err)
	}

	schemaSQL, err := DumpSchema(ctx, dsn, image)
	if err != nil {
		return nil, fmt.Errorf("schema dump failed: %w", err)
	}
	schemaSQL = StripForeignKeys(schemaSQL)
	schemaSQL = StripPsqlMeta(schemaSQL)

	tables, err := DetectTableMetadata(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}

	sequences, err := DetectSequenceOffsets(ctx, conn, tables)
	if err != nil {
		return nil, fmt.Errorf("sequence offset detection failed: %w", err)
	}

	return &DumpResult{
		SchemaSQL:  schemaSQL,
		Extensions: exts,
		Sequences:  sequences,
		Tables:     tables,
	}, nil
}

// ApplyToShadow takes a DumpResult and applies it to the Shadow database.
// If extOpts is provided, extensions that fail to install will be auto-installed
// via apt-get inside the container.
func ApplyToShadow(ctx context.Context, shadowConnStr string, result *DumpResult, extOpts *ExtInstallOptions) error {
	if err := InstallExtensions(ctx, shadowConnStr, result.Extensions, extOpts); err != nil {
		return err
	}
	if err := ApplySchema(ctx, shadowConnStr, result.SchemaSQL); err != nil {
		return err
	}
	if err := ApplySequenceOffsets(ctx, shadowConnStr, result.Sequences); err != nil {
		return err
	}
	return nil
}

// quoteIdent quotes a PostgreSQL identifier to handle reserved words and special chars.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
