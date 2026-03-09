package schema

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/connstr"
)

// isRecoveryConflict reports whether err is a PostgreSQL "conflict with
// recovery" error (SQLSTATE 40001) that occurs on read replicas when a
// query conflicts with WAL replay.
func isRecoveryConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

// retryOnConflict retries fn up to maxAttempts times when it returns a
// recovery-conflict error, sleeping briefly between attempts.
func retryOnConflict(maxAttempts int, fn func() error) error {
	var err error
	for i := range maxAttempts {
		if err = fn(); err == nil || !isRecoveryConflict(err) {
			return err
		}
		if i < maxAttempts-1 {
			time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
		}
	}
	return err
}

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
	PKColumns     []string `json:"pk_columns"`
	PKType        string   `json:"pk_type"` // "serial", "bigserial", "uuid", "composite", "none"
	GeneratedCols []string `json:"generated_cols,omitempty"`
}

// DumpResult holds the complete result of the schema dump and analysis.
type DumpResult struct {
	SchemaSQL   string
	Extensions  []Extension
	Sequences   map[string]SequenceOffset
	Tables      map[string]TableMeta
	ForeignKeys []coreSchema.ForeignKey
}

// managedExtensions lists cloud-provider internal extensions that are injected
// for operational purposes and cannot (or need not) be replicated in Shadow.
var managedExtensions = map[string]bool{
	"plpgsql":                  true,
	"google_vacuum_mgmt":       true, // GCP Cloud SQL
	"google_columnar_engine":   true, // GCP Cloud SQL
	"google_db_advisor":        true, // GCP Cloud SQL
	"google_ml_integration":    true, // GCP Cloud SQL
	"rds_tools":                true, // AWS RDS
	"rdsutils":                 true, // AWS RDS
	"aiven_extras":             true, // Aiven
	"azure":                    true, // Azure Database
	"citus_columnar":           true, // Azure CosmosDB for PG
	"supautils":                true, // Supabase
}

// DetectExtensions queries Prod for all installed extensions, filtering out
// cloud-provider managed extensions that cannot be replicated locally.
func DetectExtensions(ctx context.Context, conn *pgx.Conn) ([]Extension, error) {
	rows, err := conn.Query(ctx,
		"SELECT extname, extversion FROM pg_extension")
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
		if managedExtensions[e.Name] {
			continue
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

// StripManagedExtensions removes CREATE EXTENSION statements for cloud-provider
// managed extensions from a pg_dump schema dump.
func StripManagedExtensions(schemaSQL string) string {
	for name := range managedExtensions {
		// pg_dump emits: CREATE EXTENSION IF NOT EXISTS <name> WITH SCHEMA ...;
		// Match both quoted and unquoted extension names.
		for _, pat := range []string{
			`(?m)^CREATE EXTENSION [^\n]*\b` + regexp.QuoteMeta(name) + `\b[^\n]*;\n?`,
			`(?m)^COMMENT ON EXTENSION [^\n]*\b` + regexp.QuoteMeta(name) + `\b[^\n]*;\n?`,
		} {
			schemaSQL = regexp.MustCompile(pat).ReplaceAllString(schemaSQL, "")
		}
	}
	return schemaSQL
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

// extToPkg maps PostgreSQL extension names to their PGDG apt package suffix
// when the two differ. Extensions not listed here use the extension name as-is.
var extToPkg = map[string]string{
	"vector":  "pgvector",
	"postgis": "postgis-3",
}

// aptInstallExtension runs apt-get inside the Shadow container to install a
// PostgreSQL extension package. Official Postgres Docker images ship with PGDG
// apt repos pre-configured, so packages like postgresql-17-pgvector are available.
func aptInstallExtension(ctx context.Context, containerID string, pgMajor int, extName string) error {
	// The standard PGDG package naming convention is postgresql-<major>-<extension>.
	pkg := extName
	if mapped, ok := extToPkg[extName]; ok {
		pkg = mapped
	}
	pkgName := fmt.Sprintf("postgresql-%d-%s", pgMajor, pkg)

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
// It uses a single query against pg_catalog (instead of N+1 queries against
// information_schema) to minimise the window for read-replica recovery
// conflicts.
func DetectTableMetadata(ctx context.Context, conn *pgx.Conn) (map[string]TableMeta, error) {
	rows, err := conn.Query(ctx, `
		SELECT c.relname                          AS table_name,
		       COALESCE(a.attname, '')             AS pk_column,
		       COALESCE(format_type(a.atttypid, a.atttypmod), '') AS data_type,
		       COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '')    AS col_default,
		       COALESCE(arr.ord, 0)                AS ordinal
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_index i   ON i.indrelid = c.oid AND i.indisprimary
		LEFT JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS arr(attnum, ord)
		          ON true
		LEFT JOIN pg_attribute a  ON a.attrelid = c.oid AND a.attnum = arr.attnum
		LEFT JOIN pg_attrdef   ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE n.nspname = 'public'
		  AND c.relkind = 'r'
		ORDER BY c.relname, arr.ord`)
	if err != nil {
		return nil, fmt.Errorf("failed to query table metadata: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]TableMeta)
	// Accumulate PK columns per table as we scan.
	type pkAccum struct {
		columns  []string
		types    []string
		defaults []string
	}
	accum := make(map[string]*pkAccum)

	for rows.Next() {
		var tableName, pkCol, dataType, colDefault string
		var ordinal int
		if err := rows.Scan(&tableName, &pkCol, &dataType, &colDefault, &ordinal); err != nil {
			return nil, fmt.Errorf("failed to scan table metadata: %w", err)
		}
		if pkCol == "" {
			// Table exists but has no PK.
			if _, ok := tables[tableName]; !ok {
				tables[tableName] = TableMeta{PKType: "none"}
			}
			continue
		}
		a, ok := accum[tableName]
		if !ok {
			a = &pkAccum{}
			accum[tableName] = a
		}
		a.columns = append(a.columns, pkCol)
		a.types = append(a.types, dataType)
		a.defaults = append(a.defaults, colDefault)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for tableName, a := range accum {
		pkType := classifyPKType(a.types, a.defaults)
		if len(a.columns) > 1 {
			pkType = "composite"
		}
		tables[tableName] = TableMeta{
			PKColumns: a.columns,
			PKType:    pkType,
		}
	}

	return tables, nil
}

// fkActionCode maps PostgreSQL single-char FK action codes to their string names.
var fkActionCode = map[byte]string{
	'a': "NO ACTION",
	'r': "RESTRICT",
	'c': "CASCADE",
	'n': "SET NULL",
	'd': "SET DEFAULT",
}

// DetectForeignKeys queries Prod for all foreign key constraints in the public schema.
// Uses pg_catalog for a single efficient query (same pattern as DetectTableMetadata).
func DetectForeignKeys(ctx context.Context, conn *pgx.Conn) ([]coreSchema.ForeignKey, error) {
	rows, err := conn.Query(ctx, `
		SELECT con.conname                         AS constraint_name,
		       child_cls.relname                    AS child_table,
		       child_att.attname                    AS child_column,
		       parent_cls.relname                   AS parent_table,
		       parent_att.attname                   AS parent_column,
		       con.confdeltype                      AS delete_action,
		       con.confupdtype                      AS update_action,
		       child_ord.ord                        AS ordinal
		FROM pg_constraint con
		JOIN pg_class child_cls ON child_cls.oid = con.conrelid
		JOIN pg_namespace child_ns ON child_ns.oid = child_cls.relnamespace
		JOIN pg_class parent_cls ON parent_cls.oid = con.confrelid
		JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS child_ord(attnum, ord) ON true
		JOIN pg_attribute child_att
		     ON child_att.attrelid = con.conrelid AND child_att.attnum = child_ord.attnum
		JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS parent_ord(attnum, ord) ON true
		JOIN pg_attribute parent_att
		     ON parent_att.attrelid = con.confrelid AND parent_att.attnum = parent_ord.attnum
		WHERE con.contype = 'f'
		  AND child_ns.nspname = 'public'
		  AND child_ord.ord = parent_ord.ord
		ORDER BY child_cls.relname, con.conname, child_ord.ord`)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	// Accumulate columns per constraint since multi-column FKs produce multiple rows.
	type fkAccum struct {
		constraintName string
		childTable     string
		parentTable    string
		childColumns   []string
		parentColumns  []string
		deleteAction   byte
		updateAction   byte
	}
	// Use ordered key to preserve deterministic output.
	var orderedKeys []string
	accum := make(map[string]*fkAccum)

	for rows.Next() {
		var constraintName, childTable, childCol, parentTable, parentCol string
		var deleteAction, updateAction byte
		var ordinal int
		if err := rows.Scan(&constraintName, &childTable, &childCol, &parentTable, &parentCol,
			&deleteAction, &updateAction, &ordinal); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}

		key := childTable + "." + constraintName
		a, ok := accum[key]
		if !ok {
			a = &fkAccum{
				constraintName: constraintName,
				childTable:     childTable,
				parentTable:    parentTable,
				deleteAction:   deleteAction,
				updateAction:   updateAction,
			}
			accum[key] = a
			orderedKeys = append(orderedKeys, key)
		}
		a.childColumns = append(a.childColumns, childCol)
		a.parentColumns = append(a.parentColumns, parentCol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]coreSchema.ForeignKey, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		a := accum[key]
		onDelete := fkActionCode[a.deleteAction]
		if onDelete == "" {
			onDelete = "NO ACTION"
		}
		onUpdate := fkActionCode[a.updateAction]
		if onUpdate == "" {
			onUpdate = "NO ACTION"
		}
		result = append(result, coreSchema.ForeignKey{
			ConstraintName: a.constraintName,
			ChildTable:     a.childTable,
			ChildColumns:   a.childColumns,
			ParentTable:    a.parentTable,
			ParentColumns:  a.parentColumns,
			OnDelete:       onDelete,
			OnUpdate:       onUpdate,
		})
	}

	return result, nil
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

// DetectGeneratedColumns queries Prod for GENERATED ALWAYS AS STORED columns
// and merges them into the existing table metadata. These columns must be
// excluded from hydration INSERTs because PostgreSQL rejects explicit values
// for generated columns.
//
// Requires PostgreSQL 12+ (which introduced generated columns). On older
// versions the query fails gracefully and no columns are marked.
func DetectGeneratedColumns(ctx context.Context, conn *pgx.Conn, tables map[string]TableMeta) {
	rows, err := conn.Query(ctx, `
		SELECT c.relname  AS table_name,
		       a.attname  AS column_name
		FROM pg_attribute a
		JOIN pg_class c     ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relkind = 'r'
		  AND a.attgenerated = 's'
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`)
	if err != nil {
		// pg_attribute.attgenerated doesn't exist on PG < 12; ignore gracefully.
		return
	}
	defer rows.Close()

	genCols := make(map[string][]string)
	for rows.Next() {
		var tableName, colName string
		if err := rows.Scan(&tableName, &colName); err != nil {
			return
		}
		genCols[tableName] = append(genCols[tableName], colName)
	}

	for tableName, cols := range genCols {
		if meta, ok := tables[tableName]; ok {
			meta.GeneratedCols = cols
			tables[tableName] = meta
		}
	}
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
		if err := retryOnConflict(5, func() error {
			return conn.QueryRow(ctx, query).Scan(&maxVal)
		}); err != nil {
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
	// Read-replica queries may hit "conflict with recovery" (SQLSTATE 40001)
	// when WAL replay invalidates a running query. Retry transparently.
	const maxRetries = 5

	var exts []Extension
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		exts, e = DetectExtensions(ctx, conn)
		return e
	}); err != nil {
		return nil, fmt.Errorf("extension detection failed: %w", err)
	}

	schemaSQL, err := DumpSchema(ctx, dsn, image)
	if err != nil {
		return nil, fmt.Errorf("schema dump failed: %w", err)
	}
	schemaSQL = StripForeignKeys(schemaSQL)
	schemaSQL = StripPsqlMeta(schemaSQL)
	schemaSQL = StripManagedExtensions(schemaSQL)

	var tables map[string]TableMeta
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		tables, e = DetectTableMetadata(ctx, conn)
		return e
	}); err != nil {
		return nil, fmt.Errorf("table metadata detection failed: %w", err)
	}

	// Detect GENERATED ALWAYS AS STORED columns so hydration INSERTs can
	// exclude them (PostgreSQL rejects explicit values for generated columns).
	DetectGeneratedColumns(ctx, conn, tables)

	var sequences map[string]SequenceOffset
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		sequences, e = DetectSequenceOffsets(ctx, conn, tables)
		return e
	}); err != nil {
		return nil, fmt.Errorf("sequence offset detection failed: %w", err)
	}

	var foreignKeys []coreSchema.ForeignKey
	if err := retryOnConflict(maxRetries, func() error {
		var e error
		foreignKeys, e = DetectForeignKeys(ctx, conn)
		return e
	}); err != nil {
		return nil, fmt.Errorf("foreign key detection failed: %w", err)
	}

	return &DumpResult{
		SchemaSQL:   schemaSQL,
		Extensions:  exts,
		Sequences:   sequences,
		Tables:      tables,
		ForeignKeys: foreignKeys,
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
