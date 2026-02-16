package proxy

import (
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// DDLHandler handles DDL operations for a single MySQL connection.
// It executes DDL on Shadow, parses the changes, and updates the schema registry.
type DDLHandler struct {
	shadowConn     net.Conn
	schemaRegistry *coreSchema.Registry
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger
}

// HandleDDL executes a DDL statement on Shadow and updates the schema registry.
func (dh *DDLHandler) HandleDDL(
	clientConn net.Conn,
	rawPkt []byte,
	cl *core.Classification,
) error {
	// Execute DDL on Shadow, relay response to client.
	if err := forwardAndRelay(rawPkt, dh.shadowConn, clientConn); err != nil {
		return fmt.Errorf("DDL forward: %w", err)
	}

	if dh.schemaRegistry == nil {
		return nil
	}

	// Parse DDL to extract schema changes.
	changes := parseMySQLDDLChanges(cl.RawSQL)
	if len(changes) == 0 {
		return nil
	}

	// Apply changes to the schema registry.
	for _, ch := range changes {
		dh.applyChange(ch)
	}

	// Persist the registry.
	if err := coreSchema.WriteRegistry(dh.moriDir, dh.schemaRegistry); err != nil {
		if dh.verbose {
			log.Printf("[conn %d] failed to persist schema registry: %v", dh.connID, err)
		}
	}

	return nil
}

type mysqlDDLChangeKind int

const (
	mysqlDDLAddColumn    mysqlDDLChangeKind = iota
	mysqlDDLDropColumn
	mysqlDDLRenameColumn
	mysqlDDLDropTable
	mysqlDDLCreateTable
)

type mysqlDDLChange struct {
	kind    mysqlDDLChangeKind
	table   string
	column  string
	colType string
	oldName string
	newName string
	defVal  *string
}

// Regex patterns for MySQL DDL parsing.
var (
	reAlterAddCol    = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tblPat + `\s+ADD\s+(?:COLUMN\s+)?` + colPat + `\s+(\S+(?:\([^)]*\))?)`)
	reAlterDropCol   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tblPat + `\s+DROP\s+(?:COLUMN\s+)?` + colPat)
	reAlterRenameCol = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tblPat + `\s+RENAME\s+COLUMN\s+` + colPat + `\s+TO\s+` + colPat2)
	reCreateTable    = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tblPat)
	reDropTable      = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + tblPat)
	reDefault        = regexp.MustCompile(`(?i)\bDEFAULT\s+(\S+)`)
)

const (
	tblPat  = "(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))"
	colPat  = "(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))"
	colPat2 = "(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))"
)

// extractName returns the first non-empty submatch from a regex group pair
// (backtick-quoted or unquoted identifier).
func extractName(matches []string, idx1, idx2 int) string {
	if idx1 < len(matches) && matches[idx1] != "" {
		return matches[idx1]
	}
	if idx2 < len(matches) && matches[idx2] != "" {
		return matches[idx2]
	}
	return ""
}

// parseMySQLDDLChanges parses a MySQL DDL statement using regex patterns.
func parseMySQLDDLChanges(sql string) []mysqlDDLChange {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "CREATE TABLE") {
		m := reCreateTable.FindStringSubmatch(trimmed)
		if m == nil {
			return nil
		}
		table := extractName(m, 1, 2)
		if table == "" {
			return nil
		}
		return []mysqlDDLChange{{kind: mysqlDDLCreateTable, table: table}}
	}

	if strings.HasPrefix(upper, "DROP TABLE") {
		m := reDropTable.FindStringSubmatch(trimmed)
		if m == nil {
			return nil
		}
		table := extractName(m, 1, 2)
		if table == "" {
			return nil
		}
		return []mysqlDDLChange{{kind: mysqlDDLDropTable, table: table}}
	}

	if !strings.HasPrefix(upper, "ALTER TABLE") {
		return nil
	}

	// Check for RENAME COLUMN first (more specific).
	if m := reAlterRenameCol.FindStringSubmatch(trimmed); m != nil {
		table := extractName(m, 1, 2)
		oldCol := extractName(m, 3, 4)
		newCol := extractName(m, 5, 6)
		if table != "" && oldCol != "" && newCol != "" {
			return []mysqlDDLChange{{
				kind:    mysqlDDLRenameColumn,
				table:   table,
				oldName: oldCol,
				newName: newCol,
			}}
		}
	}

	// Check for DROP COLUMN.
	if m := reAlterDropCol.FindStringSubmatch(trimmed); m != nil {
		// Make sure this isn't a RENAME COLUMN match.
		if !strings.Contains(upper, "RENAME COLUMN") {
			table := extractName(m, 1, 2)
			col := extractName(m, 3, 4)
			if table != "" && col != "" {
				return []mysqlDDLChange{{
					kind:   mysqlDDLDropColumn,
					table:  table,
					column: col,
				}}
			}
		}
	}

	// Check for ADD COLUMN.
	if m := reAlterAddCol.FindStringSubmatch(trimmed); m != nil {
		table := extractName(m, 1, 2)
		col := extractName(m, 3, 4)
		colType := ""
		if len(m) > 5 {
			colType = m[5]
		}
		if table != "" && col != "" {
			ch := mysqlDDLChange{
				kind:    mysqlDDLAddColumn,
				table:   table,
				column:  col,
				colType: colType,
			}
			// Check for DEFAULT.
			if dm := reDefault.FindStringSubmatch(trimmed); dm != nil {
				defVal := strings.Trim(dm[1], "'\"")
				ch.defVal = &defVal
			}
			return []mysqlDDLChange{ch}
		}
	}

	return nil
}

// applyChange records a single schema change in the registry.
func (dh *DDLHandler) applyChange(ch mysqlDDLChange) {
	switch ch.kind {
	case mysqlDDLAddColumn:
		col := coreSchema.Column{
			Name:    ch.column,
			Type:    ch.colType,
			Default: ch.defVal,
		}
		dh.schemaRegistry.RecordAddColumn(ch.table, col)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: ADD COLUMN %s.%s (%s)", dh.connID, ch.table, ch.column, ch.colType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("ADD COLUMN %s.%s (%s)", ch.table, ch.column, ch.colType))

	case mysqlDDLDropColumn:
		dh.schemaRegistry.RecordDropColumn(ch.table, ch.column)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: DROP COLUMN %s.%s", dh.connID, ch.table, ch.column)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("DROP COLUMN %s.%s", ch.table, ch.column))

	case mysqlDDLRenameColumn:
		dh.schemaRegistry.RecordRenameColumn(ch.table, ch.oldName, ch.newName)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: RENAME COLUMN %s.%s -> %s", dh.connID, ch.table, ch.oldName, ch.newName)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("RENAME COLUMN %s.%s -> %s", ch.table, ch.oldName, ch.newName))

	case mysqlDDLDropTable:
		dh.schemaRegistry.RemoveTable(ch.table)
		if dh.deltaMap != nil {
			dh.deltaMap.ClearTable(ch.table)
		}
		if dh.tombstones != nil {
			dh.tombstones.ClearTable(ch.table)
		}
		if dh.verbose {
			log.Printf("[conn %d] schema registry: DROP TABLE %s", dh.connID, ch.table)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("DROP TABLE %s", ch.table))

	case mysqlDDLCreateTable:
		dh.schemaRegistry.RecordNewTable(ch.table)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: CREATE TABLE %s", dh.connID, ch.table)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("CREATE TABLE %s", ch.table))
	}
}
