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
	mysqlDDLModifyColumn // MODIFY COLUMN col new_type
	mysqlDDLChangeColumn // CHANGE COLUMN old_name new_name new_type
	mysqlDDLRenameTable  // RENAME TABLE old TO new
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
	reRenameTable    = regexp.MustCompile(`(?i)RENAME\s+TABLE\s+` + tblPat + `\s+TO\s+` + tblPat)

	// Clause-level regex patterns for multi-statement ALTER TABLE parsing.
	// These match individual clauses (without the "ALTER TABLE <name>" prefix).
	reClauseAdd    = regexp.MustCompile(`(?i)^ADD\s+(?:COLUMN\s+)?` + colPat + `\s+(\S+(?:\([^)]*\))?)`)
	reClauseDrop   = regexp.MustCompile(`(?i)^DROP\s+(?:COLUMN\s+)?` + colPat)
	reClauseRename = regexp.MustCompile(`(?i)^RENAME\s+COLUMN\s+` + colPat + `\s+TO\s+` + colPat2)
	reClauseModify = regexp.MustCompile(`(?i)^MODIFY\s+(?:COLUMN\s+)?` + colPat + `\s+(\w+(?:\([^)]*\))?)`)
	reClauseChange = regexp.MustCompile(`(?i)^CHANGE\s+(?:COLUMN\s+)?` + colPat + `\s+` + colPat2 + `\s+(\w+(?:\([^)]*\))?)`)
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

// splitAlterClauses splits the clause portion of an ALTER TABLE statement on
// top-level commas (i.e., commas that are not inside parentheses). This handles
// multi-statement ALTER TABLE such as:
//
//	ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b, RENAME COLUMN c TO d
func splitAlterClauses(clausePart string) []string {
	var clauses []string
	depth := 0
	start := 0
	for i := 0; i < len(clausePart); i++ {
		switch clausePart[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				clause := strings.TrimSpace(clausePart[start:i])
				if clause != "" {
					clauses = append(clauses, clause)
				}
				start = i + 1
			}
		}
	}
	// Last clause.
	last := strings.TrimSpace(clausePart[start:])
	if last != "" {
		clauses = append(clauses, last)
	}
	return clauses
}

// reAlterTablePrefix matches the "ALTER TABLE <name>" prefix and captures the
// table name so we can strip it and process clauses individually.
var reAlterTablePrefix = regexp.MustCompile(`(?i)^ALTER\s+TABLE\s+` + tblPat + `\s+`)

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

	// RENAME TABLE old TO new
	if strings.HasPrefix(upper, "RENAME TABLE") {
		m := reRenameTable.FindStringSubmatch(trimmed)
		if m == nil {
			return nil
		}
		oldTable := extractName(m, 1, 2)
		newTable := extractName(m, 3, 4)
		if oldTable != "" && newTable != "" {
			return []mysqlDDLChange{{
				kind:    mysqlDDLRenameTable,
				table:   oldTable,
				oldName: oldTable,
				newName: newTable,
			}}
		}
		return nil
	}

	if !strings.HasPrefix(upper, "ALTER TABLE") {
		return nil
	}

	// Extract table name and the clause portion after "ALTER TABLE <name>".
	prefixMatch := reAlterTablePrefix.FindStringSubmatch(trimmed)
	if prefixMatch == nil {
		return nil
	}
	table := extractName(prefixMatch, 1, 2)
	if table == "" {
		return nil
	}

	// Everything after "ALTER TABLE <name> " is the clause(s).
	clausePart := trimmed[len(prefixMatch[0]):]

	// Split on top-level commas to handle multi-statement ALTER TABLE.
	clauses := splitAlterClauses(clausePart)

	var changes []mysqlDDLChange
	for _, clause := range clauses {
		if ch, ok := parseAlterClause(table, clause); ok {
			changes = append(changes, ch)
		}
	}

	return changes
}

// parseAlterClause parses a single ALTER TABLE clause (without the "ALTER TABLE <name>" prefix)
// and returns the corresponding change. Returns ok=false if the clause is not recognized.
func parseAlterClause(table, clause string) (mysqlDDLChange, bool) {
	// CHANGE COLUMN must be checked before MODIFY to avoid false matches.
	if m := reClauseChange.FindStringSubmatch(clause); m != nil {
		oldCol := extractName(m, 1, 2)
		newCol := extractName(m, 3, 4)
		colType := ""
		if len(m) > 5 {
			colType = m[5]
		}
		if oldCol != "" && newCol != "" {
			return mysqlDDLChange{
				kind:    mysqlDDLChangeColumn,
				table:   table,
				column:  newCol,
				colType: colType,
				oldName: oldCol,
				newName: newCol,
			}, true
		}
	}

	// MODIFY COLUMN col new_type
	if m := reClauseModify.FindStringSubmatch(clause); m != nil {
		col := extractName(m, 1, 2)
		colType := ""
		if len(m) > 3 {
			colType = m[3]
		}
		if col != "" {
			return mysqlDDLChange{
				kind:    mysqlDDLModifyColumn,
				table:   table,
				column:  col,
				colType: colType,
			}, true
		}
	}

	// RENAME COLUMN old TO new (more specific, check before DROP).
	if m := reClauseRename.FindStringSubmatch(clause); m != nil {
		oldCol := extractName(m, 1, 2)
		newCol := extractName(m, 3, 4)
		if oldCol != "" && newCol != "" {
			return mysqlDDLChange{
				kind:    mysqlDDLRenameColumn,
				table:   table,
				oldName: oldCol,
				newName: newCol,
			}, true
		}
	}

	// DROP COLUMN (but not RENAME COLUMN).
	upperClause := strings.ToUpper(clause)
	if !strings.HasPrefix(upperClause, "RENAME") {
		if m := reClauseDrop.FindStringSubmatch(clause); m != nil {
			col := extractName(m, 1, 2)
			if col != "" {
				return mysqlDDLChange{
					kind:   mysqlDDLDropColumn,
					table:  table,
					column: col,
				}, true
			}
		}
	}

	// ADD COLUMN.
	if m := reClauseAdd.FindStringSubmatch(clause); m != nil {
		col := extractName(m, 1, 2)
		colType := ""
		if len(m) > 3 {
			colType = m[3]
		}
		if col != "" {
			ch := mysqlDDLChange{
				kind:    mysqlDDLAddColumn,
				table:   table,
				column:  col,
				colType: colType,
			}
			// Check for DEFAULT.
			if dm := reDefault.FindStringSubmatch(clause); dm != nil {
				defVal := strings.Trim(dm[1], "'\"")
				ch.defVal = &defVal
			}
			return ch, true
		}
	}

	return mysqlDDLChange{}, false
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

	case mysqlDDLModifyColumn:
		dh.schemaRegistry.RecordTypeChange(ch.table, ch.column, "", ch.colType)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: MODIFY COLUMN %s.%s -> %s", dh.connID, ch.table, ch.column, ch.colType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("MODIFY COLUMN %s.%s -> %s", ch.table, ch.column, ch.colType))

	case mysqlDDLChangeColumn:
		// CHANGE COLUMN is a rename + type change.
		if ch.oldName != ch.newName {
			dh.schemaRegistry.RecordRenameColumn(ch.table, ch.oldName, ch.newName)
		}
		dh.schemaRegistry.RecordTypeChange(ch.table, ch.newName, "", ch.colType)
		if dh.verbose {
			log.Printf("[conn %d] schema registry: CHANGE COLUMN %s.%s -> %s %s", dh.connID, ch.table, ch.oldName, ch.newName, ch.colType)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("CHANGE COLUMN %s.%s -> %s %s", ch.table, ch.oldName, ch.newName, ch.colType))

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

	case mysqlDDLRenameTable:
		// Move the schema diff from old table name to new table name.
		// First, remove old table's delta and tombstone data under the old name.
		// The registry diff is dropped for the old name; a new-table entry is created
		// for the new name so the proxy knows it is shadow-only.
		dh.schemaRegistry.RemoveTable(ch.oldName)
		dh.schemaRegistry.RecordNewTable(ch.newName)
		if dh.deltaMap != nil {
			dh.deltaMap.ClearTable(ch.oldName)
		}
		if dh.tombstones != nil {
			dh.tombstones.ClearTable(ch.oldName)
		}
		if dh.verbose {
			log.Printf("[conn %d] schema registry: RENAME TABLE %s -> %s", dh.connID, ch.oldName, ch.newName)
		}
		dh.logger.Event(dh.connID, "ddl", fmt.Sprintf("RENAME TABLE %s -> %s", ch.oldName, ch.newName))
	}
}
