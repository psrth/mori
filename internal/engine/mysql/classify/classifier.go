package classify

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*MySQLClassifier)(nil)

// MySQLClassifier implements core.Classifier using regex-based parsing for MySQL.
type MySQLClassifier struct {
	tables map[string]schema.TableMeta
}

// New creates a MySQLClassifier. The tables map provides PK column names
// per table, used for extracting primary key values from WHERE clauses.
func New(tables map[string]schema.TableMeta) *MySQLClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	return &MySQLClassifier{tables: tables}
}

// Classify parses a SQL string and returns its classification.
func (c *MySQLClassifier) Classify(query string) (*core.Classification, error) {
	cl := &core.Classification{RawSQL: query}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return cl, nil
	}

	// Strip leading comments.
	trimmed = stripLeadingComments(trimmed)
	upper := strings.ToUpper(trimmed)

	switch {
	case hasPrefix(upper, "SELECT"):
		c.classifySelect(trimmed, upper, cl)
	case hasPrefix(upper, "INSERT"):
		c.classifyInsert(trimmed, upper, cl)
	case hasPrefix(upper, "UPDATE"):
		c.classifyUpdate(trimmed, upper, cl)
	case hasPrefix(upper, "DELETE"):
		c.classifyDelete(trimmed, upper, cl)
	case hasPrefix(upper, "REPLACE"):
		c.classifyReplace(trimmed, upper, cl)
	case hasPrefix(upper, "CREATE"):
		c.classifyCreate(upper, cl)
	case hasPrefix(upper, "ALTER"):
		c.classifyAlter(upper, cl)
	case hasPrefix(upper, "DROP"):
		c.classifyDrop(upper, cl)
	case hasPrefix(upper, "TRUNCATE"):
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
		cl.Tables = extractTableAfterKeyword(upper, "TRUNCATE")
	case hasPrefix(upper, "BEGIN"), hasPrefix(upper, "START TRANSACTION"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubBegin
	case hasPrefix(upper, "COMMIT"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubCommit
	case hasPrefix(upper, "ROLLBACK"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRollback
	case hasPrefix(upper, "SET"), hasPrefix(upper, "SHOW"),
		hasPrefix(upper, "EXPLAIN"), hasPrefix(upper, "DESCRIBE"),
		hasPrefix(upper, "DESC"), hasPrefix(upper, "USE"),
		hasPrefix(upper, "HELP"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case hasPrefix(upper, "WITH"):
		// CTE: check if it ends with a write operation.
		c.classifyCTE(trimmed, upper, cl)
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}

	return cl, nil
}

// ClassifyWithParams classifies a parameterized query with bound values.
// For MySQL, placeholders are ? and are positional.
func (c *MySQLClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
	cl, err := c.Classify(query)
	if err != nil {
		return nil, err
	}

	for i, pk := range cl.PKs {
		if pk.PK == "?" {
			if i < len(params) {
				cl.PKs[i].PK = fmt.Sprintf("%v", params[i])
			}
		}
	}

	return cl, nil
}

func (c *MySQLClassifier) classifySelect(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect

	// Extract tables from FROM clause.
	cl.Tables = extractFromTables(upper)

	// Detect JOINs.
	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)

	// Detect LIMIT.
	if m := reLimit.FindStringSubmatch(upper); len(m) > 1 {
		cl.HasLimit = true
		if n, err := strconv.Atoi(m[1]); err == nil {
			cl.Limit = n
		}
	}

	// Detect ORDER BY.
	if idx := strings.Index(upper, "ORDER BY"); idx >= 0 {
		cl.OrderBy = extractOrderBy(raw, idx)
	}

	// Detect aggregates.
	cl.HasAggregate = reAggregate.MatchString(upper) || strings.Contains(upper, "GROUP BY")

	// Detect set operations.
	cl.HasSetOp = reSetOp.MatchString(upper)

	// Detect complex reads (subqueries in FROM).
	if reSubqueryFrom.MatchString(upper) {
		cl.IsComplexRead = true
	}

	// Extract PKs from WHERE clause.
	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *MySQLClassifier) classifyInsert(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert

	if m := reInsertTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}

	// INSERT ... SELECT: extract tables from the SELECT portion.
	if idx := strings.Index(upper, "SELECT"); idx > 0 {
		selectPart := upper[idx:]
		selectTables := extractFromTables(selectPart)
		for _, t := range selectTables {
			cl.Tables = appendUnique(cl.Tables, t)
		}
	}
}

func (c *MySQLClassifier) classifyUpdate(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	// UPDATE table1, table2 SET ... or UPDATE table1 JOIN table2 ...
	cl.Tables = extractUpdateTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *MySQLClassifier) classifyDelete(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete

	cl.Tables = extractDeleteTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *MySQLClassifier) classifyReplace(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert // REPLACE is semantically INSERT-or-UPDATE

	if m := reReplaceTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MySQLClassifier) classifyCreate(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubCreate

	if m := reCreateTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	} else if m := reCreateIndex.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MySQLClassifier) classifyAlter(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubAlter

	if m := reAlterTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MySQLClassifier) classifyDrop(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubDrop

	if m := reDropTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MySQLClassifier) classifyCTE(raw, upper string, cl *core.Classification) {
	// Check for write keywords after the CTE definition.
	if strings.Contains(upper, "INSERT") ||
		strings.Contains(upper, "UPDATE") ||
		strings.Contains(upper, "DELETE") {
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
	} else {
		cl.OpType = core.OpRead
		cl.SubType = core.SubSelect
	}

	cl.Tables = extractFromTables(upper)
	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)
	cl.HasAggregate = reAggregate.MatchString(upper) || strings.Contains(upper, "GROUP BY")
	cl.IsComplexRead = true
}

// extractPKs extracts primary key values from WHERE clauses.
func (c *MySQLClassifier) extractPKs(raw string, tables []string) []core.TablePK {
	var pks []core.TablePK

	upper := strings.ToUpper(raw)
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return nil
	}
	whereClause := raw[whereIdx+5:]

	for _, table := range tables {
		meta, ok := c.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		for _, pkCol := range meta.PKColumns {
			pattern := fmt.Sprintf(`(?i)\b%s\s*=\s*(?:'([^']*)'|(\d+)|(\?))`, regexp.QuoteMeta(pkCol))
			re := regexp.MustCompile(pattern)
			if m := re.FindStringSubmatch(whereClause); len(m) > 0 {
				val := m[1]
				if val == "" {
					val = m[2]
				}
				if val == "" {
					val = m[3]
				}
				pks = append(pks, core.TablePK{Table: table, PK: val})
			}
		}
	}

	return pks
}

// ---------------------------------------------------------------------------
// Regex patterns for MySQL SQL parsing
// ---------------------------------------------------------------------------

var (
	reJoin         = regexp.MustCompile(`(?i)\b(INNER|LEFT|RIGHT|CROSS|NATURAL|STRAIGHT_JOIN)\s+JOIN\b`)
	reLimit        = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	reAggregate    = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|JSON_ARRAYAGG|JSON_OBJECTAGG)\s*\(`)
	reSetOp        = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|EXCEPT)\b`)
	reSubqueryFrom = regexp.MustCompile(`(?i)\bFROM\s*\(`)

	reInsertTable  = regexp.MustCompile(`(?i)INSERT\s+(?:IGNORE\s+)?INTO\s+` + tablePattern)
	reReplaceTable = regexp.MustCompile(`(?i)REPLACE\s+INTO\s+` + tablePattern)
	reCreateTable  = regexp.MustCompile(`(?i)CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tablePattern)
	reCreateIndex  = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+\S+\s+ON\s+` + tablePattern)
	reAlterTable   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tablePattern)
	reDropTable    = regexp.MustCompile(`(?i)DROP\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+EXISTS\s+)?` + tablePattern)

	reFromClause = regexp.MustCompile(`(?i)\bFROM\s+(` + tableListPattern + `)`)
	reJoinTable  = regexp.MustCompile(`(?i)\bJOIN\s+` + tablePattern)
)

const (
	// tablePattern matches a single table name (with optional backtick quoting and schema prefix).
	tablePattern = "(`?[a-zA-Z_][a-zA-Z0-9_]*`?(?:\\.`?[a-zA-Z_][a-zA-Z0-9_]*`?)?)"
	// tableListPattern matches a comma-separated list of table names.
	tableListPattern = `(?:` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*(?:,\s*` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*)*)`
)

var reTableInList = regexp.MustCompile(`(?i)` + tablePattern)

// ---------------------------------------------------------------------------
// Table extraction helpers
// ---------------------------------------------------------------------------

func extractFromTables(upper string) []string {
	var tables []string

	// Extract FROM clause tables.
	if m := reFromClause.FindStringSubmatch(upper); len(m) > 1 {
		fromPart := m[1]
		// Stop at keywords that end the FROM clause.
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "FOR"} {
			if idx := strings.Index(strings.ToUpper(fromPart), kw); idx >= 0 {
				fromPart = fromPart[:idx]
			}
		}
		// Split by comma and extract table names.
		parts := strings.Split(fromPart, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			// Take first token as table name.
			tokens := strings.Fields(part)
			if len(tokens) > 0 {
				tables = appendUnique(tables, cleanTableName(tokens[0]))
			}
		}
	}

	// Extract JOIN tables.
	for _, m := range reJoinTable.FindAllStringSubmatch(upper, -1) {
		if len(m) > 1 {
			tables = appendUnique(tables, cleanTableName(m[1]))
		}
	}

	return tables
}

func extractUpdateTables(upper string) []string {
	var tables []string

	// UPDATE table1 [, table2] SET ...
	setIdx := strings.Index(upper, " SET ")
	if setIdx < 0 {
		return nil
	}
	tablePart := upper[len("UPDATE"):setIdx]

	// Handle JOIN syntax.
	joinIdx := strings.Index(tablePart, " JOIN ")
	if joinIdx >= 0 {
		// Before JOIN: first table.
		before := strings.TrimSpace(tablePart[:joinIdx])
		tokens := strings.Fields(before)
		if len(tokens) > 0 {
			tables = appendUnique(tables, cleanTableName(tokens[0]))
		}
		// After JOIN: extract join tables.
		for _, m := range reJoinTable.FindAllStringSubmatch(upper, -1) {
			if len(m) > 1 {
				tables = appendUnique(tables, cleanTableName(m[1]))
			}
		}
		return tables
	}

	// Comma-separated table list.
	parts := strings.Split(tablePart, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		tokens := strings.Fields(part)
		if len(tokens) > 0 {
			tables = appendUnique(tables, cleanTableName(tokens[0]))
		}
	}
	return tables
}

func extractDeleteTables(upper string) []string {
	var tables []string

	// DELETE FROM table ...
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil
	}
	rest := upper[fromIdx+4:]

	// Cut at WHERE, USING, ORDER, LIMIT.
	for _, kw := range []string{"WHERE", "USING", "ORDER", "LIMIT"} {
		if idx := strings.Index(rest, kw); idx >= 0 {
			rest = rest[:idx]
		}
	}

	parts := strings.Split(rest, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		tokens := strings.Fields(part)
		if len(tokens) > 0 {
			tables = appendUnique(tables, cleanTableName(tokens[0]))
		}
	}

	// Also extract JOIN tables.
	for _, m := range reJoinTable.FindAllStringSubmatch(upper, -1) {
		if len(m) > 1 {
			tables = appendUnique(tables, cleanTableName(m[1]))
		}
	}

	return tables
}

func extractTableAfterKeyword(upper, keyword string) []string {
	idx := strings.Index(upper, keyword)
	if idx < 0 {
		return nil
	}
	rest := strings.TrimSpace(upper[idx+len(keyword):])
	// Skip optional TABLE keyword.
	if strings.HasPrefix(rest, "TABLE") {
		rest = strings.TrimSpace(rest[5:])
	}
	tokens := strings.Fields(rest)
	if len(tokens) > 0 {
		return []string{cleanTableName(tokens[0])}
	}
	return nil
}

func extractOrderBy(raw string, idx int) string {
	rest := raw[idx:]
	// Stop at LIMIT or end of string.
	upper := strings.ToUpper(rest)
	if lIdx := strings.Index(upper, "LIMIT"); lIdx >= 0 {
		rest = rest[:lIdx]
	}
	return strings.TrimSpace(rest)
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

func cleanTableName(name string) string {
	name = strings.Trim(name, "`\"")
	// Remove schema prefix if present.
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.Trim(name, "`\"")
	return strings.ToLower(name)
}

func appendUnique(slice []string, items ...string) []string {
	seen := make(map[string]bool, len(slice))
	for _, s := range slice {
		seen[s] = true
	}
	for _, item := range items {
		if item == "" {
			continue
		}
		if !seen[item] {
			slice = append(slice, item)
			seen[item] = true
		}
	}
	return slice
}

func stripLeadingComments(s string) string {
	for {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "--") {
			if idx := strings.Index(s, "\n"); idx >= 0 {
				s = s[idx+1:]
			} else {
				return ""
			}
		} else if strings.HasPrefix(s, "/*") {
			if idx := strings.Index(s, "*/"); idx >= 0 {
				s = s[idx+2:]
			} else {
				return ""
			}
		} else {
			return s
		}
	}
}

func hasPrefix(upper, prefix string) bool {
	if len(upper) < len(prefix) {
		return false
	}
	if upper[:len(prefix)] != prefix {
		return false
	}
	// Ensure it's a word boundary.
	if len(upper) > len(prefix) {
		next := upper[len(prefix)]
		if next >= 'A' && next <= 'Z' {
			return false
		}
	}
	return true
}
