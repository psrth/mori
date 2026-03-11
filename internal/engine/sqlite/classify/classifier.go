package classify

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/engine/sqlite/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*SQLiteClassifier)(nil)

// SQLiteClassifier implements core.Classifier using regex-based parsing for SQLite.
type SQLiteClassifier struct {
	tables map[string]schema.TableMeta
}

// New creates a SQLiteClassifier.
func New(tables map[string]schema.TableMeta) *SQLiteClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	return &SQLiteClassifier{tables: tables}
}

// Classify parses a SQL string and returns its classification.
func (c *SQLiteClassifier) Classify(query string) (*core.Classification, error) {
	cl := &core.Classification{RawSQL: query}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return cl, nil
	}

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
		c.classifyReplace(upper, cl)
	case hasPrefix(upper, "TRUNCATE"):
		// SQLite doesn't have TRUNCATE natively, but some ORMs may send it.
		c.classifyTruncate(upper, cl)
	case hasPrefix(upper, "CREATE"):
		c.classifyCreate(upper, cl)
	case hasPrefix(upper, "ALTER"):
		c.classifyAlter(upper, cl)
	case hasPrefix(upper, "DROP"):
		c.classifyDrop(upper, cl)
	case hasPrefix(upper, "BEGIN"),
		hasPrefix(upper, "START TRANSACTION"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubBegin
	case hasPrefix(upper, "COMMIT"),
		hasPrefix(upper, "END"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubCommit
	case hasPrefix(upper, "ROLLBACK"):
		// Distinguish ROLLBACK TO SAVEPOINT from full ROLLBACK.
		if strings.Contains(upper, " TO ") {
			cl.OpType = core.OpTransaction
			cl.SubType = core.SubRollback // Router checks for ROLLBACK TO in the raw SQL
		} else {
			cl.OpType = core.OpTransaction
			cl.SubType = core.SubRollback
		}
	case hasPrefix(upper, "SAVEPOINT"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubSavepoint
	case hasPrefix(upper, "RELEASE"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRelease
	case hasPrefix(upper, "EXPLAIN"):
		c.classifyExplain(trimmed, upper, cl)
	case hasPrefix(upper, "PRAGMA"),
		hasPrefix(upper, "ANALYZE"),
		hasPrefix(upper, "VACUUM"),
		hasPrefix(upper, "REINDEX"),
		hasPrefix(upper, "ATTACH"),
		hasPrefix(upper, "DETACH"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		// Detect metadata queries against sqlite_master / sqlite_schema.
		cl.IsMetadataQuery = strings.Contains(upper, "SQLITE_MASTER") ||
			strings.Contains(upper, "SQLITE_SCHEMA") ||
			hasPrefix(upper, "PRAGMA")
	case hasPrefix(upper, "WITH"):
		c.classifyCTE(trimmed, upper, cl)
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}

	return cl, nil
}

// ClassifyWithParams classifies a parameterized query with bound values.
func (c *SQLiteClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
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

func (c *SQLiteClassifier) classifySelect(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect

	cl.Tables = extractFromTables(upper)
	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)

	if m := reLimit.FindStringSubmatch(upper); len(m) > 1 {
		cl.HasLimit = true
		if n, err := strconv.Atoi(m[1]); err == nil {
			cl.Limit = n
		}
	}

	if idx := strings.Index(upper, "ORDER BY"); idx >= 0 {
		cl.OrderBy = extractOrderBy(raw, idx)
	}

	cl.HasAggregate = reAggregate.MatchString(upper) || strings.Contains(upper, "GROUP BY")
	cl.HasSetOp = reSetOp.MatchString(upper)

	// Detect DISTINCT.
	afterSelect := strings.TrimSpace(upper[6:]) // after "SELECT"
	cl.HasDistinct = strings.HasPrefix(afterSelect, "DISTINCT")

	// Detect window functions: OVER ( pattern.
	cl.HasWindowFunc = reWindowFunc.MatchString(upper)

	// Detect complex aggregates (GROUP_CONCAT in SQLite).
	cl.HasComplexAgg = reComplexAgg.MatchString(upper)

	if reSubqueryFrom.MatchString(upper) {
		cl.IsComplexRead = true
	}

	// Detect metadata queries.
	cl.IsMetadataQuery = strings.Contains(upper, "SQLITE_MASTER") ||
		strings.Contains(upper, "SQLITE_SCHEMA")

	// Detect RETURNING clause.
	cl.HasReturning = reReturning.MatchString(upper)

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *SQLiteClassifier) classifyInsert(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert

	if m := reInsertTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}

	if idx := strings.Index(upper, "SELECT"); idx > 0 {
		selectPart := upper[idx:]
		selectTables := extractFromTables(selectPart)
		for _, t := range selectTables {
			cl.Tables = appendUnique(cl.Tables, t)
		}
	}

	// Detect ON CONFLICT (SQLite 3.24+) or INSERT OR REPLACE.
	cl.HasOnConflict = reOnConflict.MatchString(upper) ||
		reInsertOrReplace.MatchString(upper)

	// Detect RETURNING clause (SQLite 3.35+).
	cl.HasReturning = reReturning.MatchString(upper)
}

func (c *SQLiteClassifier) classifyUpdate(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	cl.Tables = extractUpdateTables(upper)
	cl.IsJoin = len(cl.Tables) > 1
	cl.PKs = c.extractPKs(raw, cl.Tables)

	// Detect RETURNING clause (SQLite 3.35+).
	cl.HasReturning = reReturning.MatchString(upper)
}

func (c *SQLiteClassifier) classifyDelete(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite

	// Check if this is a DELETE FROM <table> with no WHERE — treat as truncate.
	cl.Tables = extractDeleteTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	if !strings.Contains(upper, "WHERE") && len(cl.Tables) == 1 {
		cl.SubType = core.SubTruncate
		return
	}

	cl.SubType = core.SubDelete
	cl.PKs = c.extractPKs(raw, cl.Tables)

	// Detect RETURNING clause (SQLite 3.35+).
	cl.HasReturning = reReturning.MatchString(upper)
}

func (c *SQLiteClassifier) classifyReplace(upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert
	cl.HasOnConflict = true // REPLACE is semantically an upsert.

	if m := reReplaceTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *SQLiteClassifier) classifyTruncate(upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubTruncate

	// Try to extract table name from "TRUNCATE [TABLE] <table>".
	if m := reTruncateTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *SQLiteClassifier) classifyExplain(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpOther
	cl.SubType = core.SubExplain

	// Extract tables from the inner query.
	innerStart := len("EXPLAIN")
	if strings.HasPrefix(upper[innerStart:], " QUERY PLAN") {
		innerStart += len(" QUERY PLAN")
	}
	if innerStart < len(upper) {
		innerUpper := strings.TrimSpace(upper[innerStart:])
		cl.Tables = extractFromTables(innerUpper)
	}
}

func (c *SQLiteClassifier) classifyCreate(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubCreate

	if m := reCreateTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	} else if m := reCreateIndex.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *SQLiteClassifier) classifyAlter(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubAlter

	if m := reAlterTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *SQLiteClassifier) classifyDrop(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubDrop

	if m := reDropTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *SQLiteClassifier) classifyCTE(raw, upper string, cl *core.Classification) {
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
	cl.HasWindowFunc = reWindowFunc.MatchString(upper)
	cl.IsComplexRead = true
}

func (c *SQLiteClassifier) extractPKs(raw string, tables []string) []core.TablePK {
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
			// Support both "col = value" and "value = col" patterns.
			pattern := fmt.Sprintf(`(?i)(?:\b%s\s*=\s*(?:'([^']*)'|(-?\d+(?:\.\d+)?)|(\?))|(?:'([^']*)'|(-?\d+(?:\.\d+)?))\s*=\s*%s\b)`,
				regexp.QuoteMeta(pkCol), regexp.QuoteMeta(pkCol))
			re := regexp.MustCompile(pattern)
			if m := re.FindStringSubmatch(whereClause); len(m) > 0 {
				val := ""
				for _, v := range m[1:] {
					if v != "" {
						val = v
						break
					}
				}
				if val != "" {
					pks = append(pks, core.TablePK{Table: table, PK: val})
				}
			}
		}
	}

	return pks
}

// ---------------------------------------------------------------------------
// Regex patterns for SQLite SQL parsing
// ---------------------------------------------------------------------------

var (
	reJoin         = regexp.MustCompile(`(?i)\b(INNER|LEFT|RIGHT|CROSS|NATURAL)\s+JOIN\b`)
	reLimit        = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	reAggregate    = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|TOTAL)\s*\(`)
	reSetOp        = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|EXCEPT)\b`)
	reSubqueryFrom = regexp.MustCompile(`(?i)\bFROM\s*\(`)
	reWindowFunc   = regexp.MustCompile(`(?i)\bOVER\s*\(`)
	reComplexAgg   = regexp.MustCompile(`(?i)\bGROUP_CONCAT\s*\(`)
	reReturning    = regexp.MustCompile(`(?i)\bRETURNING\b`)
	reOnConflict   = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`)
	reInsertOrReplace = regexp.MustCompile(`(?i)INSERT\s+OR\s+REPLACE\b`)

	reInsertTable  = regexp.MustCompile(`(?i)INSERT\s+(?:OR\s+(?:REPLACE|IGNORE|ABORT|ROLLBACK|FAIL)\s+)?INTO\s+` + tablePattern)
	reReplaceTable = regexp.MustCompile(`(?i)REPLACE\s+INTO\s+` + tablePattern)
	reTruncateTable = regexp.MustCompile(`(?i)TRUNCATE\s+(?:TABLE\s+)?` + tablePattern)
	reCreateTable  = regexp.MustCompile(`(?i)CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tablePattern)
	reCreateIndex  = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+ON\s+` + tablePattern)
	reAlterTable   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tablePattern)
	reDropTable    = regexp.MustCompile(`(?i)DROP\s+(?:TABLE|INDEX|VIEW|TRIGGER)\s+(?:IF\s+EXISTS\s+)?` + tablePattern)

	reFromClause = regexp.MustCompile(`(?i)\bFROM\s+(` + tableListPattern + `)`)
	reJoinTable  = regexp.MustCompile(`(?i)\bJOIN\s+` + tablePattern)
)

const (
	tablePattern     = `("?[a-zA-Z_][a-zA-Z0-9_]*"?(?:\."?[a-zA-Z_][a-zA-Z0-9_]*"?)?)`
	tableListPattern = `(?:` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*(?:,\s*` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*)*)`
)

// ---------------------------------------------------------------------------
// Table extraction helpers
// ---------------------------------------------------------------------------

func extractFromTables(upper string) []string {
	var tables []string

	if m := reFromClause.FindStringSubmatch(upper); len(m) > 1 {
		fromPart := m[1]
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT"} {
			if idx := strings.Index(strings.ToUpper(fromPart), kw); idx >= 0 {
				fromPart = fromPart[:idx]
			}
		}
		parts := strings.Split(fromPart, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			tokens := strings.Fields(part)
			if len(tokens) > 0 {
				tables = appendUnique(tables, cleanTableName(tokens[0]))
			}
		}
	}

	for _, m := range reJoinTable.FindAllStringSubmatch(upper, -1) {
		if len(m) > 1 {
			tables = appendUnique(tables, cleanTableName(m[1]))
		}
	}

	return tables
}

func extractUpdateTables(upper string) []string {
	var tables []string

	setIdx := strings.Index(upper, " SET ")
	if setIdx < 0 {
		return nil
	}
	tablePart := upper[len("UPDATE"):setIdx]
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

	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil
	}
	rest := upper[fromIdx+4:]
	for _, kw := range []string{"WHERE", "ORDER", "LIMIT", "RETURNING"} {
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

	return tables
}

func extractOrderBy(raw string, idx int) string {
	rest := raw[idx:]
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
	name = strings.Trim(name, `"`)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.Trim(name, `"`)
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
	if len(upper) > len(prefix) {
		next := upper[len(prefix)]
		if next >= 'A' && next <= 'Z' {
			return false
		}
	}
	return true
}
