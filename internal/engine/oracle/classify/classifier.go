package classify

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/oracle/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*OracleClassifier)(nil)

// OracleClassifier implements core.Classifier using regex-based parsing for Oracle SQL.
type OracleClassifier struct {
	tables map[string]schema.TableMeta
}

// New creates an OracleClassifier. The tables map provides PK column names
// per table, used for extracting primary key values from WHERE clauses.
func New(tables map[string]schema.TableMeta) *OracleClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	return &OracleClassifier{tables: tables}
}

// Classify parses a SQL string and returns its classification.
func (c *OracleClassifier) Classify(query string) (*core.Classification, error) {
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
	case hasPrefix(upper, "MERGE"):
		c.classifyMerge(upper, cl)
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
		// Bare BEGIN is a transaction start. DECLARE...BEGIN is PL/SQL.
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubBegin
	case hasPrefix(upper, "COMMIT"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubCommit
	case hasPrefix(upper, "ROLLBACK"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRollback
	case hasPrefix(upper, "SAVEPOINT"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubOther
	case hasPrefix(upper, "EXPLAIN"), hasPrefix(upper, "SET"),
		hasPrefix(upper, "SHOW"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case hasPrefix(upper, "DECLARE"):
		// PL/SQL anonymous block — treat as write for safety.
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
	case hasPrefix(upper, "WITH"):
		c.classifyCTE(trimmed, upper, cl)
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}

	return cl, nil
}

// ClassifyWithParams classifies a parameterized query with bound values.
// Oracle uses :1, :2 (positional) or :name (named) bind variables.
func (c *OracleClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
	cl, err := c.Classify(query)
	if err != nil {
		return nil, err
	}

	for i, pk := range cl.PKs {
		if strings.HasPrefix(pk.PK, ":") {
			suffix := pk.PK[1:]
			// Try positional (:1, :2, ...).
			if idx, err := strconv.Atoi(suffix); err == nil && idx >= 1 && idx <= len(params) {
				cl.PKs[i].PK = fmt.Sprintf("%v", params[idx-1])
			}
		}
	}

	return cl, nil
}

func (c *OracleClassifier) classifySelect(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect

	cl.Tables = extractFromTables(upper)

	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)

	// Detect FETCH FIRST N ROWS ONLY (Oracle 12c+).
	if m := reFetchFirst.FindStringSubmatch(upper); len(m) > 1 {
		cl.HasLimit = true
		if n, err := strconv.Atoi(m[1]); err == nil {
			cl.Limit = n
		}
	}
	// Detect ROWNUM <= N (older Oracle style).
	if !cl.HasLimit {
		if m := reRownum.FindStringSubmatch(upper); len(m) > 1 {
			cl.HasLimit = true
			if n, err := strconv.Atoi(m[1]); err == nil {
				cl.Limit = n
			}
		}
	}

	if idx := strings.Index(upper, "ORDER BY"); idx >= 0 {
		cl.OrderBy = extractOrderBy(raw, idx)
	}

	cl.HasAggregate = reAggregate.MatchString(upper) || strings.Contains(upper, "GROUP BY")
	cl.HasSetOp = reSetOp.MatchString(upper)

	if reSubqueryFrom.MatchString(upper) {
		cl.IsComplexRead = true
	}

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *OracleClassifier) classifyInsert(raw, upper string, cl *core.Classification) {
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

func (c *OracleClassifier) classifyUpdate(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	cl.Tables = extractUpdateTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *OracleClassifier) classifyDelete(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete

	cl.Tables = extractDeleteTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *OracleClassifier) classifyMerge(upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubOther

	// MERGE INTO target USING source ON ...
	if m := reMergeInto.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
	if m := reMergeUsing.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *OracleClassifier) classifyCreate(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubCreate

	if m := reCreateTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	} else if m := reCreateIndex.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
	// CREATE SEQUENCE, CREATE VIEW, etc. — no table extraction needed.
}

func (c *OracleClassifier) classifyAlter(upper string, cl *core.Classification) {
	// ALTER SESSION SET is OpOther, not DDL.
	if strings.HasPrefix(upper, "ALTER SESSION") {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
		return
	}

	cl.OpType = core.OpDDL
	cl.SubType = core.SubAlter

	if m := reAlterTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *OracleClassifier) classifyDrop(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubDrop

	if m := reDropTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *OracleClassifier) classifyCTE(raw, upper string, cl *core.Classification) {
	if strings.Contains(upper, "INSERT") ||
		strings.Contains(upper, "UPDATE") ||
		strings.Contains(upper, "DELETE") ||
		strings.Contains(upper, "MERGE") {
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
func (c *OracleClassifier) extractPKs(raw string, tables []string) []core.TablePK {
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
			// Oracle bind variables: :1, :2, :name
			pattern := fmt.Sprintf(`(?i)\b%s\s*=\s*(?:'([^']*)'|(\d+)|(:\w+))`, regexp.QuoteMeta(pkCol))
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
// Regex patterns for Oracle SQL parsing
// ---------------------------------------------------------------------------

var (
	reJoin         = regexp.MustCompile(`(?i)\b(INNER|LEFT|RIGHT|CROSS|NATURAL|FULL)\s+(OUTER\s+)?JOIN\b`)
	reFetchFirst   = regexp.MustCompile(`(?i)\bFETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY`)
	reRownum       = regexp.MustCompile(`(?i)\bROWNUM\s*<=?\s*(\d+)`)
	reAggregate    = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|LISTAGG|XMLAGG|WM_CONCAT|MEDIAN|STATS_MODE)\s*\(`)
	reSetOp        = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|MINUS)\b`)
	reSubqueryFrom = regexp.MustCompile(`(?i)\bFROM\s*\(`)

	reInsertTable = regexp.MustCompile(`(?i)INSERT\s+INTO\s+` + tablePattern)
	reMergeInto   = regexp.MustCompile(`(?i)MERGE\s+INTO\s+` + tablePattern)
	reMergeUsing  = regexp.MustCompile(`(?i)\bUSING\s+` + tablePattern)
	reCreateTable = regexp.MustCompile(`(?i)CREATE\s+(?:GLOBAL\s+TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tablePattern)
	reCreateIndex = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+\S+\s+ON\s+` + tablePattern)
	reAlterTable  = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tablePattern)
	reDropTable   = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + tablePattern)

	reFromClause = regexp.MustCompile(`(?i)\bFROM\s+(` + tableListPattern + `)`)
	reJoinTable  = regexp.MustCompile(`(?i)\bJOIN\s+` + tablePattern)
)

const (
	// tablePattern matches a single table name (with optional double-quote quoting and schema prefix).
	tablePattern = `("?[a-zA-Z_][a-zA-Z0-9_$#]*"?(?:\."?[a-zA-Z_][a-zA-Z0-9_$#]*"?)?)`
	// tableListPattern matches a comma-separated list of table names.
	tableListPattern = `(?:` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*(?:,\s*` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*)*)`
)

// ---------------------------------------------------------------------------
// Table extraction helpers
// ---------------------------------------------------------------------------

func extractFromTables(upper string) []string {
	var tables []string

	if m := reFromClause.FindStringSubmatch(upper); len(m) > 1 {
		fromPart := m[1]
		// Stop at keywords that end the FROM clause.
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "FETCH", "UNION", "INTERSECT", "MINUS", "FOR", "START", "CONNECT"} {
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
				name := cleanTableName(tokens[0])
				// Skip DUAL pseudo-table.
				if name != "dual" {
					tables = appendUnique(tables, name)
				}
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

	tokens := strings.Fields(strings.TrimSpace(tablePart))
	if len(tokens) > 0 {
		tables = appendUnique(tables, cleanTableName(tokens[0]))
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

	for _, kw := range []string{"WHERE", "USING", "ORDER", "RETURNING"} {
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

func extractTableAfterKeyword(upper, keyword string) []string {
	idx := strings.Index(upper, keyword)
	if idx < 0 {
		return nil
	}
	rest := strings.TrimSpace(upper[idx+len(keyword):])
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
	upper := strings.ToUpper(rest)
	for _, kw := range []string{"FETCH", "OFFSET"} {
		if lIdx := strings.Index(upper, kw); lIdx >= 0 {
			rest = rest[:lIdx]
		}
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
