package classify

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/duckdb/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*DuckDBClassifier)(nil)

// DuckDBClassifier implements core.Classifier using regex-based parsing for DuckDB.
// DuckDB SQL is ~90% PostgreSQL compatible, so this classifier handles
// PG-style syntax with DuckDB-specific extensions.
type DuckDBClassifier struct {
	tables map[string]schema.TableMeta
}

// New creates a DuckDBClassifier.
func New(tables map[string]schema.TableMeta) *DuckDBClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	return &DuckDBClassifier{tables: tables}
}

// Classify parses a SQL string and returns its classification.
func (c *DuckDBClassifier) Classify(query string) (*core.Classification, error) {
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
		cl.OpType = core.OpTransaction
		// Distinguish full ROLLBACK from ROLLBACK TO SAVEPOINT.
		if strings.Contains(upper, " TO ") {
			cl.SubType = core.SubRollback // Handled specially in TxnHandler
		} else {
			cl.SubType = core.SubRollback
		}
	case hasPrefix(upper, "SAVEPOINT"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubSavepoint
	case hasPrefix(upper, "RELEASE"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRelease
	case hasPrefix(upper, "TRUNCATE"):
		c.classifyTruncate(upper, cl)
	case hasPrefix(upper, "EXPLAIN"):
		cl.OpType = core.OpOther
		// EXPLAIN ANALYZE is a write operation (it executes the query).
		if strings.Contains(upper, "ANALYZE") {
			cl.SubType = core.SubNotSupported
			cl.NotSupportedMsg = "mori: EXPLAIN ANALYZE is not supported (it executes the query)"
		} else {
			cl.SubType = core.SubExplain
			cl.Tables = extractFromTables(upper)
		}
	case hasPrefix(upper, "SET"),
		hasPrefix(upper, "RESET"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubSet
	case hasPrefix(upper, "SHOW"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubShow
	case hasPrefix(upper, "COPY"),
		hasPrefix(upper, "EXPORT"),
		hasPrefix(upper, "IMPORT"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "mori: COPY/EXPORT/IMPORT operations are not supported through the proxy"
	case hasPrefix(upper, "DESCRIBE"),
		hasPrefix(upper, "PRAGMA"),
		hasPrefix(upper, "CALL"),
		hasPrefix(upper, "INSTALL"),
		hasPrefix(upper, "LOAD"),
		hasPrefix(upper, "ATTACH"),
		hasPrefix(upper, "DETACH"):
		cl.OpType = core.OpOther
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
func (c *DuckDBClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
	cl, err := c.Classify(query)
	if err != nil {
		return nil, err
	}

	for i, pk := range cl.PKs {
		if pk.PK == "$"+fmt.Sprintf("%d", i+1) || pk.PK == "?" {
			if i < len(params) {
				cl.PKs[i].PK = fmt.Sprintf("%v", params[i])
			}
		}
	}

	return cl, nil
}

func (c *DuckDBClassifier) classifySelect(raw, upper string, cl *core.Classification) {
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
	cl.HasWindowFunc = reWindowFunc.MatchString(upper)

	// Detect DISTINCT.
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx >= 0 {
		afterSelect := strings.TrimSpace(upper[selectIdx+6:])
		if strings.HasPrefix(afterSelect, "DISTINCT") {
			cl.HasDistinct = true
		}
	}

	if reSubqueryFrom.MatchString(upper) {
		cl.IsComplexRead = true
	}

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *DuckDBClassifier) classifyInsert(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert

	if m := reInsertTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}

	// Detect ON CONFLICT (upsert) — DuckDB supports ON CONFLICT DO UPDATE/NOTHING.
	if reOnConflict.MatchString(upper) {
		cl.HasOnConflict = true
	}
	// Also detect INSERT OR REPLACE / INSERT OR IGNORE (DuckDB-specific syntax).
	if reInsertOrReplace.MatchString(upper) {
		cl.HasOnConflict = true
	}

	// Detect RETURNING clause.
	if reReturning.MatchString(upper) {
		cl.HasReturning = true
	}

	if idx := strings.Index(upper, "SELECT"); idx > 0 {
		selectPart := upper[idx:]
		selectTables := extractFromTables(selectPart)
		for _, t := range selectTables {
			cl.Tables = appendUnique(cl.Tables, t)
		}
	}

	// For upserts, also extract PKs from the WHERE-like context.
	if cl.HasOnConflict {
		cl.PKs = c.extractPKs(raw, cl.Tables)
	}
}

func (c *DuckDBClassifier) classifyUpdate(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	cl.Tables = extractUpdateTables(upper)
	cl.IsJoin = len(cl.Tables) > 1
	cl.PKs = c.extractPKs(raw, cl.Tables)
	cl.HasReturning = reReturning.MatchString(upper)
}

func (c *DuckDBClassifier) classifyDelete(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete

	cl.Tables = extractDeleteTables(upper)
	cl.IsJoin = len(cl.Tables) > 1
	cl.PKs = c.extractPKs(raw, cl.Tables)
	cl.HasReturning = reReturning.MatchString(upper)
}

func (c *DuckDBClassifier) classifyCreate(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubCreate

	if m := reCreateTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	} else if m := reCreateIndex.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *DuckDBClassifier) classifyAlter(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubAlter

	if m := reAlterTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *DuckDBClassifier) classifyDrop(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubDrop

	if m := reDropTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *DuckDBClassifier) classifyTruncate(upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubTruncate

	if m := reTruncate.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *DuckDBClassifier) classifyCTE(_, upper string, cl *core.Classification) {
	// Use word-boundary matching to avoid false positives on table/column
	// names that contain write keywords (e.g. "insert_history").
	if reCTEWrite.MatchString(upper) {
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

func (c *DuckDBClassifier) extractPKs(raw string, tables []string) []core.TablePK {
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
			val := extractPKValue(whereClause, pkCol)
			if val != "" {
				pks = append(pks, core.TablePK{Table: table, PK: val})
			}
		}
	}

	return pks
}

// extractPKValue extracts the PK value from a WHERE clause for a given column.
// Supports:
//   - column = value (and value = column reversal)
//   - CAST(value AS type) and value::type unwrapping
//   - Negative numbers, UUID literals, quoted strings
//   - $N and ? parameter placeholders
//   - IN clause with single value: column IN ('value')
func extractPKValue(whereClause, pkCol string) string {
	quotedCol := regexp.QuoteMeta(pkCol)

	// Pattern: column = value (normal order).
	// Supports: 'string', negative numbers, positive numbers, $N, ?, CAST(...), value::type, UUID
	normalPattern := fmt.Sprintf(`(?i)\b%s\s*=\s*(?:`+
		`'([^']*)'`+ // group 1: quoted string
		`|(-?\d+(?:\.\d+)?)`+ // group 2: number (possibly negative)
		`|(\$\d+)`+ // group 3: $N param
		`|(\?)`+ // group 4: ? param
		`|CAST\s*\(\s*'([^']*)'\s+AS\s+\w+\s*\)`+ // group 5: CAST('val' AS type)
		`|'([^']*)'::[\w]+`+ // group 6: 'val'::type
		`|([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`+ // group 7: UUID literal
		`)`, quotedCol)
	reNormal := regexp.MustCompile(normalPattern)
	if m := reNormal.FindStringSubmatch(whereClause); len(m) > 0 {
		return firstNonEmpty(m[1], m[2], m[3], m[4], m[5], m[6], m[7])
	}

	// Pattern: value = column (reversed order).
	reversedPattern := fmt.Sprintf(`(?i)(?:`+
		`'([^']*)'`+ // group 1: quoted string
		`|(-?\d+(?:\.\d+)?)`+ // group 2: number
		`|(\$\d+)`+ // group 3: $N param
		`|(\?)`+ // group 4: ? param
		`|CAST\s*\(\s*'([^']*)'\s+AS\s+\w+\s*\)`+ // group 5: CAST
		`|'([^']*)'::[\w]+`+ // group 6: ::type
		`|([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`+ // group 7: UUID
		`)\s*=\s*\b%s\b`, quotedCol)
	reReversed := regexp.MustCompile(reversedPattern)
	if m := reReversed.FindStringSubmatch(whereClause); len(m) > 0 {
		return firstNonEmpty(m[1], m[2], m[3], m[4], m[5], m[6], m[7])
	}

	// Pattern: column IN ('singleValue') — extract if only one value.
	inPattern := fmt.Sprintf(`(?i)\b%s\s+IN\s*\(\s*'([^']*)'\s*\)`, quotedCol)
	reIn := regexp.MustCompile(inPattern)
	if m := reIn.FindStringSubmatch(whereClause); len(m) > 1 {
		return m[1]
	}

	return ""
}

// firstNonEmpty returns the first non-empty string from the arguments.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Regex patterns for DuckDB SQL parsing
// ---------------------------------------------------------------------------

var (
	reJoin         = regexp.MustCompile(`(?i)\b(INNER|LEFT|RIGHT|CROSS|NATURAL|FULL)\s+JOIN\b`)
	reLimit        = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	reAggregate    = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|STRING_AGG|LIST|ARRAY_AGG|GROUP_CONCAT)\s*\(`)
	reSetOp        = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|EXCEPT)\b`)
	reSubqueryFrom = regexp.MustCompile(`(?i)\bFROM\s*\(`)
	reWindowFunc   = regexp.MustCompile(`(?i)\b\w+\s*\(\s*[^)]*\)\s+OVER\s*\(`)
	reOnConflict   = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`)
	reInsertOrReplace = regexp.MustCompile(`(?i)\bINSERT\s+OR\s+REPLACE\b`)
	reReturning    = regexp.MustCompile(`(?i)\bRETURNING\b`)
	reTruncate     = regexp.MustCompile(`(?i)TRUNCATE\s+(?:TABLE\s+)?` + tablePattern)

	reInsertTable  = regexp.MustCompile(`(?i)INSERT\s+(?:OR\s+(?:REPLACE|IGNORE)\s+)?INTO\s+` + tablePattern)
	reCreateTable  = regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tablePattern)
	reCreateIndex  = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+ON\s+` + tablePattern)
	reAlterTable   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tablePattern)
	reDropTable    = regexp.MustCompile(`(?i)DROP\s+(?:TABLE|INDEX|VIEW|SEQUENCE|MACRO|TYPE)\s+(?:IF\s+EXISTS\s+)?` + tablePattern)

	reFromClause = regexp.MustCompile(`(?i)\bFROM\s+(` + tableListPattern + `)`)
	reJoinTable  = regexp.MustCompile(`(?i)\bJOIN\s+` + tablePattern)
	reCTEWrite   = regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE)\b`)
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
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "QUALIFY", "WINDOW"} {
			if idx := strings.Index(strings.ToUpper(fromPart), kw); idx >= 0 {
				fromPart = fromPart[:idx]
			}
		}
		for part := range strings.SplitSeq(fromPart, ",") {
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
	for part := range strings.SplitSeq(tablePart, ",") {
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
	for _, kw := range []string{"WHERE", "ORDER", "LIMIT", "USING", "RETURNING"} {
		if idx := strings.Index(rest, kw); idx >= 0 {
			rest = rest[:idx]
		}
	}

	for part := range strings.SplitSeq(rest, ",") {
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
			if _, after, found := strings.Cut(s, "\n"); found {
				s = after
			} else {
				return ""
			}
		} else if strings.HasPrefix(s, "/*") {
			if _, after, found := strings.Cut(s, "*/"); found {
				s = after
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
