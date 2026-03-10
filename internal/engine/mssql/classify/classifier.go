package classify

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*MSSQLClassifier)(nil)

// MSSQLClassifier implements core.Classifier using regex-based parsing for T-SQL.
type MSSQLClassifier struct {
	tables map[string]schema.TableMeta
}

// New creates a MSSQLClassifier. The tables map provides PK column names
// per table, used for extracting primary key values from WHERE clauses.
func New(tables map[string]schema.TableMeta) *MSSQLClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	return &MSSQLClassifier{tables: tables}
}

// Classify parses a T-SQL string and returns its classification.
func (c *MSSQLClassifier) Classify(query string) (*core.Classification, error) {
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
		cl.SubType = core.SubTruncate
		cl.Tables = extractTableAfterKeyword(upper, "TRUNCATE")
	case hasPrefix(upper, "BEGIN TRAN"), hasPrefix(upper, "BEGIN TRANSACTION"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubBegin
	case hasPrefix(upper, "BEGIN"):
		// T-SQL BEGIN (not BEGIN TRAN) is a block statement — classify as other.
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case hasPrefix(upper, "COMMIT"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubCommit
	case hasPrefix(upper, "SAVE TRAN"), hasPrefix(upper, "SAVE TRANSACTION"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubSavepoint
	case hasPrefix(upper, "ROLLBACK TRAN"), hasPrefix(upper, "ROLLBACK TRANSACTION"):
		// Distinguish full ROLLBACK from partial ROLLBACK TO savepoint.
		// "ROLLBACK TRAN[SACTION] name" (with a name) is a partial rollback to savepoint.
		cl.OpType = core.OpTransaction
		cl.SubType = c.classifyRollbackSub(trimmed, upper)
	case hasPrefix(upper, "ROLLBACK"):
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRollback
	case hasPrefix(upper, "SET SHOWPLAN_ALL"), hasPrefix(upper, "SET SHOWPLAN_XML"),
		hasPrefix(upper, "SET STATISTICS PROFILE"), hasPrefix(upper, "SET STATISTICS XML"):
		// P3 §4.1: MSSQL's EXPLAIN equivalents.
		cl.OpType = core.OpOther
		cl.SubType = core.SubExplain
	case hasPrefix(upper, "BULK INSERT"):
		cl.OpType = core.OpWrite
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "BULK INSERT is not supported through the Mori proxy"
		cl.Tables = extractTableAfterKeyword(upper, "BULK INSERT")
	case c.isSpRenameColumn(trimmed, upper):
		// P1 §2.7: sp_rename for column renaming → classify as DDL.
		cl.OpType = core.OpDDL
		cl.SubType = core.SubAlter
	case hasPrefix(upper, "EXEC"), hasPrefix(upper, "EXECUTE"):
		cl.OpType = core.OpWrite
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "EXEC/EXECUTE statements are not supported through the Mori proxy"
	case hasPrefix(upper, "SET"), hasPrefix(upper, "PRINT"),
		hasPrefix(upper, "DECLARE"), hasPrefix(upper, "USE"),
		hasPrefix(upper, "DBCC"), hasPrefix(upper, "WAITFOR"),
		hasPrefix(upper, "RAISERROR"), hasPrefix(upper, "THROW"),
		hasPrefix(upper, "GRANT"), hasPrefix(upper, "REVOKE"),
		hasPrefix(upper, "DENY"):
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case hasPrefix(upper, "IF"):
		// T-SQL IF ... can contain any statement. Classify based on body.
		c.classifyConditional(trimmed, upper, cl)
	case hasPrefix(upper, "WITH"):
		c.classifyCTE(trimmed, upper, cl)
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}

	return cl, nil
}

// ClassifyWithParams classifies a parameterized query with bound values.
// For MSSQL, parameters use @p1, @p2 style named placeholders.
func (c *MSSQLClassifier) ClassifyWithParams(query string, params []any) (*core.Classification, error) {
	cl, err := c.Classify(query)
	if err != nil {
		return nil, err
	}

	for i, pk := range cl.PKs {
		if strings.HasPrefix(pk.PK, "@p") {
			idxStr := pk.PK[2:]
			if idx, err := strconv.Atoi(idxStr); err == nil && idx > 0 && idx <= len(params) {
				cl.PKs[i].PK = fmt.Sprintf("%v", params[idx-1])
			}
		}
	}

	return cl, nil
}

func (c *MSSQLClassifier) classifySelect(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect

	// Extract tables from FROM clause.
	cl.Tables = extractFromTables(upper)

	// Detect JOINs.
	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)

	// Detect TOP (MSSQL equivalent of LIMIT).
	if m := reTop.FindStringSubmatch(upper); len(m) > 1 {
		cl.HasLimit = true
		if n, err := strconv.Atoi(m[1]); err == nil {
			cl.Limit = n
		}
	}

	// Detect LIMIT (some MSSQL drivers support OFFSET/FETCH syntax).
	if m := reFetch.FindStringSubmatch(upper); len(m) > 1 {
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

	// P3 §4.1: Detect window functions.
	if reWindowFunc.MatchString(upper) {
		cl.IsComplexRead = true
		cl.HasWindowFunc = true
	}

	// P3 §4.1: Detect DISTINCT.
	if reDistinct.MatchString(upper) {
		cl.HasDistinct = true
	}

	// P3 §4.1: Detect complex aggregates (STRING_AGG, etc.).
	if reComplexAgg.MatchString(upper) {
		cl.HasComplexAgg = true
	}

	// P3 §4.1: Detect metadata queries against sys.* or INFORMATION_SCHEMA.*.
	if reMetadataQuery.MatchString(upper) {
		cl.IsMetadataQuery = true
	}

	// Detect complex reads (subqueries in FROM, PIVOT, UNPIVOT, APPLY).
	if reSubqueryFrom.MatchString(upper) {
		cl.IsComplexRead = true
	}
	if rePivot.MatchString(upper) {
		cl.IsComplexRead = true
	}
	if reApply.MatchString(upper) {
		cl.IsComplexRead = true
		cl.IsJoin = true
	}

	// Extract PKs from WHERE clause.
	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *MSSQLClassifier) classifyInsert(_raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert

	if m := reInsertTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}

	// P3 §4.1: Detect OUTPUT clause (MSSQL's RETURNING equivalent).
	if reOutput.MatchString(upper) {
		cl.HasReturning = true
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

func (c *MSSQLClassifier) classifyUpdate(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	cl.Tables = extractUpdateTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	// P3 §4.1: Detect OUTPUT clause.
	if reOutput.MatchString(upper) {
		cl.HasReturning = true
	}

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *MSSQLClassifier) classifyDelete(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete

	cl.Tables = extractDeleteTables(upper)
	cl.IsJoin = len(cl.Tables) > 1

	// P3 §4.1: Detect OUTPUT clause.
	if reOutput.MatchString(upper) {
		cl.HasReturning = true
	}

	cl.PKs = c.extractPKs(raw, cl.Tables)
}

func (c *MSSQLClassifier) classifyMerge(upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubOther // MERGE is a combined INSERT/UPDATE/DELETE

	// MERGE INTO target ...
	if m := reMergeTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}

	// ... USING source ...
	if m := reMergeUsing.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}

	// P0 §1.5 / P3 §4.1: Detect MERGE-as-upsert pattern.
	if strings.Contains(upper, "WHEN MATCHED") || strings.Contains(upper, "WHEN NOT MATCHED") {
		cl.HasOnConflict = true
	}
}

func (c *MSSQLClassifier) classifyCreate(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubCreate

	if m := reCreateTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	} else if m := reCreateIndex.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MSSQLClassifier) classifyAlter(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubAlter

	if m := reAlterTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MSSQLClassifier) classifyDrop(upper string, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubDrop

	if m := reDropTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MSSQLClassifier) classifyCTE(_raw, upper string, cl *core.Classification) {
	// Check for write keywords after the CTE definition.
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

// isSpRenameColumn detects EXEC sp_rename 't.old', 'new', 'COLUMN' patterns.
func (c *MSSQLClassifier) isSpRenameColumn(_raw, upper string) bool {
	return reSpRenameColumn.MatchString(upper)
}

// reSpRenameColumn matches sp_rename with 'COLUMN' as the third arg.
var reSpRenameColumn = regexp.MustCompile(`(?i)(?:EXEC(?:UTE)?\s+)?SP_RENAME\s+.*,\s*.*,\s*'COLUMN'`)

// classifyRollbackSub distinguishes full ROLLBACK TRAN from partial
// ROLLBACK TRAN <name> (savepoint rollback) in T-SQL.
func (c *MSSQLClassifier) classifyRollbackSub(raw, _upper string) core.SubType {
	fields := strings.Fields(raw)
	// "ROLLBACK TRAN" or "ROLLBACK TRANSACTION" with no extra name → full rollback.
	// "ROLLBACK TRAN sp1" → partial rollback to savepoint.
	if len(fields) <= 2 {
		return core.SubRollback
	}
	// fields[0] = ROLLBACK, fields[1] = TRAN/TRANSACTION, fields[2] = <name>
	name := fields[2]
	// If name is a keyword (like semicolon), treat as full rollback.
	if name == ";" || name == "" {
		return core.SubRollback
	}
	return core.SubSavepoint
}

func (c *MSSQLClassifier) classifyConditional(_raw, upper string, cl *core.Classification) {
	// T-SQL IF ... can wrap any statement. Detect writes within the body.
	if strings.Contains(upper, "INSERT") ||
		strings.Contains(upper, "UPDATE") ||
		strings.Contains(upper, "DELETE") ||
		strings.Contains(upper, "MERGE") ||
		strings.Contains(upper, "CREATE") ||
		strings.Contains(upper, "ALTER") ||
		strings.Contains(upper, "DROP") ||
		strings.Contains(upper, "TRUNCATE") {
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
		cl.Tables = extractFromTables(upper)
	} else {
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}
}

// extractPKs extracts primary key values from WHERE clauses.
func (c *MSSQLClassifier) extractPKs(raw string, tables []string) []core.TablePK {
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
			// P3 §4.2: Support multiple PK value patterns:
			// 1. col = 'value', col = 123, col = @p1 (standard)
			// 2. 'value' = col, 123 = col (reversed)
			// 3. col = CAST(value AS type) / col = CONVERT(type, value)
			// 4. Negative numbers: col = -123
			// 5. GUID literals: col = '{...}'
			escaped := regexp.QuoteMeta(pkCol)
			// Standard: col = value
			pattern := fmt.Sprintf(`(?i)\b\[?%s\]?\s*=\s*(?:'([^']*)'|(-?\d+)|(@p\d+)|\{([0-9A-Fa-f-]+)\})`, escaped)
			re := regexp.MustCompile(pattern)
			if m := re.FindStringSubmatch(whereClause); len(m) > 0 {
				val := firstNonEmpty(m[1], m[2], m[3], m[4])
				if val != "" {
					pks = append(pks, core.TablePK{Table: table, PK: val})
					continue
				}
			}

			// P3 §4.2: Reversed form: value = col
			patternReversed := fmt.Sprintf(`(?i)(?:'([^']*)'|(-?\d+))\s*=\s*\[?%s\]?`, escaped)
			reReversed := regexp.MustCompile(patternReversed)
			if m := reReversed.FindStringSubmatch(whereClause); len(m) > 0 {
				val := firstNonEmpty(m[1], m[2])
				if val != "" {
					pks = append(pks, core.TablePK{Table: table, PK: val})
					continue
				}
			}

			// P3 §4.2: CAST/CONVERT unwrapping: col = CAST(value AS type)
			patternCast := fmt.Sprintf(`(?i)\b\[?%s\]?\s*=\s*CAST\s*\(\s*'?([^')]+)'?\s+AS\s+`, escaped)
			reCast := regexp.MustCompile(patternCast)
			if m := reCast.FindStringSubmatch(whereClause); len(m) > 1 {
				val := strings.TrimSpace(m[1])
				if val != "" {
					pks = append(pks, core.TablePK{Table: table, PK: val})
					continue
				}
			}

			// P3 §4.2: CONVERT unwrapping: col = CONVERT(type, value)
			patternConvert := fmt.Sprintf(`(?i)\b\[?%s\]?\s*=\s*CONVERT\s*\(\s*\w+\s*,\s*'?([^')]+)'?\s*\)`, escaped)
			reConvert := regexp.MustCompile(patternConvert)
			if m := reConvert.FindStringSubmatch(whereClause); len(m) > 1 {
				val := strings.TrimSpace(m[1])
				if val != "" {
					pks = append(pks, core.TablePK{Table: table, PK: val})
				}
			}
		}
	}

	return pks
}

// ---------------------------------------------------------------------------
// Regex patterns for T-SQL parsing
// ---------------------------------------------------------------------------

var (
	reJoin         = regexp.MustCompile(`(?i)\b(INNER|LEFT|RIGHT|CROSS|FULL|OUTER)\s+JOIN\b`)
	reTop          = regexp.MustCompile(`(?i)\bTOP\s*\(?(\d+)\)?`)
	reFetch        = regexp.MustCompile(`(?i)\bFETCH\s+(?:NEXT|FIRST)\s+(\d+)\s+ROWS?\s+ONLY`)
	reAggregate    = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|STRING_AGG|CHECKSUM_AGG|STDEV|STDEVP|VAR|VARP|COUNT_BIG)\s*\(`)
	reSetOp        = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|EXCEPT)\b`)
	reSubqueryFrom = regexp.MustCompile(`(?i)\bFROM\s*\(`)
	reWindowFunc   = regexp.MustCompile(`(?i)\b(ROW_NUMBER|RANK|DENSE_RANK|NTILE|LAG|LEAD|FIRST_VALUE|LAST_VALUE)\s*\(`)
	rePivot        = regexp.MustCompile(`(?i)\b(PIVOT|UNPIVOT)\b`)
	reApply        = regexp.MustCompile(`(?i)\b(CROSS|OUTER)\s+APPLY\b`)

	// P3 §4.1: Feature flag patterns.
	reDistinct      = regexp.MustCompile(`(?i)\bSELECT\s+DISTINCT\b`)
	reComplexAgg    = regexp.MustCompile(`(?i)\b(STRING_AGG|GROUPING|GROUPING_ID)\s*\(`)
	reMetadataQuery = regexp.MustCompile(`(?i)\b(SYS\.|INFORMATION_SCHEMA\.)`)
	reOutput        = regexp.MustCompile(`(?i)\bOUTPUT\s+(INSERTED|DELETED)\.`)

	reInsertTable  = regexp.MustCompile(`(?i)INSERT\s+INTO\s+` + tablePattern)
	reMergeTable   = regexp.MustCompile(`(?i)MERGE\s+(?:INTO\s+)?` + tablePattern)
	reMergeUsing   = regexp.MustCompile(`(?i)USING\s+` + tablePattern)
	reCreateTable  = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tablePattern)
	reCreateIndex  = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\s+\S+\s+ON\s+` + tablePattern)
	reAlterTable   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + tablePattern)
	reDropTable    = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + tablePattern)

	reFromClause = regexp.MustCompile(`(?i)\bFROM\s+(` + tableListPattern + `)`)
	reJoinTable  = regexp.MustCompile(`(?i)\bJOIN\s+` + tablePattern)
)

const (
	// tablePattern matches a single table name (with optional bracket quoting and schema prefix).
	tablePattern = `(\[?[a-zA-Z_][a-zA-Z0-9_]*\]?(?:\.\[?[a-zA-Z_][a-zA-Z0-9_]*\]?)?)`
	// tableListPattern matches a comma-separated list of table names.
	tableListPattern = `(?:` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*(?:,\s*` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*)*)`
)

// ---------------------------------------------------------------------------
// Table extraction helpers
// ---------------------------------------------------------------------------

func extractFromTables(upper string) []string {
	var tables []string

	// Extract FROM clause tables.
	if m := reFromClause.FindStringSubmatch(upper); len(m) > 1 {
		fromPart := m[1]
		// Stop at keywords that end the FROM clause.
		// Use word-boundary check to avoid matching keywords as prefixes of
		// table names (e.g. "ORDER" inside "ORDERS").
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "UNION", "INTERSECT", "EXCEPT", "FOR", "OPTION"} {
			if idx := indexKeyword(fromPart, kw); idx >= 0 {
				fromPart = fromPart[:idx]
			}
		}
		// Split by comma and extract table names.
		for part := range strings.SplitSeq(fromPart, ",") {
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

	// UPDATE table SET ...
	setIdx := strings.Index(upper, " SET ")
	if setIdx < 0 {
		return nil
	}
	tablePart := upper[len("UPDATE"):setIdx]

	// Handle JOIN syntax.
	joinIdx := strings.Index(tablePart, " JOIN ")
	if joinIdx >= 0 {
		before := strings.TrimSpace(tablePart[:joinIdx])
		tokens := strings.Fields(before)
		if len(tokens) > 0 {
			tables = appendUnique(tables, cleanTableName(tokens[0]))
		}
		for _, m := range reJoinTable.FindAllStringSubmatch(upper, -1) {
			if len(m) > 1 {
				tables = appendUnique(tables, cleanTableName(m[1]))
			}
		}
		return tables
	}

	// Simple UPDATE table SET ...
	tablePart = strings.TrimSpace(tablePart)
	tokens := strings.Fields(tablePart)
	if len(tokens) > 0 {
		tables = appendUnique(tables, cleanTableName(tokens[0]))
	}
	return tables
}

func extractDeleteTables(upper string) []string {
	var tables []string

	_, rest, found := strings.Cut(upper, "FROM")
	if !found {
		return nil
	}

	// Cut at WHERE, ORDER, OPTION.
	for _, kw := range []string{"WHERE", "ORDER", "OPTION"} {
		if idx := indexKeyword(rest, kw); idx >= 0 {
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
	upper := strings.ToUpper(rest)
	// Stop at OFFSET, FETCH, OPTION, or end of string.
	for _, kw := range []string{"OFFSET", "OPTION"} {
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
	name = strings.Trim(name, "[]`\"")
	// Remove schema prefix if present.
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.Trim(name, "[]`\"")
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

// indexKeyword finds the keyword kw in s only at a word boundary.
// It returns -1 if the keyword is not found as a standalone word.
func indexKeyword(s, kw string) int {
	upper := strings.ToUpper(s)
	kw = strings.ToUpper(kw)
	off := 0
	for {
		idx := strings.Index(upper[off:], kw)
		if idx < 0 {
			return -1
		}
		absIdx := off + idx
		end := absIdx + len(kw)
		// Check that the character after the keyword is not a letter/digit/underscore
		// (i.e. it's a word boundary).
		if end < len(upper) {
			ch := upper[end]
			if (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
				off = absIdx + 1
				continue
			}
		}
		// Also check that the character before is a word boundary.
		if absIdx > 0 {
			ch := upper[absIdx-1]
			if (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
				off = absIdx + 1
				continue
			}
		}
		return absIdx
	}
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
