package classify

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*MySQLClassifier)(nil)

// MySQLClassifier implements core.Classifier using vitess sqlparser for MySQL.
type MySQLClassifier struct {
	tables map[string]schema.TableMeta
	parser *sqlparser.Parser
}

// New creates a MySQLClassifier. The tables map provides PK column names
// per table, used for extracting primary key values from WHERE clauses.
func New(tables map[string]schema.TableMeta) *MySQLClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	parser, err := sqlparser.New(sqlparser.Options{
		MySQLServerVersion: "8.0.30",
	})
	if err != nil {
		// Fallback: create with defaults.
		parser, _ = sqlparser.New(sqlparser.Options{})
	}
	return &MySQLClassifier{tables: tables, parser: parser}
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

	// Try AST-based classification first.
	stmt, err := c.parser.Parse(trimmed)
	if err != nil {
		// Fallback to regex-based classification for unparseable SQL.
		return c.classifyRegex(trimmed, cl)
	}

	c.classifyStmt(stmt, cl)
	return cl, nil
}

// ClassifyWithParams classifies a parameterized query with bound values.
// For MySQL, placeholders are ? and are positional.
func (c *MySQLClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
	cl, err := c.Classify(query)
	if err != nil {
		return nil, err
	}

	paramIdx := 0
	for i, pk := range cl.PKs {
		if pk.PK == "?" {
			if paramIdx < len(params) {
				cl.PKs[i].PK = fmt.Sprintf("%v", params[paramIdx])
				paramIdx++
			}
		}
	}

	return cl, nil
}

// classifyStmt dispatches to the appropriate AST-based classifier.
func (c *MySQLClassifier) classifyStmt(stmt sqlparser.Statement, cl *core.Classification) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		c.classifySelect(s, cl)
	case *sqlparser.Union:
		c.classifyUnion(s, cl)
	case *sqlparser.Insert:
		c.classifyInsert(s, cl)
	case *sqlparser.Update:
		c.classifyUpdate(s, cl)
	case *sqlparser.Delete:
		c.classifyDelete(s, cl)
	case *sqlparser.CreateTable:
		cl.OpType = core.OpDDL
		cl.SubType = core.SubCreate
		if s.Table.Name.String() != "" {
			cl.Tables = appendUnique(cl.Tables, normalizeTableName(s.Table.Name.String()))
		}
	case *sqlparser.AlterTable:
		cl.OpType = core.OpDDL
		// Vitess parses CREATE INDEX as AlterTable with AddIndexDefinition.
		// Detect this case and classify as SubCreate for consistency.
		isCreateIndex := false
		for _, opt := range s.AlterOptions {
			if _, ok := opt.(*sqlparser.AddIndexDefinition); ok {
				isCreateIndex = true
				break
			}
		}
		if isCreateIndex && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(cl.RawSQL)), "CREATE") {
			cl.SubType = core.SubCreate
		} else {
			cl.SubType = core.SubAlter
		}
		if s.Table.Name.String() != "" {
			cl.Tables = appendUnique(cl.Tables, normalizeTableName(s.Table.Name.String()))
		}
	case *sqlparser.DropTable:
		cl.OpType = core.OpDDL
		cl.SubType = core.SubDrop
		for _, table := range s.FromTables {
			cl.Tables = appendUnique(cl.Tables, normalizeTableName(table.Name.String()))
		}
	case *sqlparser.TruncateTable:
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
		if s.Table.Name.String() != "" {
			cl.Tables = appendUnique(cl.Tables, normalizeTableName(s.Table.Name.String()))
		}
	case *sqlparser.Begin:
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubBegin
	case *sqlparser.Commit:
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubCommit
	case *sqlparser.Rollback:
		cl.OpType = core.OpTransaction
		cl.SubType = core.SubRollback
	case *sqlparser.Set:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case *sqlparser.Show:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case *sqlparser.ExplainStmt:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case *sqlparser.ExplainTab:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	case *sqlparser.Use:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}
}

// classifySelect handles SELECT statements.
func (c *MySQLClassifier) classifySelect(sel *sqlparser.Select, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect

	// Extract tables from FROM clause.
	tables := extractTablesFromTableExprs(sel.From)
	cl.Tables = appendUnique(cl.Tables, tables...)

	// Detect JOINs.
	cl.IsJoin = len(cl.Tables) > 1 || hasJoinInFrom(sel.From)

	// Handle CTEs.
	if sel.With != nil && len(sel.With.CTEs) > 0 {
		cl.IsComplexRead = true
		for _, cte := range sel.With.CTEs {
			if cte.Subquery != nil {
				cteTables := extractTablesFromTableStmt(cte.Subquery)
				cl.Tables = appendUnique(cl.Tables, cteTables...)
			}
		}
	}

	// Detect subqueries in FROM.
	if hasSubqueryInFrom(sel.From) {
		cl.IsComplexRead = true
	}

	// Extract LIMIT.
	if sel.Limit != nil && sel.Limit.Rowcount != nil {
		cl.HasLimit = true
		cl.Limit = extractIntFromExpr(sel.Limit.Rowcount)
	}

	// Detect ORDER BY.
	if len(sel.OrderBy) > 0 {
		cl.OrderBy = extractOrderByFromRaw(cl.RawSQL)
	}

	// Detect aggregates.
	if sel.GroupBy != nil && len(sel.GroupBy.Exprs) > 0 {
		cl.HasAggregate = true
	}
	if !cl.HasAggregate && sel.SelectExprs != nil {
		for _, expr := range sel.SelectExprs.Exprs {
			if hasAggregateExpr(expr) {
				cl.HasAggregate = true
				break
			}
		}
	}

	// Extract PKs from WHERE.
	if sel.Where != nil {
		cl.PKs = c.extractPKsFromExpr(sel.Where.Expr, cl.Tables)
	}
}

// classifyUnion handles UNION/INTERSECT/EXCEPT.
func (c *MySQLClassifier) classifyUnion(u *sqlparser.Union, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect
	cl.HasSetOp = true

	// Extract tables from both sides.
	leftTables := extractTablesFromTableStmt(u.Left)
	rightTables := extractTablesFromTableStmt(u.Right)
	cl.Tables = appendUnique(cl.Tables, leftTables...)
	cl.Tables = appendUnique(cl.Tables, rightTables...)

	// Extract LIMIT.
	if u.Limit != nil && u.Limit.Rowcount != nil {
		cl.HasLimit = true
		cl.Limit = extractIntFromExpr(u.Limit.Rowcount)
	}

	// Extract ORDER BY.
	if len(u.OrderBy) > 0 {
		cl.OrderBy = extractOrderByFromRaw(cl.RawSQL)
	}
}

// classifyInsert handles INSERT/REPLACE statements.
func (c *MySQLClassifier) classifyInsert(ins *sqlparser.Insert, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert

	if ins.Table != nil {
		tableName := extractTableNameFromAliased(ins.Table)
		if tableName != "" {
			cl.Tables = appendUnique(cl.Tables, normalizeTableName(tableName))
		}
	}

	// INSERT ... SELECT: extract tables from the SELECT.
	if ins.Rows != nil {
		selectTables := extractTablesFromInsertRows(ins.Rows)
		cl.Tables = appendUnique(cl.Tables, selectTables...)
	}
}

// classifyUpdate handles UPDATE statements.
func (c *MySQLClassifier) classifyUpdate(upd *sqlparser.Update, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	tables := extractTablesFromTableExprs(upd.TableExprs)
	cl.Tables = appendUnique(cl.Tables, tables...)
	cl.IsJoin = len(cl.Tables) > 1

	if upd.Where != nil {
		cl.PKs = c.extractPKsFromExpr(upd.Where.Expr, cl.Tables)
	}
}

// classifyDelete handles DELETE statements.
func (c *MySQLClassifier) classifyDelete(del *sqlparser.Delete, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete

	tables := extractTablesFromTableExprs(del.TableExprs)
	cl.Tables = appendUnique(cl.Tables, tables...)
	cl.IsJoin = len(cl.Tables) > 1

	if del.Where != nil {
		cl.PKs = c.extractPKsFromExpr(del.Where.Expr, cl.Tables)
	}
}

// extractTablesFromTableExprs extracts table names from table expressions.
func extractTablesFromTableExprs(exprs sqlparser.TableExprs) []string {
	var tables []string
	for _, expr := range exprs {
		tables = append(tables, extractTablesFromTableExpr(expr)...)
	}
	return tables
}

func extractTablesFromTableExpr(expr sqlparser.TableExpr) []string {
	var tables []string
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tname, ok := t.Expr.(sqlparser.TableName); ok {
			name := tname.Name.String()
			if name != "" {
				tables = append(tables, normalizeTableName(name))
			}
		}
	case *sqlparser.JoinTableExpr:
		tables = append(tables, extractTablesFromTableExpr(t.LeftExpr)...)
		tables = append(tables, extractTablesFromTableExpr(t.RightExpr)...)
	case *sqlparser.ParenTableExpr:
		tables = append(tables, extractTablesFromTableExprs(t.Exprs)...)
	}
	return tables
}

// extractTablesFromTableStmt extracts table names from a TableStatement.
func extractTablesFromTableStmt(stmt sqlparser.TableStatement) []string {
	if stmt == nil {
		return nil
	}
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return extractTablesFromTableExprs(s.From)
	case *sqlparser.Union:
		var tables []string
		tables = append(tables, extractTablesFromTableStmt(s.Left)...)
		tables = append(tables, extractTablesFromTableStmt(s.Right)...)
		return tables
	default:
		return nil
	}
}

// extractTableNameFromAliased extracts the table name from an AliasedTableExpr.
func extractTableNameFromAliased(ate *sqlparser.AliasedTableExpr) string {
	if ate == nil {
		return ""
	}
	if tname, ok := ate.Expr.(sqlparser.TableName); ok {
		return tname.Name.String()
	}
	return ""
}

// extractTablesFromInsertRows extracts tables from INSERT ... SELECT rows.
func extractTablesFromInsertRows(rows sqlparser.InsertRows) []string {
	if rows == nil {
		return nil
	}
	switch r := rows.(type) {
	case *sqlparser.Select:
		return extractTablesFromTableExprs(r.From)
	case *sqlparser.Union:
		var tables []string
		tables = append(tables, extractTablesFromTableStmt(r.Left)...)
		tables = append(tables, extractTablesFromTableStmt(r.Right)...)
		return tables
	default:
		return nil
	}
}

// hasJoinInFrom checks if FROM clause contains explicit JOINs.
func hasJoinInFrom(exprs sqlparser.TableExprs) bool {
	for _, expr := range exprs {
		if _, ok := expr.(*sqlparser.JoinTableExpr); ok {
			return true
		}
	}
	return false
}

// hasSubqueryInFrom checks if FROM contains subqueries (derived tables).
func hasSubqueryInFrom(exprs sqlparser.TableExprs) bool {
	for _, expr := range exprs {
		if ate, ok := expr.(*sqlparser.AliasedTableExpr); ok {
			if _, ok := ate.Expr.(*sqlparser.DerivedTable); ok {
				return true
			}
		}
	}
	return false
}

// hasAggregateExpr checks if a select expression contains an aggregate function.
func hasAggregateExpr(expr sqlparser.SelectExpr) bool {
	ae, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return false
	}
	return containsAggregate(ae.Expr)
}

func containsAggregate(expr sqlparser.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		name := strings.ToLower(e.Name.String())
		switch name {
		case "count", "sum", "avg", "min", "max",
			"group_concat", "json_arrayagg", "json_objectagg",
			"std", "stddev", "stddev_pop", "stddev_samp",
			"var_pop", "var_samp", "variance",
			"bit_and", "bit_or", "bit_xor":
			return true
		}
	case *sqlparser.CountStar:
		return true
	}
	return false
}

// extractIntFromExpr extracts an integer from a sqlparser expression.
func extractIntFromExpr(expr sqlparser.Expr) int {
	if lit, ok := expr.(*sqlparser.Literal); ok {
		if n, err := strconv.Atoi(lit.Val); err == nil {
			return n
		}
	}
	return 0
}

// extractPKsFromExpr extracts primary key values from WHERE expressions.
func (c *MySQLClassifier) extractPKsFromExpr(expr sqlparser.Expr, tables []string) []core.TablePK {
	var pks []core.TablePK

	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != sqlparser.EqualOp {
			return nil
		}
		colName := extractColumnName(e.Left)
		if colName == "" {
			return nil
		}
		val := extractValue(e.Right)
		if val == "" {
			return nil
		}

		// Match column against known PK columns.
		for _, table := range tables {
			meta, ok := c.tables[table]
			if !ok || len(meta.PKColumns) == 0 {
				continue
			}
			for _, pkCol := range meta.PKColumns {
				if strings.EqualFold(colName, pkCol) {
					pks = append(pks, core.TablePK{Table: table, PK: val})
				}
			}
		}

	case *sqlparser.AndExpr:
		pks = append(pks, c.extractPKsFromExpr(e.Left, tables)...)
		pks = append(pks, c.extractPKsFromExpr(e.Right, tables)...)
	}

	return pks
}

// extractColumnName extracts the column name from an expression.
func extractColumnName(expr sqlparser.Expr) string {
	if col, ok := expr.(*sqlparser.ColName); ok {
		return col.Name.String()
	}
	return ""
}

// extractValue extracts a string value from an expression.
func extractValue(expr sqlparser.Expr) string {
	switch e := expr.(type) {
	case *sqlparser.Literal:
		return e.Val
	case *sqlparser.Argument:
		return "?"
	}
	return ""
}

// extractOrderByFromRaw extracts the ORDER BY clause from raw SQL.
func extractOrderByFromRaw(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.LastIndex(upper, "ORDER BY")
	if idx < 0 {
		return ""
	}
	rest := sql[idx+8:]
	// Stop at LIMIT or end of string.
	if lIdx := strings.Index(strings.ToUpper(rest), "LIMIT"); lIdx >= 0 {
		rest = rest[:lIdx]
	}
	return strings.TrimSpace(rest)
}

// normalizeTableName cleans up a table name for consistent lookup.
func normalizeTableName(name string) string {
	name = strings.Trim(name, "`\"")
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	name = strings.Trim(name, "`\"")
	return strings.ToLower(name)
}

// ---------------------------------------------------------------------------
// Regex fallback for unparseable SQL
// ---------------------------------------------------------------------------

// classifyRegex is the fallback regex-based classifier for SQL that vitess
// cannot parse (e.g., MySQL-specific syntax edge cases).
func (c *MySQLClassifier) classifyRegex(trimmed string, cl *core.Classification) (*core.Classification, error) {
	upper := strings.ToUpper(strings.TrimSpace(stripLeadingComments(trimmed)))

	switch {
	case hasPrefix(upper, "SELECT"):
		c.classifySelectRegex(trimmed, upper, cl)
	case hasPrefix(upper, "INSERT"):
		c.classifyInsertRegex(upper, cl)
	case hasPrefix(upper, "UPDATE"):
		c.classifyUpdateRegex(trimmed, upper, cl)
	case hasPrefix(upper, "DELETE"):
		c.classifyDeleteRegex(trimmed, upper, cl)
	case hasPrefix(upper, "REPLACE"):
		cl.OpType = core.OpWrite
		cl.SubType = core.SubInsert
		if m := reReplaceTable.FindStringSubmatch(upper); len(m) > 1 {
			cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
		}
	case hasPrefix(upper, "CREATE"):
		cl.OpType = core.OpDDL
		cl.SubType = core.SubCreate
	case hasPrefix(upper, "ALTER"):
		cl.OpType = core.OpDDL
		cl.SubType = core.SubAlter
	case hasPrefix(upper, "DROP"):
		cl.OpType = core.OpDDL
		cl.SubType = core.SubDrop
	case hasPrefix(upper, "TRUNCATE"):
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
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
		c.classifyCTERegex(trimmed, upper, cl)
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}

	return cl, nil
}

func (c *MySQLClassifier) classifySelectRegex(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect
	cl.Tables = extractFromTablesRegex(upper)
	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)
	if m := reLimit.FindStringSubmatch(upper); len(m) > 1 {
		cl.HasLimit = true
		if n, err := strconv.Atoi(m[1]); err == nil {
			cl.Limit = n
		}
	}
	if idx := strings.Index(upper, "ORDER BY"); idx >= 0 {
		cl.OrderBy = extractOrderByRegex(raw, idx)
	}
	cl.HasAggregate = reAggregate.MatchString(upper) || strings.Contains(upper, "GROUP BY")
	cl.HasSetOp = reSetOp.MatchString(upper)
	if reSubqueryFrom.MatchString(upper) {
		cl.IsComplexRead = true
	}
	cl.PKs = c.extractPKsRegex(raw, cl.Tables)
}

func (c *MySQLClassifier) classifyInsertRegex(upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert
	if m := reInsertTable.FindStringSubmatch(upper); len(m) > 1 {
		cl.Tables = appendUnique(cl.Tables, cleanTableName(m[1]))
	}
}

func (c *MySQLClassifier) classifyUpdateRegex(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate
	cl.Tables = extractUpdateTablesRegex(upper)
	cl.IsJoin = len(cl.Tables) > 1
	cl.PKs = c.extractPKsRegex(raw, cl.Tables)
}

func (c *MySQLClassifier) classifyDeleteRegex(raw, upper string, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete
	cl.Tables = extractDeleteTablesRegex(upper)
	cl.IsJoin = len(cl.Tables) > 1
	cl.PKs = c.extractPKsRegex(raw, cl.Tables)
}

func (c *MySQLClassifier) classifyCTERegex(raw, upper string, cl *core.Classification) {
	if strings.Contains(upper, "INSERT") ||
		strings.Contains(upper, "UPDATE") ||
		strings.Contains(upper, "DELETE") {
		cl.OpType = core.OpWrite
		cl.SubType = core.SubOther
	} else {
		cl.OpType = core.OpRead
		cl.SubType = core.SubSelect
	}
	cl.Tables = extractFromTablesRegex(upper)
	cl.IsJoin = len(cl.Tables) > 1 || reJoin.MatchString(upper)
	cl.HasAggregate = reAggregate.MatchString(upper) || strings.Contains(upper, "GROUP BY")
	cl.IsComplexRead = true
}

// extractPKsRegex extracts primary key values from WHERE clauses using regex.
func (c *MySQLClassifier) extractPKsRegex(raw string, tables []string) []core.TablePK {
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
// Regex patterns (used as fallback)
// ---------------------------------------------------------------------------

var (
	reJoin         = regexp.MustCompile(`(?i)\b(INNER|LEFT|RIGHT|CROSS|NATURAL|STRAIGHT_JOIN)\s+JOIN\b`)
	reLimit        = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	reAggregate    = regexp.MustCompile(`(?i)\b(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|JSON_ARRAYAGG|JSON_OBJECTAGG)\s*\(`)
	reSetOp        = regexp.MustCompile(`(?i)\b(UNION|INTERSECT|EXCEPT)\b`)
	reSubqueryFrom = regexp.MustCompile(`(?i)\bFROM\s*\(`)

	reInsertTable  = regexp.MustCompile(`(?i)INSERT\s+(?:IGNORE\s+)?INTO\s+` + tablePattern)
	reReplaceTable = regexp.MustCompile(`(?i)REPLACE\s+INTO\s+` + tablePattern)

	reFromClause = regexp.MustCompile(`(?i)\bFROM\s+(` + tableListPattern + `)`)
	reJoinTable  = regexp.MustCompile(`(?i)\bJOIN\s+` + tablePattern)
)

const (
	tablePattern     = "(`?[a-zA-Z_][a-zA-Z0-9_]*`?(?:\\.`?[a-zA-Z_][a-zA-Z0-9_]*`?)?)"
	tableListPattern = `(?:` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*(?:,\s*` + tablePattern + `(?:\s+(?:AS\s+)?[a-zA-Z_]\w*)?\s*)*)`
)

func extractFromTablesRegex(upper string) []string {
	var tables []string
	if m := reFromClause.FindStringSubmatch(upper); len(m) > 1 {
		fromPart := m[1]
		for _, kw := range []string{"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "FOR"} {
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

func extractUpdateTablesRegex(upper string) []string {
	var tables []string
	setIdx := strings.Index(upper, " SET ")
	if setIdx < 0 {
		return nil
	}
	tablePart := upper[len("UPDATE"):setIdx]
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

func extractDeleteTablesRegex(upper string) []string {
	var tables []string
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return nil
	}
	rest := upper[fromIdx+4:]
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
	for _, m := range reJoinTable.FindAllStringSubmatch(upper, -1) {
		if len(m) > 1 {
			tables = appendUnique(tables, cleanTableName(m[1]))
		}
	}
	return tables
}

func extractOrderByRegex(raw string, idx int) string {
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
	name = strings.Trim(name, "`\"")
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
	if len(upper) > len(prefix) {
		next := upper[len(prefix)]
		if next >= 'A' && next <= 'Z' {
			return false
		}
	}
	return true
}
