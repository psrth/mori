package classify

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// Compile-time interface check.
var _ core.Classifier = (*PgClassifier)(nil)

// PgClassifier implements core.Classifier using pg_query_go's PostgreSQL parser.
type PgClassifier struct {
	tables map[string]schema.TableMeta
}

// New creates a PgClassifier. The tables map provides PK column names
// per table, used for extracting primary key values from WHERE clauses.
// If tables is nil, PK extraction is skipped.
func New(tables map[string]schema.TableMeta) *PgClassifier {
	if tables == nil {
		tables = make(map[string]schema.TableMeta)
	}
	return &PgClassifier{tables: tables}
}

// Classify parses a SQL string and returns its classification.
func (c *PgClassifier) Classify(query string) (*core.Classification, error) {
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return &core.Classification{
			OpType:  core.OpOther,
			SubType: core.SubOther,
			RawSQL:  query,
		}, nil
	}

	node := stmts[0].GetStmt()
	if node == nil {
		return &core.Classification{
			OpType:  core.OpOther,
			SubType: core.SubOther,
			RawSQL:  query,
		}, nil
	}

	cl := &core.Classification{RawSQL: query}
	c.classifyNode(node, cl)
	return cl, nil
}

// ClassifyWithParams classifies a parameterized query with bound values.
// PK placeholders like $1 are resolved from the params slice.
func (c *PgClassifier) ClassifyWithParams(query string, params []interface{}) (*core.Classification, error) {
	cl, err := c.Classify(query)
	if err != nil {
		return nil, err
	}

	for i, pk := range cl.PKs {
		if strings.HasPrefix(pk.PK, "$") {
			var paramIdx int
			if _, err := fmt.Sscanf(pk.PK, "$%d", &paramIdx); err == nil {
				if paramIdx >= 1 && paramIdx <= len(params) {
					cl.PKs[i].PK = fmt.Sprintf("%v", params[paramIdx-1])
				}
			}
		}
	}

	return cl, nil
}

// classifyNode dispatches to the appropriate statement classifier.
func (c *PgClassifier) classifyNode(node *pg_query.Node, cl *core.Classification) {
	switch {
	case node.GetSelectStmt() != nil:
		c.classifySelect(node.GetSelectStmt(), cl)
	case node.GetInsertStmt() != nil:
		c.classifyInsert(node.GetInsertStmt(), cl)
	case node.GetUpdateStmt() != nil:
		c.classifyUpdate(node.GetUpdateStmt(), cl)
	case node.GetDeleteStmt() != nil:
		c.classifyDelete(node.GetDeleteStmt(), cl)
	case node.GetCreateStmt() != nil:
		cl.OpType = core.OpDDL
		cl.SubType = core.SubCreate
		if rel := node.GetCreateStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	case node.GetAlterTableStmt() != nil:
		cl.OpType = core.OpDDL
		cl.SubType = core.SubAlter
		if rel := node.GetAlterTableStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	case node.GetDropStmt() != nil:
		c.classifyDrop(node.GetDropStmt(), cl)
	case node.GetIndexStmt() != nil:
		cl.OpType = core.OpDDL
		cl.SubType = core.SubCreate
		if rel := node.GetIndexStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	case node.GetRenameStmt() != nil:
		cl.OpType = core.OpDDL
		cl.SubType = core.SubAlter
		if rel := node.GetRenameStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	case node.GetTransactionStmt() != nil:
		c.classifyTransaction(node.GetTransactionStmt(), cl)
	case node.GetVariableSetStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubSet
	case node.GetVariableShowStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubShow
	case node.GetExplainStmt() != nil:
		c.classifyExplain(node.GetExplainStmt(), cl)
	case node.GetTruncateStmt() != nil:
		cl.OpType = core.OpWrite
		cl.SubType = core.SubTruncate
		for _, rel := range node.GetTruncateStmt().GetRelations() {
			if rv := rel.GetRangeVar(); rv != nil {
				cl.Tables = appendUnique(cl.Tables, relName(rv))
			}
		}
	case node.GetPrepareStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubPrepare
	case node.GetExecuteStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubExecute
	case node.GetDeallocateStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubDeallocate
	case node.GetDeclareCursorStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubCursor
		cl.HasCursor = true
		// Extract tables from the cursor's query.
		if query := node.GetDeclareCursorStmt().GetQuery(); query != nil {
			if sel := query.GetSelectStmt(); sel != nil {
				tables := extractTablesFromNodes(sel.GetFromClause())
				cl.Tables = appendUnique(cl.Tables, tables...)
			}
		}
	case node.GetFetchStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubCursor
		cl.HasCursor = true
	case node.GetClosePortalStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubCursor
		cl.HasCursor = true
	case node.GetListenStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubListen
	case node.GetUnlistenStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubListen
	case node.GetNotifyStmt() != nil:
		cl.OpType = core.OpWrite
		cl.SubType = core.SubNotify
		cl.NotSupportedMsg = "NOTIFY is not supported through Mori — it would affect production subscribers"
	case node.GetCopyStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "COPY is not supported through Mori — use INSERT/SELECT for data transfer"
	case node.GetLockStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "LOCK TABLE is not supported through Mori — explicit locking is unsafe across split backends"
	case node.GetDoStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "DO $$ anonymous blocks are not supported through Mori — queries inside PL/pgSQL are opaque to the proxy"
	case node.GetCallStmt() != nil:
		cl.OpType = core.OpOther
		cl.SubType = core.SubNotSupported
		cl.NotSupportedMsg = "CALL (stored procedures) is not supported through Mori — procedure bodies are opaque to the proxy"
	default:
		cl.OpType = core.OpOther
		cl.SubType = core.SubOther
	}
}

// classifySelect handles SELECT statements including UNION/INTERSECT/EXCEPT and CTEs.
func (c *PgClassifier) classifySelect(sel *pg_query.SelectStmt, cl *core.Classification) {
	// Handle UNION/INTERSECT/EXCEPT (set operations).
	// Each branch accumulates tables but IsJoin is only set per-branch
	// (a UNION of two single-table SELECTs is not a JOIN).
	if sel.GetOp() != pg_query.SetOperation_SETOP_NONE {
		cl.OpType = core.OpRead
		cl.SubType = core.SubSelect
		cl.HasSetOp = true
		if sel.GetLarg() != nil {
			c.classifySelect(sel.GetLarg(), cl)
		}
		if sel.GetRarg() != nil {
			c.classifySelect(sel.GetRarg(), cl)
		}
		if sel.GetLimitCount() != nil {
			cl.HasLimit = true
			cl.Limit = extractIntValue(sel.GetLimitCount())
		}
		if len(sel.GetSortClause()) > 0 {
			cl.OrderBy = extractOrderByFromRaw(cl.RawSQL)
		}
		return
	}

	// Default classification for SELECT.
	cl.OpType = core.OpRead
	cl.SubType = core.SubSelect

	// Collect CTE names so we can exclude them from the real table list.
	var cteNames map[string]bool
	if wc := sel.GetWithClause(); wc != nil {
		cl.IsComplexRead = true
		cteNames = make(map[string]bool)
		for _, cteNode := range wc.GetCtes() {
			cte := cteNode.GetCommonTableExpr()
			if cte == nil {
				continue
			}
			cteNames[cte.GetCtename()] = true
			cteQuery := cte.GetCtequery()
			if isMutatingNode(cteQuery) {
				cl.OpType = core.OpWrite
				cl.SubType = core.SubOther
			}
			c.extractTablesFromCTE(cteQuery, cl)
		}
	}

	// Detect derived tables (subqueries in FROM clause).
	if hasRangeSubselect(sel.GetFromClause()) {
		cl.IsComplexRead = true
	}

	// Extract tables from FROM clause, filtering out CTE names.
	tables := extractTablesFromNodes(sel.GetFromClause())
	for _, t := range tables {
		if cteNames != nil && cteNames[t] {
			continue
		}
		cl.Tables = appendUnique(cl.Tables, t)
	}

	// Detect JOINs based on this SELECT's own FROM clause.
	fromTables := extractTablesFromNodes(sel.GetFromClause())
	realFromCount := 0
	for _, t := range fromTables {
		if cteNames == nil || !cteNames[t] {
			realFromCount++
		}
	}
	cl.IsJoin = cl.IsJoin || realFromCount > 1 || hasJoinExpr(sel.GetFromClause())

	// Extract LIMIT.
	if sel.GetLimitCount() != nil {
		cl.HasLimit = true
		cl.Limit = extractIntValue(sel.GetLimitCount())
	}

	// Extract ORDER BY from raw SQL.
	if len(sel.GetSortClause()) > 0 {
		cl.OrderBy = extractOrderByFromRaw(cl.RawSQL)
	}

	// Extract PKs from WHERE clause.
	if sel.GetWhereClause() != nil {
		pks := c.extractPKsFromExpr(sel.GetWhereClause(), cl.Tables)
		cl.PKs = append(cl.PKs, pks...)
	}

	// Detect DISTINCT.
	if len(sel.GetDistinctClause()) > 0 {
		cl.HasDistinct = true
	}

	// Detect aggregates: GROUP BY or aggregate functions in target list.
	if len(sel.GetGroupClause()) > 0 {
		cl.HasAggregate = true
	}
	for _, target := range sel.GetTargetList() {
		if !cl.HasAggregate && hasAggregateFunc(target) {
			cl.HasAggregate = true
		}
		if hasWindowFunc(target) {
			cl.HasWindowFunc = true
			cl.IsComplexRead = true
		}
		if hasComplexAggFunc(target) {
			cl.HasComplexAgg = true
		}
	}
}

// classifyExplain handles EXPLAIN statements.
// EXPLAIN ANALYZE is not supported (it executes the query).
// Regular EXPLAIN extracts tables from the inner query.
func (c *PgClassifier) classifyExplain(stmt *pg_query.ExplainStmt, cl *core.Classification) {
	// Check for ANALYZE option.
	for _, opt := range stmt.GetOptions() {
		if de := opt.GetDefElem(); de != nil {
			if strings.ToLower(de.GetDefname()) == "analyze" {
				cl.OpType = core.OpOther
				cl.SubType = core.SubNotSupported
				cl.NotSupportedMsg = "EXPLAIN ANALYZE is not supported through Mori — it executes the query, which is unsafe on modified state"
				return
			}
		}
	}

	cl.OpType = core.OpOther
	cl.SubType = core.SubExplain

	// Extract tables from the inner query for dirty-table detection.
	if query := stmt.GetQuery(); query != nil {
		if sel := query.GetSelectStmt(); sel != nil {
			tables := extractTablesFromNodes(sel.GetFromClause())
			cl.Tables = appendUnique(cl.Tables, tables...)
		} else if ins := query.GetInsertStmt(); ins != nil {
			if rel := ins.GetRelation(); rel != nil {
				cl.Tables = appendUnique(cl.Tables, relName(rel))
			}
		} else if upd := query.GetUpdateStmt(); upd != nil {
			if rel := upd.GetRelation(); rel != nil {
				cl.Tables = appendUnique(cl.Tables, relName(rel))
			}
		} else if del := query.GetDeleteStmt(); del != nil {
			if rel := del.GetRelation(); rel != nil {
				cl.Tables = appendUnique(cl.Tables, relName(rel))
			}
		}
	}
}

// hasWindowFunc recursively checks if a node contains a window function call
// (i.e., a FuncCall with an OVER clause).
func hasWindowFunc(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if rt := node.GetResTarget(); rt != nil {
		return hasWindowFunc(rt.GetVal())
	}
	if fc := node.GetFuncCall(); fc != nil {
		if fc.GetOver() != nil {
			return true
		}
		for _, arg := range fc.GetArgs() {
			if hasWindowFunc(arg) {
				return true
			}
		}
	}
	if ae := node.GetAExpr(); ae != nil {
		return hasWindowFunc(ae.GetLexpr()) || hasWindowFunc(ae.GetRexpr())
	}
	if tc := node.GetTypeCast(); tc != nil {
		return hasWindowFunc(tc.GetArg())
	}
	return false
}

// hasComplexAggFunc recursively checks if a node contains a complex aggregate
// function that can't be re-aggregated in Go (array_agg, json_agg, string_agg, etc.).
func hasComplexAggFunc(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if rt := node.GetResTarget(); rt != nil {
		return hasComplexAggFunc(rt.GetVal())
	}
	if fc := node.GetFuncCall(); fc != nil {
		if fc.GetOver() != nil {
			return false // Window functions handled separately.
		}
		for _, nameNode := range fc.GetFuncname() {
			if s := nameNode.GetString_(); s != nil {
				name := strings.ToLower(s.GetSval())
				switch name {
				case "array_agg", "string_agg", "json_agg", "jsonb_agg",
					"json_object_agg", "jsonb_object_agg":
					return true
				}
			}
		}
		for _, arg := range fc.GetArgs() {
			if hasComplexAggFunc(arg) {
				return true
			}
		}
	}
	if ae := node.GetAExpr(); ae != nil {
		return hasComplexAggFunc(ae.GetLexpr()) || hasComplexAggFunc(ae.GetRexpr())
	}
	if tc := node.GetTypeCast(); tc != nil {
		return hasComplexAggFunc(tc.GetArg())
	}
	return false
}

// hasAggregateFunc recursively checks if a node contains an aggregate function call.
func hasAggregateFunc(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if rt := node.GetResTarget(); rt != nil {
		return hasAggregateFunc(rt.GetVal())
	}
	if fc := node.GetFuncCall(); fc != nil {
		// Window functions (SUM(...) OVER (...)) are NOT aggregates — they
		// don't reduce row count. Skip if an OVER clause is present.
		if fc.GetOver() != nil {
			return false
		}
		// Check if the function name is a known aggregate.
		for _, nameNode := range fc.GetFuncname() {
			if s := nameNode.GetString_(); s != nil {
				name := strings.ToLower(s.GetSval())
				switch name {
				case "count", "sum", "avg", "min", "max",
					"array_agg", "string_agg", "bool_and", "bool_or",
					"json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg":
					return true
				}
			}
		}
		// Also check arguments for nested aggregates.
		for _, arg := range fc.GetArgs() {
			if hasAggregateFunc(arg) {
				return true
			}
		}
	}
	// Check common expression wrappers.
	if ae := node.GetAExpr(); ae != nil {
		return hasAggregateFunc(ae.GetLexpr()) || hasAggregateFunc(ae.GetRexpr())
	}
	if tc := node.GetTypeCast(); tc != nil {
		return hasAggregateFunc(tc.GetArg())
	}
	return false
}

// extractTablesFromCTE extracts tables referenced in a CTE subquery.
func (c *PgClassifier) extractTablesFromCTE(n *pg_query.Node, cl *core.Classification) {
	if n == nil {
		return
	}
	switch {
	case n.GetSelectStmt() != nil:
		tables := extractTablesFromNodes(n.GetSelectStmt().GetFromClause())
		cl.Tables = appendUnique(cl.Tables, tables...)
	case n.GetInsertStmt() != nil:
		if rel := n.GetInsertStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	case n.GetUpdateStmt() != nil:
		if rel := n.GetUpdateStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	case n.GetDeleteStmt() != nil:
		if rel := n.GetDeleteStmt().GetRelation(); rel != nil {
			cl.Tables = appendUnique(cl.Tables, relName(rel))
		}
	}
}

// classifyInsert handles INSERT statements.
func (c *PgClassifier) classifyInsert(ins *pg_query.InsertStmt, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubInsert

	if rel := ins.GetRelation(); rel != nil {
		cl.Tables = appendUnique(cl.Tables, relName(rel))
	}

	// Detect ON CONFLICT (upsert).
	if ins.GetOnConflictClause() != nil {
		cl.HasOnConflict = true
	}

	// Detect RETURNING.
	if len(ins.GetReturningList()) > 0 {
		cl.HasReturning = true
	}

	// INSERT...SELECT: extract tables from the SELECT portion.
	if ins.GetSelectStmt() != nil {
		if subSel := ins.GetSelectStmt().GetSelectStmt(); subSel != nil {
			subTables := extractTablesFromNodes(subSel.GetFromClause())
			cl.Tables = appendUnique(cl.Tables, subTables...)
		}
	}
}

// classifyUpdate handles UPDATE statements.
func (c *PgClassifier) classifyUpdate(upd *pg_query.UpdateStmt, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubUpdate

	if rel := upd.GetRelation(); rel != nil {
		cl.Tables = appendUnique(cl.Tables, relName(rel))
	}

	// UPDATE ... FROM (PostgreSQL extension).
	tables := extractTablesFromNodes(upd.GetFromClause())
	cl.Tables = appendUnique(cl.Tables, tables...)

	cl.IsJoin = len(cl.Tables) > 1

	// Detect RETURNING.
	if len(upd.GetReturningList()) > 0 {
		cl.HasReturning = true
	}

	if upd.GetWhereClause() != nil {
		pks := c.extractPKsFromExpr(upd.GetWhereClause(), cl.Tables)
		cl.PKs = append(cl.PKs, pks...)
	}
}

// classifyDelete handles DELETE statements.
func (c *PgClassifier) classifyDelete(del *pg_query.DeleteStmt, cl *core.Classification) {
	cl.OpType = core.OpWrite
	cl.SubType = core.SubDelete

	if rel := del.GetRelation(); rel != nil {
		cl.Tables = appendUnique(cl.Tables, relName(rel))
	}

	// DELETE ... USING (PostgreSQL extension).
	tables := extractTablesFromNodes(del.GetUsingClause())
	cl.Tables = appendUnique(cl.Tables, tables...)

	cl.IsJoin = len(cl.Tables) > 1

	// Detect RETURNING.
	if len(del.GetReturningList()) > 0 {
		cl.HasReturning = true
	}

	if del.GetWhereClause() != nil {
		pks := c.extractPKsFromExpr(del.GetWhereClause(), cl.Tables)
		cl.PKs = append(cl.PKs, pks...)
	}
}

// classifyDrop handles DROP statements.
func (c *PgClassifier) classifyDrop(stmt *pg_query.DropStmt, cl *core.Classification) {
	cl.OpType = core.OpDDL
	cl.SubType = core.SubDrop
	for _, obj := range stmt.GetObjects() {
		if list := obj.GetList(); list != nil {
			var parts []string
			for _, item := range list.GetItems() {
				if s := item.GetString_(); s != nil {
					parts = append(parts, s.GetSval())
				}
			}
			if len(parts) > 0 {
				cl.Tables = appendUnique(cl.Tables, strings.Join(parts, "."))
			}
		}
	}
}

// classifyTransaction handles transaction control statements.
func (c *PgClassifier) classifyTransaction(stmt *pg_query.TransactionStmt, cl *core.Classification) {
	cl.OpType = core.OpTransaction
	switch stmt.GetKind() {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN,
		pg_query.TransactionStmtKind_TRANS_STMT_START:
		cl.SubType = core.SubBegin
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
		cl.SubType = core.SubCommit
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		cl.SubType = core.SubRollback
	case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
		cl.SubType = core.SubSavepoint
	case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
		cl.SubType = core.SubRelease
	default:
		cl.SubType = core.SubOther
	}
}
