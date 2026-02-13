package proxy

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// evaluateWhere evaluates a WHERE clause AST node against a row.
// Returns true if the row satisfies the condition, or true if evaluation
// fails for any reason (safe fallback: keep the row).
func evaluateWhere(node *pg_query.Node, row map[string]string, nulls map[string]bool) bool {
	eval := &whereEval{row: row, nulls: nulls}
	result, ok := eval.evalBool(node)
	if !ok {
		return true
	}
	return result
}

type whereEval struct {
	row   map[string]string
	nulls map[string]bool
}

// evalBool evaluates a node as a boolean expression.
// Returns (result, ok). If ok is false, the expression couldn't be evaluated.
func (e *whereEval) evalBool(node *pg_query.Node) (bool, bool) {
	if node == nil {
		return true, false
	}
	if be := node.GetBoolExpr(); be != nil {
		return e.evalBoolExpr(be)
	}
	if ae := node.GetAExpr(); ae != nil {
		return e.evalAExpr(ae)
	}
	if nt := node.GetNullTest(); nt != nil {
		return e.evalNullTest(nt)
	}
	return true, false // Can't evaluate — keep row
}

func (e *whereEval) evalBoolExpr(be *pg_query.BoolExpr) (bool, bool) {
	switch be.GetBoolop() {
	case pg_query.BoolExprType_AND_EXPR:
		for _, arg := range be.GetArgs() {
			val, ok := e.evalBool(arg)
			if !ok {
				return true, false
			}
			if !val {
				return false, true
			}
		}
		return true, true

	case pg_query.BoolExprType_OR_EXPR:
		for _, arg := range be.GetArgs() {
			val, ok := e.evalBool(arg)
			if !ok {
				return true, false
			}
			if val {
				return true, true
			}
		}
		return false, true

	case pg_query.BoolExprType_NOT_EXPR:
		if len(be.GetArgs()) > 0 {
			val, ok := e.evalBool(be.GetArgs()[0])
			if !ok {
				return true, false
			}
			return !val, true
		}
	}
	return true, false
}

func (e *whereEval) evalAExpr(ae *pg_query.A_Expr) (bool, bool) {
	kind := ae.GetKind()

	switch kind {
	case pg_query.A_Expr_Kind_AEXPR_OP:
		return e.evalOp(ae)
	case pg_query.A_Expr_Kind_AEXPR_IN:
		return e.evalIn(ae)
	case pg_query.A_Expr_Kind_AEXPR_LIKE:
		return e.evalLike(ae, false)
	case pg_query.A_Expr_Kind_AEXPR_ILIKE:
		return e.evalLike(ae, true)
	case pg_query.A_Expr_Kind_AEXPR_BETWEEN:
		return e.evalBetween(ae, false)
	case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN:
		return e.evalBetween(ae, true)
	}
	return true, false
}

func (e *whereEval) evalOp(ae *pg_query.A_Expr) (bool, bool) {
	var opName string
	for _, n := range ae.GetName() {
		if s := n.GetString_(); s != nil {
			opName = s.GetSval()
		}
	}

	lVal, lNull, lOk := e.resolveValue(ae.GetLexpr())
	rVal, rNull, rOk := e.resolveValue(ae.GetRexpr())
	if !lOk || !rOk {
		return true, false
	}

	if lNull || rNull {
		return false, true // NULL comparison → false in WHERE context
	}

	switch opName {
	case "=":
		if lVal == rVal {
			return true, true
		}
		// Handle boolean normalization: 't'/'f' vs 'true'/'false'.
		if lb, rb := normBool(lVal), normBool(rVal); lb != "" && rb != "" {
			return lb == rb, true
		}
		return false, true
	case "!=", "<>":
		if lVal == rVal {
			return false, true
		}
		if lb, rb := normBool(lVal), normBool(rVal); lb != "" && rb != "" {
			return lb != rb, true
		}
		return true, true
	case "<":
		return compareValues(lVal, rVal) < 0, true
	case ">":
		return compareValues(lVal, rVal) > 0, true
	case "<=":
		return compareValues(lVal, rVal) <= 0, true
	case ">=":
		return compareValues(lVal, rVal) >= 0, true
	case "~~": // LIKE
		return matchLike(lVal, rVal), true
	case "~~*": // ILIKE
		return matchLike(strings.ToLower(lVal), strings.ToLower(rVal)), true
	case "!~~": // NOT LIKE
		return !matchLike(lVal, rVal), true
	case "!~~*": // NOT ILIKE
		return !matchLike(strings.ToLower(lVal), strings.ToLower(rVal)), true
	}
	return true, false
}

func (e *whereEval) evalIn(ae *pg_query.A_Expr) (bool, bool) {
	lVal, lNull, lOk := e.resolveValue(ae.GetLexpr())
	if !lOk || lNull {
		return false, lOk
	}

	rexpr := ae.GetRexpr()
	if rexpr == nil {
		return true, false
	}
	list := rexpr.GetList()
	if list == nil {
		return true, false
	}
	for _, item := range list.GetItems() {
		val, isNull, ok := e.resolveValue(item)
		if !ok || isNull {
			continue
		}
		if lVal == val {
			return true, true
		}
	}
	return false, true
}

func (e *whereEval) evalLike(ae *pg_query.A_Expr, caseInsensitive bool) (bool, bool) {
	lVal, lNull, lOk := e.resolveValue(ae.GetLexpr())
	rVal, rNull, rOk := e.resolveValue(ae.GetRexpr())
	if !lOk || !rOk || lNull || rNull {
		return false, lOk && rOk
	}
	if caseInsensitive {
		return matchLike(strings.ToLower(lVal), strings.ToLower(rVal)), true
	}
	return matchLike(lVal, rVal), true
}

func (e *whereEval) evalBetween(ae *pg_query.A_Expr, negate bool) (bool, bool) {
	lVal, lNull, lOk := e.resolveValue(ae.GetLexpr())
	if !lOk || lNull {
		return false, lOk
	}
	rexpr := ae.GetRexpr()
	if rexpr == nil {
		return true, false
	}
	list := rexpr.GetList()
	if list == nil || len(list.GetItems()) != 2 {
		return true, false
	}

	lo, loNull, loOk := e.resolveValue(list.GetItems()[0])
	hi, hiNull, hiOk := e.resolveValue(list.GetItems()[1])
	if !loOk || !hiOk || loNull || hiNull {
		return false, loOk && hiOk
	}

	inRange := compareValues(lVal, lo) >= 0 && compareValues(lVal, hi) <= 0
	if negate {
		return !inRange, true
	}
	return inRange, true
}

func (e *whereEval) evalNullTest(nt *pg_query.NullTest) (bool, bool) {
	arg := nt.GetArg()
	if arg == nil {
		return true, false
	}
	colName := resolveColumnRefName(arg)
	if colName == "" {
		return true, false
	}
	isNull := e.nulls[colName]
	if nt.GetNulltesttype() == pg_query.NullTestType_IS_NULL {
		return isNull, true
	}
	return !isNull, true
}

// resolveValue extracts a scalar value from a node.
// Returns (value, isNull, ok).
func (e *whereEval) resolveValue(node *pg_query.Node) (string, bool, bool) {
	if node == nil {
		return "", false, false
	}

	// Column reference.
	if node.GetColumnRef() != nil {
		colName := resolveColumnRefName(node)
		if colName == "" {
			return "", false, false
		}
		if e.nulls[colName] {
			return "", true, true
		}
		val, exists := e.row[colName]
		if !exists {
			return "", false, false
		}
		return val, false, true
	}

	// Constant.
	if ac := node.GetAConst(); ac != nil {
		if ac.GetIsnull() {
			return "", true, true
		}
		if sv := ac.GetSval(); sv != nil {
			return sv.GetSval(), false, true
		}
		if iv := ac.GetIval(); iv != nil {
			return strconv.Itoa(int(iv.GetIval())), false, true
		}
		if fv := ac.GetFval(); fv != nil {
			return fv.GetFval(), false, true
		}
		if bv := ac.GetBoolval(); bv != nil {
			if bv.GetBoolval() {
				return "t", false, true
			}
			return "f", false, true
		}
	}

	// Type cast (e.g., 'active'::text).
	if tc := node.GetTypeCast(); tc != nil {
		return e.resolveValue(tc.GetArg())
	}

	return "", false, false
}

// resolveColumnRefName extracts the bare column name from a ColumnRef node.
// For qualified names like "u"."name", returns just "name".
func resolveColumnRefName(node *pg_query.Node) string {
	cr := node.GetColumnRef()
	if cr == nil {
		return ""
	}
	var parts []string
	for _, f := range cr.GetFields() {
		if s := f.GetString_(); s != nil {
			parts = append(parts, s.GetSval())
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// normBool normalizes boolean string representations for comparison.
// Returns "" if the string is not a recognized boolean value.
func normBool(s string) string {
	switch strings.ToLower(s) {
	case "t", "true", "1", "yes", "on":
		return "t"
	case "f", "false", "0", "no", "off":
		return "f"
	}
	return ""
}

// matchLike evaluates SQL LIKE pattern matching.
// % matches any sequence of characters, _ matches any single character.
func matchLike(str, pattern string) bool {
	return matchLikeAt(str, pattern, 0, 0)
}

func matchLikeAt(str, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		if pattern[pi] == '%' {
			pi++
			for si <= len(str) {
				if matchLikeAt(str, pattern, si, pi) {
					return true
				}
				si++
			}
			return false
		}
		if si >= len(str) {
			return false
		}
		if pattern[pi] == '_' || pattern[pi] == str[si] {
			si++
			pi++
		} else {
			return false
		}
	}
	return si == len(str)
}

// extractWhereAST parses a SELECT SQL and returns the WHERE clause AST node.
func extractWhereAST(sql string) *pg_query.Node {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return nil
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return nil
	}
	return sel.GetWhereClause()
}

// filterByWhere removes patched rows that no longer satisfy the WHERE clause
// after their column values were replaced with Shadow data.
func filterByWhere(
	whereAST *pg_query.Node,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
	deltaIndices []int,
) ([][]string, [][]bool) {
	deltaSet := make(map[int]bool)
	for _, idx := range deltaIndices {
		deltaSet[idx] = true
	}

	var filtered [][]string
	var filteredNulls [][]bool
	for i, row := range values {
		if !deltaSet[i] {
			// Clean row — keep it (already satisfied WHERE on Prod).
			filtered = append(filtered, row)
			filteredNulls = append(filteredNulls, nulls[i])
			continue
		}

		// Patched row — evaluate WHERE.
		rowMap := make(map[string]string, len(columns))
		nullMap := make(map[string]bool, len(columns))
		for j, col := range columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
				nullMap[col.Name] = nulls[i][j]
			}
		}

		if evaluateWhere(whereAST, rowMap, nullMap) {
			filtered = append(filtered, row)
			filteredNulls = append(filteredNulls, nulls[i])
		}
	}

	return filtered, filteredNulls
}

// extractJoinTableAliases parses a SELECT SQL and returns a map from
// table name to its alias. If a table has no alias, maps to itself.
func extractJoinTableAliases(sql string) map[string]string {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return nil
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return nil
	}
	aliases := make(map[string]string)
	for _, node := range sel.GetFromClause() {
		collectTableAliases(node, aliases)
	}
	return aliases
}

func collectTableAliases(node *pg_query.Node, result map[string]string) {
	if node == nil {
		return
	}
	if rv := node.GetRangeVar(); rv != nil {
		tableName := rv.GetRelname()
		if s := rv.GetSchemaname(); s != "" {
			tableName = s + "." + tableName
		}
		alias := tableName
		if a := rv.GetAlias(); a != nil && a.GetAliasname() != "" {
			alias = a.GetAliasname()
		}
		result[tableName] = alias
		return
	}
	if je := node.GetJoinExpr(); je != nil {
		collectTableAliases(je.GetLarg(), result)
		collectTableAliases(je.GetRarg(), result)
	}
}
