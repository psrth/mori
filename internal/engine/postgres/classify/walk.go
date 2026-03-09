package classify

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// relName extracts a table name from a RangeVar, including schema prefix if present.
func relName(rv *pg_query.RangeVar) string {
	name := rv.GetRelname()
	if s := rv.GetSchemaname(); s != "" {
		return s + "." + name
	}
	return name
}

// extractTablesFromNodes walks a list of FROM-clause nodes and returns all table names.
func extractTablesFromNodes(nodes []*pg_query.Node) []string {
	var tables []string
	for _, n := range nodes {
		tables = append(tables, extractTablesFromNode(n)...)
	}
	return tables
}

// extractTablesFromNode extracts table names from a single FROM-clause node,
// recursing into JoinExpr and RangeSubselect nodes.
func extractTablesFromNode(n *pg_query.Node) []string {
	if n == nil {
		return nil
	}
	switch {
	case n.GetRangeVar() != nil:
		return []string{relName(n.GetRangeVar())}
	case n.GetJoinExpr() != nil:
		je := n.GetJoinExpr()
		left := extractTablesFromNode(je.GetLarg())
		right := extractTablesFromNode(je.GetRarg())
		return append(left, right...)
	case n.GetRangeSubselect() != nil:
		sub := n.GetRangeSubselect().GetSubquery()
		if sub != nil {
			if sel := sub.GetSelectStmt(); sel != nil {
				return extractTablesFromNodes(sel.GetFromClause())
			}
		}
	}
	return nil
}

// hasJoinExpr returns true if any node in the list is a JoinExpr.
func hasJoinExpr(nodes []*pg_query.Node) bool {
	for _, n := range nodes {
		if n.GetJoinExpr() != nil {
			return true
		}
	}
	return false
}

// hasRangeSubselect returns true if any node in the list is a derived table (subquery in FROM).
func hasRangeSubselect(nodes []*pg_query.Node) bool {
	for _, n := range nodes {
		if n != nil && n.GetRangeSubselect() != nil {
			return true
		}
	}
	return false
}

// extractIntValue extracts an integer from a constant node.
// Returns 0 if the node is not a simple integer literal.
func extractIntValue(n *pg_query.Node) int {
	if n == nil {
		return 0
	}
	if ac := n.GetAConst(); ac != nil {
		if iv := ac.GetIval(); iv != nil {
			return int(iv.GetIval())
		}
	}
	if iv := n.GetInteger(); iv != nil {
		return int(iv.GetIval())
	}
	return 0
}

// extractOrderByFromRaw extracts the ORDER BY clause from raw SQL text.
// Returns the column/expression part without the "ORDER BY" keywords.
func extractOrderByFromRaw(rawSQL string) string {
	upper := strings.ToUpper(rawSQL)
	idx := strings.LastIndex(upper, "ORDER BY")
	if idx < 0 {
		return ""
	}
	rest := rawSQL[idx+len("ORDER BY"):]
	for _, kw := range []string{"LIMIT", "OFFSET", "FOR ", "FETCH "} {
		if pos := strings.Index(strings.ToUpper(rest), kw); pos >= 0 {
			rest = rest[:pos]
		}
	}
	return strings.TrimSpace(rest)
}

// appendUnique appends values to a slice, skipping duplicates.
func appendUnique(slice []string, vals ...string) []string {
	seen := make(map[string]bool, len(slice))
	for _, s := range slice {
		seen[s] = true
	}
	for _, v := range vals {
		if !seen[v] {
			slice = append(slice, v)
			seen[v] = true
		}
	}
	return slice
}

// isMutatingNode returns true if the node is an INSERT, UPDATE, or DELETE statement.
func isMutatingNode(n *pg_query.Node) bool {
	if n == nil {
		return false
	}
	return n.GetInsertStmt() != nil ||
		n.GetUpdateStmt() != nil ||
		n.GetDeleteStmt() != nil
}

// extractIntrospectedTable walks a WHERE clause AST looking for equality
// comparisons like table_name = 'X' or relname = 'X', and returns the
// string literal value. This is used to detect which user table a metadata
// query (information_schema / pg_catalog) is inspecting.
func extractIntrospectedTable(node *pg_query.Node) string {
	if node == nil {
		return ""
	}

	// Check A_Expr (binary expression like col = 'val').
	if ae := node.GetAExpr(); ae != nil {
		// Check for AND/OR — recurse into both sides.
		if ae.GetKind() == pg_query.A_Expr_Kind_AEXPR_OP {
			colName := extractColumnRefName(ae.GetLexpr())
			if colName == "table_name" || colName == "relname" {
				if val := extractStringConst(ae.GetRexpr()); val != "" {
					return val
				}
			}
			// Check reversed: 'X' = table_name
			colName = extractColumnRefName(ae.GetRexpr())
			if colName == "table_name" || colName == "relname" {
				if val := extractStringConst(ae.GetLexpr()); val != "" {
					return val
				}
			}
		}
		// Recurse into operands for AND/OR chains.
		if v := extractIntrospectedTable(ae.GetLexpr()); v != "" {
			return v
		}
		if v := extractIntrospectedTable(ae.GetRexpr()); v != "" {
			return v
		}
	}

	// BoolExpr (AND/OR) — recurse into args.
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			if v := extractIntrospectedTable(arg); v != "" {
				return v
			}
		}
	}

	return ""
}

// extractColumnRefName returns the column name from a ColumnRef node, or "".
func extractColumnRefName(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	cr := node.GetColumnRef()
	if cr == nil {
		return ""
	}
	fields := cr.GetFields()
	if len(fields) == 0 {
		return ""
	}
	// Take the last field (handles both "table_name" and "c.table_name").
	last := fields[len(fields)-1]
	if s := last.GetString_(); s != nil {
		return strings.ToLower(s.GetSval())
	}
	return ""
}

// extractStringConst returns the string value from an A_Const string node, or "".
func extractStringConst(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	ac := node.GetAConst()
	if ac == nil {
		return ""
	}
	if sv := ac.GetSval(); sv != nil {
		return sv.GetSval()
	}
	return ""
}
