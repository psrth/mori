package proxy

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	coreSchema "github.com/mori-dev/mori/internal/core/schema"
)

// rewriteSQLForProd rewrites a SELECT statement to be compatible with the Prod
// schema. It strips shadow-only columns from SELECT, WHERE, ORDER BY, and
// GROUP BY clauses using AST manipulation.
//
// Returns (rewritten SQL, skipProd). skipProd is true if the entire query is
// irrelevant for Prod (e.g., all WHERE conditions reference shadow-only columns
// with non-NULL comparisons, making it impossible for any Prod row to match).
func rewriteSQLForProd(sql string, registry *coreSchema.Registry, tables []string) (string, bool) {
	if registry == nil {
		return sql, false
	}

	// Collect all added column names across the query's tables.
	addedCols := collectAddedColumns(registry, tables)
	if len(addedCols) == 0 {
		return sql, false
	}

	result, err := pg_query.Parse(sql)
	if err != nil {
		return sql, false
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return sql, false
	}
	sel := stmts[0].GetStmt().GetSelectStmt()
	if sel == nil {
		return sql, false
	}

	modified := false
	skipProd := false

	// Rewrite WHERE clause.
	if where := sel.GetWhereClause(); where != nil {
		newWhere, action := rewriteWhereNode(where, addedCols)
		switch action {
		case rewriteRemoved:
			// All conditions were on added columns with IS NULL (vacuously true) →
			// remove WHERE entirely.
			sel.WhereClause = nil
			modified = true
		case rewriteFalse:
			// All conditions resolve to FALSE → no Prod rows can match.
			skipProd = true
		case rewriteModified:
			sel.WhereClause = newWhere
			modified = true
		}
	}

	if skipProd {
		return sql, true
	}

	// Rewrite SELECT list: strip added columns.
	newTargets, selModified := rewriteTargetList(sel.GetTargetList(), addedCols)
	if selModified {
		sel.TargetList = newTargets
		modified = true
	}

	// Rewrite ORDER BY: strip sort clauses on added columns.
	if sortClauses := sel.GetSortClause(); len(sortClauses) > 0 {
		var kept []*pg_query.Node
		for _, sc := range sortClauses {
			sortBy := sc.GetSortBy()
			if sortBy == nil {
				kept = append(kept, sc)
				continue
			}
			colName := resolveNodeColumnName(sortBy.GetNode())
			if colName != "" && addedCols[strings.ToLower(colName)] {
				modified = true
				continue // Strip this ORDER BY column.
			}
			kept = append(kept, sc)
		}
		sel.SortClause = kept
	}

	// Rewrite GROUP BY: strip group clauses on added columns.
	if groupClauses := sel.GetGroupClause(); len(groupClauses) > 0 {
		var kept []*pg_query.Node
		for _, gc := range groupClauses {
			colName := resolveNodeColumnName(gc)
			if colName != "" && addedCols[strings.ToLower(colName)] {
				modified = true
				continue // Strip this GROUP BY column.
			}
			kept = append(kept, gc)
		}
		sel.GroupClause = kept
	}

	if !modified {
		return sql, false
	}

	deparsed, err := pg_query.Deparse(result)
	if err != nil {
		return sql, false
	}

	return deparsed, false
}

type rewriteAction int

const (
	rewriteNone     rewriteAction = iota // No change needed.
	rewriteModified                      // Node was rewritten.
	rewriteRemoved                       // Node should be removed (vacuously true).
	rewriteFalse                         // Node is always false for Prod rows.
)

// rewriteWhereNode recursively rewrites a WHERE clause AST node, handling
// conditions on shadow-only (added) columns.
//
// Rules:
//   - `added_col IS NULL` → vacuously true (Prod rows don't have the column → effectively NULL)
//   - `added_col IS NOT NULL` → always false (Prod rows never have the column)
//   - `added_col = value` / comparisons → always false
//   - Boolean AND/OR combinations are simplified.
func rewriteWhereNode(node *pg_query.Node, addedCols map[string]bool) (*pg_query.Node, rewriteAction) {
	if node == nil {
		return nil, rewriteNone
	}

	// NullTest: col IS NULL / col IS NOT NULL
	if nt := node.GetNullTest(); nt != nil {
		colName := resolveNodeColumnName(nt.GetArg())
		if colName != "" && addedCols[strings.ToLower(colName)] {
			if nt.GetNulltesttype() == pg_query.NullTestType_IS_NULL {
				return nil, rewriteRemoved // Vacuously true for Prod rows.
			}
			return nil, rewriteFalse // IS NOT NULL → always false.
		}
		return node, rewriteNone
	}

	// A_Expr: comparisons, LIKE, IN, etc.
	if ae := node.GetAExpr(); ae != nil {
		if nodeReferencesAddedCol(ae.GetLexpr(), addedCols) || nodeReferencesAddedCol(ae.GetRexpr(), addedCols) {
			return nil, rewriteFalse // Comparison on added col → always false for Prod.
		}
		return node, rewriteNone
	}

	// BoolExpr: AND / OR / NOT
	if be := node.GetBoolExpr(); be != nil {
		return rewriteBoolExpr(be, addedCols)
	}

	// SubLink (subqueries in WHERE): check if they reference added columns.
	if sl := node.GetSubLink(); sl != nil {
		if nodeTreeReferencesAddedCol(sl.GetTestexpr(), addedCols) {
			return nil, rewriteFalse
		}
		return node, rewriteNone
	}

	return node, rewriteNone
}

// rewriteBoolExpr handles AND/OR/NOT expressions.
func rewriteBoolExpr(be *pg_query.BoolExpr, addedCols map[string]bool) (*pg_query.Node, rewriteAction) {
	switch be.GetBoolop() {
	case pg_query.BoolExprType_AND_EXPR:
		var kept []*pg_query.Node
		for _, arg := range be.GetArgs() {
			newArg, action := rewriteWhereNode(arg, addedCols)
			switch action {
			case rewriteFalse:
				return nil, rewriteFalse // FALSE AND anything → FALSE
			case rewriteRemoved:
				continue // TRUE AND x → x (skip this term)
			case rewriteModified:
				kept = append(kept, newArg)
			default:
				kept = append(kept, arg)
			}
		}
		if len(kept) == 0 {
			return nil, rewriteRemoved // All terms were vacuously true.
		}
		if len(kept) == 1 {
			return kept[0], rewriteModified
		}
		be.Args = kept
		return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: be}}, rewriteModified

	case pg_query.BoolExprType_OR_EXPR:
		var kept []*pg_query.Node
		anyRemoved := false
		for _, arg := range be.GetArgs() {
			newArg, action := rewriteWhereNode(arg, addedCols)
			switch action {
			case rewriteRemoved:
				return nil, rewriteRemoved // TRUE OR anything → TRUE
			case rewriteFalse:
				anyRemoved = true
				continue // FALSE OR x → x
			case rewriteModified:
				kept = append(kept, newArg)
				anyRemoved = true
			default:
				kept = append(kept, arg)
			}
		}
		if len(kept) == 0 {
			return nil, rewriteFalse // All terms were false.
		}
		if len(kept) == 1 && anyRemoved {
			return kept[0], rewriteModified
		}
		if anyRemoved {
			be.Args = kept
			return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: be}}, rewriteModified
		}
		return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: be}}, rewriteNone

	case pg_query.BoolExprType_NOT_EXPR:
		if len(be.GetArgs()) > 0 {
			_, action := rewriteWhereNode(be.GetArgs()[0], addedCols)
			switch action {
			case rewriteRemoved:
				return nil, rewriteFalse // NOT TRUE → FALSE
			case rewriteFalse:
				return nil, rewriteRemoved // NOT FALSE → TRUE
			}
		}
	}
	return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: be}}, rewriteNone
}

// rewriteTargetList strips added columns from the SELECT target list.
// Returns the new list and whether any modifications were made.
func rewriteTargetList(targets []*pg_query.Node, addedCols map[string]bool) ([]*pg_query.Node, bool) {
	if len(targets) == 0 {
		return targets, false
	}

	// Check if this is SELECT * — no rewriting needed.
	if len(targets) == 1 {
		rt := targets[0].GetResTarget()
		if rt != nil && rt.GetVal() != nil {
			if cr := rt.GetVal().GetColumnRef(); cr != nil {
				for _, f := range cr.GetFields() {
					if f.GetAStar() != nil {
						return targets, false // SELECT *
					}
				}
			}
		}
	}

	var kept []*pg_query.Node
	modified := false
	for _, t := range targets {
		rt := t.GetResTarget()
		if rt == nil {
			kept = append(kept, t)
			continue
		}
		colName := resolveNodeColumnName(rt.GetVal())
		if colName != "" && addedCols[strings.ToLower(colName)] {
			modified = true
			continue // Strip added column.
		}
		kept = append(kept, t)
	}

	if len(kept) == 0 && modified {
		// All columns were added — replace with SELECT *.
		star := &pg_query.Node{
			Node: &pg_query.Node_ResTarget{
				ResTarget: &pg_query.ResTarget{
					Val: &pg_query.Node{
						Node: &pg_query.Node_ColumnRef{
							ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{
									{Node: &pg_query.Node_AStar{AStar: &pg_query.A_Star{}}},
								},
							},
						},
					},
				},
			},
		}
		return []*pg_query.Node{star}, true
	}

	return kept, modified
}

// collectAddedColumns builds a set of lowercase column names that were added
// via DDL in Shadow (and don't exist in Prod) across all given tables.
func collectAddedColumns(registry *coreSchema.Registry, tables []string) map[string]bool {
	added := make(map[string]bool)
	for _, t := range tables {
		diff := registry.GetDiff(t)
		if diff == nil {
			continue
		}
		for _, col := range diff.Added {
			added[strings.ToLower(col.Name)] = true
		}
	}
	return added
}

// resolveNodeColumnName extracts the bare column name from a node that may be
// a ColumnRef (possibly qualified like "t"."col") or a ResTarget.
func resolveNodeColumnName(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if cr := node.GetColumnRef(); cr != nil {
		var parts []string
		for _, f := range cr.GetFields() {
			if s := f.GetString_(); s != nil {
				parts = append(parts, s.GetSval())
			}
		}
		if len(parts) > 0 {
			return parts[len(parts)-1] // Return bare column name.
		}
	}
	return ""
}

// nodeReferencesAddedCol checks if a node directly references an added column.
func nodeReferencesAddedCol(node *pg_query.Node, addedCols map[string]bool) bool {
	if node == nil {
		return false
	}
	colName := resolveNodeColumnName(node)
	if colName != "" && addedCols[strings.ToLower(colName)] {
		return true
	}
	// Check type casts: 'value'::type
	if tc := node.GetTypeCast(); tc != nil {
		return nodeReferencesAddedCol(tc.GetArg(), addedCols)
	}
	return false
}

// nodeTreeReferencesAddedCol recursively checks if any node in a tree
// references an added column.
func nodeTreeReferencesAddedCol(node *pg_query.Node, addedCols map[string]bool) bool {
	if node == nil {
		return false
	}
	if nodeReferencesAddedCol(node, addedCols) {
		return true
	}
	if be := node.GetBoolExpr(); be != nil {
		for _, arg := range be.GetArgs() {
			if nodeTreeReferencesAddedCol(arg, addedCols) {
				return true
			}
		}
	}
	if ae := node.GetAExpr(); ae != nil {
		return nodeTreeReferencesAddedCol(ae.GetLexpr(), addedCols) ||
			nodeTreeReferencesAddedCol(ae.GetRexpr(), addedCols)
	}
	return false
}
