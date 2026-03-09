package classify

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

// extractPKsFromExpr walks a WHERE clause expression tree and extracts
// (table, pk_value) pairs where a known PK column is compared via equality
// to a literal value.
func (c *PgClassifier) extractPKsFromExpr(node *pg_query.Node, tables []string) []core.TablePK {
	if node == nil {
		return nil
	}

	var pks []core.TablePK

	switch {
	case node.GetBoolExpr() != nil:
		for _, arg := range node.GetBoolExpr().GetArgs() {
			pks = append(pks, c.extractPKsFromExpr(arg, tables)...)
		}
	case node.GetAExpr() != nil:
		expr := node.GetAExpr()
		if isEqualityOp(expr) {
			if pk := c.tryExtractPKFromEquality(expr, tables); pk != nil {
				pks = append(pks, *pk)
			}
		}
	}

	return pks
}

// isEqualityOp returns true if the A_Expr represents an equality (=) operation.
func isEqualityOp(expr *pg_query.A_Expr) bool {
	if expr.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
		return false
	}
	for _, nameNode := range expr.GetName() {
		if s := nameNode.GetString_(); s != nil && s.GetSval() == "=" {
			return true
		}
	}
	return false
}

// tryExtractPKFromEquality attempts to extract a TablePK from an equality
// expression where one side is a column reference on a known PK column
// and the other is a literal value.
func (c *PgClassifier) tryExtractPKFromEquality(
	expr *pg_query.A_Expr, tables []string,
) *core.TablePK {
	// Try: column = literal
	colName, colTable := extractColumnRef(expr.GetLexpr())
	val := extractLiteralValue(expr.GetRexpr())

	if colName == "" || val == "" {
		// Try: literal = column
		colName, colTable = extractColumnRef(expr.GetRexpr())
		val = extractLiteralValue(expr.GetLexpr())
	}

	if colName == "" || val == "" {
		return nil
	}

	table := c.resolveColumnTable(colName, colTable, tables)
	if table == "" {
		return nil
	}

	meta, ok := c.tables[table]
	if !ok {
		return nil
	}
	if !isPKColumn(meta, colName) {
		return nil
	}

	return &core.TablePK{Table: table, PK: val}
}

// extractColumnRef extracts column name and optional table qualifier from a ColumnRef node.
func extractColumnRef(n *pg_query.Node) (colName string, tableName string) {
	if n == nil {
		return "", ""
	}
	cr := n.GetColumnRef()
	if cr == nil {
		return "", ""
	}
	fields := cr.GetFields()
	switch len(fields) {
	case 1:
		if s := fields[0].GetString_(); s != nil {
			return s.GetSval(), ""
		}
	case 2:
		var tbl, col string
		if s := fields[0].GetString_(); s != nil {
			tbl = s.GetSval()
		}
		if s := fields[1].GetString_(); s != nil {
			col = s.GetSval()
		}
		return col, tbl
	}
	return "", ""
}

// extractLiteralValue extracts a string representation of a literal value.
// Handles integers, strings, floats, parameter references ($N), and
// type casts (e.g., 'value'::uuid).
func extractLiteralValue(n *pg_query.Node) string {
	if n == nil {
		return ""
	}
	if ac := n.GetAConst(); ac != nil {
		if iv := ac.GetIval(); iv != nil {
			return fmt.Sprintf("%d", iv.GetIval())
		}
		if sv := ac.GetSval(); sv != nil {
			return sv.GetSval()
		}
		if fv := ac.GetFval(); fv != nil {
			return fv.GetFval()
		}
	}
	if iv := n.GetInteger(); iv != nil {
		return fmt.Sprintf("%d", iv.GetIval())
	}
	if sv := n.GetString_(); sv != nil {
		return sv.GetSval()
	}
	if pr := n.GetParamRef(); pr != nil {
		return fmt.Sprintf("$%d", pr.GetNumber())
	}
	// TypeCast: unwrap 'value'::type to extract the inner literal.
	if tc := n.GetTypeCast(); tc != nil {
		return extractLiteralValue(tc.GetArg())
	}
	return ""
}

// resolveColumnTable determines which table an unqualified column belongs to.
// If colTable is specified (qualified reference), use it directly.
// Otherwise, if there is exactly one table, use that.
// If multiple tables, check which one has this column as a PK.
func (c *PgClassifier) resolveColumnTable(colName, colTable string, tables []string) string {
	if colTable != "" {
		return colTable
	}
	if len(tables) == 1 {
		return tables[0]
	}
	for _, t := range tables {
		meta, ok := c.tables[t]
		if ok && isPKColumn(meta, colName) {
			return t
		}
	}
	return ""
}

// isPKColumn checks if the given column name is a PK column for the table.
func isPKColumn(meta schema.TableMeta, colName string) bool {
	for _, pk := range meta.PKColumns {
		if pk == colName {
			return true
		}
	}
	return false
}
