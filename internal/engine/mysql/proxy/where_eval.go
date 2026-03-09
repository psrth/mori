package proxy

import (
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// evaluateWhere evaluates a Vitess WHERE expression against a row.
// Returns true if the row matches the WHERE clause, or true on evaluation
// failure (safe fallback: keep the row).
func evaluateWhere(expr sqlparser.Expr, row map[string]string, nulls map[string]bool) bool {
	result, ok := evalExpr(expr, row, nulls)
	if !ok {
		return true // Safe fallback.
	}
	return result
}

// evalExpr evaluates a Vitess expression and returns (result, ok).
// ok=false means evaluation failed.
func evalExpr(expr sqlparser.Expr, row map[string]string, nulls map[string]bool) (bool, bool) {
	if expr == nil {
		return true, true
	}

	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, lok := evalExpr(e.Left, row, nulls)
		if lok && !left {
			return false, true
		}
		right, rok := evalExpr(e.Right, row, nulls)
		if rok && !right {
			return false, true
		}
		if !lok || !rok {
			return true, false
		}
		return left && right, true

	case *sqlparser.OrExpr:
		left, lok := evalExpr(e.Left, row, nulls)
		if lok && left {
			return true, true
		}
		right, rok := evalExpr(e.Right, row, nulls)
		if rok && right {
			return true, true
		}
		if !lok || !rok {
			return true, false
		}
		return false, true

	case *sqlparser.NotExpr:
		val, ok := evalExpr(e.Expr, row, nulls)
		if !ok {
			return true, false
		}
		return !val, true

	case *sqlparser.ComparisonExpr:
		return evalComparison(e, row, nulls)

	case *sqlparser.IsExpr:
		return evalIsExpr(e, row, nulls)

	case *sqlparser.BetweenExpr:
		return evalBetweenExpr(e, row, nulls)

	default:
		return true, false // Unsupported expression type.
	}
}

// evalComparison evaluates a comparison expression (=, !=, <, >, <=, >=, LIKE, IN, etc).
func evalComparison(expr *sqlparser.ComparisonExpr, row map[string]string, nulls map[string]bool) (bool, bool) {
	switch expr.Operator {
	case sqlparser.InOp, sqlparser.NotInOp:
		return evalIn(expr, row, nulls)
	case sqlparser.LikeOp, sqlparser.NotLikeOp:
		return evalLike(expr, row, nulls)
	}

	left, leftNull, lok := resolveValue(expr.Left, row, nulls)
	right, rightNull, rok := resolveValue(expr.Right, row, nulls)
	if !lok || !rok {
		return true, false
	}

	// NULL comparison: NULL = anything is false in SQL.
	if leftNull || rightNull {
		return false, true
	}

	cmp := compareValues(left, right)

	switch expr.Operator {
	case sqlparser.EqualOp:
		return cmp == 0, true
	case sqlparser.NotEqualOp:
		return cmp != 0, true
	case sqlparser.LessThanOp:
		return cmp < 0, true
	case sqlparser.GreaterThanOp:
		return cmp > 0, true
	case sqlparser.LessEqualOp:
		return cmp <= 0, true
	case sqlparser.GreaterEqualOp:
		return cmp >= 0, true
	default:
		return true, false
	}
}

// evalIsExpr evaluates IS NULL / IS NOT NULL / IS TRUE / IS FALSE.
func evalIsExpr(expr *sqlparser.IsExpr, row map[string]string, nulls map[string]bool) (bool, bool) {
	_, isNull, ok := resolveValue(expr.Left, row, nulls)
	if !ok {
		return true, false
	}

	switch expr.Right {
	case sqlparser.IsNullOp:
		return isNull, true
	case sqlparser.IsNotNullOp:
		return !isNull, true
	default:
		return true, false
	}
}

// evalBetweenExpr evaluates BETWEEN / NOT BETWEEN.
func evalBetweenExpr(expr *sqlparser.BetweenExpr, row map[string]string, nulls map[string]bool) (bool, bool) {
	val, valNull, vok := resolveValue(expr.Left, row, nulls)
	lo, loNull, lok := resolveValue(expr.From, row, nulls)
	hi, hiNull, hok := resolveValue(expr.To, row, nulls)
	if !vok || !lok || !hok {
		return true, false
	}
	if valNull || loNull || hiNull {
		return false, true
	}

	cmpLo := compareValues(val, lo)
	cmpHi := compareValues(val, hi)
	between := cmpLo >= 0 && cmpHi <= 0

	if !expr.IsBetween {
		// NOT BETWEEN case.
		return !between, true
	}
	return between, true
}

// evalIn evaluates IN / NOT IN.
func evalIn(expr *sqlparser.ComparisonExpr, row map[string]string, nulls map[string]bool) (bool, bool) {
	left, leftNull, lok := resolveValue(expr.Left, row, nulls)
	if !lok {
		return true, false
	}
	if leftNull {
		return false, true
	}

	tuple, ok := expr.Right.(sqlparser.ValTuple)
	if !ok {
		return true, false
	}

	found := false
	for _, item := range tuple {
		val, isNull, ok := resolveValue(item, row, nulls)
		if !ok || isNull {
			continue
		}
		if compareValues(left, val) == 0 {
			found = true
			break
		}
	}

	if expr.Operator == sqlparser.NotInOp {
		return !found, true
	}
	return found, true
}

// evalLike evaluates LIKE / NOT LIKE.
func evalLike(expr *sqlparser.ComparisonExpr, row map[string]string, nulls map[string]bool) (bool, bool) {
	str, strNull, sok := resolveValue(expr.Left, row, nulls)
	pattern, patNull, pok := resolveValue(expr.Right, row, nulls)
	if !sok || !pok {
		return true, false
	}
	if strNull || patNull {
		return false, true
	}

	matched := matchLike(str, pattern)

	if expr.Operator == sqlparser.NotLikeOp {
		return !matched, true
	}
	return matched, true
}

// resolveValue extracts a value from a Vitess expression using the row data.
// Returns (value, isNull, ok).
func resolveValue(expr sqlparser.Expr, row map[string]string, nulls map[string]bool) (string, bool, bool) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		name := e.Name.String()
		if nulls[name] {
			return "", true, true
		}
		val, ok := row[name]
		if !ok {
			// Try case-insensitive lookup.
			lower := strings.ToLower(name)
			for k, v := range row {
				if strings.ToLower(k) == lower {
					if nulls[k] {
						return "", true, true
					}
					return v, false, true
				}
			}
			return "", false, false
		}
		return val, false, true

	case *sqlparser.Literal:
		switch e.Type {
		case sqlparser.StrVal:
			return e.Val, false, true
		case sqlparser.IntVal:
			return e.Val, false, true
		case sqlparser.FloatVal:
			return e.Val, false, true
		case sqlparser.HexVal:
			return e.Val, false, true
		}
		return e.Val, false, true

	case *sqlparser.NullVal:
		return "", true, true

	case sqlparser.BoolVal:
		if e {
			return "1", false, true
		}
		return "0", false, true

	case *sqlparser.UnaryExpr:
		val, isNull, ok := resolveValue(e.Expr, row, nulls)
		if !ok || isNull {
			return val, isNull, ok
		}
		if e.Operator == sqlparser.UMinusOp {
			if n, err := strconv.ParseFloat(val, 64); err == nil {
				return strconv.FormatFloat(-n, 'f', -1, 64), false, true
			}
		}
		return val, false, ok

	case *sqlparser.CastExpr:
		return resolveValue(e.Expr, row, nulls)

	default:
		return "", false, false
	}
}

// matchLike implements SQL LIKE pattern matching.
// % matches any sequence, _ matches any single character.
func matchLike(str, pattern string) bool {
	return matchLikeAt(str, pattern, 0, 0)
}

func matchLikeAt(str, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		if si >= len(str) {
			// Remaining pattern must be all % to match.
			for pi < len(pattern) {
				if pattern[pi] != '%' {
					return false
				}
				pi++
			}
			return true
		}

		switch pattern[pi] {
		case '%':
			// Skip consecutive %.
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi >= len(pattern) {
				return true // Trailing % matches everything.
			}
			// Try matching from each position.
			for si <= len(str) {
				if matchLikeAt(str, pattern, si, pi) {
					return true
				}
				si++
			}
			return false
		case '_':
			si++
			pi++
		default:
			if str[si] != pattern[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si >= len(str)
}

// extractWhereAST parses SQL and extracts the WHERE clause as a Vitess Expr.
func extractWhereAST(sql string) sqlparser.Expr {
	parser, _ := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return nil
	}
	return sel.Where.Expr
}

// filterByWhere filters rows by a WHERE clause. Only evaluates patched (delta)
// rows; clean rows (from Prod) are kept as-is since they already satisfied WHERE.
func filterByWhere(
	whereExpr sqlparser.Expr,
	columns []ColumnInfo,
	values [][]string,
	nullsArr [][]bool,
	deltaIndices []int,
) ([][]string, [][]bool) {
	if whereExpr == nil || len(deltaIndices) == 0 {
		return values, nullsArr
	}

	deltaSet := make(map[int]bool, len(deltaIndices))
	for _, idx := range deltaIndices {
		deltaSet[idx] = true
	}

	var filteredValues [][]string
	var filteredNulls [][]bool

	for i, row := range values {
		if !deltaSet[i] {
			// Clean row — keep as-is.
			filteredValues = append(filteredValues, row)
			filteredNulls = append(filteredNulls, nullsArr[i])
			continue
		}

		// Build row map for evaluation.
		rowMap := make(map[string]string, len(columns))
		nullMap := make(map[string]bool, len(columns))
		for j, col := range columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
			}
			if j < len(nullsArr[i]) {
				nullMap[col.Name] = nullsArr[i][j]
			}
		}

		if evaluateWhere(whereExpr, rowMap, nullMap) {
			filteredValues = append(filteredValues, row)
			filteredNulls = append(filteredNulls, nullsArr[i])
		}
	}

	return filteredValues, filteredNulls
}
