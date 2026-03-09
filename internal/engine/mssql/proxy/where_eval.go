package proxy

import (
	"regexp"
	"strconv"
	"strings"
)

// evalWhere evaluates a SQL WHERE clause against a row of column values.
// Returns true if the row matches the condition, or true on evaluation failure
// (fail-open to avoid false exclusions).
func _evalWhere(whereClause string, columns []TDSColumnInfo, values []string, nulls []bool) bool {
	trimmed := strings.TrimSpace(whereClause)
	if trimmed == "" || strings.EqualFold(trimmed, "1=1") {
		return true
	}

	// Build column lookup.
	colMap := make(map[string]int)
	for i, col := range columns {
		colMap[strings.ToLower(col.Name)] = i
	}

	return evalExpr(trimmed, colMap, values, nulls)
}

// evalExpr evaluates a simple expression tree (supports AND, OR, NOT, comparisons).
func evalExpr(expr string, colMap map[string]int, values []string, nulls []bool) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}

	// Try splitting by OR (lowest precedence).
	parts := splitByKeyword(expr, " OR ")
	if len(parts) > 1 {
		for _, part := range parts {
			if evalExpr(part, colMap, values, nulls) {
				return true
			}
		}
		return false
	}

	// Try splitting by AND.
	parts = splitByKeyword(expr, " AND ")
	if len(parts) > 1 {
		for _, part := range parts {
			if !evalExpr(part, colMap, values, nulls) {
				return false
			}
		}
		return true
	}

	// Handle NOT.
	upper := strings.ToUpper(strings.TrimSpace(expr))
	if strings.HasPrefix(upper, "NOT ") {
		return !evalExpr(expr[4:], colMap, values, nulls)
	}

	// Handle parenthesized expression.
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		return evalExpr(expr[1:len(expr)-1], colMap, values, nulls)
	}

	// Handle IS NULL / IS NOT NULL.
	if m := reIsNull.FindStringSubmatch(expr); len(m) > 1 {
		col := strings.ToLower(strings.Trim(m[1], "[]"))
		idx, ok := colMap[col]
		if !ok {
			return true // fail-open
		}
		return len(nulls) > idx && nulls[idx]
	}
	if m := reIsNotNull.FindStringSubmatch(expr); len(m) > 1 {
		col := strings.ToLower(strings.Trim(m[1], "[]"))
		idx, ok := colMap[col]
		if !ok {
			return true
		}
		return idx < len(nulls) && !nulls[idx]
	}

	// Handle IN.
	if m := reIN.FindStringSubmatch(expr); len(m) > 2 {
		col := strings.ToLower(strings.Trim(m[1], "[]"))
		idx, ok := colMap[col]
		if !ok {
			return true
		}
		if idx < len(nulls) && nulls[idx] {
			return false
		}
		val := values[idx]
		for iv := range strings.SplitSeq(m[2], ",") {
			iv = strings.TrimSpace(iv)
			iv = strings.Trim(iv, "'")
			if strings.EqualFold(val, iv) {
				return true
			}
		}
		return false
	}

	// Handle BETWEEN.
	if m := reBetween.FindStringSubmatch(expr); len(m) > 3 {
		col := strings.ToLower(strings.Trim(m[1], "[]"))
		idx, ok := colMap[col]
		if !ok {
			return true
		}
		if idx < len(nulls) && nulls[idx] {
			return false
		}
		val := values[idx]
		low := strings.Trim(strings.TrimSpace(m[2]), "'")
		high := strings.Trim(strings.TrimSpace(m[3]), "'")
		return compareValues(val, low) >= 0 && compareValues(val, high) <= 0
	}

	// Handle LIKE.
	if m := reLike.FindStringSubmatch(expr); len(m) > 2 {
		col := strings.ToLower(strings.Trim(m[1], "[]"))
		idx, ok := colMap[col]
		if !ok {
			return true
		}
		if idx < len(nulls) && nulls[idx] {
			return false
		}
		val := values[idx]
		pattern := strings.Trim(strings.TrimSpace(m[2]), "'")
		return matchLike(val, pattern)
	}

	// Handle comparison operators: =, !=, <>, <, >, <=, >=.
	for _, op := range []string{"<=", ">=", "!=", "<>", "=", "<", ">"} {
		parts := splitByOp(expr, op)
		if len(parts) == 2 {
			left := resolveValue(strings.TrimSpace(parts[0]), colMap, values, nulls)
			right := resolveValue(strings.TrimSpace(parts[1]), colMap, values, nulls)
			if left == nil || right == nil {
				return false // NULL comparison
			}
			cmp := compareValues(*left, *right)
			switch op {
			case "=":
				return cmp == 0
			case "!=", "<>":
				return cmp != 0
			case "<":
				return cmp < 0
			case ">":
				return cmp > 0
			case "<=":
				return cmp <= 0
			case ">=":
				return cmp >= 0
			}
		}
	}

	// Can't evaluate — fail-open.
	return true
}

// resolveValue resolves a token to its string value (column ref or literal).
func resolveValue(token string, colMap map[string]int, values []string, nulls []bool) *string {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}

	// String literal.
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		val := token[1 : len(token)-1]
		return &val
	}

	// N-string literal.
	if len(token) >= 3 && (token[0] == 'N' || token[0] == 'n') && token[1] == '\'' && token[len(token)-1] == '\'' {
		val := token[2 : len(token)-1]
		return &val
	}

	// NULL.
	if strings.EqualFold(token, "NULL") {
		return nil
	}

	// Numeric literal.
	if _, err := strconv.ParseFloat(token, 64); err == nil {
		return &token
	}

	// Column reference.
	col := strings.ToLower(strings.Trim(token, "[]"))
	if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
		col = col[dotIdx+1:]
	}
	idx, ok := colMap[col]
	if !ok {
		return &token // Unknown — return as literal.
	}
	if idx < len(nulls) && nulls[idx] {
		return nil // NULL column.
	}
	val := values[idx]
	return &val
}

// splitByKeyword splits an expression by a keyword, respecting parentheses.
func splitByKeyword(expr, keyword string) []string {
	var parts []string
	depth := 0
	inString := false
	kwUpper := strings.ToUpper(keyword)
	exprUpper := strings.ToUpper(expr)
	start := 0

	for i := 0; i < len(expr); i++ {
		if inString {
			if expr[i] == '\'' {
				if i+1 < len(expr) && expr[i+1] == '\'' {
					i++ // Escaped quote.
				} else {
					inString = false
				}
			}
			continue
		}
		switch expr[i] {
		case '\'':
			inString = true
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+len(keyword) <= len(expr) {
				if exprUpper[i:i+len(keyword)] == kwUpper {
					parts = append(parts, expr[start:i])
					start = i + len(keyword)
					i += len(keyword) - 1
				}
			}
		}
	}
	parts = append(parts, expr[start:])
	return parts
}

// splitByOp splits by a comparison operator, respecting strings.
func splitByOp(expr, op string) []string {
	depth := 0
	inString := false
	for i := 0; i < len(expr); i++ {
		if inString {
			if expr[i] == '\'' {
				if i+1 < len(expr) && expr[i+1] == '\'' {
					i++
				} else {
					inString = false
				}
			}
			continue
		}
		switch expr[i] {
		case '\'':
			inString = true
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
				// Make sure we match the longest operator first.
				return []string{expr[:i], expr[i+len(op):]}
			}
		}
	}
	return nil
}

// compareValues compares two string values, attempting numeric comparison first.
func compareValues(a, b string) int {
	na, errA := strconv.ParseFloat(a, 64)
	nb, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		if na < nb {
			return -1
		}
		if na > nb {
			return 1
		}
		return 0
	}
	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

// matchLike implements SQL LIKE pattern matching (% and _ wildcards).
func matchLike(value, pattern string) bool {
	vLower := strings.ToLower(value)
	pLower := strings.ToLower(pattern)
	return matchLikeHelper(vLower, pLower, 0, 0)
}

func matchLikeHelper(v, p string, vi, pi int) bool {
	for pi < len(p) {
		if p[pi] == '%' {
			pi++
			for vi <= len(v) {
				if matchLikeHelper(v, p, vi, pi) {
					return true
				}
				vi++
			}
			return false
		}
		if vi >= len(v) {
			return false
		}
		if p[pi] == '_' || p[pi] == v[vi] {
			vi++
			pi++
		} else {
			return false
		}
	}
	return vi == len(v)
}

// Regex patterns for WHERE evaluation.
var (
	reIsNull    = regexp.MustCompile(`(?i)(\[?\w+\]?)\s+IS\s+NULL`)
	reIsNotNull = regexp.MustCompile(`(?i)(\[?\w+\]?)\s+IS\s+NOT\s+NULL`)
	reIN        = regexp.MustCompile(`(?i)(\[?\w+\]?)\s+IN\s*\(([^)]+)\)`)
	reBetween   = regexp.MustCompile(`(?i)(\[?\w+\]?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)`)
	reLike      = regexp.MustCompile(`(?i)(\[?\w+\]?)\s+LIKE\s+(.+)`)
)
