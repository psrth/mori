package proxy

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// evaluateWhere evaluates a WHERE clause string against a single row.
// row maps column names to string values; nulls maps column names to null flags.
// Returns true if the row satisfies the condition.
// Returns true on any evaluation failure (safe fallback: keep the row).
func evaluateWhere(condition string, row map[string]string, nulls map[string]bool) bool {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return true
	}
	e := &whereEval{row: row, nulls: nulls}
	result, ok := e.eval(condition)
	if !ok {
		return true // safe fallback
	}
	return result
}

// evaluateWhereViaDuckDB uses DuckDB to evaluate a WHERE clause on a row.
// Falls back to this when regex parsing can't handle the expression.
func evaluateWhereViaDuckDB(condition string, row map[string]string, nulls map[string]bool, shadowDB *sql.DB) bool {
	if shadowDB == nil {
		return true
	}
	// Build a SELECT with literal column values.
	var setClauses []string
	for col, val := range row {
		if nulls[col] {
			setClauses = append(setClauses, fmt.Sprintf("NULL AS %q", col))
		} else {
			setClauses = append(setClauses, fmt.Sprintf("'%s' AS %q", strings.ReplaceAll(val, "'", "''"), col))
		}
	}
	if len(setClauses) == 0 {
		return true
	}
	query := fmt.Sprintf("SELECT 1 FROM (SELECT %s) _t WHERE %s",
		strings.Join(setClauses, ", "), condition)
	var one sql.NullString
	if err := shadowDB.QueryRow(query).Scan(&one); err != nil {
		return true // safe fallback
	}
	return true
}

// filterByWhere removes rows from merged results that do not satisfy the WHERE clause.
// Only evaluates rows at indices listed in deltaIndices (patched rows).
// Clean rows pass through unfiltered.
func filterByWhere(
	whereClause string,
	columns []string,
	rows [][]sql.NullString,
	rowNulls [][]bool,
	deltaIndices map[int]bool,
	shadowDB *sql.DB,
) ([][]sql.NullString, [][]bool) {
	if whereClause == "" || len(deltaIndices) == 0 {
		return rows, rowNulls
	}

	var filteredRows [][]sql.NullString
	var filteredNulls [][]bool

	for i, row := range rows {
		if !deltaIndices[i] {
			// Clean row — pass through.
			filteredRows = append(filteredRows, row)
			if i < len(rowNulls) {
				filteredNulls = append(filteredNulls, rowNulls[i])
			}
			continue
		}

		// Build row map for evaluation.
		rowMap := make(map[string]string, len(columns))
		nullMap := make(map[string]bool, len(columns))
		for j, col := range columns {
			if j < len(row) {
				if (i < len(rowNulls) && j < len(rowNulls[i]) && rowNulls[i][j]) || !row[j].Valid {
					nullMap[col] = true
				} else {
					rowMap[col] = row[j].String
				}
			}
		}

		// Try regex-based evaluation first.
		result, ok := (&whereEval{row: rowMap, nulls: nullMap}).eval(whereClause)
		if !ok {
			// Fallback to DuckDB evaluation.
			result = evaluateWhereViaDuckDB(whereClause, rowMap, nullMap, shadowDB)
		}

		if result {
			filteredRows = append(filteredRows, row)
			if i < len(rowNulls) {
				filteredNulls = append(filteredNulls, rowNulls[i])
			}
		}
	}

	return filteredRows, filteredNulls
}

// whereEval is the internal evaluation context.
type whereEval struct {
	row   map[string]string
	nulls map[string]bool
}

// eval evaluates a WHERE clause expression. Returns (result, ok).
// ok=false means the expression couldn't be parsed.
func (e *whereEval) eval(expr string) (bool, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, true
	}

	// Strip outer parentheses.
	expr = stripOuterParens(expr)

	// Handle NOT prefix.
	upper := strings.ToUpper(strings.TrimSpace(expr))
	if strings.HasPrefix(upper, "NOT ") {
		inner := strings.TrimSpace(expr[4:])
		result, ok := e.eval(inner)
		if !ok {
			return true, false
		}
		return !result, true
	}

	// Split by OR (lowest precedence).
	if parts, ok := splitBoolOp(expr, "OR"); ok && len(parts) > 1 {
		for _, part := range parts {
			result, evalOK := e.eval(part)
			if !evalOK {
				return true, false
			}
			if result {
				return true, true
			}
		}
		return false, true
	}

	// Split by AND.
	if parts, ok := splitBoolOp(expr, "AND"); ok && len(parts) > 1 {
		for _, part := range parts {
			result, evalOK := e.eval(part)
			if !evalOK {
				return true, false
			}
			if !result {
				return false, true
			}
		}
		return true, true
	}

	// Try specific patterns.
	if result, ok := e.evalIsNull(expr); ok {
		return result, true
	}
	if result, ok := e.evalBetween(expr); ok {
		return result, true
	}
	if result, ok := e.evalIn(expr); ok {
		return result, true
	}
	if result, ok := e.evalLike(expr); ok {
		return result, true
	}
	if result, ok := e.evalComparison(expr); ok {
		return result, true
	}

	return true, false // can't parse — safe fallback
}

// evalIsNull handles "col IS NULL" and "col IS NOT NULL".
func (e *whereEval) evalIsNull(expr string) (bool, bool) {
	upper := strings.ToUpper(strings.TrimSpace(expr))

	reIsNotNull := regexp.MustCompile(`(?i)^(.+?)\s+IS\s+NOT\s+NULL$`)
	if m := reIsNotNull.FindStringSubmatch(expr); len(m) > 1 {
		col := resolveColName(strings.TrimSpace(m[1]))
		if e.nulls[col] {
			return false, true
		}
		_, exists := e.row[col]
		return exists, true
	}

	reIsNull := regexp.MustCompile(`(?i)^(.+?)\s+IS\s+NULL$`)
	if m := reIsNull.FindStringSubmatch(expr); len(m) > 1 {
		col := resolveColName(strings.TrimSpace(m[1]))
		if e.nulls[col] {
			return true, true
		}
		_, exists := e.row[col]
		return !exists, true
	}

	_ = upper
	return false, false
}

// evalBetween handles "col BETWEEN a AND b" and "col NOT BETWEEN a AND b".
func (e *whereEval) evalBetween(expr string) (bool, bool) {
	reBetween := regexp.MustCompile(`(?i)^(.+?)\s+(NOT\s+)?BETWEEN\s+(.+?)\s+AND\s+(.+?)$`)
	m := reBetween.FindStringSubmatch(expr)
	if len(m) < 5 {
		return false, false
	}

	colVal, colOK := e.resolveValue(strings.TrimSpace(m[1]))
	lowVal, lowOK := e.resolveValue(strings.TrimSpace(m[3]))
	highVal, highOK := e.resolveValue(strings.TrimSpace(m[4]))
	if !colOK || !lowOK || !highOK {
		return true, false
	}

	negated := strings.TrimSpace(m[2]) != ""
	inRange := compareValues(colVal, lowVal) >= 0 && compareValues(colVal, highVal) <= 0
	if negated {
		return !inRange, true
	}
	return inRange, true
}

// evalIn handles "col IN (val1, val2, ...)".
func (e *whereEval) evalIn(expr string) (bool, bool) {
	reIn := regexp.MustCompile(`(?i)^(.+?)\s+(NOT\s+)?IN\s*\((.+)\)$`)
	m := reIn.FindStringSubmatch(expr)
	if len(m) < 4 {
		return false, false
	}

	colVal, colOK := e.resolveValue(strings.TrimSpace(m[1]))
	if !colOK {
		return true, false
	}

	negated := strings.TrimSpace(m[2]) != ""
	items := splitValueList(m[3])

	for _, item := range items {
		itemVal, itemOK := e.resolveValue(strings.TrimSpace(item))
		if !itemOK {
			continue
		}
		if strings.EqualFold(colVal, itemVal) {
			return !negated, true
		}
	}
	return negated, true
}

// evalLike handles "col LIKE pattern" and "col ILIKE pattern".
func (e *whereEval) evalLike(expr string) (bool, bool) {
	reLike := regexp.MustCompile(`(?i)^(.+?)\s+(NOT\s+)?(I?LIKE)\s+(.+)$`)
	m := reLike.FindStringSubmatch(expr)
	if len(m) < 5 {
		return false, false
	}

	colVal, colOK := e.resolveValue(strings.TrimSpace(m[1]))
	patternVal, patOK := e.resolveValue(strings.TrimSpace(m[4]))
	if !colOK || !patOK {
		return true, false
	}

	negated := strings.TrimSpace(m[2]) != ""
	caseInsensitive := strings.EqualFold(m[3], "ILIKE")

	matched := matchLike(colVal, patternVal, caseInsensitive)
	if negated {
		return !matched, true
	}
	return matched, true
}

// evalComparison handles binary comparisons: =, !=, <>, <, >, <=, >=.
func (e *whereEval) evalComparison(expr string) (bool, bool) {
	// Order matters: check multi-char operators first.
	for _, op := range []string{"!=", "<>", "<=", ">=", "=", "<", ">"} {
		idx := findOperator(expr, op)
		if idx < 0 {
			continue
		}

		left := strings.TrimSpace(expr[:idx])
		right := strings.TrimSpace(expr[idx+len(op):])

		leftVal, leftOK := e.resolveValue(left)
		rightVal, rightOK := e.resolveValue(right)
		if !leftOK || !rightOK {
			return true, false
		}

		cmp := compareValues(leftVal, rightVal)
		switch op {
		case "=":
			return cmp == 0, true
		case "!=", "<>":
			return cmp != 0, true
		case "<":
			return cmp < 0, true
		case ">":
			return cmp > 0, true
		case "<=":
			return cmp <= 0, true
		case ">=":
			return cmp >= 0, true
		}
	}
	return false, false
}

// resolveValue resolves a token to its string value.
// Handles column references, string literals, numeric literals, and NULL.
func (e *whereEval) resolveValue(token string) (string, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}

	upper := strings.ToUpper(token)
	if upper == "NULL" {
		return "", false // NULL can't be compared
	}
	if upper == "TRUE" {
		return "t", true
	}
	if upper == "FALSE" {
		return "f", true
	}

	// String literal.
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		return token[1 : len(token)-1], true
	}

	// Numeric literal.
	if _, err := strconv.ParseFloat(token, 64); err == nil {
		return token, true
	}

	// Column reference.
	col := resolveColName(token)
	if e.nulls[col] {
		return "", false // NULL
	}
	if val, ok := e.row[col]; ok {
		return val, true
	}

	// Try case-insensitive column lookup.
	lowerCol := strings.ToLower(col)
	for k, v := range e.row {
		if strings.ToLower(k) == lowerCol {
			return v, true
		}
	}

	return "", false
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

// resolveColName strips surrounding quotes from a column reference.
func resolveColName(token string) string {
	token = strings.TrimSpace(token)
	token = strings.Trim(token, `"`)
	// Handle qualified names: table.column or "table"."column"
	if idx := strings.LastIndex(token, "."); idx >= 0 {
		token = token[idx+1:]
	}
	return strings.Trim(token, `"`)
}

// compareValues compares two string values numerically if possible, else lexicographically.
func compareValues(a, b string) int {
	af, aErr := strconv.ParseFloat(a, 64)
	bf, bErr := strconv.ParseFloat(b, 64)
	if aErr == nil && bErr == nil {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	// Normalize booleans.
	a = normBool(a)
	b = normBool(b)
	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

// normBool normalizes boolean-like strings.
func normBool(s string) string {
	switch strings.ToLower(s) {
	case "t", "true", "1", "yes", "on":
		return "t"
	case "f", "false", "0", "no", "off":
		return "f"
	}
	return s
}

// matchLike performs SQL LIKE pattern matching with % and _ wildcards.
func matchLike(str, pattern string, caseInsensitive bool) bool {
	if caseInsensitive {
		str = strings.ToLower(str)
		pattern = strings.ToLower(pattern)
	}
	return matchLikeAt(str, pattern, 0, 0)
}

func matchLikeAt(str, pattern string, si, pi int) bool {
	for pi < len(pattern) {
		if si >= len(str) {
			// Remaining pattern must be all '%'.
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
			// Skip consecutive '%'.
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi >= len(pattern) {
				return true
			}
			// Try matching rest of pattern at every position.
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

// splitBoolOp splits an expression by a boolean operator (AND/OR) at the top level
// (respecting parentheses). Returns parts and true if split found.
func splitBoolOp(expr, op string) ([]string, bool) {
	upper := strings.ToUpper(expr)
	opLen := len(op)

	var parts []string
	depth := 0
	start := 0
	inQuote := false

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' && !inQuote {
			inQuote = true
			continue
		}
		if ch == '\'' && inQuote {
			inQuote = false
			continue
		}
		if inQuote {
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		if depth == 0 && i+opLen+2 <= len(upper) {
			// Check for " OP " (with spaces).
			candidate := upper[i : i+opLen+2]
			if candidate == " "+op+" " {
				parts = append(parts, strings.TrimSpace(expr[start:i]))
				start = i + opLen + 2
				i += opLen + 1
			}
		}
	}

	if len(parts) > 0 {
		parts = append(parts, strings.TrimSpace(expr[start:]))
		return parts, true
	}
	return nil, false
}

// stripOuterParens strips a single level of outer parentheses if they wrap the whole expression.
func stripOuterParens(expr string) string {
	expr = strings.TrimSpace(expr)
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return expr
	}
	// Verify the closing paren matches the opening one.
	depth := 0
	for i, ch := range expr {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		if depth == 0 && i < len(expr)-1 {
			return expr // closing paren matches something in the middle
		}
	}
	return strings.TrimSpace(expr[1 : len(expr)-1])
}

// findOperator finds the position of a comparison operator at the top level (not inside parens/quotes).
func findOperator(expr, op string) int {
	depth := 0
	inQuote := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		}
		if depth == 0 && i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
			// Ensure it's not part of a longer operator (e.g., "=" shouldn't match "<=").
			if op == "=" && i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
				continue
			}
			if op == "<" && i+1 < len(expr) && (expr[i+1] == '=' || expr[i+1] == '>') {
				continue
			}
			if op == ">" && i+1 < len(expr) && expr[i+1] == '=' {
				continue
			}
			return i
		}
	}
	return -1
}

// splitValueList splits a comma-separated value list respecting quotes.
func splitValueList(s string) []string {
	var parts []string
	depth := 0
	inQuote := false
	start := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' {
			inQuote = !inQuote
		}
		if !inQuote {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			if ch == ',' && depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

// extractWhereClause extracts the WHERE clause from a SQL string.
// Returns the condition text after WHERE, or "" if no WHERE clause.
func extractWhereClause(sqlStr string) string {
	upper := strings.ToUpper(sqlStr)
	idx := strings.Index(upper, " WHERE ")
	if idx < 0 {
		return ""
	}
	rest := sqlStr[idx+7:]
	// Truncate at GROUP BY, ORDER BY, LIMIT, HAVING, etc.
	for _, kw := range []string{"GROUP BY", "ORDER BY", "LIMIT", "HAVING", "UNION", "INTERSECT", "EXCEPT", "QUALIFY"} {
		if ki := strings.Index(strings.ToUpper(rest), kw); ki >= 0 {
			rest = rest[:ki]
		}
	}
	return strings.TrimSpace(rest)
}
