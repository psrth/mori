package proxy

import (
	"database/sql"
	"regexp"
	"strings"

	coreSchema "github.com/mori-dev/mori/internal/core/schema"
)

// adaptColumns adjusts Prod column names for schema diffs:
// - Dropped columns are removed
// - Renamed columns use the new name
// - Added columns are appended (with NULL values)
func adaptColumns(reg *coreSchema.Registry, table string, prodCols []string) []string {
	if reg == nil {
		return prodCols
	}
	diff := reg.GetDiff(table)
	if diff == nil {
		return prodCols
	}

	droppedSet := make(map[string]bool, len(diff.Dropped))
	for _, d := range diff.Dropped {
		droppedSet[strings.ToLower(d)] = true
	}

	var result []string
	for _, col := range prodCols {
		if droppedSet[strings.ToLower(col)] {
			continue
		}
		name := col
		if newName, ok := diff.Renamed[col]; ok {
			name = newName
		}
		result = append(result, name)
	}

	// Append added columns (Prod doesn't have them, so they'll be NULL).
	for _, added := range diff.Added {
		result = append(result, added.Name)
	}

	return result
}

// adaptRows adjusts Prod rows for schema diffs:
// - Strip values for dropped columns
// - Append NULLs for added columns
func adaptRows(
	reg *coreSchema.Registry,
	table string,
	origCols []string,
	rows [][]sql.NullString,
	nulls [][]bool,
) ([][]sql.NullString, [][]bool) {
	if reg == nil {
		return rows, nulls
	}
	diff := reg.GetDiff(table)
	if diff == nil {
		return rows, nulls
	}

	droppedSet := make(map[string]bool, len(diff.Dropped))
	for _, d := range diff.Dropped {
		droppedSet[strings.ToLower(d)] = true
	}

	// Build indices of columns to keep.
	var keepIndices []int
	for i, col := range origCols {
		if !droppedSet[strings.ToLower(col)] {
			keepIndices = append(keepIndices, i)
		}
	}

	numAdded := len(diff.Added)
	var adaptedRows [][]sql.NullString
	var adaptedNulls [][]bool

	for i := range rows {
		row := make([]sql.NullString, 0, len(keepIndices)+numAdded)
		rowNulls := make([]bool, 0, len(keepIndices)+numAdded)

		for _, idx := range keepIndices {
			if idx < len(rows[i]) {
				row = append(row, rows[i][idx])
				rowNulls = append(rowNulls, nulls[i][idx])
			}
		}

		// Append NULLs for added columns.
		for range numAdded {
			row = append(row, sql.NullString{})
			rowNulls = append(rowNulls, true)
		}

		adaptedRows = append(adaptedRows, row)
		adaptedNulls = append(adaptedNulls, rowNulls)
	}

	return adaptedRows, adaptedNulls
}

// rewriteSQLForProd rewrites a SQL query to be compatible with the Prod schema.
// It strips shadow-only columns (added columns) from SELECT lists, and handles
// renamed/dropped columns in WHERE clauses using regex-based rewriting.
// Returns (rewrittenSQL, skipProd).
// skipProd=true means the table only exists in Shadow and Prod should be skipped.
func rewriteSQLForProd(sqlStr string, reg *coreSchema.Registry, tables []string) (string, bool) {
	if reg == nil || len(tables) == 0 {
		return sqlStr, false
	}

	for _, table := range tables {
		diff := reg.GetDiff(table)
		if diff == nil {
			continue
		}

		// If the table is new (only in Shadow) or fully shadowed, skip Prod.
		if diff.IsNewTable || diff.IsFullyShadowed {
			return sqlStr, true
		}

		rewritten := sqlStr

		// 1. Strip added columns from SELECT list.
		for _, added := range diff.Added {
			rewritten = stripColumnFromSelect(rewritten, added.Name)
		}

		// 2. Replace renamed columns (new_name -> old_name) so Prod sees old names.
		for oldName, newName := range diff.Renamed {
			rewritten = replaceColumnRef(rewritten, newName, oldName)
		}

		// 3. Strip WHERE conditions referencing added columns.
		for _, added := range diff.Added {
			rewritten = stripWhereCondition(rewritten, added.Name)
		}

		sqlStr = rewritten
	}

	return sqlStr, false
}

// stripColumnFromSelect removes a column from a SELECT list using regex.
// Handles: "SELECT col1, added_col, col2" -> "SELECT col1, col2"
func stripColumnFromSelect(sqlStr, colName string) string {
	// Pattern: column name possibly quoted, with optional alias, in select list
	quotedCol := regexp.QuoteMeta(colName)
	patterns := []string{
		// "col_name," at the beginning/middle of select list
		`(?i),\s*"?` + quotedCol + `"?(?:\s+(?:AS\s+)?\w+)?(?=\s*,|\s+FROM\b)`,
		// "col_name" as the last column before FROM
		`(?i)"?` + quotedCol + `"?(?:\s+(?:AS\s+)?\w+)?\s*,`,
	}

	result := sqlStr
	for _, p := range patterns {
		re := regexp.MustCompile(p)
		result = re.ReplaceAllString(result, "")
	}
	return result
}

// replaceColumnRef replaces references to a column name with another name.
// Used to map renamed columns (new_name -> old_name) for Prod queries.
func replaceColumnRef(sqlStr, fromName, toName string) string {
	// Replace quoted references: "fromName" -> "toName"
	sqlStr = strings.ReplaceAll(sqlStr, `"`+fromName+`"`, `"`+toName+`"`)

	// Replace unquoted references using word boundary matching.
	pattern := `(?i)\b` + regexp.QuoteMeta(fromName) + `\b`
	re := regexp.MustCompile(pattern)
	return re.ReplaceAllString(sqlStr, toName)
}

// stripWhereCondition removes a WHERE condition referencing a specific column.
// This is a best-effort regex approach for simple conditions.
func stripWhereCondition(sqlStr, colName string) string {
	quotedCol := regexp.QuoteMeta(colName)

	// Pattern: "AND col_name = ..." or "col_name = ... AND"
	patterns := []string{
		`(?i)\bAND\s+"?` + quotedCol + `"?\s*(?:=|!=|<>|<|>|<=|>=|IS|LIKE|IN)\s*[^,)]+`,
		`(?i)"?` + quotedCol + `"?\s*(?:=|!=|<>|<|>|<=|>=|IS|LIKE|IN)\s*[^,)]+\s*AND\b`,
	}

	result := sqlStr
	for _, p := range patterns {
		re := regexp.MustCompile(p)
		result = re.ReplaceAllString(result, "")
	}
	return result
}
