package proxy

import (
	"regexp"
	"strings"

	coreSchema "github.com/psrth/mori/internal/core/schema"
)

// rewriteForProd rewrites a SQL query to be compatible with the Prod schema.
// It strips shadow-only columns (added via DDL) from SELECT, WHERE, ORDER BY,
// GROUP BY, and HAVING clauses using regex-based rewriting.
//
// Returns (rewrittenSQL, skipProd). skipProd is true if the query is
// irrelevant for Prod (e.g., WHERE on shadow-only column with non-NULL comparison).
func rewriteForProd(sql string, registry *coreSchema.Registry, tables []string) (string, bool) {
	if registry == nil {
		return sql, false
	}

	// Collect all schema changes across the query's tables.
	addedCols := collectAddedCols(registry, tables)
	droppedCols := collectDroppedCols(registry, tables)
	renamedCols := collectRenamedCols(registry, tables) // old_name -> new_name

	if len(addedCols) == 0 && len(droppedCols) == 0 && len(renamedCols) == 0 {
		return sql, false
	}

	result := sql
	modified := false

	// 1. Handle renamed columns: replace new names with old names for Prod queries.
	for oldName, newName := range renamedCols {
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(newName) + `\b`)
		newResult := re.ReplaceAllString(result, oldName)
		if newResult != result {
			result = newResult
			modified = true
		}
	}

	// 2. Handle added columns: strip from SELECT, WHERE, ORDER BY, GROUP BY.
	for addedCol := range addedCols {
		// Strip from SELECT list.
		reSelect := regexp.MustCompile(`(?i),\s*"?` + regexp.QuoteMeta(addedCol) + `"?(?:\s+AS\s+\w+)?(?=\s*(?:,|\bFROM\b))`)
		newResult := reSelect.ReplaceAllString(result, "")
		if newResult != result {
			result = newResult
			modified = true
		}
		// Also strip if it's the first column before a comma.
		reSelectFirst := regexp.MustCompile(`(?i)\bSELECT\s+(DISTINCT\s+)?"?` + regexp.QuoteMeta(addedCol) + `"?(?:\s+AS\s+\w+)?\s*,`)
		if m := reSelectFirst.FindString(result); m != "" {
			replacement := "SELECT "
			if strings.Contains(strings.ToUpper(m), "DISTINCT") {
				replacement = "SELECT DISTINCT "
			}
			result = reSelectFirst.ReplaceAllString(result, replacement)
			modified = true
		}

		// Strip from ORDER BY.
		reOrder := regexp.MustCompile(`(?i),\s*"?` + regexp.QuoteMeta(addedCol) + `"?(?:\s+(?:ASC|DESC))?(?=\s*(?:,|\bLIMIT\b|$|;))`)
		newResult = reOrder.ReplaceAllString(result, "")
		if newResult != result {
			result = newResult
			modified = true
		}
		reOrderFirst := regexp.MustCompile(`(?i)ORDER\s+BY\s+"?` + regexp.QuoteMeta(addedCol) + `"?(?:\s+(?:ASC|DESC))?\s*,\s*`)
		newResult = reOrderFirst.ReplaceAllString(result, "ORDER BY ")
		if newResult != result {
			result = newResult
			modified = true
		}
		// If it's the only ORDER BY column, remove the entire clause.
		reOrderSolo := regexp.MustCompile(`(?i)\s+ORDER\s+BY\s+"?` + regexp.QuoteMeta(addedCol) + `"?(?:\s+(?:ASC|DESC))?\s*(?=\bLIMIT\b|$|;)`)
		newResult = reOrderSolo.ReplaceAllString(result, " ")
		if newResult != result {
			result = newResult
			modified = true
		}

		// Strip from GROUP BY.
		reGroup := regexp.MustCompile(`(?i),\s*"?` + regexp.QuoteMeta(addedCol) + `"?(?=\s*(?:,|\bHAVING\b|\bORDER\b|\bLIMIT\b|$|;))`)
		newResult = reGroup.ReplaceAllString(result, "")
		if newResult != result {
			result = newResult
			modified = true
		}

		// Strip WHERE conditions on added columns.
		// Simple cases: added_col = value, added_col IS NOT NULL -> these make query skip Prod.
		reWhereEq := regexp.MustCompile(`(?i)\b"?` + regexp.QuoteMeta(addedCol) + `"?\s*(?:=|!=|<>|<|>|<=|>=|LIKE|IN)\s*(?:'[^']*'|\d+|\([^)]*\))`)
		if reWhereEq.MatchString(result) {
			// A comparison on an added column means Prod rows can't match — skip Prod.
			return result, true
		}
		reWhereNotNull := regexp.MustCompile(`(?i)\b"?` + regexp.QuoteMeta(addedCol) + `"?\s+IS\s+NOT\s+NULL`)
		if reWhereNotNull.MatchString(result) {
			return result, true
		}

		// added_col IS NULL is vacuously true for Prod — strip it.
		reWhereNull := regexp.MustCompile(`(?i)\s+AND\s+"?` + regexp.QuoteMeta(addedCol) + `"?\s+IS\s+NULL`)
		newResult = reWhereNull.ReplaceAllString(result, "")
		if newResult != result {
			result = newResult
			modified = true
		}
		reWhereNullFirst := regexp.MustCompile(`(?i)"?` + regexp.QuoteMeta(addedCol) + `"?\s+IS\s+NULL\s+AND\s+`)
		newResult = reWhereNullFirst.ReplaceAllString(result, "")
		if newResult != result {
			result = newResult
			modified = true
		}
	}

	// 3. Handle dropped columns: remove from Prod query entirely.
	for _, droppedCol := range droppedCols {
		// Strip from SELECT list.
		reSelect := regexp.MustCompile(`(?i),\s*"?` + regexp.QuoteMeta(droppedCol) + `"?(?:\s+AS\s+\w+)?(?=\s*(?:,|\bFROM\b))`)
		newResult := reSelect.ReplaceAllString(result, "")
		if newResult != result {
			result = newResult
			modified = true
		}
	}

	if !modified {
		return sql, false
	}

	return result, false
}

// collectAddedCols builds a set of lowercase column names that were added
// via DDL in Shadow (and don't exist in Prod) across all given tables.
func collectAddedCols(registry *coreSchema.Registry, tables []string) map[string]bool {
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

// collectDroppedCols builds a slice of dropped column names across all given tables.
func collectDroppedCols(registry *coreSchema.Registry, tables []string) []string {
	var dropped []string
	for _, t := range tables {
		diff := registry.GetDiff(t)
		if diff == nil {
			continue
		}
		dropped = append(dropped, diff.Dropped...)
	}
	return dropped
}

// collectRenamedCols builds a map of old_name -> new_name across all given tables.
func collectRenamedCols(registry *coreSchema.Registry, tables []string) map[string]string {
	renamed := make(map[string]string)
	for _, t := range tables {
		diff := registry.GetDiff(t)
		if diff == nil {
			continue
		}
		for old, nw := range diff.Renamed {
			renamed[old] = nw
		}
	}
	return renamed
}
