package merge

import "github.com/mori-dev/mori/internal/core"

// Empty returns true if the result set has no rows.
func Empty(rs *core.ResultSet) bool {
	return rs == nil || len(rs.Rows) == 0
}

// RowCount returns the number of rows in the result set.
func RowCount(rs *core.ResultSet) int {
	if rs == nil {
		return 0
	}
	return len(rs.Rows)
}
