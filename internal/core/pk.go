package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// SerializeCompositePK creates a deterministic string key from composite PK values.
// For single-column PKs it returns the value directly; for composite it returns
// a JSON object like {"col1":"val1","col2":"val2"}.
func SerializeCompositePK(pkCols []string, values []string) string {
	if len(pkCols) == 1 {
		return values[0]
	}
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		parts[i] = fmt.Sprintf("%q:%q", col, values[i])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// DeserializeCompositePK parses a serialized PK string back into individual
// column→value pairs. For single-column PKs the raw string is returned as-is.
func DeserializeCompositePK(pk string, pkColumns []string) map[string]string {
	if len(pkColumns) == 1 {
		return map[string]string{pkColumns[0]: pk}
	}
	out := make(map[string]string, len(pkColumns))
	if err := json.Unmarshal([]byte(pk), &out); err != nil {
		// Fallback: treat as single value for the first column.
		out[pkColumns[0]] = pk
	}
	return out
}

// BuildCompositeWHERE builds a WHERE clause fragment like
// "col1" = 'val1' AND "col2" = 'val2' using the provided quoting functions.
func BuildCompositeWHERE(pkColumns []string, pkValues map[string]string, quoteIdent, quoteLiteral func(string) string) string {
	parts := make([]string, 0, len(pkColumns))
	for _, col := range pkColumns {
		val := pkValues[col]
		parts = append(parts, fmt.Sprintf("%s = %s", quoteIdent(col), quoteLiteral(val)))
	}
	return strings.Join(parts, " AND ")
}
