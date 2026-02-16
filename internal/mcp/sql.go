package mcp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

// execSQLQuery handles queries that return rows via database/sql.
func execSQLQuery(ctx context.Context, db *sql.DB, query string) (*mcp.CallToolResult, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return mcpError(fmt.Sprintf("Query error: %v", err)), nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return mcpError(fmt.Sprintf("Error reading columns: %v", err)), nil
	}

	var results []map[string]any
	for rows.Next() {
		// Create a slice of interface{} to scan into.
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return mcpError(fmt.Sprintf("Error reading row: %v", err)), nil
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for JSON serialization.
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return mcpError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return formatRows(results)
}

// execSQLExec handles mutations via database/sql.
func execSQLExec(ctx context.Context, db *sql.DB, query string) (*mcp.CallToolResult, error) {
	result, err := db.ExecContext(ctx, query)
	if err != nil {
		return mcpError(fmt.Sprintf("Query error: %v", err)), nil
	}

	affected, _ := result.RowsAffected()
	return mcpOK(fmt.Sprintf("OK: %d row(s) affected", affected)), nil
}
