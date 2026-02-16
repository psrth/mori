package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mark3labs/mcp-go/mcp"
)

// pgwireHandler implements db_query for pgwire-compatible engines:
// PostgreSQL, CockroachDB, SQLite, DuckDB.
type pgwireHandler struct {
	connStr string
}

func newPgwireHandler(cfg EngineConfig) *pgwireHandler {
	connStr := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.ProxyPort, cfg.DBName)
	return &pgwireHandler{connStr: connStr}
}

func (h *pgwireHandler) handleDBQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query, err := request.RequireString("query")
	if err != nil {
		return mcpError("Error: 'query' argument is required"), nil
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return mcpError("Error: query cannot be empty"), nil
	}

	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(connCtx, h.connStr)
	if err != nil {
		return mcpError(fmt.Sprintf("Error connecting to database: %v", err)), nil
	}
	defer conn.Close(ctx)

	if isReadQuery(query) {
		return h.execQuery(ctx, conn, query)
	}
	return h.execExec(ctx, conn, query)
}

func (h *pgwireHandler) execQuery(ctx context.Context, conn *pgx.Conn, query string) (*mcp.CallToolResult, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return mcpError(fmt.Sprintf("Query error: %v", err)), nil
	}
	defer rows.Close()

	columns := make([]string, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		columns[i] = fd.Name
	}

	var results []map[string]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return mcpError(fmt.Sprintf("Error reading row: %v", err)), nil
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return mcpError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return formatRows(results)
}

func (h *pgwireHandler) execExec(ctx context.Context, conn *pgx.Conn, query string) (*mcp.CallToolResult, error) {
	tag, err := conn.Exec(ctx, query)
	if err != nil {
		return mcpError(fmt.Sprintf("Query error: %v", err)), nil
	}
	return mcpOK(fmt.Sprintf("OK: %s", tag.String())), nil
}

// isReadQuery returns true for queries expected to return rows.
func isReadQuery(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "SHOW")
}

// mcpError returns an MCP error result.
func mcpError(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.NewTextContent(msg)},
		IsError: true,
	}
}

// mcpOK returns an MCP success result.
func mcpOK(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.NewTextContent(msg)},
	}
}

// getOptionalInt reads an optional numeric parameter from an MCP request.
// Returns 0 if the parameter is missing or not a number.
func getOptionalInt(request mcp.CallToolRequest, name string) int {
	args := request.GetArguments()
	v, ok := args[name]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return 0
	}
}

// formatRows marshals query results to JSON and returns an MCP result.
func formatRows(results []map[string]any) (*mcp.CallToolResult, error) {
	if results == nil {
		results = []map[string]any{}
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return mcpError(fmt.Sprintf("Error marshaling results: %v", err)), nil
	}
	text := fmt.Sprintf("%d row(s) returned\n\n%s", len(results), string(data))
	return mcpOK(text), nil
}
