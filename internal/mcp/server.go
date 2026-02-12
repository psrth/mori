package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Server wraps an MCP server that exposes a db_query tool.
// It connects to the Mori proxy as a regular PostgreSQL client,
// so all queries go through the proxy's routing/merge logic.
type Server struct {
	mcpPort   int
	connStr   string
	httpSrv   *server.StreamableHTTPServer
}

// New creates a new MCP server that will connect to the Mori proxy.
func New(mcpPort, proxyPort int, dbName, user, password string) *Server {
	connStr := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable",
		user, password, proxyPort, dbName)

	return &Server{
		mcpPort: mcpPort,
		connStr: connStr,
	}
}

// ListenAndServe starts the MCP HTTP server. It blocks until the context
// is cancelled or the server is shut down.
func (s *Server) ListenAndServe(ctx context.Context) error {
	mcpSrv := server.NewMCPServer("mori", "0.1.0")

	mcpSrv.AddTool(
		mcp.NewTool("db_query",
			mcp.WithDescription("Execute a SQL query against the database through Mori"),
			mcp.WithString("query",
				mcp.Description("SQL query to execute"),
				mcp.Required(),
			),
		),
		s.handleDBQuery,
	)

	s.httpSrv = server.NewStreamableHTTPServer(mcpSrv)

	addr := fmt.Sprintf("127.0.0.1:%d", s.mcpPort)
	log.Printf("MCP server listening on %s/mcp", addr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.httpSrv.Start(addr)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

// Shutdown gracefully shuts down the MCP HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv != nil {
		return s.httpSrv.Shutdown(ctx)
	}
	return nil
}

// handleDBQuery is the MCP tool handler for db_query. It connects to the
// Mori proxy via pgx, executes the query, and returns results as JSON.
func (s *Server) handleDBQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query, err := request.RequireString("query")
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent("Error: 'query' argument is required")},
			IsError: true,
		}, nil
	}

	query = strings.TrimSpace(query)
	if query == "" {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent("Error: query cannot be empty")},
			IsError: true,
		}, nil
	}

	// Connect to the Mori proxy.
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(connCtx, s.connStr)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent(fmt.Sprintf("Error connecting to database: %v", err))},
			IsError: true,
		}, nil
	}
	defer conn.Close(ctx)

	// Detect if the query is a SELECT/WITH (returns rows) or a mutation.
	upper := strings.ToUpper(strings.TrimSpace(query))
	isRead := strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "SHOW")

	if isRead {
		return s.execQuery(ctx, conn, query)
	}
	return s.execExec(ctx, conn, query)
}

// execQuery handles queries that return rows (SELECT, WITH, etc.).
func (s *Server) execQuery(ctx context.Context, conn *pgx.Conn, query string) (*mcp.CallToolResult, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent(fmt.Sprintf("Query error: %v", err))},
			IsError: true,
		}, nil
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
			return &mcp.CallToolResult{
				Content: []mcp.Content{mcp.NewTextContent(fmt.Sprintf("Error reading row: %v", err))},
				IsError: true,
			}, nil
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent(fmt.Sprintf("Query error: %v", err))},
			IsError: true,
		}, nil
	}

	if results == nil {
		results = []map[string]any{}
	}

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent(fmt.Sprintf("Error marshaling results: %v", err))},
			IsError: true,
		}, nil
	}

	text := fmt.Sprintf("%d row(s) returned\n\n%s", len(results), string(data))
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.NewTextContent(text)},
	}, nil
}

// execExec handles mutations (INSERT, UPDATE, DELETE, DDL, etc.).
func (s *Server) execExec(ctx context.Context, conn *pgx.Conn, query string) (*mcp.CallToolResult, error) {
	tag, err := conn.Exec(ctx, query)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{mcp.NewTextContent(fmt.Sprintf("Query error: %v", err))},
			IsError: true,
		}, nil
	}

	text := fmt.Sprintf("OK: %s", tag.String())
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.NewTextContent(text)},
	}, nil
}
