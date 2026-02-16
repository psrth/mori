package mcp

import (
	"context"
	"fmt"
	"log"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// EngineConfig holds engine-specific connection parameters for the MCP server.
type EngineConfig struct {
	Engine    string // engine ID from config: "postgres", "mysql", "redis", etc.
	ProxyPort int    // port the Mori proxy is listening on
	DBName    string
	User      string
	Password  string
}

// Server wraps an MCP server that exposes engine-appropriate tools.
// It connects to the Mori proxy as a regular database client,
// so all queries go through the proxy's routing/merge logic.
type Server struct {
	mcpPort int
	cfg     EngineConfig
	httpSrv *server.StreamableHTTPServer
}

// New creates a new MCP server for the given engine configuration.
func New(mcpPort int, cfg EngineConfig) *Server {
	return &Server{
		mcpPort: mcpPort,
		cfg:     cfg,
	}
}

// ListenAndServe starts the MCP HTTP server. It blocks until the context
// is cancelled or the server is shut down.
func (s *Server) ListenAndServe(ctx context.Context) error {
	mcpSrv := server.NewMCPServer("mori", "0.1.0")

	s.registerTools(mcpSrv)

	s.httpSrv = server.NewStreamableHTTPServer(mcpSrv)

	addr := fmt.Sprintf("127.0.0.1:%d", s.mcpPort)
	log.Printf("MCP server listening on %s/mcp (engine: %s)", addr, s.cfg.Engine)

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

// registerTools registers engine-appropriate MCP tools on the server.
func (s *Server) registerTools(mcpSrv *server.MCPServer) {
	switch s.cfg.Engine {
	case "redis":
		s.registerRedisTools(mcpSrv)
	case "firestore":
		s.registerFirestoreTools(mcpSrv)
	default:
		// All SQL engines get db_query. The connector varies by protocol.
		s.registerSQLTools(mcpSrv)
	}
}

// registerSQLTools registers the db_query tool for SQL engines.
// Uses pgx for pgwire engines (postgres, cockroachdb, sqlite, duckdb) and
// database/sql for MySQL wire and TDS engines (mysql, mariadb, mssql).
func (s *Server) registerSQLTools(mcpSrv *server.MCPServer) {
	var handler func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	switch s.cfg.Engine {
	case "mysql", "mariadb":
		h := newMySQLHandler(s.cfg)
		handler = h.handleDBQuery
	case "mssql":
		h := newMSSQLHandler(s.cfg)
		handler = h.handleDBQuery
	default:
		// postgres, cockroachdb, sqlite, duckdb — all speak pgwire
		h := newPgwireHandler(s.cfg)
		handler = h.handleDBQuery
	}

	mcpSrv.AddTool(
		mcp.NewTool("db_query",
			mcp.WithDescription("Execute a SQL query against the database through Mori"),
			mcp.WithString("query",
				mcp.Description("SQL query to execute"),
				mcp.Required(),
			),
		),
		handler,
	)
}
