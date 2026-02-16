package mcp

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/mark3labs/mcp-go/mcp"
)

// mssqlHandler implements db_query for MSSQL engines.
// Connects to the Mori proxy using the TDS protocol.
type mssqlHandler struct {
	connStr string
}

func newMSSQLHandler(cfg EngineConfig) *mssqlHandler {
	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   fmt.Sprintf("127.0.0.1:%d", cfg.ProxyPort),
	}
	q := u.Query()
	if cfg.DBName != "" {
		q.Set("database", cfg.DBName)
	}
	u.RawQuery = q.Encode()
	return &mssqlHandler{connStr: u.String()}
}

func (h *mssqlHandler) handleDBQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	db, err := sql.Open("sqlserver", h.connStr)
	if err != nil {
		return mcpError(fmt.Sprintf("Error connecting to database: %v", err)), nil
	}
	defer db.Close()

	if isReadQuery(query) {
		return execSQLQuery(connCtx, db, query)
	}
	return execSQLExec(connCtx, db, query)
}
