package mcp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mark3labs/mcp-go/mcp"
)

// mysqlHandler implements db_query for MySQL and MariaDB engines.
// Connects to the Mori proxy using the MySQL wire protocol.
type mysqlHandler struct {
	dsn string
}

func newMySQLHandler(cfg EngineConfig) *mysqlHandler {
	// Format: user:pass@tcp(host:port)/database
	dsn := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s",
		cfg.User, cfg.Password, cfg.ProxyPort, cfg.DBName)
	return &mysqlHandler{dsn: dsn}
}

func (h *mysqlHandler) handleDBQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	db, err := sql.Open("mysql", h.dsn)
	if err != nil {
		return mcpError(fmt.Sprintf("Error connecting to database: %v", err)), nil
	}
	defer db.Close()

	if isReadQuery(query) {
		return execSQLQuery(connCtx, db, query)
	}
	return execSQLExec(connCtx, db, query)
}
