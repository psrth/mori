//go:build e2e_mssql

package e2e_mssql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// connect returns a *sql.DB connected through the Mori proxy.
func connect(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlserver", proxyDSN)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// connectDirect returns a *sql.DB connected directly to the prod database.
func connectDirect(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlserver", prodDSN)
	if err != nil {
		t.Fatalf("connect to prod: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// mustExec runs Exec through the proxy and fails on error.
func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", truncSQL(query), err)
	}
}

// mustQuery runs a query and returns the rows as []map[string]any.
func mustQuery(t *testing.T, db *sql.DB, query string, args ...any) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("mustQuery(%q): %v", truncSQL(query), err)
	}
	defer rows.Close()
	return collectRows(t, rows)
}

// assertRowCount asserts that COUNT(*) on a table equals expected.
func assertRowCount(t *testing.T, db *sql.DB, table string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var count int
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		t.Fatalf("assertRowCount(%s): %v", table, err)
	}
	if count != expected {
		t.Errorf("assertRowCount(%s): got %d, want %d", table, count, expected)
	}
}

// assertQueryRowCount runs a query and asserts the number of rows returned.
func assertQueryRowCount(t *testing.T, db *sql.DB, expected int, query string, args ...any) {
	t.Helper()
	rows := mustQuery(t, db, query, args...)
	if len(rows) != expected {
		t.Errorf("assertQueryRowCount: got %d rows, want %d (query: %s)", len(rows), expected, truncSQL(query))
	}
}

// assertNoError runs a query and verifies it does not error.
func assertNoError(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Errorf("assertNoError(%q): %v", truncSQL(query), err)
		return
	}
	rows.Close()
}

// assertQueryError runs a query and verifies it returns an error.
func assertQueryError(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return // Got error, as expected.
	}
	// Drain rows to surface deferred errors.
	for rows.Next() {
	}
	if rows.Err() != nil {
		rows.Close()
		return
	}
	rows.Close()
	t.Errorf("assertQueryError(%q): expected error, got nil", truncSQL(query))
}

// queryScalar runs a query that returns a single value.
func queryScalar[T any](t *testing.T, db *sql.DB, query string, args ...any) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var val T
	err := db.QueryRowContext(ctx, query, args...).Scan(&val)
	if err != nil {
		t.Fatalf("queryScalar(%q): %v", truncSQL(query), err)
	}
	return val
}

// collectRows reads all rows into []map[string]any.
func collectRows(t *testing.T, rows *sql.Rows) []map[string]any {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("collectRows columns: %v", err)
	}

	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("collectRows scan: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			val := values[i]
			// Convert []byte to string for easier assertion.
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("collectRows: %v", err)
	}
	return result
}

// truncSQL truncates SQL for error messages.
func truncSQL(s string) string {
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}

// mcpSessionID caches the MCP session ID for reuse across test calls.
var mcpSessionID string

// mcpInitialize performs the MCP initialize handshake if not already done.
func mcpInitialize(t *testing.T) (string, error) {
	t.Helper()
	if mcpSessionID != "" {
		return mcpSessionID, nil
	}

	initReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      0,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":   map[string]any{},
			"clientInfo": map[string]any{
				"name":    "mori-e2e-test",
				"version": "1.0.0",
			},
		},
	}

	bodyBytes, err := json.Marshal(initReq)
	if err != nil {
		return "", fmt.Errorf("marshal init request: %w", err)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/mcp", mcpPort)
	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create init request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("init HTTP request: %w", err)
	}
	defer resp.Body.Close()

	io.ReadAll(resp.Body)

	sessionID := resp.Header.Get("Mcp-Session-Id")
	if sessionID == "" {
		return "", fmt.Errorf("no Mcp-Session-Id in init response (status %d)", resp.StatusCode)
	}

	notifReq := map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	}
	notifBytes, _ := json.Marshal(notifReq)
	notifHTTP, _ := http.NewRequest("POST", url, bytes.NewReader(notifBytes))
	notifHTTP.Header.Set("Content-Type", "application/json")
	notifHTTP.Header.Set("Accept", "application/json, text/event-stream")
	notifHTTP.Header.Set("Mcp-Session-Id", sessionID)
	notifResp, err := client.Do(notifHTTP)
	if err != nil {
		return "", fmt.Errorf("initialized notification: %w", err)
	}
	notifResp.Body.Close()

	mcpSessionID = sessionID
	return sessionID, nil
}

// queryMCP sends a db_query tool call to the MCP server via HTTP.
func queryMCP(t *testing.T, sql string) (string, error) {
	t.Helper()

	sessionID, err := mcpInitialize(t)
	if err != nil {
		return "", fmt.Errorf("MCP initialize: %w", err)
	}

	reqBody := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "db_query",
			"arguments": map[string]any{
				"query": sql,
			},
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/mcp", mcpPort)
	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Session-Id", sessionID)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var rpcResp map[string]any
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return string(respBody), nil
	}

	if errObj, ok := rpcResp["error"]; ok {
		return "", fmt.Errorf("RPC error: %v", errObj)
	}

	result, ok := rpcResp["result"].(map[string]any)
	if !ok {
		return string(respBody), nil
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) == 0 {
		return "", fmt.Errorf("no content in response")
	}
	first, ok := content[0].(map[string]any)
	if !ok {
		return "", fmt.Errorf("unexpected content format")
	}
	text, _ := first["text"].(string)
	isError, _ := result["isError"].(bool)
	if isError {
		return text, fmt.Errorf("MCP error: %s", text)
	}
	return text, nil
}
