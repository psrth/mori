//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// connect returns a pgx.Conn connected through the Mori proxy.
func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, proxyDSN)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// connectDirect returns a pgx.Conn connected directly to the Prod database.
func connectDirect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, prodDSN)
	if err != nil {
		t.Fatalf("connect to prod: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// mustExec runs an Exec through the proxy and fails on error.
func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", truncSQL(sql), err)
	}
}

// mustQuery runs a query and returns the rows as []map[string]any.
func mustQuery(t *testing.T, conn *pgx.Conn, sql string, args ...any) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("mustQuery(%q): %v", truncSQL(sql), err)
	}
	defer rows.Close()
	return collectRows(t, rows)
}

// assertRowCount asserts that COUNT(*) on a table equals expected.
func assertRowCount(t *testing.T, conn *pgx.Conn, table string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var count int
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		t.Fatalf("assertRowCount(%s): %v", table, err)
	}
	if count != expected {
		t.Errorf("assertRowCount(%s): got %d, want %d", table, count, expected)
	}
}

// assertQueryRowCount runs a query and asserts the number of rows returned.
func assertQueryRowCount(t *testing.T, conn *pgx.Conn, expected int, sql string, args ...any) {
	t.Helper()
	rows := mustQuery(t, conn, sql, args...)
	if len(rows) != expected {
		t.Errorf("assertQueryRowCount: got %d rows, want %d (query: %s)", len(rows), expected, truncSQL(sql))
	}
}

// assertExecRowsAffected runs Exec and asserts rows affected.
func assertExecRowsAffected(t *testing.T, conn *pgx.Conn, expected int64, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tag, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		t.Fatalf("assertExecRowsAffected(%q): %v", truncSQL(sql), err)
	}
	if tag.RowsAffected() != expected {
		t.Errorf("assertExecRowsAffected(%q): got %d, want %d", truncSQL(sql), tag.RowsAffected(), expected)
	}
}

// assertNoError runs a query and verifies it doesn't error.
func assertNoError(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		t.Errorf("assertNoError(%q): %v", truncSQL(sql), err)
		return
	}
	rows.Close()
}

// assertQueryError runs a query and verifies it returns an error.
func assertQueryError(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sql, args...)
	if err == nil {
		rows.Close()
		t.Errorf("assertQueryError(%q): expected error, got nil", truncSQL(sql))
	}
}

// queryScalar runs a query that returns a single value.
func queryScalar[T any](t *testing.T, conn *pgx.Conn, sql string, args ...any) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var val T
	err := conn.QueryRow(ctx, sql, args...).Scan(&val)
	if err != nil {
		t.Fatalf("queryScalar(%q): %v", truncSQL(sql), err)
	}
	return val
}

// collectRows reads all rows into []map[string]any.
func collectRows(t *testing.T, rows pgx.Rows) []map[string]any {
	t.Helper()
	var result []map[string]any
	cols := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("collectRows: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, fd := range cols {
			row[fd.Name] = values[i]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("collectRows: %v", err)
	}
	return result
}

// truncSQL truncates SQL for error messages.
func truncSQL(sql string) string {
	if len(sql) > 80 {
		return sql[:77] + "..."
	}
	return sql
}

// queryMCP sends a db_query tool call to the MCP server via HTTP.
// Returns the text content from the response.
func queryMCP(t *testing.T, sql string) (string, error) {
	t.Helper()

	// Build MCP JSON-RPC request for the tools/call method.
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

	// Parse the JSON-RPC response.
	var rpcResp map[string]any
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		// It might be SSE format; try to extract JSON from it.
		return string(respBody), nil
	}

	if errObj, ok := rpcResp["error"]; ok {
		return "", fmt.Errorf("RPC error: %v", errObj)
	}

	// Extract text from result.content[0].text
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
