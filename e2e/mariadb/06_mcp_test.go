//go:build e2e_mariadb

package e2e_mariadb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

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
				"name":    "mori-e2e-mariadb-test",
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

	// Send initialized notification.
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

func TestMCP(t *testing.T) {
	t.Run("select_query_returns_rows", func(t *testing.T) {
		result, err := queryMCP(t, "SELECT id, name FROM roles LIMIT 3")
		if err != nil {
			t.Fatalf("MCP query: %v", err)
		}
		if !strings.Contains(result, "row(s) returned") {
			t.Errorf("expected row count in response, got: %s", truncSQL(result))
		}
		t.Logf("MCP SELECT result (first 200 chars): %.200s", result)
	})

	t.Run("select_with_where", func(t *testing.T) {
		result, err := queryMCP(t, "SELECT username FROM users WHERE id = 1")
		if err != nil {
			t.Fatalf("MCP query: %v", err)
		}
		if !strings.Contains(result, "user_1") {
			t.Errorf("expected user_1 in result, got: %s", truncSQL(result))
		}
	})

	t.Run("insert_via_mcp", func(t *testing.T) {
		result, err := queryMCP(t, "INSERT INTO settings (`key`, value) VALUES ('mcp-tag', 'mcp-test')")
		if err != nil {
			t.Fatalf("MCP INSERT: %v", err)
		}
		if !strings.Contains(result, "OK") {
			t.Errorf("expected OK in response, got: %s", result)
		}
	})

	t.Run("insert_then_select_via_mcp", func(t *testing.T) {
		_, err := queryMCP(t, "INSERT INTO settings (`key`, value) VALUES ('mcp-verify', 'mcp-verify-val')")
		if err != nil {
			t.Fatalf("MCP INSERT: %v", err)
		}
		result, err := queryMCP(t, "SELECT `key` FROM settings WHERE `key` = 'mcp-verify'")
		if err != nil {
			t.Fatalf("MCP SELECT: %v", err)
		}
		if !strings.Contains(result, "mcp-verify") {
			t.Errorf("expected mcp-verify in result, got: %s", truncSQL(result))
		}
	})

	t.Run("update_via_mcp", func(t *testing.T) {
		result, err := queryMCP(t, "UPDATE users SET display_name = 'MCP Updated' WHERE id = 50")
		if err != nil {
			t.Fatalf("MCP UPDATE: %v", err)
		}
		if !strings.Contains(result, "OK") {
			t.Errorf("expected OK, got: %s", result)
		}
	})

	t.Run("delete_via_mcp", func(t *testing.T) {
		result, err := queryMCP(t, "DELETE FROM users WHERE id = 51")
		if err != nil {
			t.Fatalf("MCP DELETE: %v", err)
		}
		if !strings.Contains(result, "OK") {
			t.Errorf("expected OK, got: %s", result)
		}
	})

	t.Run("ddl_via_mcp", func(t *testing.T) {
		result, err := queryMCP(t, "CREATE TABLE mcp_test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))")
		if err != nil {
			t.Fatalf("MCP DDL: %v", err)
		}
		t.Logf("MCP DDL result: %s", result)

		_, err = queryMCP(t, "INSERT INTO mcp_test_table (name) VALUES ('mcp_row')")
		if err != nil {
			t.Fatalf("MCP INSERT into new table: %v", err)
		}
		result, err = queryMCP(t, "SELECT * FROM mcp_test_table")
		if err != nil {
			t.Fatalf("MCP SELECT from new table: %v", err)
		}
		if !strings.Contains(result, "mcp_row") {
			t.Errorf("expected mcp_row, got: %s", truncSQL(result))
		}
	})

	t.Run("empty_query_returns_error", func(t *testing.T) {
		_, err := queryMCP(t, "")
		if err == nil {
			t.Error("expected error for empty query")
		}
	})

	t.Run("invalid_sql_returns_error", func(t *testing.T) {
		_, err := queryMCP(t, "SELECTT * FROMM users")
		if err == nil {
			t.Error("expected error for invalid SQL")
		}
	})

	t.Run("nonexistent_table_returns_error", func(t *testing.T) {
		_, err := queryMCP(t, "SELECT * FROM nonexistent_table_xyz")
		if err == nil {
			t.Error("expected error for nonexistent table")
		}
	})
}
