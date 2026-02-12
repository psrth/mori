//go:build e2e

package e2e

import (
	"strings"
	"testing"
)

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
		result, err := queryMCP(t, "INSERT INTO tags (name, slug) VALUES ('mcp-tag', 'mcp-tag')")
		if err != nil {
			t.Fatalf("MCP INSERT: %v", err)
		}
		if !strings.Contains(result, "OK") {
			t.Errorf("expected OK in response, got: %s", result)
		}
	})

	t.Run("insert_then_select_via_mcp", func(t *testing.T) {
		_, err := queryMCP(t, "INSERT INTO tags (name, slug) VALUES ('mcp-verify', 'mcp-verify')")
		if err != nil {
			t.Fatalf("MCP INSERT: %v", err)
		}
		result, err := queryMCP(t, "SELECT name FROM tags WHERE slug = 'mcp-verify'")
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
		result, err := queryMCP(t, "CREATE TABLE mcp_test_table (id SERIAL PRIMARY KEY, name TEXT)")
		if err != nil {
			t.Fatalf("MCP DDL: %v", err)
		}
		t.Logf("MCP DDL result: %s", result)

		// Insert and select.
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
