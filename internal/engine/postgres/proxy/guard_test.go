package proxy

import (
	"net"
	"testing"
	"time"

	"github.com/psrth/mori/internal/core"
)

// ---------------------------------------------------------------------------
// mockConn — minimal net.Conn for testing SafeProdConn
// ---------------------------------------------------------------------------

type mockConn struct {
	written []byte
}

func (m *mockConn) Write(b []byte) (int, error) {
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockConn) Read(b []byte) (int, error)         { return 0, nil }
func (m *mockConn) Close() error                        { return nil }
func (m *mockConn) LocalAddr() net.Addr                 { return nil }
func (m *mockConn) RemoteAddr() net.Addr                { return nil }
func (m *mockConn) SetDeadline(t time.Time) error       { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error   { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error  { return nil }

// ---------------------------------------------------------------------------
// TestValidateRouteDecision
// ---------------------------------------------------------------------------

func TestValidateRouteDecision_AllowsReadToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpRead, SubType: core.SubSelect}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for read to prod, got: %v", err)
	}
}

func TestValidateRouteDecision_BlocksWriteToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for write to prod, got nil")
	}
}

func TestValidateRouteDecision_BlocksDDLToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpDDL, SubType: core.SubAlter}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for DDL to prod, got nil")
	}
}

func TestValidateRouteDecision_AllowsWriteToShadow(t *testing.T) {
	cl := &core.Classification{OpType: core.OpWrite, SubType: core.SubInsert}
	err := validateRouteDecision(cl, core.StrategyShadowWrite, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for write to shadow, got: %v", err)
	}
}

func TestValidateRouteDecision_AllowsTransactionToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpTransaction, SubType: core.SubBegin}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for transaction to prod, got: %v", err)
	}
}

func TestValidateRouteDecision_NilClassification(t *testing.T) {
	err := validateRouteDecision(nil, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for nil classification, got: %v", err)
	}
}

func TestValidateRouteDecision_AllowsOtherToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpOther, SubType: core.SubOther}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for other to prod, got: %v", err)
	}
}

func TestValidateRouteDecision_BlocksDeleteToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpWrite, SubType: core.SubDelete}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for delete to prod, got nil")
	}
}

func TestValidateRouteDecision_BlocksUpdateToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpWrite, SubType: core.SubUpdate}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for update to prod, got nil")
	}
}

func TestValidateRouteDecision_BlocksCreateToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpDDL, SubType: core.SubCreate}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for create DDL to prod, got nil")
	}
}

func TestValidateRouteDecision_BlocksDropToProd(t *testing.T) {
	cl := &core.Classification{OpType: core.OpDDL, SubType: core.SubDrop}
	err := validateRouteDecision(cl, core.StrategyProdDirect, 1, nil)
	if err == nil {
		t.Error("expected error for drop DDL to prod, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestLooksLikeWrite — table-driven
// ---------------------------------------------------------------------------

func TestLooksLikeWrite(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		// Safe (reads and session commands)
		{"select", "SELECT * FROM users", false},
		{"select_lower", "select 1", false},
		{"set", "SET search_path TO public", false},
		{"show", "SHOW server_version", false},
		{"explain", "EXPLAIN ANALYZE SELECT 1", false},
		{"begin", "BEGIN", false},
		{"commit", "COMMIT", false},
		{"rollback", "ROLLBACK", false},
		{"savepoint", "SAVEPOINT sp1", false},
		{"release", "RELEASE SAVEPOINT sp1", false},
		{"deallocate", "DEALLOCATE ALL", false},
		{"close", "CLOSE my_cursor", false},
		{"fetch", "FETCH ALL FROM my_cursor", false},
		{"declare", "DECLARE my_cursor CURSOR FOR SELECT 1", false},
		{"discard", "DISCARD ALL", false},
		{"listen", "LISTEN my_channel", false},
		{"notify", "NOTIFY my_channel", false},
		{"unlisten", "UNLISTEN my_channel", false},
		{"reset", "RESET ALL", false},

		// Write operations
		{"insert", "INSERT INTO users (name) VALUES ('alice')", true},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", true},
		{"delete", "DELETE FROM users WHERE id = 1", true},
		{"truncate", "TRUNCATE TABLE users", true},
		{"create_table", "CREATE TABLE foo (id INT)", true},
		{"alter_table", "ALTER TABLE users ADD COLUMN age INT", true},
		{"drop_table", "DROP TABLE users", true},
		{"grant", "GRANT SELECT ON users TO reader", true},
		{"revoke", "REVOKE SELECT ON users FROM reader", true},

		// CTE writes
		{"cte_insert", "WITH data AS (SELECT 1) INSERT INTO users SELECT * FROM data", true},
		{"cte_update", "WITH ids AS (SELECT id FROM users) UPDATE users SET active = true WHERE id IN (SELECT id FROM ids)", true},
		{"cte_delete", "WITH old AS (SELECT id FROM users WHERE age > 100) DELETE FROM users WHERE id IN (SELECT id FROM old)", true},
		{"cte_select_only", "WITH data AS (SELECT 1) SELECT * FROM data", false},

		// Whitespace handling
		{"leading_space", "  INSERT INTO users (name) VALUES ('alice')", true},
		{"leading_tab", "\tDELETE FROM users", true},
		{"leading_newline", "\nUPDATE users SET x = 1", true},

		// Case insensitive
		{"insert_mixed_case", "Insert INTO users (name) VALUES ('x')", true},
		{"select_mixed_case", "Select * FROM users", false},

		// Multi-statement detection
		{"multi_stmt_delete", "SELECT 1; DELETE FROM users", true},
		{"multi_stmt_insert", "SELECT 1; INSERT INTO t VALUES (1)", true},
		{"multi_stmt_drop", "SELECT 1; DROP TABLE t", true},
		{"multi_stmt_safe", "SELECT 1; SELECT 2", false},
		{"multi_stmt_trailing_semi", "SELECT 1;", false},

		// String literal false positives (semicolons inside quotes are not delimiters)
		{"string_literal_semicolon", "SELECT 'x;DELETE FROM t'", false},
		{"string_literal_escaped_quote", "SELECT 'it''s;here'", false},

		// Comment bypass (comments before write keywords must be stripped)
		{"line_comment_before_write", "SELECT 1; --comment\nDELETE FROM t", true},
		{"block_comment_before_write", "SELECT 1; /* comment */ DELETE FROM t", true},

		// Block comment with apostrophe must not poison semicolon detection
		{"block_comment_apostrophe", "SELECT /* it's */ 1; DELETE FROM users", true},

		// CTE name containing keyword substring must not cause false positive
		{"cte_name_insert_log", "WITH insert_log AS (SELECT 1) SELECT * FROM insert_log", false},

		// Postgres-specific write commands
		{"call_proc", "CALL my_proc()", true},
		{"do_block", "DO $$ BEGIN NULL; END $$", true},
		{"copy_to", "COPY users TO STDOUT", true},

		// Edge cases
		{"empty", "", false},
		{"unknown_command", "VACUUM", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := looksLikeWrite(tt.sql)
			if got != tt.want {
				t.Errorf("looksLikeWrite(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestSafeProdConn
// ---------------------------------------------------------------------------

func TestSafeProdConn_BlocksWriteQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 42, false, nil)

	// Build a pgwire 'Q' message containing an INSERT.
	qMsg := buildQueryMsg("INSERT INTO users (name) VALUES ('alice')")
	_, err := spc.Write(qMsg)
	if err == nil {
		t.Error("expected error when writing INSERT through SafeProdConn, got nil")
	}
	if len(inner.written) > 0 {
		t.Error("INSERT query reached inner connection despite guard")
	}
}

func TestSafeProdConn_AllowsSelectQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 42, false, nil)

	// Build a pgwire 'Q' message containing a SELECT.
	qMsg := buildQueryMsg("SELECT * FROM users")
	n, err := spc.Write(qMsg)
	if err != nil {
		t.Errorf("unexpected error for SELECT: %v", err)
	}
	if n != len(qMsg) {
		t.Errorf("Write returned %d, want %d", n, len(qMsg))
	}
	if len(inner.written) != len(qMsg) {
		t.Errorf("inner.written length = %d, want %d", len(inner.written), len(qMsg))
	}
}

func TestSafeProdConn_BlocksUpdateQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	qMsg := buildQueryMsg("UPDATE users SET name = 'bob' WHERE id = 1")
	_, err := spc.Write(qMsg)
	if err == nil {
		t.Error("expected error when writing UPDATE through SafeProdConn, got nil")
	}
	if len(inner.written) > 0 {
		t.Error("UPDATE query reached inner connection despite guard")
	}
}

func TestSafeProdConn_BlocksDeleteQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	qMsg := buildQueryMsg("DELETE FROM users WHERE id = 1")
	_, err := spc.Write(qMsg)
	if err == nil {
		t.Error("expected error when writing DELETE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_BlocksDDLQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	qMsg := buildQueryMsg("DROP TABLE users")
	_, err := spc.Write(qMsg)
	if err == nil {
		t.Error("expected error when writing DROP TABLE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_AllowsNonQueryMessages(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	// A non-Q message (e.g., 'X' for Terminate) should pass through.
	terminateMsg := buildPGMsg('X', nil)
	n, err := spc.Write(terminateMsg)
	if err != nil {
		t.Errorf("unexpected error for non-Q message: %v", err)
	}
	if n != len(terminateMsg) {
		t.Errorf("Write returned %d, want %d", n, len(terminateMsg))
	}
}

func TestSafeProdConn_AllowsShortMessages(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	// A message shorter than 5 bytes should pass through without inspection.
	short := []byte{0x01, 0x02, 0x03}
	n, err := spc.Write(short)
	if err != nil {
		t.Errorf("unexpected error for short message: %v", err)
	}
	if n != len(short) {
		t.Errorf("Write returned %d, want %d", n, len(short))
	}
}

func TestSafeProdConn_AllowsBeginTransaction(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	qMsg := buildQueryMsg("BEGIN")
	_, err := spc.Write(qMsg)
	if err != nil {
		t.Errorf("unexpected error for BEGIN: %v", err)
	}
}

func TestSafeProdConn_AllowsSetCommand(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	qMsg := buildQueryMsg("SET search_path TO public")
	_, err := spc.Write(qMsg)
	if err != nil {
		t.Errorf("unexpected error for SET: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestBuildGuardErrorResponse
// ---------------------------------------------------------------------------

func TestBuildGuardErrorResponse(t *testing.T) {
	resp := buildGuardErrorResponse("test error message")

	// Should start with 'E' (ErrorResponse) and end with 'Z' (ReadyForQuery).
	if len(resp) == 0 {
		t.Fatal("empty response")
	}
	if resp[0] != 'E' {
		t.Errorf("first message type = %q, want 'E'", resp[0])
	}

	// Find the ReadyForQuery message at the end.
	// ReadyForQuery is 'Z' + 4-byte length (5) + 'I' = 6 bytes total.
	if len(resp) < 6 {
		t.Fatal("response too short to contain ReadyForQuery")
	}
	lastMsg := resp[len(resp)-6:]
	if lastMsg[0] != 'Z' {
		t.Errorf("last message type = %q, want 'Z'", lastMsg[0])
	}
}
