package proxy

import (
	"net"
	"testing"
	"time"

	"github.com/mori-dev/mori/internal/core"
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

// ---------------------------------------------------------------------------
// TestLooksLikeWrite
// ---------------------------------------------------------------------------

func TestLooksLikeWrite(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		// Safe
		{"select", "SELECT * FROM users", false},
		{"set", "SET NOCOUNT ON", false},
		{"begin", "BEGIN", false},
		{"commit", "COMMIT", false},
		{"rollback", "ROLLBACK", false},
		{"declare", "DECLARE @id INT", false},
		{"use", "USE mydb", false},
		{"print", "PRINT 'hello'", false},
		{"dbcc", "DBCC CHECKDB", false},

		// Write
		{"insert", "INSERT INTO users (name) VALUES ('alice')", true},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", true},
		{"delete", "DELETE FROM users WHERE id = 1", true},
		{"merge", "MERGE INTO users USING source ON ...", true},
		{"truncate", "TRUNCATE TABLE users", true},
		{"create", "CREATE TABLE foo (id INT)", true},
		{"alter", "ALTER TABLE users ADD age INT", true},
		{"drop", "DROP TABLE users", true},
		{"grant", "GRANT SELECT ON users TO reader", true},
		{"bulk_insert", "BULK INSERT users FROM 'data.csv'", true},
		{"exec", "EXEC my_proc", true},
		{"execute", "EXECUTE sp_help", true},

		// CTE writes
		{"cte_insert", "WITH data AS (SELECT 1) INSERT INTO users SELECT * FROM data", true},
		{"cte_select_only", "WITH data AS (SELECT 1) SELECT * FROM data", false},
		{"cte_merge", "WITH src AS (SELECT 1) MERGE INTO users USING src ON ...", true},

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

		// Edge cases
		{"empty", "", false},
		{"unknown", "WAITFOR DELAY '00:00:01'", false},
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

	// Build a TDS SQL_BATCH packet containing an INSERT.
	pkt := buildSQLBatchMessage("INSERT INTO users (name) VALUES ('alice')")
	_, err := spc.Write(pkt)
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

	pkt := buildSQLBatchMessage("SELECT * FROM users")
	n, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for SELECT: %v", err)
	}
	if n != len(pkt) {
		t.Errorf("Write returned %d, want %d", n, len(pkt))
	}
}

func TestSafeProdConn_BlocksUpdateQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildSQLBatchMessage("UPDATE users SET name = 'bob' WHERE id = 1")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing UPDATE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_BlocksDeleteQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildSQLBatchMessage("DELETE FROM users WHERE id = 1")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing DELETE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_BlocksDDLQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildSQLBatchMessage("DROP TABLE users")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing DROP TABLE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_BlocksMergeQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildSQLBatchMessage("MERGE INTO users USING source ON ...")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing MERGE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_AllowsNonSQLBatchPackets(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	// Build a PRELOGIN packet (non-SQL_BATCH).
	pkt := buildTDSPacket(typePrelogin, statusEOM, []byte{0x00})
	n, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for PRELOGIN: %v", err)
	}
	if n != len(pkt) {
		t.Errorf("Write returned %d, want %d", n, len(pkt))
	}
}

func TestSafeProdConn_AllowsBeginTran(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildSQLBatchMessage("BEGIN TRANSACTION")
	_, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for BEGIN TRANSACTION: %v", err)
	}
}

func TestSafeProdConn_AllowsSetCommand(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildSQLBatchMessage("SET NOCOUNT ON")
	_, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for SET: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestBuildErrorResponse
// ---------------------------------------------------------------------------

func TestBuildErrorResponse(t *testing.T) {
	resp := buildErrorResponse("test error message")
	if len(resp) < tdsHeaderSize+1 {
		t.Fatal("response too short")
	}
	// Check it's a tabular result packet.
	if resp[0] != typeTabularResult {
		t.Errorf("packet type = %#x, want %#x", resp[0], typeTabularResult)
	}
	// Check EOM status.
	if resp[1]&statusEOM == 0 {
		t.Error("expected EOM status flag")
	}
}

// ---------------------------------------------------------------------------
// TestTDSPacketRoundTrip
// ---------------------------------------------------------------------------

func TestBuildTDSPacket(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	pkt := buildTDSPacket(typeSQLBatch, statusEOM, payload)
	if len(pkt) != tdsHeaderSize+len(payload) {
		t.Errorf("packet length = %d, want %d", len(pkt), tdsHeaderSize+len(payload))
	}
	if pkt[0] != typeSQLBatch {
		t.Errorf("type = %#x, want %#x", pkt[0], typeSQLBatch)
	}
	if pkt[1] != statusEOM {
		t.Errorf("status = %#x, want %#x", pkt[1], statusEOM)
	}
}

// ---------------------------------------------------------------------------
// TestUTF16LE
// ---------------------------------------------------------------------------

func TestDecodeUTF16LE(t *testing.T) {
	// "SELECT 1" in UTF-16LE.
	input := encodeUTF16LE("SELECT 1")
	got := decodeUTF16LE(input)
	if got != "SELECT 1" {
		t.Errorf("decodeUTF16LE() = %q, want %q", got, "SELECT 1")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := "INSERT INTO [users] (name) VALUES (N'hello')"
	encoded := encodeUTF16LE(original)
	decoded := decodeUTF16LE(encoded)
	if decoded != original {
		t.Errorf("round trip failed: got %q, want %q", decoded, original)
	}
}

func TestExtractSQLFromBatch(t *testing.T) {
	// Build a SQL_BATCH message and extract the SQL.
	msg := buildSQLBatchMessage("SELECT 1")
	// The payload starts after the 8-byte TDS header.
	payload := msg[tdsHeaderSize:]
	got := extractSQLFromBatch(payload)
	if got != "SELECT 1" {
		t.Errorf("extractSQLFromBatch() = %q, want %q", got, "SELECT 1")
	}
}
