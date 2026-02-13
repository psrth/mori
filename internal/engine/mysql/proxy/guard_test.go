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
		{"set", "SET NAMES utf8mb4", false},
		{"show", "SHOW TABLES", false},
		{"explain", "EXPLAIN SELECT 1", false},
		{"begin", "BEGIN", false},
		{"commit", "COMMIT", false},
		{"rollback", "ROLLBACK", false},
		{"start_txn", "START TRANSACTION", false},
		{"describe", "DESCRIBE users", false},
		{"use", "USE mydb", false},

		// Write
		{"insert", "INSERT INTO users (name) VALUES ('alice')", true},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", true},
		{"delete", "DELETE FROM users WHERE id = 1", true},
		{"replace", "REPLACE INTO users (id, name) VALUES (1, 'alice')", true},
		{"truncate", "TRUNCATE TABLE users", true},
		{"create", "CREATE TABLE foo (id INT)", true},
		{"alter", "ALTER TABLE users ADD COLUMN age INT", true},
		{"drop", "DROP TABLE users", true},
		{"grant", "GRANT SELECT ON users TO 'reader'@'%'", true},
		{"rename", "RENAME TABLE users TO old_users", true},

		// CTE writes
		{"cte_insert", "WITH data AS (SELECT 1) INSERT INTO users SELECT * FROM data", true},
		{"cte_select_only", "WITH data AS (SELECT 1) SELECT * FROM data", false},

		// Edge cases
		{"empty", "", false},
		{"unknown", "FLUSH TABLES", false},
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

	// Build a MySQL COM_QUERY packet containing an INSERT.
	pkt := buildCOMQuery(0, "INSERT INTO users (name) VALUES ('alice')")
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

	pkt := buildCOMQuery(0, "SELECT * FROM users")
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

	pkt := buildCOMQuery(0, "UPDATE users SET name = 'bob' WHERE id = 1")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing UPDATE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_BlocksDeleteQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildCOMQuery(0, "DELETE FROM users WHERE id = 1")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing DELETE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_BlocksDDLQuery(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildCOMQuery(0, "DROP TABLE users")
	_, err := spc.Write(pkt)
	if err == nil {
		t.Error("expected error when writing DROP TABLE through SafeProdConn, got nil")
	}
}

func TestSafeProdConn_AllowsNonQueryPackets(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildCOMPing(0)
	n, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for COM_PING: %v", err)
	}
	if n != len(pkt) {
		t.Errorf("Write returned %d, want %d", n, len(pkt))
	}
}

func TestSafeProdConn_AllowsBegin(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildCOMQuery(0, "BEGIN")
	_, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for BEGIN: %v", err)
	}
}

func TestSafeProdConn_AllowsSetCommand(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	pkt := buildCOMQuery(0, "SET NAMES utf8mb4")
	_, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for SET: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestBuildGuardErrorResponse
// ---------------------------------------------------------------------------

func TestBuildGuardErrorResponse(t *testing.T) {
	resp := buildGuardErrorResponse(1, "test error message")
	if len(resp) < 5 {
		t.Fatal("response too short")
	}
	// Check it's an ERR packet: payload[0] == 0xff.
	if resp[4] != iERR {
		t.Errorf("first payload byte = %#x, want %#x (ERR)", resp[4], iERR)
	}
}

// ---------------------------------------------------------------------------
// TestBuildMySQLPacket
// ---------------------------------------------------------------------------

func TestBuildMySQLPacket(t *testing.T) {
	payload := []byte{0x03, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}
	pkt := buildMySQLPacket(0, payload)
	if len(pkt) != 4+len(payload) {
		t.Errorf("packet length = %d, want %d", len(pkt), 4+len(payload))
	}
	// Check length bytes (LE).
	gotLen := int(pkt[0]) | int(pkt[1])<<8 | int(pkt[2])<<16
	if gotLen != len(payload) {
		t.Errorf("encoded payload length = %d, want %d", gotLen, len(payload))
	}
	if pkt[3] != 0 {
		t.Errorf("sequence = %d, want 0", pkt[3])
	}
}

func TestExtractQuerySQL(t *testing.T) {
	payload := append([]byte{comQuery}, []byte("SELECT 1")...)
	got := extractQuerySQL(payload)
	if got != "SELECT 1" {
		t.Errorf("extractQuerySQL() = %q, want %q", got, "SELECT 1")
	}
}
