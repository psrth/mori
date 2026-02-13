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
		{"set", "SET ROLE ALL", false},
		{"explain", "EXPLAIN PLAN FOR SELECT 1 FROM DUAL", false},
		{"begin_txn", "BEGIN", false},
		{"commit", "COMMIT", false},
		{"rollback", "ROLLBACK", false},
		{"savepoint", "SAVEPOINT sp1", false},
		{"alter_session", "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'", false},

		// Write
		{"insert", "INSERT INTO users (name) VALUES ('alice')", true},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", true},
		{"delete", "DELETE FROM users WHERE id = 1", true},
		{"merge", "MERGE INTO target USING source ON (target.id = source.id) WHEN MATCHED THEN UPDATE SET target.name = source.name", true},
		{"truncate", "TRUNCATE TABLE users", true},
		{"create", "CREATE TABLE foo (id NUMBER)", true},
		{"alter_table", "ALTER TABLE users ADD (age NUMBER)", true},
		{"drop", "DROP TABLE users", true},
		{"grant", "GRANT SELECT ON users TO app_user", true},
		{"rename", "RENAME users TO old_users", true},

		// PL/SQL
		{"declare_block", "DECLARE v_id NUMBER; BEGIN INSERT INTO users (id) VALUES (1); END;", true},

		// CTE writes
		{"cte_insert", "WITH data AS (SELECT 1 FROM DUAL) INSERT INTO users SELECT * FROM data", true},
		{"cte_select_only", "WITH data AS (SELECT 1 FROM DUAL) SELECT * FROM data", false},

		// Edge cases
		{"empty", "", false},
		{"unknown", "PURGE RECYCLEBIN", false},
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
// TestBuildTNSPacket
// ---------------------------------------------------------------------------

func TestBuildTNSPacket(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	pkt := buildTNSPacket(tnsData, payload)
	if len(pkt) != 8+len(payload) {
		t.Errorf("packet length = %d, want %d", len(pkt), 8+len(payload))
	}
	// Check length (big-endian).
	gotLen := int(pkt[0])<<8 | int(pkt[1])
	if gotLen != 8+len(payload) {
		t.Errorf("encoded packet length = %d, want %d", gotLen, 8+len(payload))
	}
	if pkt[4] != tnsData {
		t.Errorf("packet type = %d, want %d", pkt[4], tnsData)
	}
}

func TestBuildTNSRefuse(t *testing.T) {
	pkt := buildTNSRefuse("test error")
	if len(pkt) < 8 {
		t.Fatal("refuse packet too short")
	}
	if pkt[4] != tnsRefuse {
		t.Errorf("packet type = %d, want %d (Refuse)", pkt[4], tnsRefuse)
	}
}

// ---------------------------------------------------------------------------
// TestSafeProdConn
// ---------------------------------------------------------------------------

func TestSafeProdConn_AllowsNonDataPackets(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	// Build a non-Data TNS packet (Connect type).
	pkt := buildTNSPacket(tnsConnect, []byte("hello"))
	n, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for Connect packet: %v", err)
	}
	if n != len(pkt) {
		t.Errorf("Write returned %d, want %d", n, len(pkt))
	}
}

func TestSafeProdConn_AllowsDataWithSelect(t *testing.T) {
	inner := &mockConn{}
	spc := NewSafeProdConn(inner, 1, false, nil)

	// Build a Data packet containing a SELECT statement.
	sqlBytes := []byte("SELECT * FROM users")
	pkt := buildTNSPacket(tnsData, sqlBytes)
	n, err := spc.Write(pkt)
	if err != nil {
		t.Errorf("unexpected error for SELECT data packet: %v", err)
	}
	if n != len(pkt) {
		t.Errorf("Write returned %d, want %d", n, len(pkt))
	}
}
