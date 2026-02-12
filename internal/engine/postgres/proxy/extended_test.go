package proxy

import (
	"encoding/binary"
	"testing"
)

// --- Test helpers for building extended protocol messages ---

// makeParseMsg builds a raw Parse ('P') message.
func makeParseMsg(stmtName, sql string, paramOIDs ...uint32) []byte {
	var payload []byte
	payload = append(payload, []byte(stmtName)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(sql)...)
	payload = append(payload, 0)
	numParams := make([]byte, 2)
	binary.BigEndian.PutUint16(numParams, uint16(len(paramOIDs)))
	payload = append(payload, numParams...)
	for _, oid := range paramOIDs {
		oidBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(oidBuf, oid)
		payload = append(payload, oidBuf...)
	}
	return buildRawMsg('P', payload)
}

// makeBindMsg builds a raw Bind ('B') message with text-format parameters.
func makeBindMsg(portal, stmt string, params ...string) []byte {
	var payload []byte
	// portal\0
	payload = append(payload, []byte(portal)...)
	payload = append(payload, 0)
	// stmt\0
	payload = append(payload, []byte(stmt)...)
	payload = append(payload, 0)
	// format codes: 0 (all text)
	fmtCount := make([]byte, 2)
	binary.BigEndian.PutUint16(fmtCount, 0)
	payload = append(payload, fmtCount...)
	// param count
	paramCount := make([]byte, 2)
	binary.BigEndian.PutUint16(paramCount, uint16(len(params)))
	payload = append(payload, paramCount...)
	// params
	for _, p := range params {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(p)))
		payload = append(payload, lenBuf...)
		payload = append(payload, []byte(p)...)
	}
	// result format codes: 0
	resFmtCount := make([]byte, 2)
	binary.BigEndian.PutUint16(resFmtCount, 0)
	payload = append(payload, resFmtCount...)
	return buildRawMsg('B', payload)
}

// makeBindMsgWithNull builds a Bind message where some params are NULL.
// Use nil for NULL, string for non-NULL.
func makeBindMsgWithNull(portal, stmt string, params ...interface{}) []byte {
	var payload []byte
	payload = append(payload, []byte(portal)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(stmt)...)
	payload = append(payload, 0)
	fmtCount := make([]byte, 2)
	payload = append(payload, fmtCount...)
	paramCount := make([]byte, 2)
	binary.BigEndian.PutUint16(paramCount, uint16(len(params)))
	payload = append(payload, paramCount...)
	for _, p := range params {
		if p == nil {
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, 0xFFFFFFFF) // -1 = NULL
			payload = append(payload, lenBuf...)
		} else {
			s := p.(string)
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(s)))
			payload = append(payload, lenBuf...)
			payload = append(payload, []byte(s)...)
		}
	}
	resFmtCount := make([]byte, 2)
	payload = append(payload, resFmtCount...)
	return buildRawMsg('B', payload)
}

// makeDescribeMsg builds a raw Describe ('D') message.
func makeDescribeMsg(objType byte, name string) []byte {
	var payload []byte
	payload = append(payload, objType)
	payload = append(payload, []byte(name)...)
	payload = append(payload, 0)
	return buildRawMsg('D', payload)
}

// makeExecuteMsg builds a raw Execute ('E') message.
func makeExecuteMsg(portal string) []byte {
	var payload []byte
	payload = append(payload, []byte(portal)...)
	payload = append(payload, 0)
	// max_rows = 0 (unlimited)
	payload = append(payload, 0, 0, 0, 0)
	return buildRawMsg('E', payload)
}

// makeCloseMsg builds a raw Close ('C') message.
func makeCloseMsg(objType byte, name string) []byte {
	var payload []byte
	payload = append(payload, objType)
	payload = append(payload, []byte(name)...)
	payload = append(payload, 0)
	return buildRawMsg('C', payload)
}

// makeSyncMsg builds a raw Sync ('S') message.
func makeSyncMsg() []byte {
	return buildRawMsg('S', nil)
}

// makeParseCompleteMsg builds a raw ParseComplete ('1') message.
func makeParseCompleteMsg() []byte {
	return buildRawMsg('1', nil)
}

// makeBindCompleteMsg builds a raw BindComplete ('2') message.
func makeBindCompleteMsg() []byte {
	return buildRawMsg('2', nil)
}

// --- Parser unit tests ---

func TestParseParseMsgPayload(t *testing.T) {
	tests := []struct {
		name      string
		msg       []byte
		wantStmt  string
		wantSQL   string
		wantOIDs  int
		wantError bool
	}{
		{
			"unnamed statement",
			makeParseMsg("", "SELECT * FROM users WHERE id = $1"),
			"", "SELECT * FROM users WHERE id = $1", 0, false,
		},
		{
			"named statement",
			makeParseMsg("my_stmt", "UPDATE users SET name = $1 WHERE id = $2"),
			"my_stmt", "UPDATE users SET name = $1 WHERE id = $2", 0, false,
		},
		{
			"with param OIDs",
			makeParseMsg("s1", "SELECT * FROM users WHERE id = $1", 23), // 23 = int4
			"s1", "SELECT * FROM users WHERE id = $1", 1, false,
		},
		{
			"multiple param OIDs",
			makeParseMsg("s2", "INSERT INTO t VALUES ($1, $2)", 23, 25), // int4, text
			"s2", "INSERT INTO t VALUES ($1, $2)", 2, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := tt.msg[5:] // skip type + length
			stmtName, sql, paramOIDs, err := parseParseMsgPayload(payload)
			if (err != nil) != tt.wantError {
				t.Fatalf("error = %v, wantError = %v", err, tt.wantError)
			}
			if stmtName != tt.wantStmt {
				t.Errorf("stmtName = %q, want %q", stmtName, tt.wantStmt)
			}
			if sql != tt.wantSQL {
				t.Errorf("sql = %q, want %q", sql, tt.wantSQL)
			}
			if len(paramOIDs) != tt.wantOIDs {
				t.Errorf("len(paramOIDs) = %d, want %d", len(paramOIDs), tt.wantOIDs)
			}
		})
	}
}

func TestParseBindMsgPayload(t *testing.T) {
	tests := []struct {
		name       string
		msg        []byte
		wantPortal string
		wantStmt   string
		wantParams int
	}{
		{
			"unnamed portal and stmt with params",
			makeBindMsg("", "", "42", "alice"),
			"", "", 2,
		},
		{
			"named portal and stmt",
			makeBindMsg("p1", "s1", "100"),
			"p1", "s1", 1,
		},
		{
			"no params",
			makeBindMsg("", ""),
			"", "", 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := tt.msg[5:]
			portal, stmtName, _, params, err := parseBindMsgPayload(payload)
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			if portal != tt.wantPortal {
				t.Errorf("portal = %q, want %q", portal, tt.wantPortal)
			}
			if stmtName != tt.wantStmt {
				t.Errorf("stmtName = %q, want %q", stmtName, tt.wantStmt)
			}
			if len(params) != tt.wantParams {
				t.Errorf("len(params) = %d, want %d", len(params), tt.wantParams)
			}
		})
	}
}

func TestParseBindMsgPayloadWithNull(t *testing.T) {
	msg := makeBindMsgWithNull("", "", "hello", nil, "world")
	payload := msg[5:]
	_, _, _, params, err := parseBindMsgPayload(payload)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(params) != 3 {
		t.Fatalf("len(params) = %d, want 3", len(params))
	}
	if string(params[0]) != "hello" {
		t.Errorf("params[0] = %q, want %q", params[0], "hello")
	}
	if params[1] != nil {
		t.Errorf("params[1] = %v, want nil (NULL)", params[1])
	}
	if string(params[2]) != "world" {
		t.Errorf("params[2] = %q, want %q", params[2], "world")
	}
}

func TestParseDescribeMsgPayload(t *testing.T) {
	msg := makeDescribeMsg('P', "my_portal")
	payload := msg[5:]
	descType, name, err := parseDescribeMsgPayload(payload)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if descType != 'P' {
		t.Errorf("descType = %q, want 'P'", descType)
	}
	if name != "my_portal" {
		t.Errorf("name = %q, want %q", name, "my_portal")
	}
}

func TestParseDescribeMsgPayloadStmt(t *testing.T) {
	msg := makeDescribeMsg('S', "s1")
	payload := msg[5:]
	descType, name, err := parseDescribeMsgPayload(payload)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if descType != 'S' {
		t.Errorf("descType = %q, want 'S'", descType)
	}
	if name != "s1" {
		t.Errorf("name = %q, want %q", name, "s1")
	}
}

func TestParseExecuteMsgPayload(t *testing.T) {
	msg := makeExecuteMsg("")
	payload := msg[5:]
	portal, maxRows, err := parseExecuteMsgPayload(payload)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if portal != "" {
		t.Errorf("portal = %q, want empty", portal)
	}
	if maxRows != 0 {
		t.Errorf("maxRows = %d, want 0", maxRows)
	}
}

func TestParseCloseMsgPayload(t *testing.T) {
	msg := makeCloseMsg('S', "my_stmt")
	payload := msg[5:]
	closeType, name, err := parseCloseMsgPayload(payload)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if closeType != 'S' {
		t.Errorf("closeType = %q, want 'S'", closeType)
	}
	if name != "my_stmt" {
		t.Errorf("name = %q, want %q", name, "my_stmt")
	}
}

func TestIsExtendedProtocolMsg(t *testing.T) {
	extended := []byte{'P', 'B', 'D', 'E', 'C', 'S', 'H'}
	for _, b := range extended {
		if !isExtendedProtocolMsg(b) {
			t.Errorf("isExtendedProtocolMsg(%q) = false, want true", b)
		}
	}
	notExtended := []byte{'Q', 'X', 'Z', 'T', 'R'}
	for _, b := range notExtended {
		if isExtendedProtocolMsg(b) {
			t.Errorf("isExtendedProtocolMsg(%q) = true, want false", b)
		}
	}
}

func TestReconstructSQL(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		params [][]byte
		want   string
	}{
		{
			"single param",
			"SELECT * FROM users WHERE id = $1",
			[][]byte{[]byte("42")},
			"SELECT * FROM users WHERE id = '42'",
		},
		{
			"multiple params",
			"UPDATE users SET name = $1 WHERE id = $2",
			[][]byte{[]byte("alice"), []byte("7")},
			"UPDATE users SET name = 'alice' WHERE id = '7'",
		},
		{
			"NULL param",
			"UPDATE users SET email = $1 WHERE id = $2",
			[][]byte{nil, []byte("5")},
			"UPDATE users SET email = NULL WHERE id = '5'",
		},
		{
			"param with quotes",
			"INSERT INTO users (name) VALUES ($1)",
			[][]byte{[]byte("O'Brien")},
			"INSERT INTO users (name) VALUES ('O''Brien')",
		},
		{
			"$10 before $1 replacement",
			"SELECT * FROM t WHERE a = $1 AND b = $10",
			[][]byte{
				[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"),
				[]byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j"),
			},
			"SELECT * FROM t WHERE a = 'a' AND b = 'j'",
		},
		{
			"no params",
			"SELECT 1",
			nil,
			"SELECT 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reconstructSQL(tt.sql, tt.params, nil)
			if got != tt.want {
				t.Errorf("reconstructSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveParams(t *testing.T) {
	params := resolveParams([][]byte{
		[]byte("hello"),
		nil,
		[]byte("42"),
	}, nil)
	if len(params) != 3 {
		t.Fatalf("len = %d, want 3", len(params))
	}
	if params[0] != "hello" {
		t.Errorf("params[0] = %v, want %q", params[0], "hello")
	}
	if params[1] != nil {
		t.Errorf("params[1] = %v, want nil", params[1])
	}
	if params[2] != "42" {
		t.Errorf("params[2] = %v, want %q", params[2], "42")
	}
}
