package proxy

import (
	"encoding/binary"
	"net"
	"testing"
)

func TestBuildQueryMsg(t *testing.T) {
	msg := buildQueryMsg("SELECT 1")
	// Expected: 'Q' + 4-byte length + "SELECT 1\0"
	if msg[0] != 'Q' {
		t.Errorf("type byte = %q, want 'Q'", msg[0])
	}
	msgLen := binary.BigEndian.Uint32(msg[1:5])
	// length = 4 (self) + 8 (sql) + 1 (null) = 13
	if msgLen != 13 {
		t.Errorf("length = %d, want 13", msgLen)
	}
	payload := msg[5:]
	if string(payload) != "SELECT 1\x00" {
		t.Errorf("payload = %q, want %q", payload, "SELECT 1\x00")
	}
}

func TestBuildQueryMsgEmpty(t *testing.T) {
	msg := buildQueryMsg("")
	if msg[0] != 'Q' {
		t.Errorf("type byte = %q, want 'Q'", msg[0])
	}
	msgLen := binary.BigEndian.Uint32(msg[1:5])
	// length = 4 (self) + 0 (sql) + 1 (null) = 5
	if msgLen != 5 {
		t.Errorf("length = %d, want 5", msgLen)
	}
}

// makeRowDescMsg builds a raw RowDescription ('T') message for testing.
func makeRowDescMsg(columns ...string) []byte {
	// Build payload: int16(num_fields) + per field data
	var payload []byte
	numFields := make([]byte, 2)
	binary.BigEndian.PutUint16(numFields, uint16(len(columns)))
	payload = append(payload, numFields...)

	for _, name := range columns {
		payload = append(payload, []byte(name)...)
		payload = append(payload, 0) // null terminator
		// tableOID(4) + colNum(2) + typeOID(4) + typeLen(2) + typeMod(4) + format(2) = 18 bytes
		fieldMeta := make([]byte, 18)
		binary.BigEndian.PutUint32(fieldMeta[6:10], 23) // typeOID=23 (int4)
		payload = append(payload, fieldMeta...)
	}

	return buildRawMsg('T', payload)
}

// makeDataRowMsg builds a raw DataRow ('D') message for testing.
// Each value is a string; use nil for NULL.
func makeDataRowMsg(values ...interface{}) []byte {
	var payload []byte
	numCols := make([]byte, 2)
	binary.BigEndian.PutUint16(numCols, uint16(len(values)))
	payload = append(payload, numCols...)

	for _, v := range values {
		if v == nil {
			// NULL: length = -1
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, 0xFFFFFFFF) // -1 as uint32
			payload = append(payload, lenBuf...)
		} else {
			s := v.(string)
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(s)))
			payload = append(payload, lenBuf...)
			payload = append(payload, []byte(s)...)
		}
	}

	return buildRawMsg('D', payload)
}

// makeCommandCompleteMsg builds a raw CommandComplete ('C') message.
func makeCommandCompleteMsg(tag string) []byte {
	payload := append([]byte(tag), 0)
	return buildRawMsg('C', payload)
}

// makeReadyForQueryMsg builds a raw ReadyForQuery ('Z') message.
func makeReadyForQueryMsg() []byte {
	return buildRawMsg('Z', []byte{'I'}) // 'I' = idle
}

// makeErrorResponseMsg builds a raw ErrorResponse ('E') message.
func makeErrorResponseMsg(message string) []byte {
	var payload []byte
	// Severity field
	payload = append(payload, 'S')
	payload = append(payload, []byte("ERROR")...)
	payload = append(payload, 0)
	// Message field
	payload = append(payload, 'M')
	payload = append(payload, []byte(message)...)
	payload = append(payload, 0)
	// Terminator
	payload = append(payload, 0)
	return buildRawMsg('E', payload)
}

// buildRawMsg constructs a raw PG wire protocol message with type byte and payload.
func buildRawMsg(msgType byte, payload []byte) []byte {
	msgLen := 4 + len(payload)
	raw := make([]byte, 1+4+len(payload))
	raw[0] = msgType
	binary.BigEndian.PutUint32(raw[1:5], uint32(msgLen))
	copy(raw[5:], payload)
	return raw
}

func TestParseRowDescription(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
	}{
		{"single column", []string{"id"}},
		{"multiple columns", []string{"id", "name", "email"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := makeRowDescMsg(tt.columns...)
			// Extract payload (skip type byte + length)
			payload := msg[5:]
			cols, err := parseRowDescription(payload)
			if err != nil {
				t.Fatalf("parseRowDescription() error: %v", err)
			}
			if len(cols) != len(tt.columns) {
				t.Fatalf("got %d columns, want %d", len(cols), len(tt.columns))
			}
			for i, col := range cols {
				if col.Name != tt.columns[i] {
					t.Errorf("column[%d].Name = %q, want %q", i, col.Name, tt.columns[i])
				}
			}
		})
	}
}

func TestParseRowDescriptionEmpty(t *testing.T) {
	// 0 fields
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, 0)
	cols, err := parseRowDescription(payload)
	if err != nil {
		t.Fatalf("parseRowDescription() error: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("got %d columns, want 0", len(cols))
	}
}

func TestParseDataRow(t *testing.T) {
	tests := []struct {
		name       string
		values     []interface{}
		wantValues []string
		wantNulls  []bool
	}{
		{
			"single value",
			[]interface{}{"42"},
			[]string{"42"},
			[]bool{false},
		},
		{
			"multiple values",
			[]interface{}{"1", "alice", "alice@example.com"},
			[]string{"1", "alice", "alice@example.com"},
			[]bool{false, false, false},
		},
		{
			"with NULL",
			[]interface{}{"1", nil, "test"},
			[]string{"1", "", "test"},
			[]bool{false, true, false},
		},
		{
			"empty string",
			[]interface{}{""},
			[]string{""},
			[]bool{false},
		},
		{
			"all NULLs",
			[]interface{}{nil, nil},
			[]string{"", ""},
			[]bool{true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := makeDataRowMsg(tt.values...)
			payload := msg[5:]
			vals, nls, err := parseDataRow(payload)
			if err != nil {
				t.Fatalf("parseDataRow() error: %v", err)
			}
			if len(vals) != len(tt.wantValues) {
				t.Fatalf("got %d values, want %d", len(vals), len(tt.wantValues))
			}
			for i, v := range vals {
				got := string(v)
				if got != tt.wantValues[i] {
					t.Errorf("value[%d] = %q, want %q", i, got, tt.wantValues[i])
				}
			}
			for i, n := range nls {
				if n != tt.wantNulls[i] {
					t.Errorf("null[%d] = %v, want %v", i, n, tt.wantNulls[i])
				}
			}
		})
	}
}

func TestParseCommandTag(t *testing.T) {
	tests := []struct {
		tag  string
		want string
	}{
		{"INSERT 0 1", "INSERT 0 1"},
		{"UPDATE 3", "UPDATE 3"},
		{"DELETE 1", "DELETE 1"},
		{"SELECT 5", "SELECT 5"},
	}
	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			payload := append([]byte(tt.tag), 0)
			got := parseCommandTag(payload)
			if got != tt.want {
				t.Errorf("parseCommandTag() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExecQuery(t *testing.T) {
	// Create a mock backend using net.Pipe.
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	// Write mock response on server side.
	go func() {
		// First read the query message from the client side.
		readMsg(serverSide) //nolint: errcheck

		// Write response: RowDescription + DataRow + CommandComplete + ReadyForQuery
		serverSide.Write(makeRowDescMsg("id", "name"))
		serverSide.Write(makeDataRowMsg("1", "alice"))
		serverSide.Write(makeCommandCompleteMsg("SELECT 1"))
		serverSide.Write(makeReadyForQueryMsg())
	}()

	result, err := execQuery(clientSide, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("execQuery() error: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Fatalf("got %d columns, want 2", len(result.Columns))
	}
	if result.Columns[0].Name != "id" {
		t.Errorf("column[0] = %q, want %q", result.Columns[0].Name, "id")
	}
	if result.Columns[1].Name != "name" {
		t.Errorf("column[1] = %q, want %q", result.Columns[1].Name, "name")
	}

	if len(result.RowValues) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.RowValues))
	}
	if result.RowValues[0][0] != "1" {
		t.Errorf("row[0][0] = %q, want %q", result.RowValues[0][0], "1")
	}
	if result.RowValues[0][1] != "alice" {
		t.Errorf("row[0][1] = %q, want %q", result.RowValues[0][1], "alice")
	}

	if result.CommandTag != "SELECT 1" {
		t.Errorf("CommandTag = %q, want %q", result.CommandTag, "SELECT 1")
	}
	if result.Error != "" {
		t.Errorf("Error = %q, want empty", result.Error)
	}
}

func TestExecQueryMultipleRows(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	go func() {
		readMsg(serverSide) //nolint: errcheck
		serverSide.Write(makeRowDescMsg("id", "name"))
		serverSide.Write(makeDataRowMsg("1", "alice"))
		serverSide.Write(makeDataRowMsg("2", "bob"))
		serverSide.Write(makeDataRowMsg("3", nil))
		serverSide.Write(makeCommandCompleteMsg("SELECT 3"))
		serverSide.Write(makeReadyForQueryMsg())
	}()

	result, err := execQuery(clientSide, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("execQuery() error: %v", err)
	}
	if len(result.RowValues) != 3 {
		t.Fatalf("got %d rows, want 3", len(result.RowValues))
	}
	if !result.RowNulls[2][1] {
		t.Error("row[2][1] should be NULL")
	}
}

func TestExecQueryWithError(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	go func() {
		readMsg(serverSide) //nolint: errcheck
		serverSide.Write(makeErrorResponseMsg("relation \"foo\" does not exist"))
		serverSide.Write(makeReadyForQueryMsg())
	}()

	result, err := execQuery(clientSide, "SELECT * FROM foo")
	if err != nil {
		t.Fatalf("execQuery() error: %v", err)
	}
	if result.Error == "" {
		t.Error("expected non-empty error")
	}
	if result.Error != "relation \"foo\" does not exist" {
		t.Errorf("Error = %q, want %q", result.Error, "relation \"foo\" does not exist")
	}
}

func TestExecQueryNoRows(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	go func() {
		readMsg(serverSide) //nolint: errcheck
		serverSide.Write(makeRowDescMsg("id"))
		serverSide.Write(makeCommandCompleteMsg("SELECT 0"))
		serverSide.Write(makeReadyForQueryMsg())
	}()

	result, err := execQuery(clientSide, "SELECT id FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("execQuery() error: %v", err)
	}
	if len(result.RowValues) != 0 {
		t.Errorf("got %d rows, want 0", len(result.RowValues))
	}
	if result.CommandTag != "SELECT 0" {
		t.Errorf("CommandTag = %q, want %q", result.CommandTag, "SELECT 0")
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
		{"O'Brien's", "'O''Brien''s'"},
		{`no"quotes`, `'no"quotes'`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := quoteLiteral(tt.input)
			if got != tt.want {
				t.Errorf("quoteLiteral(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"user table", `"user table"`},
		{`has"quote`, `"has""quote"`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := quoteIdent(tt.input)
			if got != tt.want {
				t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBuildInsertSQL(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		columns []ColumnInfo
		values  []string
		nulls   []bool
		want    string
	}{
		{
			"simple",
			"users",
			[]ColumnInfo{{Name: "id"}, {Name: "name"}},
			[]string{"1", "alice"},
			[]bool{false, false},
			`INSERT INTO "users" ("id", "name") VALUES ('1', 'alice') ON CONFLICT DO NOTHING`,
		},
		{
			"with NULL",
			"users",
			[]ColumnInfo{{Name: "id"}, {Name: "name"}, {Name: "email"}},
			[]string{"1", "bob", ""},
			[]bool{false, false, true},
			`INSERT INTO "users" ("id", "name", "email") VALUES ('1', 'bob', NULL) ON CONFLICT DO NOTHING`,
		},
		{
			"with quotes in value",
			"users",
			[]ColumnInfo{{Name: "id"}, {Name: "name"}},
			[]string{"1", "O'Brien"},
			[]bool{false, false},
			`INSERT INTO "users" ("id", "name") VALUES ('1', 'O''Brien') ON CONFLICT DO NOTHING`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildInsertSQL(tt.table, tt.columns, tt.values, tt.nulls)
			if got != tt.want {
				t.Errorf("buildInsertSQL() = %q, want %q", got, tt.want)
			}
		})
	}
}
