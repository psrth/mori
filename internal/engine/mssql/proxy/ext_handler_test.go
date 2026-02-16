package proxy

import (
	"encoding/binary"
	"testing"
)

func TestWellKnownProcName(t *testing.T) {
	tests := []struct {
		id   uint16
		want string
	}{
		{10, "sp_executesql"},
		{11, "sp_prepare"},
		{12, "sp_execute"},
		{15, "sp_unprepare"},
		{13, "sp_prepexec"},
		{0, ""},
		{999, ""},
	}
	for _, tt := range tests {
		got := wellKnownProcName(tt.id)
		if got != tt.want {
			t.Errorf("wellKnownProcName(%d) = %q, want %q", tt.id, got, tt.want)
		}
	}
}

func TestParseRPCPayload_WellKnownProcID(t *testing.T) {
	// Build a minimal RPC payload with sp_executesql (proc ID 10)
	// and a NVARCHAR parameter containing "SELECT 1".

	// ALL_HEADERS: 4-byte length = 22, then 18-byte trans descriptor header.
	var allHeaders [22]byte
	binary.LittleEndian.PutUint32(allHeaders[0:4], 22)
	binary.LittleEndian.PutUint16(allHeaders[4:6], 18)  // header length
	binary.LittleEndian.PutUint16(allHeaders[6:8], 2)   // header type

	// NameLength = 0xFFFF (well-known proc)
	var nameLen [2]byte
	binary.LittleEndian.PutUint16(nameLen[:], 0xFFFF)

	// ProcID = 10 (sp_executesql)
	var procID [2]byte
	binary.LittleEndian.PutUint16(procID[:], 10)

	// OptionFlags = 0
	var optFlags [2]byte

	// Parameter 1: NVARCHAR with "SELECT 1"
	sqlUTF16 := encodeUTF16LE("SELECT 1")

	// Param name: 0 length (unnamed)
	paramName := []byte{0}
	// Status flags: 0
	paramStatus := []byte{0}
	// TYPE_INFO: NVARCHAR (0xE7) with maxLen 8000 + 5-byte collation
	typeInfo := []byte{0xE7}
	var maxLen [2]byte
	binary.LittleEndian.PutUint16(maxLen[:], 8000)
	collation := []byte{0x09, 0x04, 0xD0, 0x00, 0x34}
	// Data length prefix
	var dataLen [2]byte
	binary.LittleEndian.PutUint16(dataLen[:], uint16(len(sqlUTF16)))

	var payload []byte
	payload = append(payload, allHeaders[:]...)
	payload = append(payload, nameLen[:]...)
	payload = append(payload, procID[:]...)
	payload = append(payload, optFlags[:]...)
	payload = append(payload, paramName...)
	payload = append(payload, paramStatus...)
	payload = append(payload, typeInfo...)
	payload = append(payload, maxLen[:]...)
	payload = append(payload, collation...)
	payload = append(payload, dataLen[:]...)
	payload = append(payload, sqlUTF16...)

	procName, firstParam, ok := parseRPCPayload(payload)
	if !ok {
		t.Fatal("parseRPCPayload returned ok=false")
	}
	if procName != "sp_executesql" {
		t.Errorf("procName = %q, want %q", procName, "sp_executesql")
	}
	if firstParam != "SELECT 1" {
		t.Errorf("firstParam = %q, want %q", firstParam, "SELECT 1")
	}
}

func TestParseRPCPayload_NamedProc(t *testing.T) {
	// Build a payload with a named procedure "sp_executesql" (not well-known ID).
	nameUTF16 := encodeUTF16LE("sp_executesql")

	// ALL_HEADERS: minimal (just 4-byte length = 4, no actual headers).
	var allHeaders [4]byte
	binary.LittleEndian.PutUint32(allHeaders[0:4], 4)

	// NameLength in chars.
	var nameLen [2]byte
	binary.LittleEndian.PutUint16(nameLen[:], uint16(len(nameUTF16)/2))

	// OptionFlags.
	var optFlags [2]byte

	// SQL parameter: "INSERT INTO t VALUES (1)"
	sqlUTF16 := encodeUTF16LE("INSERT INTO t VALUES (1)")
	paramName := []byte{0}
	paramStatus := []byte{0}
	typeInfo := []byte{0xE7}
	var maxLen [2]byte
	binary.LittleEndian.PutUint16(maxLen[:], 8000)
	collation := []byte{0x09, 0x04, 0xD0, 0x00, 0x34}
	var dataLen [2]byte
	binary.LittleEndian.PutUint16(dataLen[:], uint16(len(sqlUTF16)))

	var payload []byte
	payload = append(payload, allHeaders[:]...)
	payload = append(payload, nameLen[:]...)
	payload = append(payload, nameUTF16...)
	payload = append(payload, optFlags[:]...)
	payload = append(payload, paramName...)
	payload = append(payload, paramStatus...)
	payload = append(payload, typeInfo...)
	payload = append(payload, maxLen[:]...)
	payload = append(payload, collation...)
	payload = append(payload, dataLen[:]...)
	payload = append(payload, sqlUTF16...)

	procName, firstParam, ok := parseRPCPayload(payload)
	if !ok {
		t.Fatal("parseRPCPayload returned ok=false")
	}
	if procName != "sp_executesql" {
		t.Errorf("procName = %q, want %q", procName, "sp_executesql")
	}
	if firstParam != "INSERT INTO t VALUES (1)" {
		t.Errorf("firstParam = %q, want %q", firstParam, "INSERT INTO t VALUES (1)")
	}
}

func TestParseRPCPayload_EmptyPayload(t *testing.T) {
	_, _, ok := parseRPCPayload(nil)
	if ok {
		t.Error("expected ok=false for nil payload")
	}

	_, _, ok = parseRPCPayload([]byte{0, 0})
	if ok {
		t.Error("expected ok=false for short payload")
	}
}

func TestParseHandleFromParam(t *testing.T) {
	tests := []struct {
		input string
		want  int32
	}{
		{"42", 42},
		{" 123 ", 123},
		{"", 0},
		{"abc", 0},
	}
	for _, tt := range tests {
		got := parseHandleFromParam(tt.input)
		if got != tt.want {
			t.Errorf("parseHandleFromParam(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestExtractReturnValueInt32(t *testing.T) {
	// Build a minimal RETURNVALUE token with value 42.
	var payload []byte
	payload = append(payload, tokenReturnValue) // token type
	payload = append(payload, 0, 0)              // ordinal
	payload = append(payload, 0)                 // param name length = 0
	payload = append(payload, 0)                 // status
	payload = append(payload, 0, 0, 0, 0)        // user type
	payload = append(payload, 0, 0)              // flags
	payload = append(payload, 0x26)              // INTN
	payload = append(payload, 4)                 // max length
	payload = append(payload, 4)                 // actual length
	var val [4]byte
	binary.LittleEndian.PutUint32(val[:], 42)
	payload = append(payload, val[:]...)

	handle, ok := extractReturnValueInt32(payload)
	if !ok {
		t.Fatal("extractReturnValueInt32 returned ok=false")
	}
	if handle != 42 {
		t.Errorf("handle = %d, want 42", handle)
	}
}

func TestExtractReturnValueInt32_NotFound(t *testing.T) {
	payload := []byte{0xFD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	_, ok := extractReturnValueInt32(payload)
	if ok {
		t.Error("expected ok=false for payload without RETURNVALUE token")
	}
}
