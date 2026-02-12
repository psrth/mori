package proxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func makeQueryMsg(sql string) []byte {
	payload := append([]byte(sql), 0) // null-terminated
	msgLen := 4 + len(payload)
	buf := make([]byte, 1+4+len(payload))
	buf[0] = 'Q'
	binary.BigEndian.PutUint32(buf[1:5], uint32(msgLen))
	copy(buf[5:], payload)
	return buf
}

func TestReadMsgQuery(t *testing.T) {
	raw := makeQueryMsg("SELECT 1")
	msg, err := readMsg(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("readMsg() error: %v", err)
	}
	if msg.Type != 'Q' {
		t.Errorf("Type = %c, want Q", msg.Type)
	}
	if got := querySQL(msg.Payload); got != "SELECT 1" {
		t.Errorf("querySQL() = %q, want %q", got, "SELECT 1")
	}
	if !bytes.Equal(msg.Raw, raw) {
		t.Error("Raw bytes do not match original")
	}
}

func TestReadMsgEmptyPayload(t *testing.T) {
	// Message with length = 4 (no payload).
	buf := []byte{'S', 0, 0, 0, 4}
	msg, err := readMsg(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("readMsg() error: %v", err)
	}
	if msg.Type != 'S' {
		t.Errorf("Type = %c, want S", msg.Type)
	}
	if len(msg.Payload) != 0 {
		t.Errorf("Payload len = %d, want 0", len(msg.Payload))
	}
}

func TestReadMsgEOF(t *testing.T) {
	_, err := readMsg(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("readMsg() on empty reader: got %v, want io.EOF", err)
	}
}

func TestReadMsgTruncated(t *testing.T) {
	// Valid header claiming 100 bytes of payload but only 2 available.
	buf := []byte{'Q', 0, 0, 0, 104, 'h', 'i'}
	_, err := readMsg(bytes.NewReader(buf))
	if err == nil {
		t.Error("readMsg() on truncated message: expected error")
	}
}

func TestReadStartupMsg(t *testing.T) {
	// Minimal startup message: length=8, protocol version 3.0 = 196608.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint32(buf[4:8], 196608)

	raw, err := readStartupMsg(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("readStartupMsg() error: %v", err)
	}
	if len(raw) != 8 {
		t.Errorf("raw len = %d, want 8", len(raw))
	}
}

func TestIsSSLRequest(t *testing.T) {
	ssl := make([]byte, 8)
	binary.BigEndian.PutUint32(ssl[0:4], 8)
	binary.BigEndian.PutUint32(ssl[4:8], 80877103)

	if !isSSLRequest(ssl) {
		t.Error("isSSLRequest() = false for SSLRequest, want true")
	}

	// Non-SSL startup message.
	startup := make([]byte, 8)
	binary.BigEndian.PutUint32(startup[0:4], 8)
	binary.BigEndian.PutUint32(startup[4:8], 196608)

	if isSSLRequest(startup) {
		t.Error("isSSLRequest() = true for StartupMessage, want false")
	}

	// Wrong length.
	short := make([]byte, 4)
	if isSSLRequest(short) {
		t.Error("isSSLRequest() = true for 4-byte message, want false")
	}
}

func TestQuerySQL(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{"null terminated", []byte("SELECT 1\x00"), "SELECT 1"},
		{"no null terminator", []byte("SELECT 1"), "SELECT 1"},
		{"empty", []byte{0}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := querySQL(tt.payload); got != tt.want {
				t.Errorf("querySQL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadMsgReadyForQuery(t *testing.T) {
	// ReadyForQuery: type 'Z', length 5, status byte 'I' (idle).
	buf := []byte{'Z', 0, 0, 0, 5, 'I'}
	msg, err := readMsg(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("readMsg() error: %v", err)
	}
	if msg.Type != 'Z' {
		t.Errorf("Type = %c, want Z", msg.Type)
	}
	if len(msg.Payload) != 1 || msg.Payload[0] != 'I' {
		t.Errorf("Payload = %v, want [I]", msg.Payload)
	}
}
