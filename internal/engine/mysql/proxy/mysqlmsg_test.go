package proxy

import (
	"testing"
)

func TestBuildOKPacketFull(t *testing.T) {
	pkt := buildOKPacketFull(3, 42)

	// MySQL packet: 3-byte payload length (LE) + 1-byte sequence + payload.
	if len(pkt) < 4 {
		t.Fatalf("packet too short: len = %d", len(pkt))
	}

	payloadLen := int(pkt[0]) | int(pkt[1])<<8 | int(pkt[2])<<16
	if payloadLen != len(pkt)-4 {
		t.Errorf("payload length header = %d, actual payload = %d", payloadLen, len(pkt)-4)
	}

	// Sequence number should be 1.
	if pkt[3] != 1 {
		t.Errorf("sequence = %d, want 1", pkt[3])
	}

	payload := pkt[4:]

	// First byte: OK header (0x00).
	if payload[0] != 0x00 {
		t.Errorf("header byte = 0x%02x, want 0x00", payload[0])
	}

	// Decode affected_rows (length-encoded integer starting at payload[1]).
	affectedRows := readLenEncUint64(payload[1:])
	if affectedRows != 3 {
		t.Errorf("affected rows = %d, want 3", affectedRows)
	}

	// Skip past the affected_rows length-encoded int to read last_insert_id.
	arSize := lenEncIntSize(payload[1:])
	lastInsertID := readLenEncUint64(payload[1+arSize:])
	if lastInsertID != 42 {
		t.Errorf("last insert ID = %d, want 42", lastInsertID)
	}
}
