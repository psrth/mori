package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

// pgMsg represents a single PostgreSQL wire protocol message.
// It holds the message type byte and the complete raw bytes for forwarding.
type pgMsg struct {
	Type    byte   // Message type identifier ('Q', 'P', 'X', etc.)
	Payload []byte // Message payload (excluding type byte and length)
	Raw     []byte // Complete raw bytes: type(1) + length(4) + payload
}

// readMsg reads a single PG wire protocol message from r.
// PG messages have the format: type(1 byte) + length(4 bytes big-endian, includes itself) + payload.
func readMsg(r io.Reader) (*pgMsg, error) {
	var typeBuf [1]byte
	if _, err := io.ReadFull(r, typeBuf[:]); err != nil {
		return nil, err
	}
	msgType := typeBuf[0]

	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("reading message length: %w", err)
	}
	msgLen := int(binary.BigEndian.Uint32(lenBuf[:]))

	if msgLen < 4 {
		return nil, fmt.Errorf("invalid message length %d", msgLen)
	}

	payloadLen := msgLen - 4
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("reading message payload: %w", err)
		}
	}

	raw := make([]byte, 1+4+payloadLen)
	raw[0] = msgType
	copy(raw[1:5], lenBuf[:])
	copy(raw[5:], payload)

	return &pgMsg{
		Type:    msgType,
		Payload: payload,
		Raw:     raw,
	}, nil
}

// readStartupMsg reads a PG startup message, which has no type byte.
// Format: length(4 bytes big-endian, includes itself) + payload.
// Used for the initial StartupMessage and SSLRequest.
func readStartupMsg(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	msgLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if msgLen < 4 {
		return nil, fmt.Errorf("invalid startup message length %d", msgLen)
	}

	raw := make([]byte, msgLen)
	copy(raw[:4], lenBuf[:])
	if msgLen > 4 {
		if _, err := io.ReadFull(r, raw[4:]); err != nil {
			return nil, fmt.Errorf("reading startup payload: %w", err)
		}
	}
	return raw, nil
}

// querySQL extracts the SQL string from a Query ('Q') message payload.
// The payload is a null-terminated string.
func querySQL(payload []byte) string {
	for i, b := range payload {
		if b == 0 {
			return string(payload[:i])
		}
	}
	return string(payload)
}

// isSSLRequest checks if a startup message is an SSLRequest.
// SSLRequest has length=8 and protocol version 80877103 (1234.5679).
func isSSLRequest(raw []byte) bool {
	if len(raw) != 8 {
		return false
	}
	code := binary.BigEndian.Uint32(raw[4:8])
	return code == 80877103
}
