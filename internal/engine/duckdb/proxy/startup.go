package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// handleStartup performs the PG startup handshake as a server.
// The proxy acts as a PostgreSQL server, handling auth locally.
func handleStartup(clientConn net.Conn) error {
	// Read initial message from client (no type byte).
	startupRaw, err := readStartupMsg(clientConn)
	if err != nil {
		return fmt.Errorf("reading startup: %w", err)
	}

	// Handle SSLRequest.
	if isSSLRequest(startupRaw) {
		// Respond with 'N' (no SSL).
		if _, err := clientConn.Write([]byte{'N'}); err != nil {
			return fmt.Errorf("sending SSL rejection: %w", err)
		}
		// Client will send the real StartupMessage next.
		startupRaw, err = readStartupMsg(clientConn)
		if err != nil {
			return fmt.Errorf("reading startup after SSL: %w", err)
		}
	}

	// Parse startup message to validate it's a proper StartupMessage.
	if len(startupRaw) < 8 {
		return fmt.Errorf("startup message too short")
	}
	protoVersion := binary.BigEndian.Uint32(startupRaw[4:8])
	major := protoVersion >> 16
	if major != 3 {
		return fmt.Errorf("unsupported protocol version %d.%d", major, protoVersion&0xFFFF)
	}

	// Send AuthenticationOk.
	authOk := buildAuthOkMsg()
	if _, err := clientConn.Write(authOk); err != nil {
		return fmt.Errorf("sending AuthenticationOk: %w", err)
	}

	// Send ParameterStatus messages.
	params := []struct{ key, val string }{
		{"server_version", "14.0 (mori-duckdb)"},
		{"server_encoding", "UTF8"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
	}
	for _, p := range params {
		msg := buildParameterStatusMsg(p.key, p.val)
		if _, err := clientConn.Write(msg); err != nil {
			return fmt.Errorf("sending ParameterStatus: %w", err)
		}
	}

	// Send BackendKeyData.
	keyData := buildBackendKeyDataMsg(1, 0)
	if _, err := clientConn.Write(keyData); err != nil {
		return fmt.Errorf("sending BackendKeyData: %w", err)
	}

	// Send ReadyForQuery.
	rfq := buildReadyForQueryMsg()
	if _, err := clientConn.Write(rfq); err != nil {
		return fmt.Errorf("sending ReadyForQuery: %w", err)
	}

	return nil
}

// buildAuthOkMsg constructs an AuthenticationOk ('R') message.
func buildAuthOkMsg() []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, 0) // AuthenticationOk
	return buildPGMsg('R', payload)
}

// buildParameterStatusMsg constructs a ParameterStatus ('S') message.
func buildParameterStatusMsg(key, value string) []byte {
	var payload []byte
	payload = append(payload, []byte(key)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(value)...)
	payload = append(payload, 0)
	return buildPGMsg('S', payload)
}

// buildBackendKeyDataMsg constructs a BackendKeyData ('K') message.
func buildBackendKeyDataMsg(pid, secretKey uint32) []byte {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[:4], pid)
	binary.BigEndian.PutUint32(payload[4:8], secretKey)
	return buildPGMsg('K', payload)
}

// readRawBytes reads exactly n bytes from the reader.
func readRawBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
