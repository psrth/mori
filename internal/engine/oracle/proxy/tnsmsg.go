package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

// tnsMsg represents a single Oracle TNS wire protocol packet.
// TNS header: length(2 BE) + checksum(2) + type(1) + reserved(1) + header_checksum(2) = 8 bytes.
type tnsMsg struct {
	PacketType byte   // TNS packet type
	Payload    []byte // Packet payload (after 8-byte header)
	Raw        []byte // Complete raw bytes including header
}

// TNS packet type constants.
const (
	tnsConnect   byte = 1
	tnsAccept    byte = 2
	tnsAck       byte = 3
	tnsRefuse    byte = 4
	tnsRedirect  byte = 5
	tnsData      byte = 6
	tnsNull      byte = 7
	tnsAbort     byte = 9
	tnsResend    byte = 11
	tnsMarker    byte = 12
	tnsAttention byte = 13
	tnsControl   byte = 14
)

// readTNSPacket reads a single TNS packet from the reader.
func readTNSPacket(r io.Reader) (*tnsMsg, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	packetLen := int(binary.BigEndian.Uint16(header[0:2]))
	packetType := header[4]

	if packetLen < 8 {
		return nil, fmt.Errorf("invalid TNS packet length: %d", packetLen)
	}

	payloadLen := packetLen - 8
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("reading TNS payload: %w", err)
		}
	}

	raw := make([]byte, packetLen)
	copy(raw[:8], header[:])
	copy(raw[8:], payload)

	return &tnsMsg{
		PacketType: packetType,
		Payload:    payload,
		Raw:        raw,
	}, nil
}

// writeTNSPacket constructs and writes a TNS packet.
func writeTNSPacket(w io.Writer, packetType byte, payload []byte) error {
	raw := buildTNSPacket(packetType, payload)
	_, err := w.Write(raw)
	return err
}

// buildTNSPacket constructs a raw TNS packet.
func buildTNSPacket(packetType byte, payload []byte) []byte {
	pktLen := 8 + len(payload)
	raw := make([]byte, pktLen)
	binary.BigEndian.PutUint16(raw[0:2], uint16(pktLen))
	// Checksum = 0.
	raw[2] = 0
	raw[3] = 0
	raw[4] = packetType
	// Reserved = 0.
	raw[5] = 0
	// Header checksum = 0.
	raw[6] = 0
	raw[7] = 0
	copy(raw[8:], payload)
	return raw
}

// buildTNSRefuse constructs a TNS Refuse packet with an error message.
func buildTNSRefuse(message string) []byte {
	// Refuse packet payload: reason(1) + data length(2) + data
	msgBytes := []byte(message)
	payload := make([]byte, 3+len(msgBytes))
	payload[0] = 1 // User reason.
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(msgBytes)))
	copy(payload[3:], msgBytes)
	return buildTNSPacket(tnsRefuse, payload)
}

// extractQueryFromTNSData attempts to extract SQL text from a TNS Data packet payload.
// Oracle TNS Data packets contain SQL in various encodings. This function uses
// heuristic byte-pattern matching to find SQL text.
func extractQueryFromTNSData(payload []byte) string {
	if len(payload) < 12 {
		return ""
	}

	// TNS Data packets have a 2-byte data flags field at the start of the payload.
	// The actual content follows. SQL text is embedded within the Oracle Net
	// protocol layer. We scan for common SQL keywords.
	text := extractPrintableSQL(payload)
	return text
}

// extractPrintableSQL scans the payload for the longest printable ASCII
// string that looks like SQL.
func extractPrintableSQL(data []byte) string {
	// Look for SQL statement patterns in the raw bytes.
	// Oracle encodes SQL in the data portion of TNS packets.
	// We look for sequences of printable bytes that start with SQL keywords.
	var best string
	for i := 0; i < len(data); i++ {
		if !isPrintableASCII(data[i]) {
			continue
		}
		// Try to extract a contiguous printable string.
		j := i
		for j < len(data) && isPrintableASCII(data[j]) {
			j++
		}
		candidate := string(data[i:j])
		if len(candidate) > len(best) && looksLikeSQL(candidate) {
			best = candidate
		}
		i = j
	}
	return best
}

func isPrintableASCII(b byte) bool {
	return b >= 0x20 && b < 0x7f
}

func looksLikeSQL(s string) bool {
	if len(s) < 6 {
		return false
	}
	upper := s
	if len(upper) > 100 {
		upper = upper[:100]
	}
	for i := range upper {
		if upper[i] >= 'a' && upper[i] <= 'z' {
			upper = upper[:i] + string(rune(upper[i]-32)) + upper[i+1:]
		}
	}
	prefixes := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP",
		"MERGE", "TRUNCATE", "BEGIN", "COMMIT", "ROLLBACK", "WITH",
		"DECLARE", "SET", "EXPLAIN", "GRANT", "REVOKE",
	}
	for _, p := range prefixes {
		if len(upper) >= len(p) && upper[:len(p)] == p {
			return true
		}
	}
	return false
}
