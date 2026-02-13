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

// readTNSPacket reads a single TNS packet using the pre-handshake 2-byte length format.
func readTNSPacket(r io.Reader) (*tnsMsg, error) {
	return readTNSPacketWith(r, false)
}

// readTNSPacketV2 reads a single TNS packet using the post-handshake 4-byte length
// format (used when the negotiated TNS version >= 315, e.g. Oracle 12c+).
func readTNSPacketV2(r io.Reader) (*tnsMsg, error) {
	return readTNSPacketWith(r, true)
}

// readTNSPacketWith reads a single TNS packet. When use32bitLen is true the packet
// length occupies the first 4 bytes of the header (post-handshake, version >= 315);
// otherwise the classic 2-byte length at bytes [0:2] is used.
func readTNSPacketWith(r io.Reader, use32bitLen bool) (*tnsMsg, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	var packetLen int
	if use32bitLen {
		packetLen = int(binary.BigEndian.Uint32(header[0:4]))
	} else {
		packetLen = int(binary.BigEndian.Uint16(header[0:2]))
	}
	packetType := header[4]

	log.Printf("[tns] readPacket: use32bit=%v header=%x packetLen=%d type=%d", use32bitLen, header, packetLen, packetType)

	if packetLen < 8 {
		return nil, fmt.Errorf("invalid TNS packet length: %d (header=%x, use32bit=%v)", packetLen, header, use32bitLen)
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

// buildTNSPacket constructs a raw TNS packet using 2-byte length (pre-handshake).
func buildTNSPacket(packetType byte, payload []byte) []byte {
	return buildTNSPacketWith(packetType, payload, false)
}

// buildTNSPacketV2 constructs a raw TNS packet using 4-byte length (post-handshake, version >= 315).
func buildTNSPacketV2(packetType byte, payload []byte) []byte {
	return buildTNSPacketWith(packetType, payload, true)
}

// buildTNSPacketWith constructs a raw TNS packet. When use32bitLen is true the
// packet length is written as a 4-byte big-endian value; otherwise 2-byte.
func buildTNSPacketWith(packetType byte, payload []byte, use32bitLen bool) []byte {
	pktLen := 8 + len(payload)
	raw := make([]byte, pktLen)
	if use32bitLen {
		binary.BigEndian.PutUint32(raw[0:4], uint32(pktLen))
	} else {
		binary.BigEndian.PutUint16(raw[0:2], uint16(pktLen))
	}
	raw[4] = packetType
	raw[5] = 0
	raw[6] = 0
	raw[7] = 0
	copy(raw[8:], payload)
	return raw
}

// buildTNSRefuse constructs a TNS Refuse packet with an error message.
// useV2 selects the 4-byte length framing used post-handshake (version >= 315).
func buildTNSRefuse(message string) []byte {
	return buildTNSRefuseWith(message, false)
}

func buildTNSRefuseV2(message string) []byte {
	return buildTNSRefuseWith(message, true)
}

func buildTNSRefuseWith(message string, use32bitLen bool) []byte {
	// Refuse packet payload: reason(1) + data length(2) + data
	msgBytes := []byte(message)
	payload := make([]byte, 3+len(msgBytes))
	payload[0] = 1 // User reason.
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(msgBytes)))
	copy(payload[3:], msgBytes)
	return buildTNSPacketWith(tnsRefuse, payload, use32bitLen)
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
