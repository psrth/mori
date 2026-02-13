package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf16"
)

// tdsMsg represents a single TDS wire protocol packet.
// TDS packets have: type(1) + status(1) + length(2 BE) + SPID(2) + packetID(1) + window(1) + payload.
type tdsMsg struct {
	Type     byte   // Packet type
	Status   byte   // Status flags
	SPID     uint16 // Session process ID
	PacketID byte   // Packet ID
	Payload  []byte // Packet payload (after 8-byte header)
	Raw      []byte // Complete raw bytes including header
}

// TDS packet types.
const (
	typeSQLBatch        byte = 0x01
	typeRPC             byte = 0x03
	typeTabularResult   byte = 0x04
	typeAttention       byte = 0x06
	typeTransMgrRequest byte = 0x0E
	typePrelogin        byte = 0x12
	typeLogin7          byte = 0x10
	typeSSPI            byte = 0x11
)

// TDS status flags.
const (
	statusNormal byte = 0x00
	statusEOM    byte = 0x01 // End of message
)

// TDS header size is always 8 bytes.
const tdsHeaderSize = 8

// TDS token types (in tabular result stream).
const (
	tokenError   byte = 0xAA
	tokenDone    byte = 0xFD
	tokenDoneInProc byte = 0xFF
	tokenDoneProc   byte = 0xFE
	tokenEnvChange  byte = 0xE3
	tokenInfo       byte = 0xAB
	tokenLoginAck   byte = 0xAD
)

// readTDSPacket reads a single TDS packet from the reader.
func readTDSPacket(r io.Reader) (*tdsMsg, error) {
	var header [tdsHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	pktType := header[0]
	status := header[1]
	pktLen := int(binary.BigEndian.Uint16(header[2:4]))
	spid := binary.BigEndian.Uint16(header[4:6])
	packetID := header[6]

	if pktLen < tdsHeaderSize {
		return nil, fmt.Errorf("invalid TDS packet length: %d", pktLen)
	}

	payloadLen := pktLen - tdsHeaderSize
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("reading TDS payload: %w", err)
		}
	}

	raw := make([]byte, pktLen)
	copy(raw[:tdsHeaderSize], header[:])
	copy(raw[tdsHeaderSize:], payload)

	return &tdsMsg{
		Type:     pktType,
		Status:   status,
		SPID:     spid,
		PacketID: packetID,
		Payload:  payload,
		Raw:      raw,
	}, nil
}

// readTDSMessage reads a complete TDS message (one or more packets until EOM).
func readTDSMessage(r io.Reader) ([]*tdsMsg, error) {
	var packets []*tdsMsg
	for {
		pkt, err := readTDSPacket(r)
		if err != nil {
			return packets, err
		}
		packets = append(packets, pkt)
		if pkt.Status&statusEOM != 0 {
			return packets, nil
		}
	}
}

// writeTDSPacket writes a raw TDS packet to the writer.
func writeTDSPacket(w io.Writer, pkt *tdsMsg) error {
	_, err := w.Write(pkt.Raw)
	return err
}

// writeTDSPackets writes multiple TDS packets to the writer.
func writeTDSPackets(w io.Writer, pkts []*tdsMsg) error {
	for _, pkt := range pkts {
		if err := writeTDSPacket(w, pkt); err != nil {
			return err
		}
	}
	return nil
}

// buildTDSPacket constructs a raw TDS packet.
func buildTDSPacket(pktType, status byte, payload []byte) []byte {
	pktLen := tdsHeaderSize + len(payload)
	raw := make([]byte, pktLen)
	raw[0] = pktType
	raw[1] = status
	binary.BigEndian.PutUint16(raw[2:4], uint16(pktLen))
	raw[4] = 0 // SPID high
	raw[5] = 0 // SPID low
	raw[6] = 1 // PacketID
	raw[7] = 0 // Window
	copy(raw[tdsHeaderSize:], payload)
	return raw
}

// rawBytesForPackets returns the concatenated raw bytes of all packets.
func rawBytesForPackets(pkts []*tdsMsg) []byte {
	var total int
	for _, pkt := range pkts {
		total += len(pkt.Raw)
	}
	buf := make([]byte, 0, total)
	for _, pkt := range pkts {
		buf = append(buf, pkt.Raw...)
	}
	return buf
}

// extractSQLFromBatch extracts the SQL text from a SQL_BATCH payload.
// The payload contains an ALL_HEADERS section followed by UTF-16LE SQL text.
func extractSQLFromBatch(payload []byte) string {
	if len(payload) < 4 {
		return ""
	}

	// ALL_HEADERS: first 4 bytes are the total length of headers.
	totalLen := int(binary.LittleEndian.Uint32(payload[0:4]))

	// Validate and skip ALL_HEADERS.
	if totalLen < 4 || totalLen > len(payload) {
		// No ALL_HEADERS or invalid; treat entire payload as SQL.
		return decodeUTF16LE(payload)
	}

	sqlBytes := payload[totalLen:]
	return decodeUTF16LE(sqlBytes)
}

// decodeUTF16LE decodes a UTF-16LE byte slice to a Go string.
func decodeUTF16LE(b []byte) string {
	if len(b) < 2 {
		return ""
	}

	// Ensure even length.
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}

	u16s := make([]uint16, len(b)/2)
	for i := range u16s {
		u16s[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}

	runes := utf16.Decode(u16s)
	return string(runes)
}

// encodeUTF16LE encodes a Go string to UTF-16LE bytes.
func encodeUTF16LE(s string) []byte {
	runes := []rune(s)
	u16s := utf16.Encode(runes)
	b := make([]byte, len(u16s)*2)
	for i, v := range u16s {
		binary.LittleEndian.PutUint16(b[i*2:], v)
	}
	return b
}

// buildSQLBatchMessage constructs a complete SQL_BATCH TDS message.
func buildSQLBatchMessage(sql string) []byte {
	sqlBytes := encodeUTF16LE(sql)

	// ALL_HEADERS: minimal header with just the total length (no actual headers).
	allHeadersLen := uint32(4) // Just the total-length field itself.
	var payload []byte
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, allHeadersLen)
	payload = append(payload, lenBuf...)
	payload = append(payload, sqlBytes...)

	return buildTDSPacket(typeSQLBatch, statusEOM, payload)
}

// buildErrorResponse constructs a TDS error token response for the write guard.
func buildErrorResponse(message string) []byte {
	// Build a simple ERROR token + DONE token.
	var tokenData []byte

	// ERROR token (0xAA).
	tokenData = append(tokenData, tokenError)
	msgBytes := encodeUTF16LE(message)
	serverBytes := encodeUTF16LE("mori")
	procBytes := encodeUTF16LE("")

	// Token length (everything after this 2-byte length).
	tokenLen := 4 + 1 + 1 + // number, state, class
		2 + len(msgBytes) + // msg length + msg
		1 + len(serverBytes) + // server name length + server name (byte count in chars)
		1 + len(procBytes) + // proc name length + proc name (byte count in chars)
		4 // line number
	lenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenBuf, uint16(tokenLen))
	tokenData = append(tokenData, lenBuf...)

	// Error number (4 bytes LE) — use 50000 (user-defined).
	numBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(numBuf, 50000)
	tokenData = append(tokenData, numBuf...)

	// State (1 byte).
	tokenData = append(tokenData, 1)
	// Class/severity (1 byte) — 16 = error.
	tokenData = append(tokenData, 16)

	// Message (length-prefixed UTF-16LE, length is in chars).
	msgLenBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(msgLenBuf, uint16(len(msgBytes)/2))
	tokenData = append(tokenData, msgLenBuf...)
	tokenData = append(tokenData, msgBytes...)

	// Server name (1-byte length in chars).
	tokenData = append(tokenData, byte(len(serverBytes)/2))
	tokenData = append(tokenData, serverBytes...)

	// Proc name (1-byte length in chars).
	tokenData = append(tokenData, byte(len(procBytes)/2))
	tokenData = append(tokenData, procBytes...)

	// Line number (4 bytes LE).
	lineBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lineBuf, 0)
	tokenData = append(tokenData, lineBuf...)

	// DONE token (0xFD) to mark end of response.
	tokenData = append(tokenData, tokenDone)
	// Status (2 bytes): 0x0002 = DONE_ERROR.
	statusBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(statusBuf, 0x0002)
	tokenData = append(tokenData, statusBuf...)
	// CurCmd (2 bytes).
	tokenData = append(tokenData, 0, 0)
	// DoneRowCount (8 bytes for TDS 7.2+).
	tokenData = append(tokenData, 0, 0, 0, 0, 0, 0, 0, 0)

	return buildTDSPacket(typeTabularResult, statusEOM, tokenData)
}
