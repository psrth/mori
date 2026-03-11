package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

// mysqlMsg represents a single MySQL wire protocol packet.
// MySQL packets have: length(3 LE) + sequence(1) + payload.
type mysqlMsg struct {
	Sequence byte   // Sequence number
	Payload  []byte // Packet payload
	Raw      []byte // Complete raw bytes: length(3) + sequence(1) + payload
}

// readMySQLPacket reads a single MySQL packet from the reader.
// MySQL packet format: payload_length(3 bytes LE) + sequence_id(1 byte) + payload.
func readMySQLPacket(r io.Reader) (*mysqlMsg, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	payloadLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	seqID := header[3]

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("reading mysql payload: %w", err)
		}
	}

	raw := make([]byte, 4+payloadLen)
	copy(raw[:4], header[:])
	copy(raw[4:], payload)

	return &mysqlMsg{
		Sequence: seqID,
		Payload:  payload,
		Raw:      raw,
	}, nil
}

// writeMySQLPacket constructs and writes a MySQL packet.
func writeMySQLPacket(w io.Writer, seq byte, payload []byte) error {
	raw := buildMySQLPacket(seq, payload)
	_, err := w.Write(raw)
	return err
}

// buildMySQLPacket constructs a raw MySQL packet.
func buildMySQLPacket(seq byte, payload []byte) []byte {
	pLen := len(payload)
	raw := make([]byte, 4+pLen)
	raw[0] = byte(pLen)
	raw[1] = byte(pLen >> 8)
	raw[2] = byte(pLen >> 16)
	raw[3] = seq
	copy(raw[4:], payload)
	return raw
}

// MySQL command bytes.
const (
	comQuit             byte = 0x01
	comInitDB           byte = 0x02
	comQuery            byte = 0x03
	comFieldList        byte = 0x04
	comPing             byte = 0x0e
	comStmtPrepare      byte = 0x16
	comStmtExecute      byte = 0x17
	comStmtSendLongData byte = 0x18
	comStmtClose        byte = 0x19
	comStmtReset        byte = 0x1a
	comStmtFetch        byte = 0x1c
)

// MySQL response packet types (first byte of payload).
const (
	iOK  byte = 0x00
	iEOF byte = 0xfe
	iERR byte = 0xff
)

// isOKPacket checks if a packet is an OK response.
func isOKPacket(payload []byte) bool {
	return len(payload) > 0 && payload[0] == iOK
}

// isEOFPacket checks if a packet is an EOF response.
func isEOFPacket(payload []byte) bool {
	return len(payload) > 0 && payload[0] == iEOF && len(payload) <= 5
}

// isERRPacket checks if a packet is an Error response.
func isERRPacket(payload []byte) bool {
	return len(payload) > 0 && payload[0] == iERR
}

// buildOKPacket constructs a MySQL OK packet.
func buildOKPacket(seq byte) []byte {
	// OK packet: header(0x00) + affected_rows(0) + last_insert_id(0) + status(0x0002) + warnings(0)
	payload := []byte{iOK, 0, 0, 0x02, 0x00, 0, 0}
	return buildMySQLPacket(seq, payload)
}

// buildERRPacket constructs a MySQL ERR packet.
func buildERRPacket(seq byte, code uint16, sqlState, message string) []byte {
	var payload []byte
	payload = append(payload, iERR)
	var codeBuf [2]byte
	binary.LittleEndian.PutUint16(codeBuf[:], code)
	payload = append(payload, codeBuf[:]...)
	payload = append(payload, '#')
	if len(sqlState) < 5 {
		sqlState = sqlState + "     "[:5-len(sqlState)]
	}
	payload = append(payload, []byte(sqlState[:5])...)
	payload = append(payload, []byte(message)...)
	return buildMySQLPacket(seq, payload)
}

// buildOKPacketWithAffectedRows constructs a MySQL OK packet with the given
// affected rows count.
func buildOKPacketWithAffectedRows(affectedRows uint64) []byte {
	// OK packet payload: header(0x00) + affected_rows(lenenc) + last_insert_id(lenenc) + status_flags(2) + warnings(2)
	var payload []byte
	payload = append(payload, 0x00) // OK header
	payload = append(payload, encodeLenEncInt(affectedRows)...)
	payload = append(payload, 0x00)       // last_insert_id = 0
	payload = append(payload, 0x02, 0x00) // status flags (SERVER_STATUS_AUTOCOMMIT)
	payload = append(payload, 0x00, 0x00) // warnings

	// MySQL packet: 3-byte length + 1-byte sequence (1) + payload
	pktLen := len(payload)
	pkt := make([]byte, 4+pktLen)
	pkt[0] = byte(pktLen)
	pkt[1] = byte(pktLen >> 8)
	pkt[2] = byte(pktLen >> 16)
	pkt[3] = 1 // sequence number
	copy(pkt[4:], payload)
	return pkt
}

// buildOKPacketFull constructs a MySQL OK packet with affected rows and last insert ID.
func buildOKPacketFull(affectedRows, lastInsertID uint64) []byte {
	var payload []byte
	payload = append(payload, 0x00) // OK header
	payload = append(payload, encodeLenEncInt(affectedRows)...)
	payload = append(payload, encodeLenEncInt(lastInsertID)...)
	payload = append(payload, 0x02, 0x00) // status flags (SERVER_STATUS_AUTOCOMMIT)
	payload = append(payload, 0x00, 0x00) // warnings

	pktLen := len(payload)
	pkt := make([]byte, 4+pktLen)
	pkt[0] = byte(pktLen)
	pkt[1] = byte(pktLen >> 8)
	pkt[2] = byte(pktLen >> 16)
	pkt[3] = 1 // sequence number
	copy(pkt[4:], payload)
	return pkt
}

// encodeLenEncInt encodes an integer as MySQL length-encoded integer.
func encodeLenEncInt(n uint64) []byte {
	if n < 251 {
		return []byte{byte(n)}
	}
	if n < 1<<16 {
		return []byte{0xfc, byte(n), byte(n >> 8)}
	}
	if n < 1<<24 {
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	}
	return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
		byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
}

// buildEOFPacket constructs a MySQL EOF packet.
func buildEOFPacket(seq byte) []byte {
	// EOF packet: header(0xfe) + warnings(2) + status(2)
	payload := []byte{iEOF, 0, 0, 0x02, 0x00}
	return buildMySQLPacket(seq, payload)
}

// extractQuerySQL extracts the SQL string from a COM_QUERY packet payload.
// The first byte is the command byte (0x03), followed by the SQL string.
func extractQuerySQL(payload []byte) string {
	if len(payload) < 2 {
		return ""
	}
	return string(payload[1:])
}
