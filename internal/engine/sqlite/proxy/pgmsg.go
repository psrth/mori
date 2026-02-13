package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

// pgMsg represents a single PostgreSQL wire protocol message.
type pgMsg struct {
	Type    byte   // Message type identifier ('Q', 'P', 'X', etc.)
	Payload []byte // Message payload (excluding type byte and length)
	Raw     []byte // Complete raw bytes: type(1) + length(4) + payload
}

// readMsg reads a single PG wire protocol message from r.
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

// readStartupMsg reads a PG startup message (no type byte).
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
func querySQL(payload []byte) string {
	for i, b := range payload {
		if b == 0 {
			return string(payload[:i])
		}
	}
	return string(payload)
}

// isSSLRequest checks if a startup message is an SSLRequest.
func isSSLRequest(raw []byte) bool {
	if len(raw) != 8 {
		return false
	}
	code := binary.BigEndian.Uint32(raw[4:8])
	return code == 80877103
}

// buildPGMsg constructs a raw PG wire protocol message with type byte and payload.
func buildPGMsg(msgType byte, payload []byte) []byte {
	msgLen := 4 + len(payload)
	raw := make([]byte, 1+4+len(payload))
	raw[0] = msgType
	binary.BigEndian.PutUint32(raw[1:5], uint32(msgLen))
	copy(raw[5:], payload)
	return raw
}

// buildRowDescMsg constructs a RowDescription ('T') message.
func buildRowDescMsg(colNames []string, colOIDs []uint32) []byte {
	var payload []byte
	numFields := make([]byte, 2)
	binary.BigEndian.PutUint16(numFields, uint16(len(colNames)))
	payload = append(payload, numFields...)

	for i, name := range colNames {
		payload = append(payload, []byte(name)...)
		payload = append(payload, 0)
		// tableOID(4) + colNum(2) = 6 bytes zeroed
		fieldMeta := make([]byte, 18)
		// typeOID at offset 6
		oid := uint32(25) // text OID by default
		if i < len(colOIDs) && colOIDs[i] != 0 {
			oid = colOIDs[i]
		}
		binary.BigEndian.PutUint32(fieldMeta[6:10], oid)
		// typeLen(2) at offset 10 = -1 for variable length
		binary.BigEndian.PutUint16(fieldMeta[10:12], 0xFFFF)
		// typeMod(4) at offset 12, format(2) at offset 16 — zero (text format)
		payload = append(payload, fieldMeta...)
	}

	return buildPGMsg('T', payload)
}

// buildDataRowMsg constructs a DataRow ('D') message.
func buildDataRowMsg(values [][]byte, nulls []bool) []byte {
	var payload []byte
	numCols := make([]byte, 2)
	binary.BigEndian.PutUint16(numCols, uint16(len(values)))
	payload = append(payload, numCols...)

	for i, v := range values {
		if nulls[i] {
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, 0xFFFFFFFF) // -1 = NULL
			payload = append(payload, lenBuf...)
		} else {
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(v)))
			payload = append(payload, lenBuf...)
			payload = append(payload, v...)
		}
	}

	return buildPGMsg('D', payload)
}

// buildCommandCompleteMsg constructs a CommandComplete ('C') message.
func buildCommandCompleteMsg(tag string) []byte {
	payload := append([]byte(tag), 0)
	return buildPGMsg('C', payload)
}

// buildReadyForQueryMsg constructs a ReadyForQuery ('Z') message with idle status.
func buildReadyForQueryMsg() []byte {
	return buildPGMsg('Z', []byte{'I'})
}

// buildErrorResponse constructs an ErrorResponse ('E') followed by ReadyForQuery.
func buildErrorResponse(message string) []byte {
	var errPayload []byte
	errPayload = append(errPayload, 'S')
	errPayload = append(errPayload, []byte("ERROR")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'C')
	errPayload = append(errPayload, []byte("MR001")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'M')
	errPayload = append(errPayload, []byte(message)...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 0) // terminator

	var result []byte
	result = append(result, buildPGMsg('E', errPayload)...)
	result = append(result, buildReadyForQueryMsg()...)
	return result
}

// buildSQLErrorResponse constructs an ErrorResponse for a SQL error with proper SQLSTATE.
func buildSQLErrorResponse(message string) []byte {
	var errPayload []byte
	errPayload = append(errPayload, 'S')
	errPayload = append(errPayload, []byte("ERROR")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'C')
	errPayload = append(errPayload, []byte("42000")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'M')
	errPayload = append(errPayload, []byte(message)...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 0) // terminator

	var result []byte
	result = append(result, buildPGMsg('E', errPayload)...)
	result = append(result, buildReadyForQueryMsg()...)
	return result
}

// buildEmptyQueryResponse constructs an EmptyQueryResponse ('I') followed by ReadyForQuery.
func buildEmptyQueryResponse() []byte {
	var result []byte
	result = append(result, buildPGMsg('I', nil)...)
	result = append(result, buildReadyForQueryMsg()...)
	return result
}
