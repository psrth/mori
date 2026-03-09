package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
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

// buildReadyForQueryMsgState constructs a ReadyForQuery ('Z') message with the given state.
// Valid states: 'I' (idle), 'T' (in transaction), 'E' (failed transaction).
func buildReadyForQueryMsgState(state byte) []byte {
	return buildPGMsg('Z', []byte{state})
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

// buildParseCompleteMsg constructs a ParseComplete ('1') message.
func buildParseCompleteMsg() []byte {
	return buildPGMsg('1', nil)
}

// buildBindCompleteMsg constructs a BindComplete ('2') message.
func buildBindCompleteMsg() []byte {
	return buildPGMsg('2', nil)
}

// buildNoDataMsg constructs a NoData ('n') message.
func buildNoDataMsg() []byte {
	return buildPGMsg('n', nil)
}

// isExtendedProtocolMsg returns true for PG extended query protocol message types.
func isExtendedProtocolMsg(t byte) bool {
	switch t {
	case 'P', 'B', 'D', 'E', 'C', 'S', 'H':
		return true
	}
	return false
}

// parseParseMsgPayload extracts the statement name and SQL text from a Parse ('P') payload.
func parseParseMsgPayload(payload []byte) (stmtName, sql string, err error) {
	if len(payload) == 0 {
		return "", "", fmt.Errorf("empty Parse payload")
	}
	pos := 0
	stmtName, pos, err = readNullTerminatedString(payload, pos)
	if err != nil {
		return "", "", fmt.Errorf("reading statement name: %w", err)
	}
	sql, _, err = readNullTerminatedString(payload, pos)
	if err != nil {
		return "", "", fmt.Errorf("reading query: %w", err)
	}
	return stmtName, sql, nil
}

// parseBindMsgPayload extracts the statement name and parameter values from a Bind ('B') payload.
// P3 4.6: Handles both text-format and binary-format parameters.
func parseBindMsgPayload(payload []byte) (stmtName string, params []string, err error) {
	if len(payload) == 0 {
		return "", nil, fmt.Errorf("empty Bind payload")
	}
	pos := 0
	// Skip portal name.
	_, pos, err = readNullTerminatedString(payload, pos)
	if err != nil {
		return "", nil, fmt.Errorf("reading portal name: %w", err)
	}
	// Read statement name.
	stmtName, pos, err = readNullTerminatedString(payload, pos)
	if err != nil {
		return "", nil, fmt.Errorf("reading statement name: %w", err)
	}
	// Read format codes (0=text, 1=binary).
	if pos+2 > len(payload) {
		return stmtName, nil, nil
	}
	fmtCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	fmtCodes := make([]int16, fmtCount)
	for i := range fmtCount {
		if pos+2 > len(payload) {
			break
		}
		fmtCodes[i] = int16(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
	}
	// Read parameter values.
	if pos+2 > len(payload) {
		return stmtName, nil, nil
	}
	paramCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	for i := range paramCount {
		if pos+4 > len(payload) {
			break
		}
		paramLen := int(int32(binary.BigEndian.Uint32(payload[pos : pos+4])))
		pos += 4
		if paramLen == -1 {
			params = append(params, "")
			continue
		}
		if pos+paramLen > len(payload) {
			break
		}

		// Determine format for this parameter.
		isBinary := false
		if len(fmtCodes) == 1 {
			isBinary = fmtCodes[0] == 1 // Single format code applies to all
		} else if i < len(fmtCodes) {
			isBinary = fmtCodes[i] == 1
		}

		if isBinary {
			// P3 4.6: Decode binary-format parameters.
			params = append(params, decodeBinaryParam(payload[pos:pos+paramLen]))
		} else {
			params = append(params, string(payload[pos:pos+paramLen]))
		}
		pos += paramLen
	}
	return stmtName, params, nil
}

// decodeBinaryParam decodes a binary-format parameter value to its text representation.
// Handles common PostgreSQL binary types based on byte length.
func decodeBinaryParam(data []byte) string {
	switch len(data) {
	case 1:
		// bool: 0 = false, 1 = true
		if data[0] == 0 {
			return "false"
		}
		return "true"
	case 2:
		// int16
		val := int16(binary.BigEndian.Uint16(data))
		return fmt.Sprintf("%d", val)
	case 4:
		// int32
		val := int32(binary.BigEndian.Uint32(data))
		return fmt.Sprintf("%d", val)
	case 8:
		// int64
		val := int64(binary.BigEndian.Uint64(data))
		return fmt.Sprintf("%d", val)
	case 16:
		// UUID: format as 8-4-4-4-12
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			data[0:4], data[4:6], data[6:8], data[8:10], data[10:16])
	default:
		// Fall back to text representation.
		return string(data)
	}
}

// parseExecuteMsgPayload extracts the portal name and max rows from an Execute ('E') payload.
// Format: portal\0 + int32(maxRows). maxRows of 0 means "return all rows".
func parseExecuteMsgPayload(payload []byte) (portal string, maxRows int32, err error) {
	if len(payload) == 0 {
		return "", 0, fmt.Errorf("empty Execute payload")
	}

	pos := 0
	portal, pos, err = readNullTerminatedString(payload, pos)
	if err != nil {
		return "", 0, fmt.Errorf("reading portal name: %w", err)
	}

	if pos+4 > len(payload) {
		return portal, 0, nil // maxRows defaults to 0 (unlimited)
	}
	maxRows = int32(binary.BigEndian.Uint32(payload[pos : pos+4]))
	return portal, maxRows, nil
}

// parseCloseMsgPayload extracts the close target type and name from a Close ('C') payload.
func parseCloseMsgPayload(payload []byte) (closeType byte, name string, err error) {
	if len(payload) < 2 {
		return 0, "", fmt.Errorf("Close payload too short")
	}
	closeType = payload[0]
	name, _, err = readNullTerminatedString(payload, 1)
	if err != nil {
		return closeType, "", fmt.Errorf("reading name: %w", err)
	}
	return closeType, name, nil
}

// readNullTerminatedString reads a null-terminated string from payload starting at pos.
func readNullTerminatedString(payload []byte, pos int) (string, int, error) {
	for i := pos; i < len(payload); i++ {
		if payload[i] == 0 {
			return string(payload[pos:i]), i + 1, nil
		}
	}
	return "", pos, fmt.Errorf("unterminated string at offset %d", pos)
}

// reconstructSQL substitutes parameter placeholders ($1, $2, ...) in SQL with quoted literal values.
func reconstructSQL(sql string, params []string) string {
	result := sql
	for i := len(params) - 1; i >= 0; i-- {
		placeholder := fmt.Sprintf("$%d", i+1)
		if params[i] == "" {
			result = strings.ReplaceAll(result, placeholder, "NULL")
		} else {
			escaped := strings.ReplaceAll(params[i], "'", "''")
			result = strings.ReplaceAll(result, placeholder, "'"+escaped+"'")
		}
	}
	return result
}
