package proxy

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// isExtendedProtocolMsg returns true for PG extended query protocol message types.
func isExtendedProtocolMsg(t byte) bool {
	switch t {
	case 'P', 'B', 'D', 'E', 'C', 'S', 'H':
		return true
	}
	return false
}

// parseParseMsgPayload extracts the statement name, SQL text, and parameter
// type OIDs from a Parse ('P') message payload.
// Format: name\0 + query\0 + int16(numParams) + uint32(oid) * numParams
func parseParseMsgPayload(payload []byte) (stmtName, sql string, paramOIDs []uint32, err error) {
	if len(payload) == 0 {
		return "", "", nil, fmt.Errorf("empty Parse payload")
	}

	// Read null-terminated statement name.
	pos := 0
	stmtName, pos, err = readNullTerminated(payload, pos)
	if err != nil {
		return "", "", nil, fmt.Errorf("reading statement name: %w", err)
	}

	// Read null-terminated query string.
	sql, pos, err = readNullTerminated(payload, pos)
	if err != nil {
		return "", "", nil, fmt.Errorf("reading query: %w", err)
	}

	// Read parameter type count and OIDs.
	if pos+2 > len(payload) {
		// No param type info — valid (count is optional in some contexts).
		return stmtName, sql, nil, nil
	}
	numParams := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2

	paramOIDs = make([]uint32, 0, numParams)
	for i := 0; i < numParams; i++ {
		if pos+4 > len(payload) {
			return stmtName, sql, paramOIDs, fmt.Errorf("truncated param OID at index %d", i)
		}
		oid := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		paramOIDs = append(paramOIDs, oid)
	}

	return stmtName, sql, paramOIDs, nil
}

// parseBindMsgPayload extracts the portal name, statement name, and parameter
// values from a Bind ('B') message payload.
// Format: portal\0 + stmt\0 + int16(fmtCount) + int16(fmt)*N +
//
//	int16(paramCount) + [int32(len) + bytes(len)]*N +
//	int16(resFmtCount) + int16(resFmt)*N
func parseBindMsgPayload(payload []byte) (portal, stmtName string, params [][]byte, err error) {
	if len(payload) == 0 {
		return "", "", nil, fmt.Errorf("empty Bind payload")
	}

	pos := 0

	// Read null-terminated portal name.
	portal, pos, err = readNullTerminated(payload, pos)
	if err != nil {
		return "", "", nil, fmt.Errorf("reading portal name: %w", err)
	}

	// Read null-terminated statement name.
	stmtName, pos, err = readNullTerminated(payload, pos)
	if err != nil {
		return "", "", nil, fmt.Errorf("reading statement name: %w", err)
	}

	// Read format codes.
	if pos+2 > len(payload) {
		return portal, stmtName, nil, fmt.Errorf("truncated format count")
	}
	fmtCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos+fmtCount*2 > len(payload) {
		return portal, stmtName, nil, fmt.Errorf("truncated format codes")
	}
	pos += fmtCount * 2 // skip int16 format codes

	// Read parameter values.
	if pos+2 > len(payload) {
		return portal, stmtName, nil, fmt.Errorf("truncated param count")
	}
	paramCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2

	params = make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		if pos+4 > len(payload) {
			return portal, stmtName, params, fmt.Errorf("truncated param length at index %d", i)
		}
		paramLen := int(int32(binary.BigEndian.Uint32(payload[pos : pos+4])))
		pos += 4

		if paramLen == -1 {
			params = append(params, nil) // NULL
			continue
		}

		if pos+paramLen > len(payload) {
			return portal, stmtName, params, fmt.Errorf("truncated param data at index %d", i)
		}
		param := make([]byte, paramLen)
		copy(param, payload[pos:pos+paramLen])
		pos += paramLen
		params = append(params, param)
	}

	return portal, stmtName, params, nil
}

// parseDescribeMsgPayload extracts the target type and name from a Describe ('D') payload.
// Format: type_byte('S' or 'P') + name\0
func parseDescribeMsgPayload(payload []byte) (descType byte, name string, err error) {
	if len(payload) < 2 {
		return 0, "", fmt.Errorf("Describe payload too short")
	}
	descType = payload[0]
	name, _, err = readNullTerminated(payload, 1)
	if err != nil {
		return descType, "", fmt.Errorf("reading name: %w", err)
	}
	return descType, name, nil
}

// parseExecuteMsgPayload extracts the portal name and max rows from an Execute ('E') payload.
// Format: portal\0 + int32(maxRows)
func parseExecuteMsgPayload(payload []byte) (portal string, maxRows int32, err error) {
	if len(payload) == 0 {
		return "", 0, fmt.Errorf("empty Execute payload")
	}

	pos := 0
	portal, pos, err = readNullTerminated(payload, pos)
	if err != nil {
		return "", 0, fmt.Errorf("reading portal name: %w", err)
	}

	if pos+4 > len(payload) {
		return portal, 0, nil // maxRows defaults to 0 (unlimited)
	}
	maxRows = int32(binary.BigEndian.Uint32(payload[pos : pos+4]))
	return portal, maxRows, nil
}

// parseCloseMsgPayload extracts the target type and name from a Close ('C') payload.
// Format: type_byte('S' or 'P') + name\0
func parseCloseMsgPayload(payload []byte) (closeType byte, name string, err error) {
	if len(payload) < 2 {
		return 0, "", fmt.Errorf("Close payload too short")
	}
	closeType = payload[0]
	name, _, err = readNullTerminated(payload, 1)
	if err != nil {
		return closeType, "", fmt.Errorf("reading name: %w", err)
	}
	return closeType, name, nil
}

// readNullTerminated reads a null-terminated string from payload starting at pos.
// Returns the string and the position after the null terminator.
func readNullTerminated(payload []byte, pos int) (string, int, error) {
	for i := pos; i < len(payload); i++ {
		if payload[i] == 0 {
			return string(payload[pos:i]), i + 1, nil
		}
	}
	return "", pos, fmt.Errorf("unterminated string at offset %d", pos)
}

// hasBinaryParams checks if a Bind message payload uses binary-format parameters.
// Returns true if any parameter format code is non-zero (binary).
func hasBinaryParams(payload []byte) bool {
	pos := 0
	// Skip portal\0
	for pos < len(payload) && payload[pos] != 0 {
		pos++
	}
	pos++ // skip null terminator
	// Skip stmt\0
	for pos < len(payload) && payload[pos] != 0 {
		pos++
	}
	pos++ // skip null terminator

	if pos+2 > len(payload) {
		return false
	}
	fmtCount := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2

	for i := 0; i < fmtCount; i++ {
		if pos+2 > len(payload) {
			return false
		}
		fmtCode := binary.BigEndian.Uint16(payload[pos : pos+2])
		pos += 2
		if fmtCode != 0 { // 0=text, 1=binary
			return true
		}
	}
	return false
}

// resolveParams converts raw Bind parameter values to []interface{} for ClassifyWithParams.
// Text-format parameters are used as-is. NULL parameters become nil.
func resolveParams(rawParams [][]byte) []interface{} {
	params := make([]interface{}, len(rawParams))
	for i, p := range rawParams {
		if p == nil {
			params[i] = nil
		} else {
			params[i] = string(p)
		}
	}
	return params
}

// reconstructSQL substitutes parameter placeholders ($1, $2, ...) in a SQL
// template with quoted literal values. Replaces from highest index down to
// prevent $1 from partially matching $10, $11, etc.
func reconstructSQL(sql string, params [][]byte) string {
	result := sql
	for i := len(params) - 1; i >= 0; i-- {
		placeholder := fmt.Sprintf("$%d", i+1)
		if params[i] == nil {
			result = strings.ReplaceAll(result, placeholder, "NULL")
		} else {
			result = strings.ReplaceAll(result, placeholder, quoteLiteral(string(params[i])))
		}
	}
	return result
}
