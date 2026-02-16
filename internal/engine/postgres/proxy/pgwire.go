package proxy

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

// buildQueryMsg constructs a PG wire protocol Query ('Q') message.
// Format: 'Q'(1 byte) + length(4 bytes, includes itself) + sql\0
func buildQueryMsg(sql string) []byte {
	payload := append([]byte(sql), 0) // null-terminated
	msgLen := 4 + len(payload)        // length includes itself

	buf := make([]byte, 1+4+len(payload))
	buf[0] = 'Q'
	binary.BigEndian.PutUint32(buf[1:5], uint32(msgLen))
	copy(buf[5:], payload)
	return buf
}

// ColumnInfo holds metadata for a single column from a RowDescription message.
type ColumnInfo struct {
	Name string
	OID  uint32 // PG type OID
}

// parseRowDescription parses a 'T' (RowDescription) message payload into column metadata.
// Format: int16(num_fields) + per field: [name\0 + tableOID(4) + colNum(2) + typeOID(4) + typeLen(2) + typeMod(4) + format(2)]
func parseRowDescription(payload []byte) ([]ColumnInfo, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("RowDescription payload too short")
	}
	numFields := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2
	columns := make([]ColumnInfo, 0, numFields)

	for i := 0; i < numFields; i++ {
		// Read null-terminated name.
		nameEnd := -1
		for j := pos; j < len(payload); j++ {
			if payload[j] == 0 {
				nameEnd = j
				break
			}
		}
		if nameEnd < 0 {
			return nil, fmt.Errorf("unterminated column name at field %d", i)
		}
		name := string(payload[pos:nameEnd])
		pos = nameEnd + 1

		// Skip tableOID(4) + colNum(2) = 6 bytes, then read typeOID(4).
		if pos+6+4 > len(payload) {
			return nil, fmt.Errorf("RowDescription truncated at field %d", i)
		}
		pos += 6 // skip tableOID + colNum
		typeOID := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4

		// Skip typeLen(2) + typeMod(4) + format(2) = 8 bytes.
		if pos+8 > len(payload) {
			return nil, fmt.Errorf("RowDescription truncated at field %d metadata", i)
		}
		pos += 8

		columns = append(columns, ColumnInfo{Name: name, OID: typeOID})
	}

	return columns, nil
}

// parseDataRow parses a 'D' (DataRow) message payload into values.
// Format: int16(num_cols) + per column: [int32(len) + bytes(len)]. len=-1 means NULL.
// Returns raw byte slices and a null indicator per column.
func parseDataRow(payload []byte) (values [][]byte, nulls []bool, err error) {
	if len(payload) < 2 {
		return nil, nil, fmt.Errorf("DataRow payload too short")
	}
	numCols := int(binary.BigEndian.Uint16(payload[:2]))
	pos := 2
	values = make([][]byte, numCols)
	nulls = make([]bool, numCols)

	for i := 0; i < numCols; i++ {
		if pos+4 > len(payload) {
			return nil, nil, fmt.Errorf("DataRow truncated at column %d", i)
		}
		colLen := int(int32(binary.BigEndian.Uint32(payload[pos : pos+4])))
		pos += 4

		if colLen == -1 {
			nulls[i] = true
			continue
		}

		if pos+colLen > len(payload) {
			return nil, nil, fmt.Errorf("DataRow truncated at column %d data", i)
		}
		values[i] = payload[pos : pos+colLen]
		pos += colLen
	}

	return values, nulls, nil
}

// parseCommandTag extracts the command tag string from a 'C' (CommandComplete) payload.
// The payload is a null-terminated string like "INSERT 0 1", "UPDATE 3", "DELETE 1".
func parseCommandTag(payload []byte) string {
	for i, b := range payload {
		if b == 0 {
			return string(payload[:i])
		}
	}
	return string(payload)
}

// parseInsertCount extracts the row count from an INSERT command tag.
// Format: "INSERT 0 N" → N. Returns 0 if the tag is not an INSERT or unparseable.
func parseInsertCount(tag string) int {
	var oid, count int
	if _, err := fmt.Sscanf(tag, "INSERT %d %d", &oid, &count); err == nil {
		return count
	}
	return 0
}

// QueryResult holds the parsed result of executing a query on a backend.
type QueryResult struct {
	Columns    []ColumnInfo // column metadata from RowDescription
	RowValues  [][]string   // parsed string values per row
	RowNulls   [][]bool     // null indicators per row
	CommandTag string       // from CommandComplete (e.g., "INSERT 0 1")
	RawMsgs    []byte       // all raw response bytes for relay
	Error      string       // ErrorResponse message text, if any
}

// execQuery sends a Query message to the backend and collects the full response.
// It reads messages until ReadyForQuery ('Z') and returns the parsed result.
func execQuery(conn net.Conn, sql string) (*QueryResult, error) {
	msg := buildQueryMsg(sql)
	if _, err := conn.Write(msg); err != nil {
		return nil, fmt.Errorf("sending query: %w", err)
	}

	result := &QueryResult{}

	for {
		respMsg, err := readMsg(conn)
		if err != nil {
			return nil, fmt.Errorf("reading response: %w", err)
		}
		result.RawMsgs = append(result.RawMsgs, respMsg.Raw...)

		switch respMsg.Type {
		case 'T': // RowDescription
			cols, err := parseRowDescription(respMsg.Payload)
			if err != nil {
				return nil, fmt.Errorf("parsing RowDescription: %w", err)
			}
			result.Columns = cols

		case 'D': // DataRow
			vals, nls, err := parseDataRow(respMsg.Payload)
			if err != nil {
				return nil, fmt.Errorf("parsing DataRow: %w", err)
			}
			strVals := make([]string, len(vals))
			for i, v := range vals {
				strVals[i] = string(v)
			}
			result.RowValues = append(result.RowValues, strVals)
			result.RowNulls = append(result.RowNulls, nls)

		case 'C': // CommandComplete
			result.CommandTag = parseCommandTag(respMsg.Payload)

		case 'E': // ErrorResponse
			result.Error = parseErrorResponse(respMsg.Payload)

		case 'Z': // ReadyForQuery
			return result, nil
		}
	}
}

// parseErrorResponse extracts a human-readable message from an ErrorResponse payload.
// The payload is a sequence of type-byte + null-terminated-string pairs, terminated by \0.
func parseErrorResponse(payload []byte) string {
	var message string
	pos := 0
	for pos < len(payload) {
		if payload[pos] == 0 {
			break
		}
		fieldType := payload[pos]
		pos++

		end := pos
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		val := string(payload[pos:end])
		pos = end + 1 // skip null terminator

		if fieldType == 'M' {
			message = val
		}
	}
	if message == "" {
		return string(payload)
	}
	return message
}

// quoteIdent quotes a SQL identifier (table name, column name) with double quotes.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteLiteral quotes a SQL string literal with single quotes and proper escaping.
func quoteLiteral(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
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

// buildRowDescMsg constructs a RowDescription ('T') message from column metadata.
func buildRowDescMsg(columns []ColumnInfo) []byte {
	var payload []byte
	numFields := make([]byte, 2)
	binary.BigEndian.PutUint16(numFields, uint16(len(columns)))
	payload = append(payload, numFields...)

	for _, col := range columns {
		payload = append(payload, []byte(col.Name)...)
		payload = append(payload, 0) // null terminator
		// tableOID(4) + colNum(2) = 6 bytes (zeroed)
		fieldMeta := make([]byte, 18)
		// typeOID at offset 6
		binary.BigEndian.PutUint32(fieldMeta[6:10], col.OID)
		// typeLen(2) at offset 10, typeMod(4) at offset 12, format(2) at offset 16 — all zero
		payload = append(payload, fieldMeta...)
	}

	return buildPGMsg('T', payload)
}

// buildDataRowMsg constructs a DataRow ('D') message from string values and null indicators.
func buildDataRowMsg(values []string, nulls []bool) []byte {
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
			data := []byte(v)
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
			payload = append(payload, lenBuf...)
			payload = append(payload, data...)
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

// buildSelectResponse constructs a complete PG SELECT response
// (RowDescription + DataRows + CommandComplete + ReadyForQuery) from in-memory data.
func buildSelectResponse(columns []ColumnInfo, rowValues [][]string, rowNulls [][]bool) []byte {
	var buf []byte
	buf = append(buf, buildRowDescMsg(columns)...)
	for i := range rowValues {
		buf = append(buf, buildDataRowMsg(rowValues[i], rowNulls[i])...)
	}
	tag := fmt.Sprintf("SELECT %d", len(rowValues))
	buf = append(buf, buildCommandCompleteMsg(tag)...)
	buf = append(buf, buildReadyForQueryMsg()...)
	return buf
}

// buildExtSelectResponse constructs a complete extended query protocol SELECT response:
// ParseComplete + BindComplete + RowDescription + DataRows + CommandComplete + ReadyForQuery.
func buildExtSelectResponse(hasParse, hasBind bool, columns []ColumnInfo, rowValues [][]string, rowNulls [][]bool) []byte {
	var buf []byte
	if hasParse {
		buf = append(buf, buildParseCompleteMsg()...)
	}
	if hasBind {
		buf = append(buf, buildBindCompleteMsg()...)
	}
	buf = append(buf, buildRowDescMsg(columns)...)
	for i := range rowValues {
		buf = append(buf, buildDataRowMsg(rowValues[i], rowNulls[i])...)
	}
	tag := fmt.Sprintf("SELECT %d", len(rowValues))
	buf = append(buf, buildCommandCompleteMsg(tag)...)
	buf = append(buf, buildReadyForQueryMsg()...)
	return buf
}

// buildInsertSQL constructs an INSERT statement from column metadata and values.
// Uses ON CONFLICT DO NOTHING to handle concurrent/duplicate hydration.
func buildInsertSQL(table string, columns []ColumnInfo, values []string, nulls []bool) string {
	colNames := make([]string, len(columns))
	valParts := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = quoteIdent(col.Name)
		if nulls[i] {
			valParts[i] = "NULL"
		} else {
			valParts[i] = quoteLiteral(values[i])
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		quoteIdent(table),
		strings.Join(colNames, ", "),
		strings.Join(valParts, ", "))
}
