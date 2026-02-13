package proxy

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"
)

// TDSColumnInfo holds metadata for a single column from a COLMETADATA token.
type TDSColumnInfo struct {
	Name string
	// We store the raw type byte and collation info for faithful re-emission.
	TypeID byte
}

// TDSQueryResult holds the parsed result of executing a query on a TDS backend.
type TDSQueryResult struct {
	Columns   []TDSColumnInfo
	RowValues [][]string
	RowNulls  [][]bool
	RawMsgs   []byte // all raw response bytes for relay
	Error     string // error message text, if any
}

// execTDSQuery sends a SQL_BATCH message to the TDS backend, reads the
// complete response (until DONE with EOM), and parses COLMETADATA + ROW
// tokens into structured data.
func execTDSQuery(conn net.Conn, sql string) (*TDSQueryResult, error) {
	msg := buildSQLBatchMessage(sql)
	if _, err := conn.Write(msg); err != nil {
		return nil, fmt.Errorf("sending query: %w", err)
	}

	result := &TDSQueryResult{}

	// Read all response packets until DONE with EOM.
	// We need to collect all payload bytes first, then parse tokens.
	var allPayload []byte
	for {
		pkt, err := readTDSPacket(conn)
		if err != nil {
			return nil, fmt.Errorf("reading response: %w", err)
		}
		result.RawMsgs = append(result.RawMsgs, pkt.Raw...)
		allPayload = append(allPayload, pkt.Payload...)
		if pkt.Status&statusEOM != 0 {
			break
		}
	}

	// Parse TDS tokens from the concatenated payload.
	parseTDSTokens(allPayload, result)
	return result, nil
}

// TDS token types for tabular result parsing.
const (
	tokenColMetadata byte = 0x81
	tokenRow         byte = 0xD1
	tokenNBCRow      byte = 0xD2
	tokenReturnValue byte = 0xAC
	tokenOrder       byte = 0xA9
	tokenColInfo     byte = 0xA5
)

// parseTDSTokens walks the token stream and populates the TDSQueryResult.
func parseTDSTokens(data []byte, result *TDSQueryResult) {
	pos := 0
	var colMeta []colMetaEntry // current column metadata for row parsing

	for pos < len(data) {
		tokenType := data[pos]
		pos++

		switch tokenType {
		case tokenColMetadata:
			var newPos int
			colMeta, newPos = parseColMetadata(data, pos)
			pos = newPos
			// Populate result columns.
			result.Columns = make([]TDSColumnInfo, len(colMeta))
			for i, cm := range colMeta {
				result.Columns[i] = TDSColumnInfo{Name: cm.name, TypeID: cm.typeID}
			}

		case tokenRow:
			var rowVals []string
			var rowNulls []bool
			var newPos int
			rowVals, rowNulls, newPos = parseRowToken(data, pos, colMeta)
			pos = newPos
			result.RowValues = append(result.RowValues, rowVals)
			result.RowNulls = append(result.RowNulls, rowNulls)

		case tokenNBCRow:
			var rowVals []string
			var rowNulls []bool
			var newPos int
			rowVals, rowNulls, newPos = parseNBCRowToken(data, pos, colMeta)
			pos = newPos
			result.RowValues = append(result.RowValues, rowVals)
			result.RowNulls = append(result.RowNulls, rowNulls)

		case tokenError:
			var errMsg string
			var newPos int
			errMsg, newPos = parseErrorToken(data, pos)
			pos = newPos
			result.Error = errMsg

		case tokenInfo:
			// INFO token: same structure as ERROR, skip it.
			if pos+2 > len(data) {
				return
			}
			tokenLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2 + tokenLen

		case tokenEnvChange:
			if pos+2 > len(data) {
				return
			}
			tokenLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2 + tokenLen

		case tokenLoginAck:
			if pos+2 > len(data) {
				return
			}
			tokenLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2 + tokenLen

		case tokenReturnValue:
			// RETURNVALUE: skip using its length-prefixed structure.
			pos = skipReturnValueToken(data, pos)

		case tokenOrder:
			if pos+2 > len(data) {
				return
			}
			tokenLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2 + tokenLen

		case tokenColInfo:
			if pos+2 > len(data) {
				return
			}
			tokenLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2 + tokenLen

		case tokenDone, tokenDoneProc, tokenDoneInProc:
			// DONE tokens: 2 (status) + 2 (curCmd) + 8 (rowCount) = 12 bytes.
			if pos+12 > len(data) {
				return
			}
			pos += 12

		default:
			// Unknown token — try to detect if it has a 2-byte length prefix.
			// Tokens >= 0x80 with specific values may have no length; bail out.
			return
		}
	}
}

// colMetaEntry holds parsed per-column metadata from COLMETADATA.
type colMetaEntry struct {
	name   string
	typeID byte
	// Parsed type info needed for row data parsing.
	isFixedLen bool
	fixedLen   int    // for fixed-length types
	maxLen     int    // for variable-length types (max bytes)
	isTextType bool   // NVARCHAR, VARCHAR, NCHAR, CHAR, etc.
	isNType    bool   // National (UTF-16) type
	precision  byte   // for NUMERIC/DECIMAL
	scale      byte   // for NUMERIC/DECIMAL
	collation  []byte // 5 bytes for string types
}

// parseColMetadata parses a COLMETADATA token starting after the token byte.
// Returns the column metadata entries and the new position.
func parseColMetadata(data []byte, pos int) ([]colMetaEntry, int) {
	if pos+2 > len(data) {
		return nil, pos
	}
	colCount := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	if colCount == 0xFFFF {
		// No metadata (e.g., for INSERT/UPDATE/DELETE).
		return nil, pos
	}

	entries := make([]colMetaEntry, colCount)
	for i := 0; i < colCount; i++ {
		if pos+8 > len(data) {
			return entries[:i], pos
		}
		// UserType (4 bytes) + Flags (2 bytes) = 6 bytes before TYPE_INFO.
		pos += 4 // UserType
		pos += 2 // Flags

		// TYPE_INFO: variable structure depending on type byte.
		if pos >= len(data) {
			return entries[:i], pos
		}
		typeID := data[pos]
		pos++
		entries[i].typeID = typeID

		switch {
		// Fixed-length types (no additional length info).
		case typeID == 0x30: // TINYINT (1 byte)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 1
		case typeID == 0x32: // BIT (1 byte)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 1
		case typeID == 0x34: // SMALLINT (2 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 2
		case typeID == 0x38: // INT (4 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 4
		case typeID == 0x3B: // REAL (4 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 4
		case typeID == 0x3E: // FLOAT (8 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 8
		case typeID == 0x7A: // SMALLMONEY (4 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 4
		case typeID == 0x3C: // MONEY (8 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 8
		case typeID == 0x7F: // BIGINT (8 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 8
		case typeID == 0x3D: // DATETIME (8 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 8
		case typeID == 0x3A: // SMALLDATETIME (4 bytes)
			entries[i].isFixedLen = true
			entries[i].fixedLen = 4

		// Variable-length types with 1-byte max length.
		case typeID == 0x26: // INTN (nullable INT)
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			pos++
		case typeID == 0x68: // BITN (nullable BIT)
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			pos++
		case typeID == 0x6D: // FLTN (nullable FLOAT)
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			pos++
		case typeID == 0x6E: // MONEYN (nullable MONEY)
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			pos++
		case typeID == 0x6F: // DATETIMEN (nullable DATETIME)
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			pos++

		// GUID type (0x24): 1-byte max length.
		case typeID == 0x24: // GUIDN (UNIQUEIDENTIFIER)
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			pos++

		// DECIMAL/NUMERIC types (0x6A, 0x6C): 1-byte len + precision + scale.
		case typeID == 0x6A || typeID == 0x6C: // DECIMALN / NUMERICN
			if pos+3 > len(data) {
				return entries[:i], pos
			}
			entries[i].fixedLen = int(data[pos])
			entries[i].precision = data[pos+1]
			entries[i].scale = data[pos+2]
			pos += 3

		// NVARCHAR, NCHAR (0xE7, 0xEF): 2-byte max length + 5-byte collation.
		case typeID == 0xE7 || typeID == 0xEF: // NVARCHAR / NCHAR
			if pos+7 > len(data) {
				return entries[:i], pos
			}
			entries[i].maxLen = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2
			entries[i].collation = data[pos : pos+5]
			pos += 5
			entries[i].isTextType = true
			entries[i].isNType = true

		// VARCHAR, CHAR (0xA7, 0xAF): 2-byte max length + 5-byte collation.
		case typeID == 0xA7 || typeID == 0xAF: // VARCHAR / CHAR
			if pos+7 > len(data) {
				return entries[:i], pos
			}
			entries[i].maxLen = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2
			entries[i].collation = data[pos : pos+5]
			pos += 5
			entries[i].isTextType = true

		// VARBINARY, BINARY (0xAD, 0xA5 is also IMAGE-like): 2-byte max length.
		case typeID == 0xAD: // VARBINARY
			if pos+2 > len(data) {
				return entries[:i], pos
			}
			entries[i].maxLen = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2

		// DATE (0x28): no additional bytes.
		case typeID == 0x28:
			entries[i].fixedLen = 3

		// TIME (0x29), DATETIME2 (0x2A), DATETIMEOFFSET (0x2B): 1-byte scale.
		case typeID == 0x29 || typeID == 0x2A || typeID == 0x2B:
			if pos >= len(data) {
				return entries[:i], pos
			}
			entries[i].scale = data[pos]
			pos++
			// Data length depends on scale.
			switch {
			case entries[i].scale <= 2:
				entries[i].fixedLen = 3
			case entries[i].scale <= 4:
				entries[i].fixedLen = 4
			default:
				entries[i].fixedLen = 5
			}
			if typeID == 0x2A {
				entries[i].fixedLen += 3 // DATE portion
			} else if typeID == 0x2B {
				entries[i].fixedLen += 3 + 2 // DATE + offset
			}

		// XML (0xF1): schema info.
		case typeID == 0xF1:
			if pos >= len(data) {
				return entries[:i], pos
			}
			schemaPresent := data[pos]
			pos++
			if schemaPresent == 1 {
				// Skip dbname, owning_schema, xml_schema_collection.
				for sk := 0; sk < 3; sk++ {
					if pos+1 > len(data) {
						return entries[:i], pos
					}
					sLen := int(data[pos])
					pos++
					pos += sLen * 2 // UTF-16
				}
			}
			entries[i].isTextType = true
			entries[i].isNType = true
			entries[i].maxLen = 0xFFFF

		// SQL_VARIANT (0x62): 4-byte max length.
		case typeID == 0x62:
			if pos+4 > len(data) {
				return entries[:i], pos
			}
			entries[i].maxLen = int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4

		default:
			// Unknown type — bail.
			return entries[:i], pos
		}

		// Read column name: 1-byte length (in UTF-16 chars) + name bytes.
		if pos >= len(data) {
			return entries[:i], pos
		}
		nameLen := int(data[pos])
		pos++
		nameBytes := nameLen * 2
		if pos+nameBytes > len(data) {
			return entries[:i], pos
		}
		entries[i].name = decodeUTF16LE(data[pos : pos+nameBytes])
		pos += nameBytes
	}

	return entries, pos
}

// parseRowToken parses a ROW token (0xD1) using current column metadata.
func parseRowToken(data []byte, pos int, cols []colMetaEntry) ([]string, []bool, int) {
	values := make([]string, len(cols))
	nulls := make([]bool, len(cols))

	for i, col := range cols {
		if pos >= len(data) {
			return values, nulls, pos
		}
		var val string
		var isNull bool
		val, isNull, pos = readColumnValue(data, pos, col)
		values[i] = val
		nulls[i] = isNull
	}

	return values, nulls, pos
}

// parseNBCRowToken parses an NBC_ROW token (0xD2) which uses a null bitmap.
func parseNBCRowToken(data []byte, pos int, cols []colMetaEntry) ([]string, []bool, int) {
	values := make([]string, len(cols))
	nulls := make([]bool, len(cols))

	// Null bitmap: ceil(len(cols)/8) bytes.
	bitmapLen := (len(cols) + 7) / 8
	if pos+bitmapLen > len(data) {
		return values, nulls, pos
	}
	bitmap := data[pos : pos+bitmapLen]
	pos += bitmapLen

	for i, col := range cols {
		// Check null bitmap.
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		if bitmap[byteIdx]&(1<<bitIdx) != 0 {
			nulls[i] = true
			continue
		}
		var val string
		val, _, pos = readColumnValueNonNull(data, pos, col)
		values[i] = val
	}

	return values, nulls, pos
}

// readColumnValue reads a single column value based on type metadata.
// Handles the nullable prefix byte for variable-length nullable types.
func readColumnValue(data []byte, pos int, col colMetaEntry) (string, bool, int) {
	switch {
	case col.isFixedLen:
		// Fixed-length types: data is exactly fixedLen bytes.
		if pos+col.fixedLen > len(data) {
			return "", true, pos
		}
		val := decodeFixedValue(data[pos:pos+col.fixedLen], col)
		pos += col.fixedLen
		return val, false, pos

	case col.typeID == 0x26: // INTN
		return readIntNValue(data, pos, col.fixedLen)

	case col.typeID == 0x68: // BITN
		return readBitNValue(data, pos)

	case col.typeID == 0x6D: // FLTN
		return readFloatNValue(data, pos)

	case col.typeID == 0x6E: // MONEYN
		return readMoneyNValue(data, pos)

	case col.typeID == 0x6F: // DATETIMEN
		return readDateTimeNValue(data, pos)

	case col.typeID == 0x24: // GUIDN
		return readGUIDNValue(data, pos)

	case col.typeID == 0x6A || col.typeID == 0x6C: // DECIMALN / NUMERICN
		return readDecimalNValue(data, pos, col)

	case col.typeID == 0x28: // DATE
		if pos+col.fixedLen > len(data) {
			return "", true, pos
		}
		val := decodeDateValue(data[pos : pos+col.fixedLen])
		pos += col.fixedLen
		return val, false, pos

	case col.typeID == 0x29 || col.typeID == 0x2A || col.typeID == 0x2B: // TIME, DATETIME2, DATETIMEOFFSET
		return readTemporalNValue(data, pos, col)

	case col.isTextType:
		return readTextValue(data, pos, col)

	case col.typeID == 0xAD: // VARBINARY
		return readBinaryValue(data, pos, col)

	case col.typeID == 0xF1: // XML
		return readTextValue(data, pos, col)

	case col.typeID == 0x62: // SQL_VARIANT
		return readVariantValue(data, pos)

	default:
		// Fallback: try to read as variable-length with 1-byte length prefix.
		if pos >= len(data) {
			return "", true, pos
		}
		dataLen := int(data[pos])
		pos++
		if dataLen == 0 {
			return "", true, pos
		}
		if pos+dataLen > len(data) {
			return "", true, pos
		}
		val := fmt.Sprintf("%x", data[pos:pos+dataLen])
		pos += dataLen
		return val, false, pos
	}
}

// readColumnValueNonNull reads a column value that is known to be non-null
// (from NBC_ROW null bitmap). Skips the nullable length prefix.
func readColumnValueNonNull(data []byte, pos int, col colMetaEntry) (string, bool, int) {
	switch {
	case col.isFixedLen:
		if pos+col.fixedLen > len(data) {
			return "", true, pos
		}
		val := decodeFixedValue(data[pos:pos+col.fixedLen], col)
		pos += col.fixedLen
		return val, false, pos

	case col.typeID == 0x26: // INTN
		// Non-null: read length byte then value.
		return readIntNValue(data, pos, col.fixedLen)

	case col.typeID == 0x68: // BITN
		return readBitNValue(data, pos)

	case col.typeID == 0x6D: // FLTN
		return readFloatNValue(data, pos)

	case col.typeID == 0x6E: // MONEYN
		return readMoneyNValue(data, pos)

	case col.typeID == 0x6F: // DATETIMEN
		return readDateTimeNValue(data, pos)

	case col.typeID == 0x24: // GUIDN
		return readGUIDNValue(data, pos)

	case col.typeID == 0x6A || col.typeID == 0x6C: // DECIMALN / NUMERICN
		return readDecimalNValue(data, pos, col)

	case col.typeID == 0x28: // DATE
		if pos+col.fixedLen > len(data) {
			return "", true, pos
		}
		val := decodeDateValue(data[pos : pos+col.fixedLen])
		pos += col.fixedLen
		return val, false, pos

	case col.typeID == 0x29 || col.typeID == 0x2A || col.typeID == 0x2B:
		return readTemporalNValue(data, pos, col)

	case col.isTextType:
		return readTextValue(data, pos, col)

	case col.typeID == 0xAD: // VARBINARY
		return readBinaryValue(data, pos, col)

	case col.typeID == 0xF1: // XML
		return readTextValue(data, pos, col)

	case col.typeID == 0x62: // SQL_VARIANT
		return readVariantValue(data, pos)

	default:
		if pos >= len(data) {
			return "", true, pos
		}
		dataLen := int(data[pos])
		pos++
		if pos+dataLen > len(data) {
			return "", true, pos
		}
		return fmt.Sprintf("%x", data[pos:pos+dataLen]), false, pos + dataLen
	}
}

// readIntNValue reads a nullable integer (INTN) value.
func readIntNValue(data []byte, pos int, _ int) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	var val int64
	switch dataLen {
	case 1:
		val = int64(data[pos])
	case 2:
		val = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
	case 4:
		val = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
	case 8:
		val = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	}
	pos += dataLen
	return fmt.Sprintf("%d", val), false, pos
}

// readBitNValue reads a nullable bit (BITN) value.
func readBitNValue(data []byte, pos int) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos >= len(data) {
		return "", true, pos
	}
	if data[pos] != 0 {
		pos++
		return "1", false, pos
	}
	pos++
	return "0", false, pos
}

// readFloatNValue reads a nullable float (FLTN) value.
func readFloatNValue(data []byte, pos int) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	switch dataLen {
	case 4:
		bits := binary.LittleEndian.Uint32(data[pos : pos+4])
		val := math.Float32frombits(bits)
		pos += 4
		return fmt.Sprintf("%g", val), false, pos
	case 8:
		bits := binary.LittleEndian.Uint64(data[pos : pos+8])
		val := math.Float64frombits(bits)
		pos += 8
		return fmt.Sprintf("%g", val), false, pos
	default:
		pos += dataLen
		return "", true, pos
	}
}

// readMoneyNValue reads a nullable money (MONEYN) value.
func readMoneyNValue(data []byte, pos int) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	switch dataLen {
	case 4: // SMALLMONEY: 4 bytes LE, units of 1/10000
		raw := int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
		pos += 4
		whole := raw / 10000
		frac := raw % 10000
		if frac < 0 {
			frac = -frac
		}
		return fmt.Sprintf("%d.%04d", whole, frac), false, pos
	case 8: // MONEY: high 4 bytes (LE) + low 4 bytes (LE), units of 1/10000
		hi := int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
		lo := int64(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		raw := (hi << 32) | lo
		pos += 8
		whole := raw / 10000
		frac := raw % 10000
		if frac < 0 {
			frac = -frac
		}
		return fmt.Sprintf("%d.%04d", whole, frac), false, pos
	default:
		pos += dataLen
		return "", true, pos
	}
}

// readDateTimeNValue reads a nullable datetime (DATETIMEN) value.
func readDateTimeNValue(data []byte, pos int) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	switch dataLen {
	case 4: // SMALLDATETIME: 2-byte days since 1900-01-01 + 2-byte minutes.
		days := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		minutes := int(binary.LittleEndian.Uint16(data[pos+2 : pos+4]))
		base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		t := base.AddDate(0, 0, days).Add(time.Duration(minutes) * time.Minute)
		pos += 4
		return t.Format("2006-01-02T15:04:05"), false, pos
	case 8: // DATETIME: 4-byte days since 1900-01-01 + 4-byte 300ths of second.
		days := int(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
		ticks := int64(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		t := base.AddDate(0, 0, days)
		ns := ticks * 10000000 / 3 // 300ths of second -> nanoseconds
		t = t.Add(time.Duration(ns) * time.Nanosecond)
		pos += 8
		return t.Format("2006-01-02T15:04:05.000"), false, pos
	default:
		val := fmt.Sprintf("%x", data[pos:pos+dataLen])
		pos += dataLen
		return val, false, pos
	}
}

// readGUIDNValue reads a nullable GUID (UNIQUEIDENTIFIER) value.
func readGUIDNValue(data []byte, pos int) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) || dataLen != 16 {
		pos += dataLen
		return "", true, pos
	}
	// GUID byte order: first 3 groups are LE, last 2 are BE.
	d := data[pos : pos+16]
	guid := fmt.Sprintf("%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
		binary.LittleEndian.Uint32(d[0:4]),
		binary.LittleEndian.Uint16(d[4:6]),
		binary.LittleEndian.Uint16(d[6:8]),
		d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15])
	pos += 16
	return guid, false, pos
}

// readDecimalNValue reads a nullable DECIMAL/NUMERIC value.
func readDecimalNValue(data []byte, pos int, col colMetaEntry) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	// First byte is sign: 1=positive, 0=negative.
	sign := data[pos]
	remaining := data[pos+1 : pos+dataLen]
	pos += dataLen

	// Read as little-endian integer.
	var raw uint64
	for i := len(remaining) - 1; i >= 0; i-- {
		raw = (raw << 8) | uint64(remaining[i])
	}

	// Apply scale.
	scale := int(col.scale)
	str := fmt.Sprintf("%d", raw)

	// Insert decimal point.
	if scale > 0 && scale < len(str) {
		intPart := str[:len(str)-scale]
		fracPart := str[len(str)-scale:]
		str = intPart + "." + fracPart
	} else if scale > 0 {
		// Need leading zeros.
		str = "0." + strings.Repeat("0", scale-len(str)) + str
	}

	if sign == 0 {
		str = "-" + str
	}

	return str, false, pos
}

// readTemporalNValue reads TIME/DATETIME2/DATETIMEOFFSET nullable values.
func readTemporalNValue(data []byte, pos int, col colMetaEntry) (string, bool, int) {
	if pos >= len(data) {
		return "", true, pos
	}
	dataLen := int(data[pos])
	pos++
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}

	startPos := pos

	// Decode the time portion. The number of bytes depends on scale.
	var timeBytes int
	switch {
	case col.scale <= 2:
		timeBytes = 3
	case col.scale <= 4:
		timeBytes = 4
	default:
		timeBytes = 5
	}

	switch col.typeID {
	case 0x29: // TIME
		if dataLen >= timeBytes {
			val := decodeTimeValue(data[pos:pos+timeBytes], col.scale)
			pos = startPos + dataLen
			return val, false, pos
		}
	case 0x2A: // DATETIME2
		if dataLen >= timeBytes+3 {
			timeVal := decodeTimeValue(data[pos:pos+timeBytes], col.scale)
			dateVal := decodeDateValue(data[pos+timeBytes : pos+timeBytes+3])
			pos = startPos + dataLen
			return dateVal + "T" + timeVal, false, pos
		}
	case 0x2B: // DATETIMEOFFSET
		if dataLen >= timeBytes+3+2 {
			timeVal := decodeTimeValue(data[pos:pos+timeBytes], col.scale)
			dateVal := decodeDateValue(data[pos+timeBytes : pos+timeBytes+3])
			offsetMin := int(int16(binary.LittleEndian.Uint16(data[pos+timeBytes+3 : pos+timeBytes+5])))
			pos = startPos + dataLen
			sign := "+"
			if offsetMin < 0 {
				sign = "-"
				offsetMin = -offsetMin
			}
			return fmt.Sprintf("%sT%s%s%02d:%02d", dateVal, timeVal, sign, offsetMin/60, offsetMin%60), false, pos
		}
	}

	// Fallback: hex.
	val := fmt.Sprintf("%x", data[startPos:startPos+dataLen])
	pos = startPos + dataLen
	return val, false, pos
}

// decodeTimeValue decodes a TDS time value (3-5 bytes) into HH:MM:SS.fraction string.
func decodeTimeValue(b []byte, scale byte) string {
	var ticks uint64
	for i := len(b) - 1; i >= 0; i-- {
		ticks = (ticks << 8) | uint64(b[i])
	}
	// ticks is in units of 10^(-scale) seconds.
	divisor := uint64(1)
	for i := byte(0); i < scale; i++ {
		divisor *= 10
	}
	totalSeconds := ticks / divisor
	fracTicks := ticks % divisor

	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	if scale == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}
	fracStr := fmt.Sprintf("%0*d", scale, fracTicks)
	return fmt.Sprintf("%02d:%02d:%02d.%s", hours, minutes, seconds, fracStr)
}

// readTextValue reads a VARCHAR/NVARCHAR/CHAR/NCHAR value.
func readTextValue(data []byte, pos int, col colMetaEntry) (string, bool, int) {
	// For NVARCHAR(MAX) / VARCHAR(MAX), maxLen=0xFFFF and length prefix is 8 bytes (PLP).
	if col.maxLen == 0xFFFF {
		return readPLPValue(data, pos, col.isNType)
	}

	// Regular length prefix: 2 bytes.
	if pos+2 > len(data) {
		return "", true, pos
	}
	dataLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	if dataLen == 0xFFFF {
		return "", true, pos // NULL
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}

	var val string
	if col.isNType {
		val = decodeUTF16LE(data[pos : pos+dataLen])
	} else {
		val = string(data[pos : pos+dataLen])
	}
	pos += dataLen
	return val, false, pos
}

// readPLPValue reads a Partially Length-Prefixed (PLP) value used for MAX types.
func readPLPValue(data []byte, pos int, isNType bool) (string, bool, int) {
	if pos+8 > len(data) {
		return "", true, pos
	}
	totalLen := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8

	// NULL indicator.
	if totalLen == 0xFFFFFFFFFFFFFFFF {
		return "", true, pos
	}

	// Read chunks until terminator (chunk length = 0).
	var allData []byte
	for {
		if pos+4 > len(data) {
			break
		}
		chunkLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if chunkLen == 0 {
			break // Terminator.
		}
		if pos+chunkLen > len(data) {
			break
		}
		allData = append(allData, data[pos:pos+chunkLen]...)
		pos += chunkLen
	}

	var val string
	if isNType {
		val = decodeUTF16LE(allData)
	} else {
		val = string(allData)
	}
	return val, false, pos
}

// readBinaryValue reads a VARBINARY value.
func readBinaryValue(data []byte, pos int, col colMetaEntry) (string, bool, int) {
	if col.maxLen == 0xFFFF {
		// VARBINARY(MAX) uses PLP.
		val, isNull, newPos := readPLPValue(data, pos, false)
		return val, isNull, newPos
	}
	if pos+2 > len(data) {
		return "", true, pos
	}
	dataLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if dataLen == 0xFFFF {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	val := fmt.Sprintf("%x", data[pos:pos+dataLen])
	pos += dataLen
	return val, false, pos
}

// readVariantValue reads a SQL_VARIANT value.
func readVariantValue(data []byte, pos int) (string, bool, int) {
	if pos+4 > len(data) {
		return "", true, pos
	}
	dataLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if dataLen == 0 {
		return "", true, pos
	}
	if pos+dataLen > len(data) {
		return "", true, pos
	}
	val := string(data[pos : pos+dataLen])
	pos += dataLen
	return val, false, pos
}

// decodeFixedValue decodes a fixed-length column value to a string.
func decodeFixedValue(data []byte, col colMetaEntry) string {
	switch col.typeID {
	case 0x30: // TINYINT
		return fmt.Sprintf("%d", data[0])
	case 0x32: // BIT
		if data[0] != 0 {
			return "1"
		}
		return "0"
	case 0x34: // SMALLINT
		return fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(data)))
	case 0x38: // INT
		return fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(data)))
	case 0x7F: // BIGINT
		return fmt.Sprintf("%d", int64(binary.LittleEndian.Uint64(data)))
	default:
		return fmt.Sprintf("%x", data)
	}
}

// decodeDateValue decodes a 3-byte DATE value to YYYY-MM-DD.
func decodeDateValue(data []byte) string {
	if len(data) < 3 {
		return ""
	}
	days := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	// Days since 0001-01-01.
	base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	t := base.AddDate(0, 0, days)
	return t.Format("2006-01-02")
}

// parseErrorToken parses an ERROR token and returns the message text.
func parseErrorToken(data []byte, pos int) (string, int) {
	if pos+2 > len(data) {
		return "unknown error", pos
	}
	tokenLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	endPos := pos + tokenLen
	if endPos > len(data) {
		return "truncated error", endPos
	}

	// Skip: number(4) + state(1) + class(1) = 6 bytes.
	if pos+6 > endPos {
		return "truncated error", endPos
	}
	pos += 6

	// Message: 2-byte length (in chars) + UTF-16LE data.
	if pos+2 > endPos {
		return "truncated error", endPos
	}
	msgChars := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2
	msgBytes := msgChars * 2
	if pos+msgBytes > endPos {
		return "truncated error", endPos
	}
	msg := decodeUTF16LE(data[pos : pos+msgBytes])

	return msg, endPos
}

// skipReturnValueToken skips a RETURNVALUE token.
func skipReturnValueToken(data []byte, pos int) int {
	// RETURNVALUE: ordinal(2) + name(BVarChar) + status(1) + userType(4) + flags(2) + TYPE_INFO + value
	// Complex to parse; skip by searching for next known token.
	// Simpler: it has no length prefix, so we can't easily skip it. Return pos to bail.
	return len(data) // bail out of token parsing
}

// buildTDSSelectResponse constructs a complete TDS tabular result from in-memory data.
// Emits all columns as NVARCHAR(4000) for simplicity.
func buildTDSSelectResponse(columns []TDSColumnInfo, values [][]string, nulls [][]bool) []byte {
	var payload []byte

	// COLMETADATA token.
	payload = append(payload, tokenColMetadata)
	colCount := len(columns)
	payload = append(payload, byte(colCount), byte(colCount>>8)) // LE uint16

	for _, col := range columns {
		// UserType (4 bytes LE) = 0.
		payload = append(payload, 0, 0, 0, 0)
		// Flags (2 bytes): 0x08 = nullable.
		payload = append(payload, 0x08, 0x00)
		// TYPE_INFO: NVARCHAR (0xE7) with max length 8000 (4000 chars * 2).
		payload = append(payload, 0xE7)
		// Max length (2 bytes LE): 8000.
		payload = append(payload, 0x40, 0x1F) // 8000
		// Collation (5 bytes): default.
		payload = append(payload, 0x09, 0x04, 0xD0, 0x00, 0x34)
		// Column name: 1-byte length (chars) + UTF-16LE name.
		nameUTF16 := encodeUTF16LE(col.Name)
		nameChars := len(nameUTF16) / 2
		payload = append(payload, byte(nameChars))
		payload = append(payload, nameUTF16...)
	}

	// ROW tokens.
	for i, row := range values {
		payload = append(payload, tokenRow)
		for j, val := range row {
			isNull := len(nulls) > i && len(nulls[i]) > j && nulls[i][j]
			if isNull {
				// NULL: 0xFFFF for NVARCHAR.
				payload = append(payload, 0xFF, 0xFF)
			} else {
				// Encode value as UTF-16LE with 2-byte length prefix.
				valUTF16 := encodeUTF16LE(val)
				dataLen := len(valUTF16)
				payload = append(payload, byte(dataLen), byte(dataLen>>8))
				payload = append(payload, valUTF16...)
			}
		}
	}

	// DONE token.
	payload = append(payload, tokenDone)
	// Status (2 bytes LE): 0x0010 = DONE_COUNT.
	rowCount := len(values)
	payload = append(payload, 0x10, 0x00)
	// CurCmd (2 bytes LE): 0x00C1 = SELECT.
	payload = append(payload, 0xC1, 0x00)
	// RowCount (8 bytes LE).
	rcBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(rcBuf, uint64(rowCount))
	payload = append(payload, rcBuf...)

	return buildTDSPacket(typeTabularResult, statusEOM, payload)
}

// quoteIdentMSSQL quotes a SQL identifier with brackets for MSSQL.
func quoteIdentMSSQL(s string) string {
	return "[" + strings.ReplaceAll(s, "]", "]]") + "]"
}

// quoteLiteralMSSQL quotes a SQL string literal with single quotes for MSSQL.
func quoteLiteralMSSQL(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
