package proxy

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// ExtHandler handles TDS RPC requests (prepared statements, sp_executesql, etc.)
// for a single MSSQL connection.
type ExtHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	classifier     core.Classifier
	router         *core.Router
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger

	// stmtCache maps prepared statement handles to their SQL text.
	stmtMu    sync.RWMutex
	stmtCache map[int32]string

	// writeHandler for dispatching writes extracted from RPCs.
	writeHandler *WriteHandler
	// readHandler for dispatching reads extracted from RPCs.
	readHandler *ReadHandler

	// Cursor materialization: tracks cursors opened on Shadow via temp tables.
	cursorMu            sync.RWMutex
	materializedCursors map[int32][]string // cursor handle → temp table names
}

// HandleRPC processes a TDS RPC request packet. It attempts to extract
// the procedure name and, for known system procedures, classifies and
// routes the embedded SQL. For unrecognized procedures, it passes through
// to Prod as a safe fallback.
func (eh *ExtHandler) HandleRPC(
	clientConn net.Conn,
	allRaw []byte,
	fullPayload []byte,
) (handled bool, err error) {
	procName, sqlText, ok := parseRPCPayload(fullPayload)
	if !ok {
		return false, nil // Unrecognized — pass through
	}

	procUpper := strings.ToUpper(procName)

	switch procUpper {
	case "SP_EXECUTESQL":
		return true, eh.handleSpExecuteSQL(clientConn, allRaw, fullPayload, sqlText)

	case "SP_PREPARE":
		return true, eh.handleSpPrepare(clientConn, allRaw, fullPayload, sqlText)

	case "SP_EXECUTE":
		return true, eh.handleSpExecute(clientConn, allRaw, fullPayload, sqlText)

	case "SP_UNPREPARE":
		eh.handleSpUnprepare(sqlText)
		// Still forward to prod — sp_unprepare must reach the server.
		return false, nil

	// P2 §3.8: Cursor operations — materialize for dirty tables.
	case "SP_CURSOROPEN", "SP_CURSORPREPARE", "SP_CURSORPREPEXEC":
		return true, eh.handleCursorOpen(clientConn, allRaw, fullPayload, sqlText)

	case "SP_CURSORFETCH":
		// Check if this cursor was materialized on Shadow.
		if handle := eh.extractCursorHandleFromRPC(fullPayload); handle != 0 {
			if eh.isMaterializedCursor(handle) {
				return true, forwardAndRelay(allRaw, eh.shadowConn, clientConn)
			}
		}
		// Default: forward to prod (cursor state lives there).
		return false, nil

	case "SP_CURSORCLOSE", "SP_CURSORUNPREPARE":
		if handle := eh.extractCursorHandleFromRPC(fullPayload); handle != 0 {
			if temps := eh.getMaterializedCursorTemps(handle); temps != nil {
				for _, t := range temps {
					dropUtilTable(eh.shadowConn, t)
				}
				eh.removeMaterializedCursor(handle)
				return true, forwardAndRelay(allRaw, eh.shadowConn, clientConn)
			}
		}
		// Default: forward to both backends to clean up cursor state.
		eh.shadowConn.Write(allRaw) //nolint: errcheck
		drainTDSResponse(eh.shadowConn)
		return false, nil

	case "SP_CURSOREXECUTE":
		// Check if this is a materialized cursor.
		if handle := eh.extractCursorHandleFromRPC(fullPayload); handle != 0 {
			if eh.isMaterializedCursor(handle) {
				return true, forwardAndRelay(allRaw, eh.shadowConn, clientConn)
			}
		}
		return false, nil

	default:
		return false, nil
	}
}

// handleSpExecuteSQL processes sp_executesql RPCs. The SQL parameter is extracted,
// classified, and routed just like a SQL_BATCH.
func (eh *ExtHandler) handleSpExecuteSQL(
	clientConn net.Conn,
	allRaw []byte,
	fullPayload []byte,
	sqlText string,
) error {
	if sqlText == "" {
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	classification, err := eh.classifier.Classify(sqlText)
	if err != nil {
		if eh.verbose {
			log.Printf("[conn %d] ext: classify error for sp_executesql, forwarding to prod: %v", eh.connID, err)
		}
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	strategy := eh.router.Route(classification)

	if eh.verbose {
		log.Printf("[conn %d] ext: sp_executesql %s/%s tables=%v -> %s | %s",
			eh.connID, classification.OpType, classification.SubType,
			classification.Tables, strategy, truncateSQL(sqlText, 100))
	}

	eh.logger.Query(eh.connID, sqlText, classification, strategy, 0)

	return eh.dispatchByStrategy(clientConn, allRaw, fullPayload, classification, strategy)
}

// handleSpPrepare extracts the SQL from sp_prepare, classifies it, caches
// the handle->SQL mapping, and forwards the RPC to prod.
func (eh *ExtHandler) handleSpPrepare(
	clientConn net.Conn,
	allRaw []byte,
	_ []byte,
	sqlText string,
) error {
	// sp_prepare must go to prod to get the handle back.
	// We forward the raw RPC and parse the returned handle from the response.
	if _, err := eh.prodConn.Write(allRaw); err != nil {
		return fmt.Errorf("sending sp_prepare to prod: %w", err)
	}

	// Read and relay the response, extracting the handle.
	handle, err := eh.relayAndExtractHandle(eh.prodConn, clientConn)
	if err != nil {
		return err
	}

	if handle != 0 && sqlText != "" {
		eh.stmtMu.Lock()
		eh.stmtCache[handle] = sqlText
		eh.stmtMu.Unlock()

		if eh.verbose {
			log.Printf("[conn %d] ext: sp_prepare handle=%d sql=%s", eh.connID, handle, truncateSQL(sqlText, 80))
		}
	}

	return nil
}

// handleSpExecute looks up the cached SQL for the handle, classifies it,
// and routes accordingly. If the handle is not found, passes through to prod.
// P3 §4.8: Extracts parameters from the RPC payload for PK substitution.
func (eh *ExtHandler) handleSpExecute(
	clientConn net.Conn,
	allRaw []byte,
	fullPayload []byte,
	handleStr string,
) error {
	handle := parseHandleFromParam(handleStr)

	eh.stmtMu.RLock()
	sqlText, found := eh.stmtCache[handle]
	eh.stmtMu.RUnlock()

	if !found {
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	// P3 §4.8: Extract parameters from the RPC payload for PK substitution.
	params := extractExecuteParams(fullPayload)
	var classification *core.Classification
	var err error
	if len(params) > 0 {
		classification, err = eh.classifier.ClassifyWithParams(sqlText, params)
	} else {
		classification, err = eh.classifier.Classify(sqlText)
	}
	if err != nil {
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	strategy := eh.router.Route(classification)

	if eh.verbose {
		log.Printf("[conn %d] ext: sp_execute handle=%d %s/%s -> %s",
			eh.connID, handle, classification.OpType, classification.SubType, strategy)
	}

	eh.logger.Query(eh.connID, sqlText, classification, strategy, 0)

	return eh.dispatchByStrategy(clientConn, allRaw, fullPayload, classification, strategy)
}

// extractExecuteParams extracts parameter values from an sp_execute RPC payload.
// The first parameter is the handle (int), followed by the bound values.
func extractExecuteParams(payload []byte) []interface{} {
	if len(payload) < 4 {
		return nil
	}

	// Skip ALL_HEADERS.
	allHeadersLen := int(binary.LittleEndian.Uint32(payload[0:4]))
	if allHeadersLen < 4 || allHeadersLen > len(payload) {
		allHeadersLen = 0
	}
	pos := allHeadersLen

	// Skip proc name/ID.
	if pos+2 > len(payload) {
		return nil
	}
	nameLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if nameLen == 0xFFFF {
		pos += 2 // proc ID
	} else {
		pos += nameLen * 2
	}

	// Skip OptionFlags.
	if pos+2 > len(payload) {
		return nil
	}
	pos += 2

	// Skip the first parameter (the handle).
	pos = skipRPCParam(payload, pos)

	// Extract remaining parameters.
	var params []interface{}
	for pos < len(payload) {
		val, newPos := extractRPCParamValue(payload, pos)
		if newPos <= pos {
			break
		}
		params = append(params, val)
		pos = newPos
	}

	return params
}

// skipRPCParam skips over a single RPC parameter and returns the new position.
func skipRPCParam(payload []byte, pos int) int {
	if pos >= len(payload) {
		return pos
	}
	// Parameter name: 1-byte length in chars.
	nameLen := int(payload[pos])
	pos++
	pos += nameLen * 2
	if pos >= len(payload) {
		return pos
	}
	// StatusFlags.
	pos++
	if pos >= len(payload) {
		return pos
	}
	// TYPE_INFO + value.
	typeID := payload[pos]
	pos++
	switch typeID {
	case 0x26, 0x68, 0x6D, 0x6E, 0x6F, 0x24: // INTN, BITN, FLTN, MONEYN, DATETIMEN, GUIDN
		if pos >= len(payload) {
			return pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return pos
		}
		dataLen := int(payload[pos])
		pos++
		pos += dataLen

	case 0x6A, 0x6C: // DECIMALN, NUMERICN
		if pos+3 > len(payload) {
			return pos
		}
		pos += 3 // max length + precision + scale
		if pos >= len(payload) {
			return pos
		}
		dataLen := int(payload[pos])
		pos++
		pos += dataLen

	case 0xE7, 0xEF: // NVARCHAR, NCHAR
		if pos+7 > len(payload) {
			return pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2 + 5 // max length + collation
		if maxLen == 0xFFFF {
			// PLP encoding — skip chunks.
			if pos+8 > len(payload) {
				return pos
			}
			pos += 8 // total length
			for {
				if pos+4 > len(payload) {
					return pos
				}
				chunkLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
				pos += 4
				if chunkLen == 0 {
					break
				}
				pos += chunkLen
			}
		} else {
			if pos+2 > len(payload) {
				return pos
			}
			dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if dataLen != 0xFFFF {
				pos += dataLen
			}
		}

	case 0xA7, 0xAF: // VARCHAR, CHAR
		if pos+7 > len(payload) {
			return pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2 + 5 // max length + collation
		if maxLen == 0xFFFF {
			if pos+8 > len(payload) {
				return pos
			}
			pos += 8
			for {
				if pos+4 > len(payload) {
					return pos
				}
				chunkLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
				pos += 4
				if chunkLen == 0 {
					break
				}
				pos += chunkLen
			}
		} else {
			if pos+2 > len(payload) {
				return pos
			}
			dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if dataLen != 0xFFFF {
				pos += dataLen
			}
		}

	case 0xAD: // VARBINARY
		if pos+2 > len(payload) {
			return pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if maxLen == 0xFFFF {
			if pos+8 > len(payload) {
				return pos
			}
			pos += 8
			for {
				if pos+4 > len(payload) {
					return pos
				}
				chunkLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
				pos += 4
				if chunkLen == 0 {
					break
				}
				pos += chunkLen
			}
		} else {
			if pos+2 > len(payload) {
				return pos
			}
			dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if dataLen != 0xFFFF {
				pos += dataLen
			}
		}

	case 0x28: // DATE (no metadata bytes, 1-byte length + 3-byte value)
		if pos >= len(payload) {
			return pos
		}
		dataLen := int(payload[pos])
		pos++
		pos += dataLen

	case 0x29, 0x2A, 0x2B: // TIME, DATETIME2, DATETIMEOFFSET
		if pos >= len(payload) {
			return pos
		}
		pos++ // scale byte
		if pos >= len(payload) {
			return pos
		}
		dataLen := int(payload[pos])
		pos++
		pos += dataLen

	case 0x62: // SQL_VARIANT
		if pos+4 > len(payload) {
			return pos
		}
		pos += 4 // max length
		if pos+4 > len(payload) {
			return pos
		}
		dataLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		pos += dataLen

	case 0x30: // TINYINT (fixed 1)
		pos++
	case 0x32: // BIT (fixed 1)
		pos++
	case 0x34: // SMALLINT (fixed 2)
		pos += 2
	case 0x38: // INT (fixed 4)
		pos += 4
	case 0x3B: // REAL (fixed 4)
		pos += 4
	case 0x3E: // FLOAT (fixed 8)
		pos += 8
	case 0x7A: // SMALLMONEY (fixed 4)
		pos += 4
	case 0x3C: // MONEY (fixed 8)
		pos += 8
	case 0x7F: // BIGINT (fixed 8)
		pos += 8
	case 0x3D: // DATETIME (fixed 8)
		pos += 8
	case 0x3A: // SMALLDATETIME (fixed 4)
		pos += 4

	default:
		// Unknown type — can't skip reliably, bail.
		return len(payload)
	}
	return pos
}

// extractRPCParamValue extracts the string value of an RPC parameter at the given position.
func extractRPCParamValue(payload []byte, pos int) (interface{}, int) {
	if pos >= len(payload) {
		return nil, pos
	}
	// Parameter name.
	nameLen := int(payload[pos])
	pos++
	pos += nameLen * 2
	if pos >= len(payload) {
		return nil, pos
	}
	// StatusFlags.
	pos++
	if pos >= len(payload) {
		return nil, pos
	}
	// TYPE_INFO.
	typeID := payload[pos]
	pos++
	switch typeID {
	case 0x26: // INTN
		if pos >= len(payload) {
			return nil, pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		switch dataLen {
		case 1:
			val := int64(payload[pos])
			pos++
			return val, pos
		case 2:
			val := int64(int16(binary.LittleEndian.Uint16(payload[pos : pos+2])))
			pos += 2
			return val, pos
		case 4:
			val := int64(int32(binary.LittleEndian.Uint32(payload[pos : pos+4])))
			pos += 4
			return val, pos
		case 8:
			val := int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			return val, pos
		default:
			pos += dataLen
			return nil, pos
		}

	case 0x68: // BITN
		if pos >= len(payload) {
			return nil, pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos >= len(payload) {
			return nil, pos
		}
		if payload[pos] != 0 {
			pos++
			return "1", pos
		}
		pos++
		return "0", pos

	case 0x6D: // FLTN
		if pos >= len(payload) {
			return nil, pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		switch dataLen {
		case 4:
			bits := binary.LittleEndian.Uint32(payload[pos : pos+4])
			val := math.Float32frombits(bits)
			pos += 4
			return fmt.Sprintf("%g", val), pos
		case 8:
			bits := binary.LittleEndian.Uint64(payload[pos : pos+8])
			val := math.Float64frombits(bits)
			pos += 8
			return fmt.Sprintf("%g", val), pos
		default:
			pos += dataLen
			return nil, pos
		}

	case 0x6E: // MONEYN
		if pos >= len(payload) {
			return nil, pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		switch dataLen {
		case 4:
			raw := int64(int32(binary.LittleEndian.Uint32(payload[pos : pos+4])))
			pos += 4
			whole := raw / 10000
			frac := raw % 10000
			if frac < 0 {
				frac = -frac
			}
			return fmt.Sprintf("%d.%04d", whole, frac), pos
		case 8:
			hi := int64(int32(binary.LittleEndian.Uint32(payload[pos : pos+4])))
			lo := int64(binary.LittleEndian.Uint32(payload[pos+4 : pos+8]))
			raw := (hi << 32) | lo
			pos += 8
			whole := raw / 10000
			frac := raw % 10000
			if frac < 0 {
				frac = -frac
			}
			return fmt.Sprintf("%d.%04d", whole, frac), pos
		default:
			pos += dataLen
			return nil, pos
		}

	case 0x6F: // DATETIMEN
		if pos >= len(payload) {
			return nil, pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		switch dataLen {
		case 4:
			days := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
			minutes := int(binary.LittleEndian.Uint16(payload[pos+2 : pos+4]))
			base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
			t := base.AddDate(0, 0, days).Add(time.Duration(minutes) * time.Minute)
			pos += 4
			return t.Format("2006-01-02T15:04:05"), pos
		case 8:
			days := int(int32(binary.LittleEndian.Uint32(payload[pos : pos+4])))
			ticks := int64(binary.LittleEndian.Uint32(payload[pos+4 : pos+8]))
			base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
			t := base.AddDate(0, 0, days)
			ns := ticks * 10000000 / 3
			t = t.Add(time.Duration(ns) * time.Nanosecond)
			pos += 8
			return t.Format("2006-01-02T15:04:05.000"), pos
		default:
			pos += dataLen
			return nil, pos
		}

	case 0x24: // GUIDN
		if pos >= len(payload) {
			return nil, pos
		}
		pos++ // max length
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) || dataLen != 16 {
			pos += dataLen
			return nil, pos
		}
		d := payload[pos : pos+16]
		guid := fmt.Sprintf("%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
			binary.LittleEndian.Uint32(d[0:4]),
			binary.LittleEndian.Uint16(d[4:6]),
			binary.LittleEndian.Uint16(d[6:8]),
			d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15])
		pos += 16
		return guid, pos

	case 0x6A, 0x6C: // DECIMALN, NUMERICN
		if pos+3 > len(payload) {
			return nil, pos
		}
		pos++                  // max length
		_ = payload[pos]       // precision
		scale := payload[pos+1] // scale
		pos += 2
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		sign := payload[pos]
		remaining := payload[pos+1 : pos+dataLen]
		pos += dataLen
		var raw uint64
		for i := len(remaining) - 1; i >= 0; i-- {
			raw = (raw << 8) | uint64(remaining[i])
		}
		str := fmt.Sprintf("%d", raw)
		sc := int(scale)
		if sc > 0 && sc < len(str) {
			str = str[:len(str)-sc] + "." + str[len(str)-sc:]
		} else if sc > 0 {
			str = "0." + strings.Repeat("0", sc-len(str)) + str
		}
		if sign == 0 {
			str = "-" + str
		}
		return str, pos

	case 0xE7, 0xEF: // NVARCHAR, NCHAR
		if pos+7 > len(payload) {
			return nil, pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2 + 5
		if maxLen == 0xFFFF {
			str := readPLPString(payload, pos)
			if pos+8 > len(payload) {
				return str, pos
			}
			pos += 8
			for {
				if pos+4 > len(payload) {
					break
				}
				chunkLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
				pos += 4
				if chunkLen == 0 {
					break
				}
				pos += chunkLen
			}
			return str, pos
		}
		if pos+2 > len(payload) {
			return nil, pos
		}
		dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if dataLen == 0xFFFF {
			return nil, pos
		}
		if pos+dataLen > len(payload) {
			return nil, pos
		}
		str := decodeUTF16LE(payload[pos : pos+dataLen])
		pos += dataLen
		return str, pos

	case 0xA7, 0xAF: // VARCHAR, CHAR
		if pos+7 > len(payload) {
			return nil, pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2 + 5
		if maxLen == 0xFFFF {
			val, _, newPos := readPLPValue(payload, pos, false)
			return val, newPos
		}
		if pos+2 > len(payload) {
			return nil, pos
		}
		dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if dataLen == 0xFFFF {
			return nil, pos
		}
		if pos+dataLen > len(payload) {
			return nil, pos
		}
		str := string(payload[pos : pos+dataLen])
		pos += dataLen
		return str, pos

	case 0xAD: // VARBINARY
		if pos+2 > len(payload) {
			return nil, pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if maxLen == 0xFFFF {
			val, _, newPos := readPLPValue(payload, pos, false)
			return val, newPos
		}
		if pos+2 > len(payload) {
			return nil, pos
		}
		dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if dataLen == 0xFFFF {
			return nil, pos
		}
		if pos+dataLen > len(payload) {
			return nil, pos
		}
		val := fmt.Sprintf("%x", payload[pos:pos+dataLen])
		pos += dataLen
		return val, pos

	case 0x28: // DATE
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		days := int(payload[pos]) | int(payload[pos+1])<<8 | int(payload[pos+2])<<16
		base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		t := base.AddDate(0, 0, days)
		pos += dataLen
		return t.Format("2006-01-02"), pos

	case 0x29, 0x2A, 0x2B: // TIME, DATETIME2, DATETIMEOFFSET
		if pos >= len(payload) {
			return nil, pos
		}
		scale := payload[pos]
		pos++
		if pos >= len(payload) {
			return nil, pos
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return nil, pos
		}
		// Use hex encoding as fallback — sufficient for classification.
		val := fmt.Sprintf("%x", payload[pos:pos+dataLen])
		pos += dataLen
		_ = scale
		return val, pos

	default:
		return nil, len(payload)
	}
}

// handleSpUnprepare evicts a handle from the cache.
func (eh *ExtHandler) handleSpUnprepare(handleStr string) {
	handle := parseHandleFromParam(handleStr)
	eh.stmtMu.Lock()
	delete(eh.stmtCache, handle)
	eh.stmtMu.Unlock()

	if eh.verbose {
		log.Printf("[conn %d] ext: sp_unprepare handle=%d", eh.connID, handle)
	}
}

// dispatchByStrategy routes a classified query to the appropriate handler/backend.
func (eh *ExtHandler) dispatchByStrategy(
	clientConn net.Conn,
	allRaw []byte,
	fullPayload []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyShadowWrite, core.StrategyHydrateAndWrite, core.StrategyShadowDelete:
		if eh.writeHandler != nil {
			return eh.writeHandler.HandleWrite(clientConn, allRaw, cl, strategy)
		}
		return forwardAndRelay(allRaw, eh.shadowConn, clientConn)

	case core.StrategyMergedRead, core.StrategyJoinPatch:
		if eh.readHandler != nil {
			return eh.readHandler.HandleRead(clientConn, allRaw, fullPayload, cl, strategy)
		}
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)

	case core.StrategyShadowDDL:
		return forwardAndRelay(allRaw, eh.shadowConn, clientConn)

	case core.StrategyTransaction:
		// Transaction within an RPC is unusual; forward to both.
		eh.shadowConn.Write(allRaw) //nolint: errcheck
		drainTDSResponse(eh.shadowConn)
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)

	case core.StrategyNotSupported:
		msg := cl.NotSupportedMsg
		if msg == "" {
			msg = core.UnsupportedTransactionMsg
		}
		errPkt := buildErrorResponse(msg)
		_, err := clientConn.Write(errPkt)
		return err

	case core.StrategyTruncate:
		// Forward to Shadow, mark table fully shadowed.
		if err := forwardAndRelay(allRaw, eh.shadowConn, clientConn); err != nil {
			return err
		}
		for _, table := range cl.Tables {
			if eh.schemaRegistry != nil {
				eh.schemaRegistry.MarkFullyShadowed(table)
			}
			if eh.deltaMap != nil {
				eh.deltaMap.ClearTable(table)
			}
			if eh.tombstones != nil {
				eh.tombstones.ClearTable(table)
			}
		}
		return nil

	case core.StrategyProdDirect:
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)

	default:
		msg := core.UnsupportedTransactionMsg
		errPkt := buildErrorResponse(msg)
		_, err := clientConn.Write(errPkt)
		return err
	}
}

// relayAndExtractHandle relays a TDS response from backend to client and
// tries to extract the prepared statement handle from a RETURNVALUE token.
func (eh *ExtHandler) relayAndExtractHandle(backend, client net.Conn) (int32, error) {
	var handle int32
	for {
		pkt, err := readTDSPacket(backend)
		if err != nil {
			return 0, fmt.Errorf("reading sp_prepare response: %w", err)
		}

		// Try to extract handle from RETURNVALUE token in payload.
		if h, ok := extractReturnValueInt32(pkt.Payload); ok {
			handle = h
		}

		if _, err := client.Write(pkt.Raw); err != nil {
			return handle, fmt.Errorf("relaying sp_prepare response: %w", err)
		}

		if pkt.Status&statusEOM != 0 {
			return handle, nil
		}
	}
}

// parseRPCPayload extracts the procedure name and the first NVARCHAR parameter
// (the SQL text) from a TDS RPC request payload.
//
// TDS RPC payload format after ALL_HEADERS:
//   - NameLength (2 bytes LE, in UTF-16 chars; 0xFFFF means proc ID follows)
//   - If NameLength != 0xFFFF: ProcName (NameLength * 2 bytes, UTF-16LE)
//   - If NameLength == 0xFFFF: ProcID (2 bytes LE, well-known stored proc ID)
//   - OptionFlags (2 bytes)
//   - Parameters...
func parseRPCPayload(payload []byte) (procName, firstParam string, ok bool) {
	if len(payload) < 4 {
		return "", "", false
	}

	// Skip ALL_HEADERS.
	allHeadersLen := int(binary.LittleEndian.Uint32(payload[0:4]))
	if allHeadersLen < 4 || allHeadersLen > len(payload) {
		// No ALL_HEADERS — start from 0.
		allHeadersLen = 0
	}

	pos := allHeadersLen
	if pos+2 > len(payload) {
		return "", "", false
	}

	nameLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2

	if nameLen == 0xFFFF {
		// Well-known procedure ID.
		if pos+2 > len(payload) {
			return "", "", false
		}
		procID := binary.LittleEndian.Uint16(payload[pos : pos+2])
		pos += 2
		procName = wellKnownProcName(procID)
	} else if nameLen > 0 {
		nameBytes := nameLen * 2
		if pos+nameBytes > len(payload) {
			return "", "", false
		}
		procName = decodeUTF16LE(payload[pos : pos+nameBytes])
		pos += nameBytes
	}

	if procName == "" {
		return "", "", false
	}

	// Skip OptionFlags (2 bytes).
	if pos+2 > len(payload) {
		return procName, "", true
	}
	pos += 2

	// Parse first parameter to extract SQL text.
	firstParam = extractFirstNVarcharParam(payload, pos)

	return procName, firstParam, true
}

// wellKnownProcName maps well-known TDS procedure IDs to names.
func wellKnownProcName(procID uint16) string {
	switch procID {
	case 1:
		return "sp_cursor"
	case 2:
		return "sp_cursoropen"
	case 3:
		return "sp_cursorprepare"
	case 4:
		return "sp_cursorexecute"
	case 5:
		return "sp_cursorprepexec"
	case 6:
		return "sp_cursorunprepare"
	case 7:
		return "sp_cursorfetch"
	case 8:
		return "sp_cursoroption"
	case 9:
		return "sp_cursorclose"
	case 10:
		return "sp_executesql"
	case 11:
		return "sp_prepare"
	case 12:
		return "sp_execute"
	case 13:
		return "sp_prepexec"
	case 14:
		return "sp_prepexecrpc"
	case 15:
		return "sp_unprepare"
	default:
		return ""
	}
}

// extractFirstNVarcharParam tries to extract the first NVARCHAR/NTEXT parameter
// from the RPC parameter data starting at pos.
//
// TDS RPC parameter format:
//   - Name: 1-byte length (chars) + UTF-16LE name
//   - StatusFlags: 1 byte
//   - TYPE_INFO: variable (type byte + type-specific metadata)
//   - Value data
func extractFirstNVarcharParam(payload []byte, pos int) string {
	if pos >= len(payload) {
		return ""
	}

	// Parameter name: 1-byte length in chars.
	nameLen := int(payload[pos])
	pos++
	pos += nameLen * 2 // Skip UTF-16LE name.

	if pos >= len(payload) {
		return ""
	}

	// StatusFlags: 1 byte.
	pos++

	if pos >= len(payload) {
		return ""
	}

	// TYPE_INFO: type byte.
	typeID := payload[pos]
	pos++

	switch typeID {
	case 0xE7: // NVARCHAR
		// 2-byte max length + 5-byte collation.
		if pos+7 > len(payload) {
			return ""
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		pos += 5 // collation

		if maxLen == 0xFFFF {
			// NVARCHAR(MAX) uses PLP encoding.
			return readPLPString(payload, pos)
		}

		// Regular NVARCHAR: 2-byte data length prefix.
		if pos+2 > len(payload) {
			return ""
		}
		dataLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if dataLen == 0xFFFF {
			return "" // NULL
		}
		if pos+dataLen > len(payload) {
			return ""
		}
		return decodeUTF16LE(payload[pos : pos+dataLen])

	case 0x26: // INTN — for sp_execute handle parameter
		if pos >= len(payload) {
			return ""
		}
		maxLen := int(payload[pos])
		pos++
		if pos >= len(payload) {
			return ""
		}
		dataLen := int(payload[pos])
		pos++
		if dataLen == 0 || pos+dataLen > len(payload) {
			return ""
		}
		// Return the int value as a string.
		switch dataLen {
		case 4:
			val := int32(binary.LittleEndian.Uint32(payload[pos : pos+4]))
			return fmt.Sprintf("%d", val)
		case 2:
			val := int16(binary.LittleEndian.Uint16(payload[pos : pos+2]))
			return fmt.Sprintf("%d", val)
		default:
			_ = maxLen
			return ""
		}

	default:
		return ""
	}
}

// readPLPString reads a PLP-encoded NVARCHAR(MAX) value as a string.
func readPLPString(payload []byte, pos int) string {
	if pos+8 > len(payload) {
		return ""
	}
	totalLen := binary.LittleEndian.Uint64(payload[pos : pos+8])
	pos += 8

	if totalLen == 0xFFFFFFFFFFFFFFFF {
		return "" // NULL
	}

	var allData []byte
	for {
		if pos+4 > len(payload) {
			break
		}
		chunkLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if chunkLen == 0 {
			break
		}
		if pos+chunkLen > len(payload) {
			break
		}
		allData = append(allData, payload[pos:pos+chunkLen]...)
		pos += chunkLen
	}

	return decodeUTF16LE(allData)
}

// handleCursorOpen processes cursor open RPCs. If the inner query references
// dirty tables (deltas, tombstones, schema diffs), materializes the merged
// data into temp tables on Shadow, rewrites the SQL, and opens the cursor
// on Shadow. Otherwise, forwards to Prod.
func (eh *ExtHandler) handleCursorOpen(
	clientConn net.Conn,
	allRaw []byte,
	fullPayload []byte,
	sqlText string,
) error {
	// If no SQL text or no read handler, forward to Prod.
	if sqlText == "" || eh.readHandler == nil {
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	// Classify the inner query.
	cl, err := eh.classifier.Classify(sqlText)
	if err != nil || cl == nil || len(cl.Tables) == 0 {
		if eh.verbose {
			log.Printf("[conn %d] ext: cursor open, classify failed or no tables, forwarding to prod", eh.connID)
		}
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	// Check for dirty tables.
	var dirtyTables []string
	for _, table := range cl.Tables {
		if eh.readHandler.isTableDirty(table) {
			dirtyTables = append(dirtyTables, table)
		}
	}

	if len(dirtyTables) == 0 {
		if eh.verbose {
			log.Printf("[conn %d] ext: cursor open, no dirty tables, forwarding to prod: %s",
				eh.connID, truncateSQL(sqlText, 80))
		}
		return forwardAndRelay(allRaw, eh.prodConn, clientConn)
	}

	if eh.verbose {
		log.Printf("[conn %d] ext: cursor open, materializing %d dirty tables: %s",
			eh.connID, len(dirtyTables), truncateSQL(sqlText, 80))
	}

	// Materialize each dirty table into a temp table on Shadow.
	rewriteMap := make(map[string]string) // original → temp table
	var tempNames []string
	for _, table := range dirtyTables {
		baseSQL := "SELECT * FROM " + quoteIdentMSSQL(table)
		baseCl := &core.Classification{
			OpType:  core.OpRead,
			SubType: core.SubSelect,
			RawSQL:  baseSQL,
			Tables:  []string{table},
		}
		columns, values, nulls, mergeErr := eh.readHandler.mergedReadCore(baseCl, baseSQL)
		if mergeErr != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: cursor materialize failed for %s: %v", eh.connID, table, mergeErr)
			}
			// Clean up and fall back to Prod.
			for _, tn := range tempNames {
				dropUtilTable(eh.shadowConn, tn)
			}
			return forwardAndRelay(allRaw, eh.prodConn, clientConn)
		}

		tempName, matErr := materializeToTempTable(eh.shadowConn, "cursor_"+table, columns, values, nulls)
		if matErr != nil {
			if eh.verbose {
				log.Printf("[conn %d] ext: cursor temp table creation failed for %s: %v", eh.connID, table, matErr)
			}
			for _, tn := range tempNames {
				dropUtilTable(eh.shadowConn, tn)
			}
			return forwardAndRelay(allRaw, eh.prodConn, clientConn)
		}
		rewriteMap[table] = tempName
		tempNames = append(tempNames, tempName)
	}

	// Rewrite the SQL to reference temp tables.
	rewrittenSQL := sqlText
	for original, temp := range rewriteMap {
		rewrittenSQL = rewriteTableReference(rewrittenSQL, original, temp)
	}

	// Rebuild the cursor open RPC with rewritten SQL.
	newRaw := eh.rebuildCursorOpenRPC(allRaw, fullPayload, sqlText, rewrittenSQL)

	// Forward to Shadow and capture the cursor handle from the response.
	if _, writeErr := eh.shadowConn.Write(newRaw); writeErr != nil {
		for _, tn := range tempNames {
			dropUtilTable(eh.shadowConn, tn)
		}
		return fmt.Errorf("sending cursor open to shadow: %w", writeErr)
	}

	handle, relayErr := eh.relayAndExtractHandle(eh.shadowConn, clientConn)
	if relayErr != nil {
		for _, tn := range tempNames {
			dropUtilTable(eh.shadowConn, tn)
		}
		return relayErr
	}

	// Track the materialized cursor.
	if handle != 0 {
		eh.cursorMu.Lock()
		if eh.materializedCursors == nil {
			eh.materializedCursors = make(map[int32][]string)
		}
		eh.materializedCursors[handle] = tempNames
		eh.cursorMu.Unlock()

		if eh.verbose {
			log.Printf("[conn %d] ext: cursor open materialized, handle=%d, %d temp tables",
				eh.connID, handle, len(tempNames))
		}
	} else {
		// No handle captured — clean up temp tables.
		for _, tn := range tempNames {
			dropUtilTable(eh.shadowConn, tn)
		}
	}

	return nil
}

// rebuildCursorOpenRPC rebuilds a cursor open RPC message with a rewritten SQL parameter.
// It finds the SQL text in the raw payload and replaces it with the new SQL.
func (eh *ExtHandler) rebuildCursorOpenRPC(allRaw, fullPayload []byte, oldSQL, newSQL string) []byte {
	// Find the old SQL in the raw payload (as UTF-16LE).
	oldUTF16 := encodeUTF16LE(oldSQL)
	newUTF16 := encodeUTF16LE(newSQL)

	// Search for the old SQL bytes in allRaw.
	idx := bytesIndex(allRaw, oldUTF16)
	if idx < 0 {
		// Can't find the SQL — return original.
		return allRaw
	}

	// Calculate the length difference.
	lenDiff := len(newUTF16) - len(oldUTF16)

	// Build new raw message with replaced SQL.
	newRaw := make([]byte, 0, len(allRaw)+lenDiff)
	newRaw = append(newRaw, allRaw[:idx]...)
	newRaw = append(newRaw, newUTF16...)
	newRaw = append(newRaw, allRaw[idx+len(oldUTF16):]...)

	// Update the 2-byte data length prefix before the SQL (if present).
	// The length prefix is 2 bytes before the SQL data.
	if idx >= 2 {
		oldDataLen := binary.LittleEndian.Uint16(allRaw[idx-2 : idx])
		if int(oldDataLen) == len(oldUTF16) {
			newDataLen := uint16(len(newUTF16))
			binary.LittleEndian.PutUint16(newRaw[idx-2:], newDataLen)
		}
	}

	// Fix TDS packet header lengths (8-byte header, bytes 2-3 = total packet length).
	// Rebuild packets to fix framing.
	return rebuildTDSPacketFraming(newRaw)
}

// bytesIndex finds the first occurrence of needle in haystack.
func bytesIndex(haystack, needle []byte) int {
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// rebuildTDSPacketFraming fixes TDS packet headers after payload modification.
// TDS packets have an 8-byte header with total length at bytes 2-3.
func rebuildTDSPacketFraming(raw []byte) []byte {
	if len(raw) < 8 {
		return raw
	}
	// For single-packet messages, just fix the length.
	pktType := raw[0]
	status := raw[1]

	// Build a single EOM packet with all the payload.
	payload := raw[8:]
	totalLen := 8 + len(payload)
	if totalLen > 0xFFFF {
		totalLen = 0xFFFF // TDS max packet size
	}

	result := make([]byte, 8+len(payload))
	result[0] = pktType
	result[1] = status | statusEOM
	binary.BigEndian.PutUint16(result[2:4], uint16(totalLen))
	result[4] = 0 // SPID
	result[5] = 0
	result[6] = 0 // PacketID
	result[7] = 0
	copy(result[8:], payload)

	return result
}

// isMaterializedCursor checks if a cursor handle was materialized on Shadow.
func (eh *ExtHandler) isMaterializedCursor(handle int32) bool {
	eh.cursorMu.RLock()
	defer eh.cursorMu.RUnlock()
	_, ok := eh.materializedCursors[handle]
	return ok
}

// getMaterializedCursorTemps returns temp table names for a materialized cursor.
func (eh *ExtHandler) getMaterializedCursorTemps(handle int32) []string {
	eh.cursorMu.RLock()
	defer eh.cursorMu.RUnlock()
	return eh.materializedCursors[handle]
}

// removeMaterializedCursor removes a cursor from the materialized tracking.
func (eh *ExtHandler) removeMaterializedCursor(handle int32) {
	eh.cursorMu.Lock()
	delete(eh.materializedCursors, handle)
	eh.cursorMu.Unlock()
}

// extractCursorHandleFromRPC extracts the cursor handle (first int param) from an RPC payload.
func (eh *ExtHandler) extractCursorHandleFromRPC(fullPayload []byte) int32 {
	_, _, ok := parseRPCPayload(fullPayload)
	if !ok {
		return 0
	}
	// The first parameter after proc name + option flags is the cursor handle.
	// Re-parse to get to the first param position.
	if len(fullPayload) < 4 {
		return 0
	}
	allHeadersLen := int(binary.LittleEndian.Uint32(fullPayload[0:4]))
	if allHeadersLen < 4 || allHeadersLen > len(fullPayload) {
		allHeadersLen = 0
	}
	pos := allHeadersLen
	if pos+2 > len(fullPayload) {
		return 0
	}
	nameLen := int(binary.LittleEndian.Uint16(fullPayload[pos : pos+2]))
	pos += 2
	if nameLen == 0xFFFF {
		pos += 2
	} else {
		pos += nameLen * 2
	}
	if pos+2 > len(fullPayload) {
		return 0
	}
	pos += 2 // OptionFlags

	// First param should be the cursor handle (INTN).
	val, _ := extractRPCParamValue(fullPayload, pos)
	switch v := val.(type) {
	case int64:
		return int32(v)
	case int32:
		return v
	case int16:
		return int32(v)
	}
	return 0
}

// parseHandleFromParam parses a string-encoded int32 handle.
func parseHandleFromParam(s string) int32 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	var val int32
	fmt.Sscanf(s, "%d", &val)
	return val
}

// extractReturnValueInt32 tries to find a RETURNVALUE token (0xAC) in the
// payload and extract a 4-byte int32 value from it. This is used to capture
// the handle returned by sp_prepare.
func extractReturnValueInt32(payload []byte) (int32, bool) {
	// RETURNVALUE token format:
	// Token(1) + Ordinal(2) + ParamName(BVarChar) + Status(1) + UserType(4) + Flags(2) + TYPE_INFO + Value
	pos := 0
	for pos < len(payload) {
		if payload[pos] != tokenReturnValue {
			pos++
			continue
		}
		pos++ // skip token byte

		// Ordinal (2 bytes).
		if pos+2 > len(payload) {
			return 0, false
		}
		pos += 2

		// ParamName: 1-byte length in chars.
		if pos >= len(payload) {
			return 0, false
		}
		paramNameLen := int(payload[pos])
		pos++
		pos += paramNameLen * 2

		// Status (1 byte).
		if pos >= len(payload) {
			return 0, false
		}
		pos++

		// UserType (4 bytes).
		if pos+4 > len(payload) {
			return 0, false
		}
		pos += 4

		// Flags (2 bytes).
		if pos+2 > len(payload) {
			return 0, false
		}
		pos += 2

		// TYPE_INFO: type byte.
		if pos >= len(payload) {
			return 0, false
		}
		typeID := payload[pos]
		pos++

		if typeID == 0x26 { // INTN
			if pos >= len(payload) {
				return 0, false
			}
			pos++ // max length byte
			if pos >= len(payload) {
				return 0, false
			}
			dataLen := int(payload[pos])
			pos++
			if dataLen == 4 && pos+4 <= len(payload) {
				val := int32(binary.LittleEndian.Uint32(payload[pos : pos+4]))
				return val, true
			}
		}

		return 0, false
	}
	return 0, false
}
