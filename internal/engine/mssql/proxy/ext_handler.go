package proxy

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

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

	// P2 §3.8: Cursor operations — forward to both backends.
	case "SP_CURSOROPEN", "SP_CURSORPREPARE", "SP_CURSORPREPEXEC":
		return true, eh.handleCursorOpen(clientConn, allRaw, fullPayload, sqlText)

	case "SP_CURSORFETCH":
		// Cursor fetch — forward to prod (cursor state lives there).
		return false, nil

	case "SP_CURSORCLOSE", "SP_CURSORUNPREPARE":
		// Forward to both backends to clean up cursor state.
		eh.shadowConn.Write(allRaw) //nolint: errcheck
		drainTDSResponse(eh.shadowConn)
		return false, nil

	case "SP_CURSOREXECUTE":
		// Forward to prod for cursor execution.
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
	// TYPE_INFO + value (simplified skip).
	typeID := payload[pos]
	pos++
	switch typeID {
	case 0x26: // INTN
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
	case 0xE7: // NVARCHAR
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
	default:
		// Unknown type — can't skip reliably, return current position.
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
		case 4:
			val := int32(binary.LittleEndian.Uint32(payload[pos : pos+4]))
			pos += 4
			return val, pos
		case 2:
			val := int16(binary.LittleEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			return val, pos
		case 8:
			val := int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			return val, pos
		default:
			pos += dataLen
			return nil, pos
		}
	case 0xE7: // NVARCHAR
		if pos+7 > len(payload) {
			return nil, pos
		}
		maxLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2 + 5
		if maxLen == 0xFFFF {
			str := readPLPString(payload, pos)
			// Skip PLP data.
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

// handleCursorOpen processes cursor open RPCs by classifying the inner query.
// If the query affects dirty tables, the cursor is opened on prod (as fallback).
// Full cursor materialization via temp tables is a future enhancement.
func (eh *ExtHandler) handleCursorOpen(
	clientConn net.Conn,
	allRaw []byte,
	_ []byte,
	sqlText string,
) error {
	// For now, forward cursor opens to prod.
	// Future: classify inner query, if dirty tables → materialize via merged read.
	if eh.verbose && sqlText != "" {
		log.Printf("[conn %d] ext: cursor open, forwarding to prod: %s",
			eh.connID, truncateSQL(sqlText, 80))
	}
	return forwardAndRelay(allRaw, eh.prodConn, clientConn)
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
