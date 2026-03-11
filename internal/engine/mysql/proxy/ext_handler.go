package proxy

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	"github.com/psrth/mori/internal/engine/mysql/schema"
	"github.com/psrth/mori/internal/logging"
)

// stmtCacheEntry holds the cached info for a prepared statement.
type stmtCacheEntry struct {
	sql       string
	numParams int
}

// ExtHandler processes MySQL COM_STMT_PREPARE, COM_STMT_EXECUTE, and
// COM_STMT_CLOSE commands for a single connection.
type ExtHandler struct {
	prodConn     net.Conn
	shadowConn   net.Conn
	classifier   core.Classifier
	router       *core.Router
	deltaMap     *delta.Map
	tombstones   *delta.TombstoneSet
	tables       map[string]schema.TableMeta
	moriDir      string
	connID       int64
	verbose      bool
	logger       *logging.Logger
	txnHandler   *TxnHandler
	writeHandler *WriteHandler
	readHandler  *ReadHandler

	mu        sync.Mutex
	stmtCache map[string]stmtCacheEntry // stmtID (as string key) -> cache entry
}

// HandleCommand dispatches a prepared statement command.
func (eh *ExtHandler) HandleCommand(clientConn net.Conn, pkt *mysqlMsg, cmd byte) error {
	switch cmd {
	case comStmtPrepare:
		return eh.handlePrepare(clientConn, pkt)
	case comStmtExecute:
		return eh.handleExecute(clientConn, pkt)
	case comStmtClose:
		return eh.handleClose(pkt)
	default:
		return forwardAndRelay(pkt.Raw, eh.prodConn, clientConn)
	}
}

// handlePrepare processes COM_STMT_PREPARE (0x16).
// Extracts SQL, classifies it, sends to prod, caches stmtID -> SQL mapping.
func (eh *ExtHandler) handlePrepare(clientConn net.Conn, pkt *mysqlMsg) error {
	// Extract SQL from payload: payload[0] is command byte, rest is SQL text.
	sql := ""
	if len(pkt.Payload) > 1 {
		sql = string(pkt.Payload[1:])
	}

	// Forward PREPARE to Prod and capture the response.
	if _, err := eh.prodConn.Write(pkt.Raw); err != nil {
		return fmt.Errorf("sending PREPARE to prod: %w", err)
	}

	// Read the response from Prod.
	resp, err := readMySQLPacket(eh.prodConn)
	if err != nil {
		return fmt.Errorf("reading PREPARE response: %w", err)
	}

	// Relay the first response packet to client.
	if _, err := clientConn.Write(resp.Raw); err != nil {
		return fmt.Errorf("relaying PREPARE response: %w", err)
	}

	// If error, we're done.
	if isERRPacket(resp.Payload) {
		return nil
	}

	// Parse COM_STMT_PREPARE_OK response:
	// status(1) + statement_id(4) + num_columns(2) + num_params(2) + reserved(1) + warning_count(2)
	if len(resp.Payload) < 12 || resp.Payload[0] != 0x00 {
		return nil
	}

	stmtID := binary.LittleEndian.Uint32(resp.Payload[1:5])
	numColumns := int(binary.LittleEndian.Uint16(resp.Payload[5:7]))
	numParams := int(binary.LittleEndian.Uint16(resp.Payload[7:9]))

	// Cache the statement.
	eh.mu.Lock()
	key := stmtIDKey(stmtID)
	eh.stmtCache[key] = stmtCacheEntry{
		sql:       sql,
		numParams: numParams,
	}
	eh.mu.Unlock()

	if eh.verbose {
		log.Printf("[conn %d] ext: PREPARE stmtID=%d params=%d cols=%d | %s",
			eh.connID, stmtID, numParams, numColumns, truncateSQL(sql, 80))
	}

	// Relay param definition packets (if any).
	if numParams > 0 {
		for i := 0; i < numParams; i++ {
			paramPkt, err := readMySQLPacket(eh.prodConn)
			if err != nil {
				return fmt.Errorf("reading param def %d: %w", i, err)
			}
			if _, err := clientConn.Write(paramPkt.Raw); err != nil {
				return fmt.Errorf("relaying param def: %w", err)
			}
		}
		// EOF after params.
		eofPkt, err := readMySQLPacket(eh.prodConn)
		if err != nil {
			return fmt.Errorf("reading param EOF: %w", err)
		}
		if _, err := clientConn.Write(eofPkt.Raw); err != nil {
			return fmt.Errorf("relaying param EOF: %w", err)
		}
	}

	// Relay column definition packets (if any).
	if numColumns > 0 {
		for i := 0; i < numColumns; i++ {
			colPkt, err := readMySQLPacket(eh.prodConn)
			if err != nil {
				return fmt.Errorf("reading column def %d: %w", i, err)
			}
			if _, err := clientConn.Write(colPkt.Raw); err != nil {
				return fmt.Errorf("relaying column def: %w", err)
			}
		}
		// EOF after columns.
		eofPkt, err := readMySQLPacket(eh.prodConn)
		if err != nil {
			return fmt.Errorf("reading column EOF: %w", err)
		}
		if _, err := clientConn.Write(eofPkt.Raw); err != nil {
			return fmt.Errorf("relaying column EOF: %w", err)
		}
	}

	return nil
}

// handleExecute processes COM_STMT_EXECUTE (0x17).
// Looks up the cached SQL for the stmtID, classifies it, and routes.
func (eh *ExtHandler) handleExecute(clientConn net.Conn, pkt *mysqlMsg) error {
	if len(pkt.Payload) < 5 {
		return forwardAndRelay(pkt.Raw, eh.prodConn, clientConn)
	}

	stmtID := binary.LittleEndian.Uint32(pkt.Payload[1:5])

	eh.mu.Lock()
	entry, ok := eh.stmtCache[stmtIDKey(stmtID)]
	eh.mu.Unlock()

	if !ok {
		// Unknown statement — forward to prod.
		if eh.verbose {
			log.Printf("[conn %d] ext: EXECUTE unknown stmtID=%d, forwarding to prod", eh.connID, stmtID)
		}
		return forwardAndRelay(pkt.Raw, eh.prodConn, clientConn)
	}

	// Parse bound parameters from the execute packet for classification.
	params := eh.parseExecuteParams(pkt.Payload, entry.numParams)

	// Classify the SQL.
	var cl *core.Classification
	var err error
	if len(params) > 0 {
		cl, err = eh.classifier.ClassifyWithParams(entry.sql, params)
	} else {
		cl, err = eh.classifier.Classify(entry.sql)
	}
	if err != nil {
		if eh.verbose {
			log.Printf("[conn %d] ext: classify error for stmtID=%d, forwarding to prod: %v", eh.connID, stmtID, err)
		}
		return forwardAndRelay(pkt.Raw, eh.prodConn, clientConn)
	}

	strategy := eh.router.Route(cl)

	// WRITE GUARD L1.
	if err := validateRouteDecision(cl, strategy, eh.connID, eh.logger); err != nil {
		strategy = core.StrategyShadowWrite
	}

	if eh.verbose {
		log.Printf("[conn %d] ext: EXECUTE stmtID=%d %s/%s tables=%v -> %s | %s",
			eh.connID, stmtID, cl.OpType, cl.SubType,
			cl.Tables, strategy, truncateSQL(entry.sql, 80))
	}

	eh.logger.Query(eh.connID, entry.sql, cl, strategy, 0)

	switch strategy {
	case core.StrategyProdDirect:
		return forwardAndRelay(pkt.Raw, eh.prodConn, clientConn)

	case core.StrategyShadowWrite:
		return eh.handleExtInsert(clientConn, pkt.Raw, cl)

	case core.StrategyHydrateAndWrite:
		return eh.handleExtUpdate(clientConn, pkt.Raw, cl)

	case core.StrategyShadowDelete:
		return eh.handleExtDelete(clientConn, pkt.Raw, cl)

	case core.StrategyMergedRead:
		return eh.handleExtMergedRead(clientConn, cl, entry.sql, params)

	case core.StrategyJoinPatch:
		return eh.handleExtJoinPatch(clientConn, cl, entry.sql, params)

	case core.StrategyShadowDDL:
		return eh.handleExtDDL(clientConn, pkt.Raw, cl)

	case core.StrategyTransaction:
		return eh.handleExtTxn(clientConn, pkt.Raw, cl, entry.sql)

	case core.StrategyNotSupported:
		msg := core.UnsupportedTransactionMsg
		if cl.NotSupportedMsg != "" {
			msg = cl.NotSupportedMsg
		}
		errPkt := buildGuardErrorResponse(1, msg)
		_, err := clientConn.Write(errPkt)
		return err

	default:
		errPkt := buildGuardErrorResponse(1, core.UnsupportedTransactionMsg)
		_, err := clientConn.Write(errPkt)
		return err
	}
}

// handleClose processes COM_STMT_CLOSE (0x19).
// Evicts the statement from cache. No response is sent for COM_STMT_CLOSE.
func (eh *ExtHandler) handleClose(pkt *mysqlMsg) error {
	if len(pkt.Payload) < 5 {
		return nil
	}

	stmtID := binary.LittleEndian.Uint32(pkt.Payload[1:5])

	eh.mu.Lock()
	delete(eh.stmtCache, stmtIDKey(stmtID))
	eh.mu.Unlock()

	// Forward CLOSE to prod (no response expected).
	eh.prodConn.Write(pkt.Raw) //nolint: errcheck

	if eh.verbose {
		log.Printf("[conn %d] ext: CLOSE stmtID=%d", eh.connID, stmtID)
	}
	return nil
}

// parseExecuteParams extracts bound parameter values from COM_STMT_EXECUTE payload.
// Returns string representations of the parameters for classification.
func (eh *ExtHandler) parseExecuteParams(payload []byte, numParams int) []interface{} {
	if numParams == 0 || len(payload) < 10 {
		return nil
	}

	// COM_STMT_EXECUTE layout:
	// command(1) + stmt_id(4) + flags(1) + iteration_count(4) = 10 bytes minimum
	pos := 10

	if numParams <= 0 {
		return nil
	}

	// Null bitmap: (numParams + 7) / 8 bytes
	nullBitmapLen := (numParams + 7) / 8
	if pos+nullBitmapLen > len(payload) {
		return nil
	}
	nullBitmap := payload[pos : pos+nullBitmapLen]
	pos += nullBitmapLen

	// new-params-bound-flag (1 byte)
	if pos >= len(payload) {
		return nil
	}
	newParamsBound := payload[pos]
	pos++

	params := make([]interface{}, numParams)

	// If new params are bound, read type info.
	var paramTypes []uint16
	if newParamsBound == 1 {
		paramTypes = make([]uint16, numParams)
		for i := 0; i < numParams; i++ {
			if pos+2 > len(payload) {
				return params
			}
			paramTypes[i] = binary.LittleEndian.Uint16(payload[pos : pos+2])
			pos += 2
		}
	}

	// Read parameter values.
	for i := 0; i < numParams; i++ {
		// Check null bitmap.
		bytePos := i / 8
		bitPos := uint(i % 8)
		if bytePos < len(nullBitmap) && nullBitmap[bytePos]&(1<<bitPos) != 0 {
			params[i] = nil
			continue
		}

		if pos >= len(payload) {
			break
		}

		// Read value based on type (best-effort: extract as string for classification).
		if paramTypes != nil && i < len(paramTypes) {
			val, consumed := readBinaryParamValue(payload[pos:], paramTypes[i])
			params[i] = val
			pos += consumed
		} else {
			// No type info available — try length-encoded string.
			val, totalSize := readLenEncString(payload[pos:])
			params[i] = string(val)
			pos += totalSize
		}
	}

	return params
}

// readBinaryParamValue reads a single parameter value from the binary protocol.
// Returns the value as a string and the number of bytes consumed.
func readBinaryParamValue(data []byte, mysqlType uint16) (string, int) {
	if len(data) == 0 {
		return "", 0
	}

	// MySQL type constants (subset for common types).
	const (
		mysqlTypeLongLong = 0x08
		mysqlTypeLong     = 0x03
		mysqlTypeShort    = 0x02
		mysqlTypeTiny     = 0x01
		mysqlTypeFloat    = 0x04
		mysqlTypeDouble   = 0x05
		mysqlTypeString   = 0xfe
		mysqlTypeVarStr   = 0x0f
		mysqlTypeBlob     = 0xfc
		mysqlTypeNull     = 0x06
	)

	baseType := mysqlType & 0xff

	switch baseType {
	case mysqlTypeTiny:
		if len(data) < 1 {
			return "", 0
		}
		return fmt.Sprintf("%d", data[0]), 1

	case mysqlTypeShort:
		if len(data) < 2 {
			return "", 0
		}
		v := binary.LittleEndian.Uint16(data[:2])
		return fmt.Sprintf("%d", v), 2

	case mysqlTypeLong:
		if len(data) < 4 {
			return "", 0
		}
		v := binary.LittleEndian.Uint32(data[:4])
		return fmt.Sprintf("%d", v), 4

	case mysqlTypeLongLong:
		if len(data) < 8 {
			return "", 0
		}
		v := binary.LittleEndian.Uint64(data[:8])
		return fmt.Sprintf("%d", v), 8

	case mysqlTypeFloat:
		if len(data) < 4 {
			return "", 0
		}
		return fmt.Sprintf("%v", data[:4]), 4

	case mysqlTypeDouble:
		if len(data) < 8 {
			return "", 0
		}
		return fmt.Sprintf("%v", data[:8]), 8

	case mysqlTypeNull:
		return "", 0

	default:
		// String types: length-encoded string.
		val, totalSize := readLenEncString(data)
		return string(val), totalSize
	}
}

// stmtIDKey converts a uint32 statement ID to a map key.
func stmtIDKey(id uint32) string {
	return fmt.Sprintf("%d", id)
}

// --- Strategy handlers for prepared statement execution ---

func (eh *ExtHandler) handleExtInsert(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	// Execute on Shadow via COM_QUERY with the resolved SQL.
	// For prepared statements we forward the original COM_STMT_EXECUTE to prod
	// since shadow doesn't have the prepared statement. Instead, we construct a
	// COM_QUERY for shadow.
	sql := eh.reconstructSQL(cl)
	if sql == "" {
		// Can't reconstruct — forward to shadow using COM_QUERY with raw SQL.
		return forwardAndRelay(rawPkt, eh.prodConn, clientConn)
	}

	shadowPkt := buildCOMQuery(0, sql)
	if err := forwardAndRelay(shadowPkt, eh.shadowConn, clientConn); err != nil {
		return err
	}
	if eh.deltaMap != nil {
		for _, table := range cl.Tables {
			eh.deltaMap.MarkInserted(table)
		}
	}
	return nil
}

func (eh *ExtHandler) handleExtUpdate(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	// Hydrate missing rows.
	if eh.writeHandler != nil && len(cl.PKs) > 0 {
		for _, pk := range cl.PKs {
			if eh.deltaMap != nil && eh.deltaMap.IsDelta(pk.Table, pk.PK) {
				continue
			}
			if err := eh.writeHandler.hydrateRow(pk.Table, pkValuesFromSingle(eh.writeHandler.tables[pk.Table], pk.PK)); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: hydration failed for (%s, %s): %v",
						eh.connID, pk.Table, pk.PK, err)
				}
			}
		}
	}

	sql := eh.reconstructSQL(cl)
	if sql == "" {
		return forwardAndRelay(rawPkt, eh.prodConn, clientConn)
	}

	shadowPkt := buildCOMQuery(0, sql)
	if err := forwardAndRelay(shadowPkt, eh.shadowConn, clientConn); err != nil {
		return err
	}

	if eh.deltaMap != nil {
		inTxn := eh.txnHandler != nil && eh.txnHandler.InTxn()
		for _, pk := range cl.PKs {
			if inTxn {
				eh.deltaMap.Stage(pk.Table, pk.PK)
			} else {
				eh.deltaMap.Add(pk.Table, pk.PK)
			}
		}
		if !inTxn {
			if err := delta.WriteDeltaMap(eh.moriDir, eh.deltaMap); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: failed to persist delta map: %v", eh.connID, err)
				}
			}
		}
	}
	return nil
}

func (eh *ExtHandler) handleExtDelete(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	sql := eh.reconstructSQL(cl)
	if sql == "" {
		return forwardAndRelay(rawPkt, eh.prodConn, clientConn)
	}

	shadowPkt := buildCOMQuery(0, sql)
	if err := forwardAndRelay(shadowPkt, eh.shadowConn, clientConn); err != nil {
		return err
	}

	if eh.tombstones != nil {
		inTxn := eh.txnHandler != nil && eh.txnHandler.InTxn()
		for _, pk := range cl.PKs {
			if inTxn {
				eh.tombstones.Stage(pk.Table, pk.PK)
			} else {
				eh.tombstones.Add(pk.Table, pk.PK)
				if eh.deltaMap != nil {
					eh.deltaMap.Remove(pk.Table, pk.PK)
				}
			}
		}
		if !inTxn {
			if err := delta.WriteTombstoneSet(eh.moriDir, eh.tombstones); err != nil {
				if eh.verbose {
					log.Printf("[conn %d] ext: failed to persist tombstones: %v", eh.connID, err)
				}
			}
			if eh.deltaMap != nil {
				if err := delta.WriteDeltaMap(eh.moriDir, eh.deltaMap); err != nil {
					if eh.verbose {
						log.Printf("[conn %d] ext: failed to persist delta map: %v", eh.connID, err)
					}
				}
			}
		}
	}
	return nil
}

func (eh *ExtHandler) handleExtMergedRead(clientConn net.Conn, cl *core.Classification, sql string, params []interface{}) error {
	if eh.readHandler == nil {
		return forwardAndRelay(buildCOMQuery(0, sql), eh.prodConn, clientConn)
	}

	// Reconstruct full SQL with parameters substituted.
	fullSQL := substituteParams(sql, params)

	clCopy := *cl
	clCopy.RawSQL = fullSQL

	columns, values, nulls, err := eh.readHandler.mergedReadCore(&clCopy, fullSQL)
	if err != nil {
		if re, ok := err.(*mysqlRelayError); ok {
			_, writeErr := clientConn.Write(re.rawMsgs)
			return writeErr
		}
		return err
	}

	response := buildMySQLSelectResponse(columns, values, nulls)
	_, err = clientConn.Write(response)
	return err
}

func (eh *ExtHandler) handleExtJoinPatch(clientConn net.Conn, cl *core.Classification, sql string, params []interface{}) error {
	if eh.readHandler == nil {
		return forwardAndRelay(buildCOMQuery(0, sql), eh.prodConn, clientConn)
	}

	fullSQL := substituteParams(sql, params)

	clCopy := *cl
	clCopy.RawSQL = fullSQL

	columns, values, nulls, err := eh.readHandler.joinPatchCore(&clCopy, fullSQL)
	if err != nil {
		if re, ok := err.(*mysqlRelayError); ok {
			_, writeErr := clientConn.Write(re.rawMsgs)
			return writeErr
		}
		return err
	}

	response := buildMySQLSelectResponse(columns, values, nulls)
	_, err = clientConn.Write(response)
	return err
}

func (eh *ExtHandler) handleExtDDL(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	sql := eh.reconstructSQL(cl)
	if sql == "" {
		return forwardAndRelay(rawPkt, eh.prodConn, clientConn)
	}
	shadowPkt := buildCOMQuery(0, sql)
	return forwardAndRelay(shadowPkt, eh.shadowConn, clientConn)
}

func (eh *ExtHandler) handleExtTxn(clientConn net.Conn, rawPkt []byte, cl *core.Classification, sql string) error {
	if eh.txnHandler == nil {
		// No TxnHandler — forward to both backends.
		eh.shadowConn.Write(rawPkt) //nolint: errcheck
		drainResponse(eh.shadowConn) //nolint: errcheck
		return forwardAndRelay(rawPkt, eh.prodConn, clientConn)
	}

	// Convert to COM_QUERY for TxnHandler.
	queryPkt := buildCOMQuery(0, sql)
	return eh.txnHandler.HandleTxn(clientConn, queryPkt, cl)
}

// reconstructSQL returns the RawSQL from the classification if available.
func (eh *ExtHandler) reconstructSQL(cl *core.Classification) string {
	return cl.RawSQL
}

// substituteParams replaces ? placeholders in SQL with parameter values.
func substituteParams(sql string, params []interface{}) string {
	if len(params) == 0 {
		return sql
	}

	var result strings.Builder
	paramIdx := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] == '?' && paramIdx < len(params) {
			if params[paramIdx] == nil {
				result.WriteString("NULL")
			} else {
				val := fmt.Sprintf("%v", params[paramIdx])
				// Escape single quotes in values.
				val = strings.ReplaceAll(val, "'", "''")
				result.WriteByte('\'')
				result.WriteString(val)
				result.WriteByte('\'')
			}
			paramIdx++
		} else {
			result.WriteByte(sql[i])
		}
	}
	return result.String()
}
