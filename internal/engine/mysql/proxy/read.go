package proxy

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// ColumnInfo holds metadata for a single column from a MySQL result set.
type ColumnInfo struct {
	Name   string
	RawDef []byte // raw column definition packet payload (for faithful relay)
}

// QueryResult holds the parsed result of executing a query on a MySQL backend.
type QueryResult struct {
	Columns      []ColumnInfo
	RowValues    [][]string
	RowNulls     [][]bool
	RawMsgs      []byte // all raw response bytes for relay
	Error        string // error message if the response was an ERR packet
	AffectedRows uint64 // from OK packet for writes
	LastInsertID uint64 // from OK packet for auto-increment inserts
}

// ReadHandler encapsulates merged read logic for a single MySQL connection.
type ReadHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	connID          int64
	verbose         bool
	logger          *logging.Logger
	maxRowsHydrate  int
}

// HandleRead dispatches a read operation based on the routing strategy.
// On success, the synthesized response has been written to clientConn.
func (rh *ReadHandler) HandleRead(
	clientConn net.Conn,
	rawPkt []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyMergedRead:
		// Sub-dispatch based on classification flags.
		if cl.HasSetOp {
			return rh.handleSetOperation(clientConn, cl)
		}
		if cl.IsComplexRead {
			return rh.handleComplexRead(clientConn, cl)
		}
		if cl.HasWindowFunc {
			return rh.handleWindowRead(clientConn, cl)
		}
		return rh.handleMergedRead(clientConn, cl)
	case core.StrategyJoinPatch:
		return rh.handleJoinPatch(clientConn, cl)
	default:
		return fmt.Errorf("unsupported read strategy: %s", strategy)
	}
}

// handleMergedRead implements the single-table merged read algorithm for MySQL.
func (rh *ReadHandler) handleMergedRead(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.mergedReadCore(cl, cl.RawSQL)
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

// mysqlRelayError wraps raw backend response bytes for errors that should be
// relayed directly to the client.
type mysqlRelayError struct {
	rawMsgs []byte
	msg     string
}

func (e *mysqlRelayError) Error() string { return e.msg }

// mergedReadCore performs the merged read algorithm and returns the result.
func (rh *ReadHandler) mergedReadCore(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	// Aggregate queries: row-level merge then re-aggregate.
	if cl.HasAggregate {
		return rh.aggregateReadCore(cl, querySQL)
	}

	if len(cl.Tables) == 0 {
		return nil, nil, nil, fmt.Errorf("merged read with no tables")
	}
	table := cl.Tables[0]

	// Step 0: Inject PK column into query if needed for dedup.
	injectedPK := ""
	effectiveSQL := querySQL
	if !cl.IsJoin {
		if meta, ok := rh.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjection(querySQL, pkCol) {
				effectiveSQL = injectPKColumn(querySQL, pkCol)
				injectedPK = pkCol
			}
		}
	}

	// Step 1: Execute query on Shadow verbatim.
	shadowResult, err := execMySQLQuery(rh.shadowConn, effectiveSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow query: %w", err)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	// Step 1.5: Rewrite Prod query for schema diffs (DDL changes).
	prodSQL := effectiveSQL
	skipProd := false
	if rh.schemaRegistry != nil {
		rewritten, shadowOnly := rh.rewriteForProd(effectiveSQL, table)
		if shadowOnly {
			skipProd = true
		} else {
			prodSQL = rewritten
		}
	}

	// Step 2: Execute query on Prod (with over-fetching for LIMIT queries).
	var prodResult *QueryResult
	if skipProd {
		prodResult = &QueryResult{}
	} else {
		if cl.HasLimit && cl.Limit > 0 {
			deltaCount := rh.deltaMap.CountForTable(table)
			tombstoneCount := rh.tombstones.CountForTable(table)
			overfetch := deltaCount + tombstoneCount
			if overfetch > 0 {
				prodSQL = rewriteLimit(prodSQL, cl.Limit+overfetch)
			}
		}

		prodResult, err = execMySQLQuery(rh.prodConn, prodSQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prod query: %w", err)
		}
		if prodResult.Error != "" {
			// If Prod query fails due to schema mismatch, fall back to Shadow-only.
			hasSchemaChange := false
			if rh.schemaRegistry != nil {
				for _, t := range cl.Tables {
					if rh.schemaRegistry.HasDiff(t) {
						hasSchemaChange = true
						break
					}
				}
			}
			if hasSchemaChange {
				prodResult = &QueryResult{}
			} else {
				return nil, nil, nil, &mysqlRelayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
			}
		}
	}

	// Step 3: Filter Prod results — remove delta and tombstoned rows.
	filteredValues, filteredNulls := rh.filterProdRows(table, prodResult)

	// Step 4: Schema adaptation.
	prodColumns := rh.adaptColumns(table, prodResult.Columns)
	adaptedValues, adaptedNulls := rh.adaptRows(table, prodResult.Columns, filteredValues, filteredNulls)

	// Step 5: Merge Shadow + filtered/adapted Prod.
	mergedColumns, mergedValues, mergedNulls := mergeResults(
		shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls,
		prodColumns, adaptedValues, adaptedNulls,
	)

	// Step 6: Deduplicate by PK.
	mergedValues, mergedNulls = rh.dedup(table, mergedColumns, mergedValues, mergedNulls)

	// Step 7: Re-sort by ORDER BY.
	if cl.OrderBy != "" {
		sortMerged(mergedColumns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Step 8: Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	// Step 9: Strip injected PK column.
	if injectedPK != "" {
		mergedColumns, mergedValues, mergedNulls = stripInjectedColumn(
			mergedColumns, mergedValues, mergedNulls, injectedPK)
	}

	return mergedColumns, mergedValues, mergedNulls, nil
}

// aggregateReadCore handles aggregate queries on affected tables.
// Delegates to fullAggregateReadCore in aggregate.go for full aggregate support.
func (rh *ReadHandler) aggregateReadCore(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	return rh.fullAggregateReadCore(cl, querySQL)
}

// handleJoinPatch implements the JOIN merged read algorithm for MySQL.
func (rh *ReadHandler) handleJoinPatch(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.joinPatchCore(cl, cl.RawSQL)
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

// joinPatchCore performs the JOIN patch algorithm.
func (rh *ReadHandler) joinPatchCore(cl *core.Classification, querySQL string) (
	columns []ColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	deltaTables := rh.identifyDeltaTables(cl.Tables)

	// If any joined table has schema diffs, use materialization approach.
	if rh.schemaRegistry != nil {
		for _, t := range cl.Tables {
			if rh.schemaRegistry.HasDiff(t) {
				return rh.joinMaterializeCore(cl, querySQL)
			}
		}
	}

	if len(deltaTables) == 0 {
		prodResult, err := execMySQLQuery(rh.prodConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prod JOIN query: %w", err)
		}
		if prodResult.Error != "" {
			return nil, nil, nil, &mysqlRelayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
		}
		return prodResult.Columns, prodResult.RowValues, prodResult.RowNulls, nil
	}

	// Step 1: Execute JOIN on Prod.
	prodResult, err := execMySQLQuery(rh.prodConn, querySQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prod JOIN query: %w", err)
	}
	if prodResult.Error != "" {
		return nil, nil, nil, &mysqlRelayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
	}

	// Step 2: Find PK column indices.
	pkIndices := rh.findPKIndicesForTables(cl.Tables, prodResult.Columns)

	// Step 3: Filter Prod rows — remove dead (tombstoned) and classify delta rows.
	var patchedValues [][]string
	var patchedNulls [][]bool

	for i, row := range prodResult.RowValues {
		action := rh.classifyJoinRow(deltaTables, pkIndices, row)
		switch action {
		case joinRowClean:
			patchedValues = append(patchedValues, row)
			patchedNulls = append(patchedNulls, prodResult.RowNulls[i])

		case joinRowDelta:
			patched, patchedN, err := rh.patchDeltaRow(
				deltaTables, pkIndices, prodResult.Columns, row, prodResult.RowNulls[i],
			)
			if err != nil {
				rh.logf("JOIN patch error, keeping Prod row: %v", err)
				patchedValues = append(patchedValues, row)
				patchedNulls = append(patchedNulls, prodResult.RowNulls[i])
				continue
			}
			if patched != nil {
				patchedValues = append(patchedValues, patched)
				patchedNulls = append(patchedNulls, patchedN)
			}

		case joinRowDead:
			continue
		}
	}

	// Step 4: Execute same JOIN on Shadow.
	shadowResult, err := execMySQLQuery(rh.shadowConn, querySQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow JOIN query: %w", err)
	}

	// Step 5: Merge Shadow + patched Prod.
	resultColumns := prodResult.Columns
	if len(shadowResult.Columns) > 0 && shadowResult.Error == "" {
		resultColumns = shadowResult.Columns
	}

	var mergedValues [][]string
	var mergedNulls [][]bool

	if shadowResult.Error == "" {
		mergedValues = append(mergedValues, shadowResult.RowValues...)
		mergedNulls = append(mergedNulls, shadowResult.RowNulls...)
	}
	mergedValues = append(mergedValues, patchedValues...)
	mergedNulls = append(mergedNulls, patchedNulls...)

	// Step 6: Deduplicate.
	allPKIndices := rh.findPKIndicesForTables(cl.Tables, resultColumns)
	mergedValues, mergedNulls = rh.dedupJoin(cl.Tables, allPKIndices, resultColumns, mergedValues, mergedNulls)

	// Step 7: Re-sort.
	if cl.OrderBy != "" {
		sortMerged(resultColumns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Step 8: Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	return resultColumns, mergedValues, mergedNulls, nil
}

// ---------------------------------------------------------------------------
// Query execution and response building
// ---------------------------------------------------------------------------

// execMySQLQuery sends a COM_QUERY and parses the full result set response.
func execMySQLQuery(conn net.Conn, sql string) (*QueryResult, error) {
	pkt := buildCOMQuery(0, sql)
	if _, err := conn.Write(pkt); err != nil {
		return nil, fmt.Errorf("sending query: %w", err)
	}

	result := &QueryResult{}

	// Read first packet — determines response type.
	first, err := readMySQLPacket(conn)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	result.RawMsgs = append(result.RawMsgs, first.Raw...)

	// ERR packet.
	if isERRPacket(first.Payload) {
		result.Error = parseMySQLError(first.Payload)
		return result, nil
	}

	// OK packet (e.g., for queries returning no result set).
	// Format: header(0x00) + affected_rows(lenenc) + last_insert_id(lenenc) + ...
	if isOKPacket(first.Payload) {
		if len(first.Payload) > 1 {
			result.AffectedRows = readLenEncUint64(first.Payload[1:])
			// Advance past affected_rows to read last_insert_id.
			arSize := lenEncIntSize(first.Payload[1:])
			liOffset := 1 + arSize
			if liOffset < len(first.Payload) {
				result.LastInsertID = readLenEncUint64(first.Payload[liOffset:])
			}
		}
		return result, nil
	}

	// Result set: first packet is column count.
	colCount := readLenEncInt(first.Payload)

	// Read column definitions.
	for i := 0; i < colCount; i++ {
		colPkt, err := readMySQLPacket(conn)
		if err != nil {
			return nil, fmt.Errorf("reading column def %d: %w", i, err)
		}
		result.RawMsgs = append(result.RawMsgs, colPkt.Raw...)

		name := parseColumnDefName(colPkt.Payload)
		result.Columns = append(result.Columns, ColumnInfo{
			Name:   name,
			RawDef: colPkt.Payload,
		})
	}

	// Read EOF after column definitions.
	eofPkt, err := readMySQLPacket(conn)
	if err != nil {
		return nil, fmt.Errorf("reading column EOF: %w", err)
	}
	result.RawMsgs = append(result.RawMsgs, eofPkt.Raw...)

	// Read rows until EOF or ERR.
	for {
		rowPkt, err := readMySQLPacket(conn)
		if err != nil {
			return nil, fmt.Errorf("reading row: %w", err)
		}
		result.RawMsgs = append(result.RawMsgs, rowPkt.Raw...)

		if isEOFPacket(rowPkt.Payload) {
			return result, nil
		}
		if isERRPacket(rowPkt.Payload) {
			result.Error = parseMySQLError(rowPkt.Payload)
			return result, nil
		}

		// Parse row data (length-encoded string sequence).
		values, nulls := parseMySQLRow(rowPkt.Payload, colCount)
		result.RowValues = append(result.RowValues, values)
		result.RowNulls = append(result.RowNulls, nulls)
	}
}

// parseMySQLError extracts an error message from an ERR packet payload.
func parseMySQLError(payload []byte) string {
	// ERR: 0xFF + error_code(2) + '#' + sql_state(5) + message
	if len(payload) < 9 {
		return string(payload)
	}
	return string(payload[9:])
}

// readLenEncInt reads a MySQL length-encoded integer from the start of data.
func readLenEncInt(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	switch {
	case data[0] < 0xfb:
		return int(data[0])
	case data[0] == 0xfc && len(data) >= 3:
		return int(data[1]) | int(data[2])<<8
	case data[0] == 0xfd && len(data) >= 4:
		return int(data[1]) | int(data[2])<<8 | int(data[3])<<16
	case data[0] == 0xfe && len(data) >= 9:
		return int(data[1]) | int(data[2])<<8 | int(data[3])<<16 | int(data[4])<<24
	default:
		return 0
	}
}

// readLenEncUint64 reads a MySQL length-encoded integer as uint64.
func readLenEncUint64(data []byte) uint64 {
	if len(data) == 0 {
		return 0
	}
	switch {
	case data[0] < 0xfb:
		return uint64(data[0])
	case data[0] == 0xfc && len(data) >= 3:
		return uint64(data[1]) | uint64(data[2])<<8
	case data[0] == 0xfd && len(data) >= 4:
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16
	case data[0] == 0xfe && len(data) >= 9:
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16 | uint64(data[4])<<24 |
			uint64(data[5])<<32 | uint64(data[6])<<40 | uint64(data[7])<<48 | uint64(data[8])<<56
	default:
		return 0
	}
}

// lenEncIntSize returns the number of bytes a length-encoded integer occupies.
func lenEncIntSize(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	switch {
	case data[0] < 0xfb:
		return 1
	case data[0] == 0xfc:
		return 3
	case data[0] == 0xfd:
		return 4
	case data[0] == 0xfe:
		return 9
	default:
		return 1
	}
}

// parseColumnDefName extracts the column name from a MySQL column definition packet.
// MySQL column definition format (Protocol::ColumnDefinition41):
//
//	catalog(lenenc_str) + schema(lenenc_str) + table(lenenc_str) +
//	org_table(lenenc_str) + name(lenenc_str) + org_name(lenenc_str) + ...
func parseColumnDefName(payload []byte) string {
	pos := 0
	// Skip: catalog, schema, table, org_table (4 lenenc strings).
	for i := 0; i < 4; i++ {
		if pos >= len(payload) {
			return ""
		}
		_, totalSize := readLenEncString(payload[pos:])
		pos += totalSize
	}
	// Read name (5th lenenc string).
	if pos >= len(payload) {
		return ""
	}
	name, _ := readLenEncString(payload[pos:])
	return string(name)
}

// readLenEncString reads a length-encoded string from data.
// Returns the string bytes and the total number of bytes consumed.
func readLenEncString(data []byte) ([]byte, int) {
	if len(data) == 0 {
		return nil, 0
	}

	// NULL (0xfb)
	if data[0] == 0xfb {
		return nil, 1
	}

	intSize := lenEncIntSize(data)
	strLen := readLenEncInt(data)
	totalSize := intSize + strLen

	if totalSize > len(data) {
		return nil, len(data)
	}
	return data[intSize:totalSize], totalSize
}

// parseMySQLRow parses a MySQL text protocol result row.
// Each column value is a length-encoded string, or 0xFB for NULL.
func parseMySQLRow(payload []byte, colCount int) ([]string, []bool) {
	values := make([]string, colCount)
	nulls := make([]bool, colCount)
	pos := 0

	for i := 0; i < colCount; i++ {
		if pos >= len(payload) {
			nulls[i] = true
			continue
		}
		if payload[pos] == 0xfb {
			// NULL
			nulls[i] = true
			pos++
			continue
		}
		val, totalSize := readLenEncString(payload[pos:])
		values[i] = string(val)
		pos += totalSize
	}
	return values, nulls
}

// buildMySQLSelectResponse constructs a complete MySQL result set response
// from in-memory data: column_count + column_defs + EOF + rows + EOF.
func buildMySQLSelectResponse(columns []ColumnInfo, rowValues [][]string, rowNulls [][]bool) []byte {
	var buf []byte
	seq := byte(1)

	// Column count packet.
	buf = append(buf, buildMySQLPacket(seq, []byte{byte(len(columns))})...)
	seq++

	// Column definition packets.
	for _, col := range columns {
		if col.RawDef != nil {
			buf = append(buf, buildMySQLPacket(seq, col.RawDef)...)
		} else {
			// Build a synthetic column definition.
			buf = append(buf, buildMySQLPacket(seq, buildColumnDef(col.Name))...)
		}
		seq++
	}

	// EOF after column definitions.
	buf = append(buf, buildEOFPacket(seq)...)
	seq++

	// Row packets.
	for i := range rowValues {
		buf = append(buf, buildMySQLPacket(seq, buildMySQLRowPayload(rowValues[i], rowNulls[i]))...)
		seq++
	}

	// Final EOF.
	buf = append(buf, buildEOFPacket(seq)...)

	return buf
}

// buildColumnDef builds a minimal MySQL column definition payload.
func buildColumnDef(name string) []byte {
	var buf []byte
	// catalog = "def"
	buf = appendLenEncString(buf, "def")
	// schema (empty)
	buf = appendLenEncString(buf, "")
	// table (empty)
	buf = appendLenEncString(buf, "")
	// org_table (empty)
	buf = appendLenEncString(buf, "")
	// name
	buf = appendLenEncString(buf, name)
	// org_name
	buf = appendLenEncString(buf, name)
	// Fixed-length fields (filler + charset + column_length + column_type + flags + decimals + filler)
	buf = append(buf, 0x0c)                         // length of fixed-length fields
	buf = append(buf, 0x21, 0x00)                    // character_set = utf8 (33)
	buf = append(buf, 0xff, 0xff, 0xff, 0x00)        // column_length
	buf = append(buf, 0xfd)                           // column_type = VAR_STRING (253)
	buf = append(buf, 0x00, 0x00)                     // flags
	buf = append(buf, 0x00)                           // decimals
	buf = append(buf, 0x00, 0x00)                     // filler
	return buf
}

// appendLenEncString appends a length-encoded string to buf.
func appendLenEncString(buf []byte, s string) []byte {
	buf = appendLenEncInt(buf, len(s))
	return append(buf, []byte(s)...)
}

// appendLenEncInt appends a length-encoded integer to buf.
func appendLenEncInt(buf []byte, n int) []byte {
	switch {
	case n < 251:
		return append(buf, byte(n))
	case n < 1<<16:
		return append(buf, 0xfc, byte(n), byte(n>>8))
	case n < 1<<24:
		return append(buf, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	default:
		return append(buf, 0xfe,
			byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			0, 0, 0, 0)
	}
}

// buildMySQLRowPayload constructs a MySQL text protocol row payload.
func buildMySQLRowPayload(values []string, nulls []bool) []byte {
	var buf []byte
	for i, v := range values {
		if i < len(nulls) && nulls[i] {
			buf = append(buf, 0xfb) // NULL
		} else {
			buf = appendLenEncString(buf, v)
		}
	}
	return buf
}

// ---------------------------------------------------------------------------
// Merge helpers (ported from PostgreSQL proxy)
// ---------------------------------------------------------------------------

func (rh *ReadHandler) logf(format string, args ...any) {
	if rh.verbose {
		prefix := fmt.Sprintf("[conn %d] ", rh.connID)
		log.Printf(prefix+format, args...)
	}
}

// needsPKInjection returns true if the query doesn't include the PK column.
func needsPKInjection(sql, pkCol string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Skip for set operations.
	if strings.Contains(upper, " UNION ") || strings.Contains(upper, " INTERSECT ") || strings.Contains(upper, " EXCEPT ") {
		return false
	}
	if strings.HasPrefix(upper, "WITH ") {
		return false
	}

	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return false
	}
	afterSelect := strings.TrimSpace(upper[selectIdx+6:])
	if strings.HasPrefix(afterSelect, "*") || strings.HasPrefix(afterSelect, "DISTINCT *") {
		return false
	}
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return false
	}
	afterFrom := strings.TrimSpace(upper[fromIdx+6:])
	if strings.HasPrefix(afterFrom, "(") {
		return false
	}

	selectList := strings.ToLower(sql[selectIdx+6 : fromIdx])
	return !containsColumn(selectList, strings.ToLower(pkCol))
}

// containsColumn checks if a select list contains a column name.
func containsColumn(selectList, col string) bool {
	parts := strings.Split(selectList, ",")
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if idx := strings.Index(strings.ToLower(name), " as "); idx >= 0 {
			name = strings.TrimSpace(name[:idx])
		}
		if dotIdx := strings.LastIndex(name, "."); dotIdx >= 0 {
			name = name[dotIdx+1:]
		}
		name = strings.Trim(name, "`\"")
		if name == col {
			return true
		}
	}
	return false
}

// injectPKColumn adds the PK column to the SELECT list for dedup.
func injectPKColumn(sql, pkCol string) string {
	upper := strings.ToUpper(sql)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return sql
	}
	insertPos := selectIdx + 6
	afterSelect := strings.TrimSpace(sql[insertPos:])
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT") {
		insertPos += (len(sql[insertPos:]) - len(afterSelect)) + 8
	}
	return sql[:insertPos] + " `" + pkCol + "`," + sql[insertPos:]
}

// stripInjectedColumn removes the injected PK column from the result set.
func stripInjectedColumn(
	columns []ColumnInfo, values [][]string, nulls [][]bool, pkCol string,
) ([]ColumnInfo, [][]string, [][]bool) {
	pkIdx := -1
	for i, col := range columns {
		if col.Name == pkCol {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return columns, values, nulls
	}

	newCols := make([]ColumnInfo, 0, len(columns)-1)
	newCols = append(newCols, columns[:pkIdx]...)
	newCols = append(newCols, columns[pkIdx+1:]...)

	newValues := make([][]string, len(values))
	newNulls := make([][]bool, len(nulls))
	for i, row := range values {
		newRow := make([]string, 0, len(row)-1)
		newRow = append(newRow, row[:pkIdx]...)
		newRow = append(newRow, row[pkIdx+1:]...)
		newValues[i] = newRow

		newNull := make([]bool, 0, len(nulls[i])-1)
		newNull = append(newNull, nulls[i][:pkIdx]...)
		newNull = append(newNull, nulls[i][pkIdx+1:]...)
		newNulls[i] = newNull
	}

	return newCols, newValues, newNulls
}

// rewriteLimit replaces the LIMIT value in SQL.
func rewriteLimit(sql string, newLimit int) string {
	upper := strings.ToUpper(sql)
	idx := strings.LastIndex(upper, "LIMIT")
	if idx < 0 {
		return sql
	}
	afterLimit := idx + 5
	rest := sql[afterLimit:]
	trimmed := strings.TrimLeft(rest, " \t")
	whitespace := len(rest) - len(trimmed)

	numEnd := 0
	for numEnd < len(trimmed) && trimmed[numEnd] >= '0' && trimmed[numEnd] <= '9' {
		numEnd++
	}
	if numEnd == 0 {
		return sql
	}

	return sql[:afterLimit] + rest[:whitespace] + strconv.Itoa(newLimit) + trimmed[numEnd:]
}

// rewriteForProd adapts SQL for Prod schema differences.
func (rh *ReadHandler) rewriteForProd(sql, table string) (string, bool) {
	diff := rh.schemaRegistry.GetDiff(table)
	if diff == nil {
		return sql, false
	}

	if _, exists := rh.tables[table]; !exists {
		return sql, true // Shadow-only table.
	}

	result := sql

	upper := strings.ToUpper(strings.TrimSpace(result))
	selectIdx := strings.Index(upper, "SELECT")
	fromIdx := strings.Index(upper, " FROM ")
	isSelectStar := false
	if selectIdx >= 0 && fromIdx > selectIdx {
		selectPart := strings.TrimSpace(upper[selectIdx+6 : fromIdx])
		isSelectStar = selectPart == "*" || strings.HasPrefix(selectPart, "DISTINCT *")
	}

	// Build sets for added and dropped columns.
	addedSet := make(map[string]bool)
	for _, col := range diff.Added {
		addedSet[strings.ToLower(col.Name)] = true
	}
	droppedSet := make(map[string]bool)
	for _, col := range diff.Dropped {
		droppedSet[strings.ToLower(col)] = true
	}

	// Check for dropped columns in WHERE — if referenced, skip Prod entirely.
	if len(droppedSet) > 0 {
		upperResult := strings.ToUpper(result)
		if whereIdx := strings.Index(upperResult, " WHERE "); whereIdx >= 0 {
			wherePart := strings.ToLower(result[whereIdx+7:])
			for col := range droppedSet {
				if containsWordBoundary(wherePart, col) {
					return result, true // shadowOnly
				}
			}
		}
	}

	if !isSelectStar && selectIdx >= 0 && fromIdx > selectIdx {
		selectList := result[selectIdx+6 : selectIdx+(fromIdx-selectIdx)]
		parts := strings.Split(selectList, ",")
		var kept []string
		for _, part := range parts {
			colName := strings.TrimSpace(part)
			checkName := colName
			if asIdx := strings.Index(strings.ToUpper(checkName), " AS "); asIdx >= 0 {
				checkName = strings.TrimSpace(checkName[:asIdx])
			}
			if dotIdx := strings.LastIndex(checkName, "."); dotIdx >= 0 {
				checkName = checkName[dotIdx+1:]
			}
			checkName = strings.Trim(checkName, "`\"")
			if !addedSet[strings.ToLower(checkName)] {
				kept = append(kept, part)
			}
		}

		if len(kept) == 0 {
			result = result[:selectIdx+6] + " *" + result[selectIdx+(fromIdx-selectIdx):]
		} else {
			result = result[:selectIdx+6] + strings.Join(kept, ",") + result[selectIdx+(fromIdx-selectIdx):]
		}
	}

	// Strip added columns from ORDER BY.
	if idx := strings.Index(strings.ToUpper(result), " ORDER BY "); idx >= 0 {
		result = stripAddedColumnsFromClause(result, idx, " ORDER BY ", addedSet)
	}

	// Strip added columns from GROUP BY.
	if idx := strings.Index(strings.ToUpper(result), " GROUP BY "); idx >= 0 {
		result = stripAddedColumnsFromClause(result, idx, " GROUP BY ", addedSet)
	}

	for oldName, newName := range diff.Renamed {
		result = replaceColumnName(result, newName, oldName)
	}

	return result, false
}

// stripAddedColumnsFromClause removes entries referencing added columns from
// an ORDER BY or GROUP BY clause. If all entries are removed, the entire
// clause is stripped from the SQL.
func stripAddedColumnsFromClause(sql string, clauseIdx int, keyword string, addedSet map[string]bool) string {
	afterClause := sql[clauseIdx+len(keyword):]

	// Find the end of this clause (next keyword or end of string).
	upperAfter := strings.ToUpper(afterClause)
	endIdx := len(afterClause)
	for _, kw := range []string{" ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT ", " UNION ", " INTERSECT ", " EXCEPT ", " FOR "} {
		if ki := strings.Index(upperAfter, kw); ki >= 0 && ki < endIdx {
			endIdx = ki
		}
	}

	clauseBody := afterClause[:endIdx]
	rest := afterClause[endIdx:]

	// Parse the column list.
	parts := strings.Split(clauseBody, ",")
	var kept []string
	for _, part := range parts {
		colName := strings.TrimSpace(part)
		// Strip direction suffix (ASC/DESC) for matching.
		checkName := colName
		fields := strings.Fields(checkName)
		if len(fields) >= 2 {
			upper := strings.ToUpper(fields[len(fields)-1])
			if upper == "ASC" || upper == "DESC" {
				checkName = strings.Join(fields[:len(fields)-1], " ")
			}
		}
		// Strip table qualifier and quotes.
		if dotIdx := strings.LastIndex(checkName, "."); dotIdx >= 0 {
			checkName = checkName[dotIdx+1:]
		}
		checkName = strings.Trim(strings.TrimSpace(checkName), "`\"")
		if !addedSet[strings.ToLower(checkName)] {
			kept = append(kept, part)
		}
	}

	if len(kept) == 0 {
		// Remove the entire clause.
		return sql[:clauseIdx] + rest
	}
	return sql[:clauseIdx] + keyword + strings.Join(kept, ",") + rest
}

// containsWordBoundary checks if s contains word as a whole-word match.
func containsWordBoundary(s, word string) bool {
	offset := 0
	for {
		idx := strings.Index(s[offset:], word)
		if idx < 0 {
			return false
		}
		pos := offset + idx
		before := pos == 0 || !isIdentChar(s[pos-1])
		after := pos+len(word) >= len(s) || !isIdentChar(s[pos+len(word)])
		if before && after {
			return true
		}
		offset = pos + len(word)
	}
}

// replaceColumnName replaces column name in SQL respecting word boundaries.
func replaceColumnName(sql, oldCol, newCol string) string {
	result := sql
	offset := 0
	for {
		idx := strings.Index(strings.ToLower(result[offset:]), strings.ToLower(oldCol))
		if idx < 0 {
			break
		}
		pos := offset + idx
		before := pos == 0 || !isIdentChar(result[pos-1])
		after := pos+len(oldCol) >= len(result) || !isIdentChar(result[pos+len(oldCol)])
		if before && after {
			result = result[:pos] + newCol + result[pos+len(oldCol):]
			offset = pos + len(newCol)
		} else {
			offset = pos + len(oldCol)
		}
	}
	return result
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

// findPKColumnIndex returns the index of the PK column in the result columns.
func (rh *ReadHandler) findPKColumnIndex(table string, columns []ColumnInfo) int {
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return -1
	}
	pkCol := meta.PKColumns[0]
	for i, col := range columns {
		if col.Name == pkCol {
			return i
		}
	}
	return -1
}

// filterProdRows removes delta and tombstoned rows from Prod result.
func (rh *ReadHandler) filterProdRows(table string, result *QueryResult) ([][]string, [][]bool) {
	pkIdx := rh.findPKColumnIndex(table, result.Columns)
	if pkIdx < 0 {
		return result.RowValues, result.RowNulls
	}

	var filteredValues [][]string
	var filteredNulls [][]bool

	for i, row := range result.RowValues {
		if pkIdx >= len(row) {
			continue
		}
		pk := row[pkIdx]
		if rh.deltaMap.IsDelta(table, pk) {
			continue
		}
		if rh.tombstones.IsTombstoned(table, pk) {
			continue
		}
		filteredValues = append(filteredValues, row)
		filteredNulls = append(filteredNulls, result.RowNulls[i])
	}

	return filteredValues, filteredNulls
}

// adaptColumns returns the column list after schema adaptation.
func (rh *ReadHandler) adaptColumns(table string, prodColumns []ColumnInfo) []ColumnInfo {
	if rh.schemaRegistry == nil {
		return prodColumns
	}
	diff := rh.schemaRegistry.GetDiff(table)
	if diff == nil {
		return prodColumns
	}

	droppedSet := make(map[string]bool, len(diff.Dropped))
	for _, d := range diff.Dropped {
		droppedSet[d] = true
	}

	var result []ColumnInfo
	for _, col := range prodColumns {
		if droppedSet[col.Name] {
			continue
		}
		name := col.Name
		if newName, ok := diff.Renamed[name]; ok {
			name = newName
		}
		result = append(result, ColumnInfo{Name: name, RawDef: col.RawDef})
	}

	for _, added := range diff.Added {
		result = append(result, ColumnInfo{Name: added.Name})
	}

	return result
}

// adaptRows adapts Prod rows for schema diffs.
func (rh *ReadHandler) adaptRows(
	table string,
	origColumns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	if rh.schemaRegistry == nil {
		return values, nulls
	}
	diff := rh.schemaRegistry.GetDiff(table)
	if diff == nil {
		return values, nulls
	}

	droppedSet := make(map[string]bool, len(diff.Dropped))
	for _, d := range diff.Dropped {
		droppedSet[d] = true
	}

	var keepIndices []int
	for i, col := range origColumns {
		if !droppedSet[col.Name] {
			keepIndices = append(keepIndices, i)
		}
	}

	numAdded := len(diff.Added)
	var adaptedValues [][]string
	var adaptedNulls [][]bool

	for i := range values {
		row := make([]string, 0, len(keepIndices)+numAdded)
		rowNulls := make([]bool, 0, len(keepIndices)+numAdded)

		for _, idx := range keepIndices {
			if idx < len(values[i]) {
				row = append(row, values[i][idx])
				rowNulls = append(rowNulls, nulls[i][idx])
			}
		}

		for range numAdded {
			row = append(row, "")
			rowNulls = append(rowNulls, true)
		}

		adaptedValues = append(adaptedValues, row)
		adaptedNulls = append(adaptedNulls, rowNulls)
	}

	return adaptedValues, adaptedNulls
}

// mergeResults combines Shadow and filtered/adapted Prod results.
func mergeResults(
	shadowCols []ColumnInfo, shadowValues [][]string, shadowNulls [][]bool,
	prodCols []ColumnInfo, prodValues [][]string, prodNulls [][]bool,
) ([]ColumnInfo, [][]string, [][]bool) {
	var columns []ColumnInfo
	if len(shadowCols) > 0 {
		columns = shadowCols
	} else {
		columns = prodCols
	}

	var mergedValues [][]string
	var mergedNulls [][]bool

	mergedValues = append(mergedValues, shadowValues...)
	mergedNulls = append(mergedNulls, shadowNulls...)

	if columnsMatch(columns, prodCols) {
		mergedValues = append(mergedValues, prodValues...)
		mergedNulls = append(mergedNulls, prodNulls...)
	} else {
		prodColIdx := buildColumnIndex(prodCols)
		for i, row := range prodValues {
			alignedRow := make([]string, len(columns))
			alignedNulls := make([]bool, len(columns))
			for j, col := range columns {
				if idx, ok := prodColIdx[col.Name]; ok && idx < len(row) {
					alignedRow[j] = row[idx]
					alignedNulls[j] = prodNulls[i][idx]
				} else {
					alignedRow[j] = ""
					alignedNulls[j] = true
				}
			}
			mergedValues = append(mergedValues, alignedRow)
			mergedNulls = append(mergedNulls, alignedNulls)
		}
	}

	return columns, mergedValues, mergedNulls
}

// dedup removes duplicate rows by PK.
func (rh *ReadHandler) dedup(
	table string,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		// No PK: deduplicate by full row hash (Shadow rows win = first occurrence kept).
		return deduplicateByFullRow(values, nulls)
	}

	pkIdx := -1
	for i, col := range columns {
		if col.Name == meta.PKColumns[0] {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return values, nulls
	}

	seen := make(map[string]bool)
	var dedupValues [][]string
	var dedupNulls [][]bool

	for i, row := range values {
		if pkIdx >= len(row) {
			continue
		}
		pk := row[pkIdx]
		if seen[pk] {
			continue
		}
		seen[pk] = true
		dedupValues = append(dedupValues, row)
		dedupNulls = append(dedupNulls, nulls[i])
	}

	return dedupValues, dedupNulls
}

// columnsMatch checks if two column slices have the same names.
func columnsMatch(a, b []ColumnInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			return false
		}
	}
	return true
}

// buildColumnIndex builds a map from column name to index.
func buildColumnIndex(cols []ColumnInfo) map[string]int {
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		idx[c.Name] = i
	}
	return idx
}

// sortMerged sorts the merged result set by the ORDER BY clause.
func sortMerged(
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
	orderBy string,
) {
	orderCols := parseOrderBy(orderBy)
	if len(orderCols) == 0 {
		return
	}

	colIndex := buildColumnIndex(columns)
	var resolved []orderByCol
	for _, oc := range orderCols {
		if idx, ok := colIndex[oc.name]; ok {
			oc.idx = idx
			resolved = append(resolved, oc)
		}
	}
	if len(resolved) == 0 {
		return
	}

	indices := make([]int, len(values))
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, b int) bool {
		for _, oc := range resolved {
			va := values[indices[a]][oc.idx]
			vb := values[indices[b]][oc.idx]
			cmp := compareValues(va, vb)
			if cmp == 0 {
				continue
			}
			if oc.desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	sortedValues := make([][]string, len(values))
	sortedNulls := make([][]bool, len(nulls))
	for i, idx := range indices {
		sortedValues[i] = values[idx]
		sortedNulls[i] = nulls[idx]
	}
	copy(values, sortedValues)
	copy(nulls, sortedNulls)
}

// compareValues compares two string values numerically if possible.
func compareValues(a, b string) int {
	na, errA := strconv.ParseFloat(a, 64)
	nb, errB := strconv.ParseFloat(b, 64)
	if errA == nil && errB == nil {
		if na < nb {
			return -1
		}
		if na > nb {
			return 1
		}
		return 0
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type orderByCol struct {
	name string
	desc bool
	idx  int
}

func parseOrderBy(orderBy string) []orderByCol {
	orderBy = strings.TrimRight(orderBy, "; \t\n")
	if orderBy == "" {
		return nil
	}

	parts := strings.Split(orderBy, ",")
	var result []orderByCol
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) == 0 {
			continue
		}
		col := strings.Trim(fields[0], "`\"")
		if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
			col = col[dotIdx+1:]
		}
		desc := false
		if len(fields) >= 2 && strings.EqualFold(fields[1], "DESC") {
			desc = true
		}
		result = append(result, orderByCol{name: col, desc: desc, idx: -1})
	}
	return result
}

// ---------------------------------------------------------------------------
// JOIN helpers
// ---------------------------------------------------------------------------

type joinRowAction int

const (
	joinRowClean joinRowAction = iota
	joinRowDelta
	joinRowDead
)

// identifyDeltaTables returns tables with deltas or tombstones.
func (rh *ReadHandler) identifyDeltaTables(tables []string) []string {
	var result []string
	for _, t := range tables {
		if rh.deltaMap.CountForTable(t) > 0 || rh.tombstones.CountForTable(t) > 0 {
			result = append(result, t)
		}
	}
	return result
}

// findPKIndicesForTables returns a map from table name to PK column index.
func (rh *ReadHandler) findPKIndicesForTables(tables []string, columns []ColumnInfo) map[string]int {
	result := make(map[string]int)
	for _, table := range tables {
		meta, ok := rh.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]
		for i, col := range columns {
			if col.Name == pkCol || col.Name == table+"."+pkCol {
				result[table] = i
				break
			}
		}
	}
	return result
}

// classifyJoinRow determines whether a Prod JOIN row is clean, delta, or dead.
func (rh *ReadHandler) classifyJoinRow(
	deltaTables []string,
	pkIndices map[string]int,
	row []string,
) joinRowAction {
	for _, table := range deltaTables {
		idx, ok := pkIndices[table]
		if !ok || idx >= len(row) {
			continue
		}
		pk := row[idx]
		if rh.tombstones.IsTombstoned(table, pk) {
			return joinRowDead
		}
		if rh.deltaMap.IsDelta(table, pk) {
			return joinRowDelta
		}
	}
	return joinRowClean
}

// patchDeltaRow replaces delta columns in a Prod row with Shadow values.
func (rh *ReadHandler) patchDeltaRow(
	deltaTables []string,
	pkIndices map[string]int,
	columns []ColumnInfo,
	row []string,
	rowNulls []bool,
) ([]string, []bool, error) {
	patched := make([]string, len(row))
	copy(patched, row)
	pNulls := make([]bool, len(rowNulls))
	copy(pNulls, rowNulls)

	for _, table := range deltaTables {
		idx, ok := pkIndices[table]
		if !ok || idx >= len(row) {
			continue
		}
		pk := row[idx]
		if !rh.deltaMap.IsDelta(table, pk) {
			continue
		}

		meta, ok := rh.tables[table]
		if !ok || len(meta.PKColumns) == 0 {
			continue
		}
		pkCol := meta.PKColumns[0]
		selectSQL := fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` = '%s'",
			table, pkCol, strings.ReplaceAll(pk, "'", "''"))

		shadowRow, err := execMySQLQuery(rh.shadowConn, selectSQL)
		if err != nil {
			return nil, nil, fmt.Errorf("shadow fetch for patch: %w", err)
		}
		if len(shadowRow.RowValues) == 0 {
			continue
		}

		shadowColIdx := buildColumnIndex(shadowRow.Columns)
		for j, col := range columns {
			if sIdx, ok := shadowColIdx[col.Name]; ok {
				patched[j] = shadowRow.RowValues[0][sIdx]
				pNulls[j] = shadowRow.RowNulls[0][sIdx]
			}
		}
	}

	return patched, pNulls, nil
}

// dedupJoin deduplicates merged JOIN rows using a composite key.
func (rh *ReadHandler) dedupJoin(
	tables []string,
	pkIndices map[string]int,
	columns []ColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	seenIdx := make(map[int]bool)
	var keyIndices []int
	ambiguous := false

	for _, table := range tables {
		if idx, ok := pkIndices[table]; ok {
			if seenIdx[idx] {
				ambiguous = true
				break
			}
			seenIdx[idx] = true
			keyIndices = append(keyIndices, idx)
		}
	}

	if ambiguous || len(keyIndices) == 0 {
		keyIndices = make([]int, len(columns))
		for i := range keyIndices {
			keyIndices[i] = i
		}
	}

	seen := make(map[string]bool)
	var dedupValues [][]string
	var dedupNulls [][]bool

	for i, row := range values {
		var keyParts []string
		for _, idx := range keyIndices {
			if idx < len(row) {
				keyParts = append(keyParts, row[idx])
			}
		}
		key := strings.Join(keyParts, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		dedupValues = append(dedupValues, row)
		dedupNulls = append(dedupNulls, nulls[i])
	}

	return dedupValues, dedupNulls
}

// handleJoinWithMaterialization handles JOINs where tables have schema diffs
// by materializing dirty tables into temp tables.
func (rh *ReadHandler) handleJoinWithMaterialization(clientConn net.Conn, cl *core.Classification) error {
	columns, values, nulls, err := rh.joinMaterializeCore(cl, cl.RawSQL)
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

// joinMaterializeCore materializes dirty tables into temp tables, rewrites
// the JOIN query to use the temp tables, and executes on Shadow.
func (rh *ReadHandler) joinMaterializeCore(cl *core.Classification, querySQL string) (
	[]ColumnInfo, [][]string, [][]bool, error,
) {
	// Collect dirty tables (tables with deltas, tombstones, or schema diffs).
	var dirtyTables []string
	seen := make(map[string]bool)
	for _, t := range cl.Tables {
		if seen[t] {
			continue
		}
		seen[t] = true
		isDirty := false
		if rh.deltaMap != nil && rh.deltaMap.CountForTable(t) > 0 {
			isDirty = true
		}
		if rh.tombstones != nil && rh.tombstones.CountForTable(t) > 0 {
			isDirty = true
		}
		if rh.schemaRegistry != nil && rh.schemaRegistry.HasDiff(t) {
			isDirty = true
		}
		if isDirty {
			dirtyTables = append(dirtyTables, t)
		}
	}

	if len(dirtyTables) == 0 {
		// No dirty tables — execute on Shadow verbatim.
		result, err := execMySQLQuery(rh.shadowConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("shadow JOIN query: %w", err)
		}
		if result.Error != "" {
			return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
		}
		return result.Columns, result.RowValues, result.RowNulls, nil
	}

	// Materialize each dirty table into a temp table via merged read.
	var utilTables []string
	tableMap := make(map[string]string)
	defer func() {
		for _, ut := range utilTables {
			dropUtilTable(rh.shadowConn, ut)
		}
	}()

	for _, table := range dirtyTables {
		selectSQL := "SELECT * FROM `" + table + "`"
		utilName := utilTableName(selectSQL)

		_, err := rh.materializeToUtilTable(selectSQL, utilName, rh.maxRowsHydrate)
		if err != nil {
			rh.logf("JOIN materialization failed for %s: %v", table, err)
			// Fall back to Shadow-only.
			result, qerr := execMySQLQuery(rh.shadowConn, querySQL)
			if qerr != nil {
				return nil, nil, nil, qerr
			}
			if result.Error != "" {
				return nil, nil, nil, &mysqlRelayError{rawMsgs: result.RawMsgs, msg: result.Error}
			}
			return result.Columns, result.RowValues, result.RowNulls, nil
		}
		tableMap[table] = utilName
		utilTables = append(utilTables, utilName)
	}

	// Rewrite the original SQL, replacing table names with temp table names.
	rewritten := querySQL
	for origTable, utilName := range tableMap {
		// Replace backtick-quoted table name.
		rewritten = strings.Replace(rewritten, "`"+origTable+"`", "`"+utilName+"`", -1)
		// Replace unquoted table name (word boundary aware).
		rewritten = replaceColumnName(rewritten, origTable, "`"+utilName+"`")
	}

	// Execute rewritten query on Shadow.
	result, err := execMySQLQuery(rh.shadowConn, rewritten)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow materialized JOIN query: %w", err)
	}
	if result.Error != "" {
		rh.logf("materialized JOIN query error: %s, falling back to shadow-only", result.Error)
		// Fall back to Shadow-only with original query.
		fallback, ferr := execMySQLQuery(rh.shadowConn, querySQL)
		if ferr != nil {
			return nil, nil, nil, ferr
		}
		if fallback.Error != "" {
			return nil, nil, nil, &mysqlRelayError{rawMsgs: fallback.RawMsgs, msg: fallback.Error}
		}
		return fallback.Columns, fallback.RowValues, fallback.RowNulls, nil
	}

	return result.Columns, result.RowValues, result.RowNulls, nil
}
