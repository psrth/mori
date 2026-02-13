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
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// ReadHandler encapsulates merged read logic for a single MSSQL connection.
type ReadHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	connID         int64
	verbose        bool
	logger         *logging.Logger
}

// HandleRead dispatches a read operation based on the routing strategy.
// fullPayload is the concatenated SQL_BATCH payload (ALL_HEADERS + SQL) from the client.
func (rh *ReadHandler) HandleRead(
	clientConn net.Conn,
	rawMsg []byte,
	fullPayload []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyMergedRead:
		return rh.handleMergedRead(clientConn, fullPayload, cl)
	case core.StrategyJoinPatch:
		// For JOINs, fall back to prod for now (same as Postgres v1).
		return forwardAndRelay(rawMsg, rh.prodConn, clientConn)
	default:
		return fmt.Errorf("unsupported read strategy: %s", strategy)
	}
}

// handleMergedRead implements the merged read algorithm for MSSQL.
// fullPayload is the client's original SQL_BATCH payload containing ALL_HEADERS + SQL.
func (rh *ReadHandler) handleMergedRead(clientConn net.Conn, fullPayload []byte, cl *core.Classification) error {
	columns, values, nulls, err := rh.mergedReadCore(cl, cl.RawSQL)
	if err != nil {
		// If it's a relay error, forward the raw response.
		if re, ok := err.(*relayError); ok {
			_, writeErr := clientConn.Write(re.rawMsgs)
			return writeErr
		}
		return err
	}

	// Build a synthetic SELECT that returns the merged data.
	// Reuse the client's original ALL_HEADERS (which include the MARS
	// transaction descriptor) so the server responds with matching
	// MARS session headers that the client driver expects.
	syntheticSQL := buildSyntheticSelect(columns, values, nulls)

	allHeaders := extractAllHeaders(fullPayload)
	var batchMsg []byte
	if allHeaders != nil {
		batchMsg = buildSQLBatchWithHeaders(allHeaders, syntheticSQL)
	} else {
		batchMsg = buildSQLBatchMessage(syntheticSQL)
	}
	return forwardAndRelay(batchMsg, rh.prodConn, clientConn)
}

// relayError wraps raw backend response bytes for errors that should be
// relayed directly to the client.
type relayError struct {
	rawMsgs []byte
	msg     string
}

func (e *relayError) Error() string { return e.msg }

// mergedReadCore performs the merged read algorithm and returns the result.
func (rh *ReadHandler) mergedReadCore(cl *core.Classification, querySQL string) (
	columns []TDSColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	// Aggregate queries: convert to row-level merge then re-aggregate.
	if cl.HasAggregate {
		return rh.aggregateReadCore(cl, querySQL)
	}

	if len(cl.Tables) == 0 {
		return nil, nil, nil, fmt.Errorf("merged read with no tables")
	}
	table := cl.Tables[0]

	// Step 0: Inject PK column if needed for dedup.
	injectedPK := ""
	effectiveSQL := querySQL
	if !cl.IsJoin {
		if meta, ok := rh.tables[table]; ok && len(meta.PKColumns) > 0 {
			pkCol := meta.PKColumns[0]
			if needsPKInjectionMSSQL(querySQL, pkCol) {
				effectiveSQL = injectPKColumnMSSQL(querySQL, pkCol)
				injectedPK = pkCol
			}
		}
	}

	// Step 1: Execute query on Shadow.
	shadowResult, err := execTDSQuery(rh.shadowConn, effectiveSQL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("shadow query: %w", err)
	}
	if shadowResult.Error != "" {
		return nil, nil, nil, &relayError{rawMsgs: shadowResult.RawMsgs, msg: shadowResult.Error}
	}

	// Step 1.5: Check if table only exists in Shadow (DDL CREATE TABLE).
	skipProd := false
	prodSQL := effectiveSQL
	if rh.schemaRegistry != nil {
		if _, exists := rh.tables[table]; !exists {
			skipProd = true
		}
	}

	// Step 2: Execute query on Prod.
	var prodResult *TDSQueryResult
	if skipProd {
		prodResult = &TDSQueryResult{}
	} else {
		// Over-fetch for LIMIT queries.
		if cl.HasLimit && cl.Limit > 0 {
			deltaCount := rh.deltaMap.CountForTable(table)
			tombstoneCount := rh.tombstones.CountForTable(table)
			overfetch := deltaCount + tombstoneCount
			if overfetch > 0 {
				prodSQL = rewriteTopMSSQL(prodSQL, cl.Limit+overfetch)
			}
		}

		prodResult, err = execTDSQuery(rh.prodConn, prodSQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prod query: %w", err)
		}
		if prodResult.Error != "" {
			// If Prod fails due to schema mismatch, retry with new columns stripped.
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
				strippedSQL := rh.stripNewColumnsFromQuery(prodSQL, cl.Tables)
				if strippedSQL != "" && strippedSQL != prodSQL {
					prodResult, err = execTDSQuery(rh.prodConn, strippedSQL)
					if err != nil || prodResult.Error != "" {
						// Stripped query also failed — fall back to shadow-only.
						prodResult = &TDSQueryResult{}
					}
				} else {
					prodResult = &TDSQueryResult{}
				}
			} else {
				return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
			}
		}
	}

	// Step 3: Filter Prod results — remove delta and tombstoned rows.
	filteredValues, filteredNulls := rh.filterProdRows(table, prodResult)

	// Step 4: Merge Shadow + filtered Prod.
	mergedColumns, mergedValues, mergedNulls := mergeResultsMSSQL(
		shadowResult.Columns, shadowResult.RowValues, shadowResult.RowNulls,
		prodResult.Columns, filteredValues, filteredNulls,
	)

	// Step 5: Dedup by PK (Shadow rows first, so Shadow version wins).
	mergedValues, mergedNulls = rh.dedup(table, mergedColumns, mergedValues, mergedNulls)

	// Step 6: Re-sort by ORDER BY.
	if cl.OrderBy != "" {
		sortMergedMSSQL(mergedColumns, mergedValues, mergedNulls, cl.OrderBy)
	}

	// Step 7: Apply LIMIT.
	if cl.HasLimit && cl.Limit > 0 && len(mergedValues) > cl.Limit {
		mergedValues = mergedValues[:cl.Limit]
		mergedNulls = mergedNulls[:cl.Limit]
	}

	// Step 8: Strip injected PK column.
	if injectedPK != "" {
		mergedColumns, mergedValues, mergedNulls = stripInjectedColumnMSSQL(
			mergedColumns, mergedValues, mergedNulls, injectedPK)
	}

	return mergedColumns, mergedValues, mergedNulls, nil
}

// aggregateReadCore handles aggregate queries by converting to row-level merge.
func (rh *ReadHandler) aggregateReadCore(cl *core.Classification, querySQL string) (
	columns []TDSColumnInfo, values [][]string, nulls [][]bool, err error,
) {
	if len(cl.Tables) == 0 {
		return nil, nil, nil, fmt.Errorf("aggregate read with no tables")
	}
	table := cl.Tables[0]

	baseSQL := rh.buildAggregateBaseQuery(querySQL, table)
	if baseSQL == "" {
		// Complex aggregate — fall back to Prod and relay the raw response
		// directly to preserve native column types (INT, DECIMAL, etc.).
		prodResult, err := execTDSQuery(rh.prodConn, querySQL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prod aggregate query: %w", err)
		}
		return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
	}

	baseCl := *cl
	baseCl.HasAggregate = false
	baseCl.HasLimit = false
	baseCl.Limit = 0
	baseCl.OrderBy = ""

	_, baseValues, _, mergeErr := rh.mergedReadCore(&baseCl, baseSQL)
	if mergeErr != nil {
		if _, ok := mergeErr.(*relayError); ok {
			// Base query failed on shadow — fall back to prod and relay raw
			// response to preserve native types.
			prodResult, pErr := execTDSQuery(rh.prodConn, querySQL)
			if pErr != nil {
				return nil, nil, nil, mergeErr
			}
			return nil, nil, nil, &relayError{rawMsgs: prodResult.RawMsgs, msg: prodResult.Error}
		}
		return nil, nil, nil, mergeErr
	}

	count := len(baseValues)
	countStr := fmt.Sprintf("%d", count)

	return []TDSColumnInfo{{Name: "_mori_count", TypeID: 0x26}},
		[][]string{{countStr}},
		[][]bool{{false}},
		nil
}

// buildAggregateBaseQuery converts a COUNT(*) to SELECT pk FROM table [WHERE ...].
func (rh *ReadHandler) buildAggregateBaseQuery(sql, table string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	if strings.Contains(upper, "GROUP BY") {
		return ""
	}

	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return ""
	}
	pkCol := meta.PKColumns[0]

	selectIdx := strings.Index(upper, "SELECT")
	fromIdx := strings.Index(upper, " FROM ")
	if selectIdx < 0 || fromIdx < 0 {
		return ""
	}

	return "SELECT " + quoteIdentMSSQL(pkCol) + sql[fromIdx:]
}

// stripNewColumnsFromQuery removes columns from the SELECT list that were added
// via DDL (exist in the schema registry as Added) and thus don't exist in Prod.
// This allows querying Prod for rows that do exist there, with NULL filled in
// for the new columns during the merge step.
func (rh *ReadHandler) stripNewColumnsFromQuery(sql string, tables []string) string {
	if rh.schemaRegistry == nil || len(tables) == 0 {
		return ""
	}

	// Collect all added column names across the relevant tables.
	addedCols := make(map[string]bool)
	for _, t := range tables {
		diff := rh.schemaRegistry.GetDiff(t)
		if diff == nil {
			continue
		}
		for _, col := range diff.Added {
			addedCols[strings.ToLower(col.Name)] = true
		}
	}
	if len(addedCols) == 0 {
		return ""
	}

	upper := strings.ToUpper(strings.TrimSpace(sql))
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return ""
	}
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return ""
	}

	// Handle SELECT * — can't strip columns from *.
	selectPart := strings.TrimSpace(sql[selectIdx+6 : fromIdx])
	upperSelectPart := strings.ToUpper(selectPart)
	if strings.HasPrefix(upperSelectPart, "*") || strings.HasPrefix(upperSelectPart, "DISTINCT *") {
		return ""
	}
	// Handle TOP N *
	if strings.HasPrefix(upperSelectPart, "TOP") {
		rest := strings.TrimSpace(upperSelectPart[3:])
		for len(rest) > 0 && (rest[0] >= '0' && rest[0] <= '9' || rest[0] == ' ' || rest[0] == '(' || rest[0] == ')') {
			rest = rest[1:]
		}
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(rest, "*") {
			return ""
		}
	}

	// Parse the select list and filter out added columns.
	parts := strings.Split(selectPart, ",")
	var kept []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		// Extract the column name (strip alias, table prefix, brackets).
		colName := trimmed
		if asIdx := strings.Index(strings.ToUpper(colName), " AS "); asIdx >= 0 {
			colName = strings.TrimSpace(colName[:asIdx])
		}
		if dotIdx := strings.LastIndex(colName, "."); dotIdx >= 0 {
			colName = colName[dotIdx+1:]
		}
		colName = strings.Trim(colName, "[]`\"")
		if addedCols[strings.ToLower(colName)] {
			continue
		}
		kept = append(kept, trimmed)
	}

	if len(kept) == 0 {
		return ""
	}

	return sql[:selectIdx+6] + " " + strings.Join(kept, ", ") + sql[fromIdx:]
}

// filterProdRows removes rows where the PK is in the delta map or tombstone set.
func (rh *ReadHandler) filterProdRows(table string, result *TDSQueryResult) ([][]string, [][]bool) {
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

// findPKColumnIndex returns the index of the PK column in the result.
func (rh *ReadHandler) findPKColumnIndex(table string, columns []TDSColumnInfo) int {
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return -1
	}
	pkCol := strings.ToLower(meta.PKColumns[0])
	for i, col := range columns {
		if strings.ToLower(col.Name) == pkCol {
			return i
		}
	}
	return -1
}

// dedup removes duplicate rows by PK, keeping the first occurrence.
func (rh *ReadHandler) dedup(
	table string,
	columns []TDSColumnInfo,
	values [][]string,
	nulls [][]bool,
) ([][]string, [][]bool) {
	meta, ok := rh.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return values, nulls
	}

	pkCol := strings.ToLower(meta.PKColumns[0])
	pkIdx := -1
	for i, col := range columns {
		if strings.ToLower(col.Name) == pkCol {
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

// mergeResultsMSSQL combines Shadow and filtered Prod results.
func mergeResultsMSSQL(
	shadowCols []TDSColumnInfo, shadowValues [][]string, shadowNulls [][]bool,
	prodCols []TDSColumnInfo, prodValues [][]string, prodNulls [][]bool,
) ([]TDSColumnInfo, [][]string, [][]bool) {
	var columns []TDSColumnInfo
	if len(shadowCols) > 0 {
		columns = shadowCols
	} else {
		columns = prodCols
	}

	var mergedValues [][]string
	var mergedNulls [][]bool

	// Shadow rows first (priority in dedup).
	mergedValues = append(mergedValues, shadowValues...)
	mergedNulls = append(mergedNulls, shadowNulls...)

	// Prod rows — align columns if needed.
	if tdsColumnsMatch(columns, prodCols) {
		mergedValues = append(mergedValues, prodValues...)
		mergedNulls = append(mergedNulls, prodNulls...)
	} else {
		prodColIdx := buildTDSColumnIndex(prodCols)
		for i, row := range prodValues {
			alignedRow := make([]string, len(columns))
			alignedNulls := make([]bool, len(columns))
			for j, col := range columns {
				if idx, ok := prodColIdx[strings.ToLower(col.Name)]; ok && idx < len(row) {
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

func tdsColumnsMatch(a, b []TDSColumnInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i].Name, b[i].Name) {
			return false
		}
	}
	return true
}

func buildTDSColumnIndex(cols []TDSColumnInfo) map[string]int {
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		idx[strings.ToLower(c.Name)] = i
	}
	return idx
}

// sortMergedMSSQL sorts the merged result set by ORDER BY clause.
func sortMergedMSSQL(columns []TDSColumnInfo, values [][]string, nulls [][]bool, orderBy string) {
	type orderByCol struct {
		name string
		desc bool
		idx  int
	}

	orderBy = strings.TrimRight(orderBy, "; \t\n")
	if orderBy == "" {
		return
	}

	parts := strings.Split(orderBy, ",")
	colIndex := buildTDSColumnIndex(columns)
	var resolved []orderByCol
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) == 0 {
			continue
		}
		col := strings.Trim(fields[0], `"[]`)
		if dotIdx := strings.LastIndex(col, "."); dotIdx >= 0 {
			col = col[dotIdx+1:]
		}
		desc := false
		if len(fields) >= 2 && strings.EqualFold(fields[1], "DESC") {
			desc = true
		}
		if idx, ok := colIndex[strings.ToLower(col)]; ok {
			resolved = append(resolved, orderByCol{name: col, desc: desc, idx: idx})
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
			cmp := compareMSSQLValues(va, vb)
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

func compareMSSQLValues(a, b string) int {
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

// needsPKInjectionMSSQL checks if PK column needs to be injected for dedup.
func needsPKInjectionMSSQL(sql, pkCol string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))

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
	if strings.HasPrefix(afterSelect, "*") || strings.HasPrefix(afterSelect, "DISTINCT *") ||
		strings.HasPrefix(afterSelect, "TOP") {
		// For TOP N *, don't inject.
		topRest := afterSelect
		if strings.HasPrefix(topRest, "TOP") {
			// Find the '*' after TOP N.
			afterTop := strings.TrimSpace(topRest[3:])
			// Skip the number.
			for len(afterTop) > 0 && (afterTop[0] >= '0' && afterTop[0] <= '9' || afterTop[0] == ' ' || afterTop[0] == '(') {
				afterTop = afterTop[1:]
			}
			afterTop = strings.TrimLeft(afterTop, ") ")
			if strings.HasPrefix(afterTop, "*") {
				return false
			}
		} else {
			return false
		}
	}

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return false
	}

	afterFrom := strings.TrimSpace(upper[fromIdx+6:])
	if strings.HasPrefix(afterFrom, "(") {
		return false
	}

	selectList := strings.ToLower(sql[selectIdx+6 : selectIdx+(fromIdx-selectIdx)])
	return !containsColumnMSSQL(selectList, strings.ToLower(pkCol))
}

func containsColumnMSSQL(selectList, col string) bool {
	parts := strings.Split(selectList, ",")
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if idx := strings.Index(strings.ToLower(name), " as "); idx >= 0 {
			name = strings.TrimSpace(name[:idx])
		}
		if dotIdx := strings.LastIndex(name, "."); dotIdx >= 0 {
			name = name[dotIdx+1:]
		}
		name = strings.Trim(name, `"[]`)
		if strings.EqualFold(name, col) {
			return true
		}
	}
	return false
}

// injectPKColumnMSSQL adds the PK column to the SELECT list.
func injectPKColumnMSSQL(sql, pkCol string) string {
	upper := strings.ToUpper(sql)
	selectIdx := strings.Index(upper, "SELECT")
	if selectIdx < 0 {
		return sql
	}
	insertPos := selectIdx + 6
	afterSelect := strings.TrimSpace(sql[insertPos:])
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT") {
		insertPos += (len(sql[insertPos:]) - len(afterSelect)) + 8
	} else if strings.HasPrefix(strings.ToUpper(afterSelect), "TOP") {
		// Skip TOP N.
		topUpper := strings.ToUpper(afterSelect)
		rest := topUpper[3:]
		rest = strings.TrimLeft(rest, " (")
		numEnd := 0
		for numEnd < len(rest) && rest[numEnd] >= '0' && rest[numEnd] <= '9' {
			numEnd++
		}
		rest = rest[numEnd:]
		rest = strings.TrimLeft(rest, ") ")
		consumed := len(afterSelect) - len(rest)
		insertPos += (len(sql[insertPos:]) - len(afterSelect)) + consumed
	}
	return sql[:insertPos] + " " + quoteIdentMSSQL(pkCol) + "," + sql[insertPos:]
}

// stripInjectedColumnMSSQL removes the injected PK column from the result set.
func stripInjectedColumnMSSQL(
	columns []TDSColumnInfo, values [][]string, nulls [][]bool, pkCol string,
) ([]TDSColumnInfo, [][]string, [][]bool) {
	pkIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col.Name, pkCol) {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return columns, values, nulls
	}

	newCols := make([]TDSColumnInfo, 0, len(columns)-1)
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

// rewriteTopMSSQL replaces TOP N in a MSSQL query with a new limit.
// MSSQL uses TOP instead of LIMIT.
func rewriteTopMSSQL(sql string, newLimit int) string {
	upper := strings.ToUpper(sql)
	topIdx := strings.Index(upper, "TOP")
	if topIdx < 0 {
		return sql
	}

	afterTop := sql[topIdx+3:]
	trimmed := strings.TrimLeft(afterTop, " (")
	whitespace := len(afterTop) - len(trimmed)

	numEnd := 0
	for numEnd < len(trimmed) && trimmed[numEnd] >= '0' && trimmed[numEnd] <= '9' {
		numEnd++
	}
	if numEnd == 0 {
		return sql
	}

	// Check for closing paren.
	rest := trimmed[numEnd:]
	closeParen := ""
	if len(rest) > 0 && rest[0] == ')' {
		closeParen = ")"
		rest = rest[1:]
	}

	return sql[:topIdx+3] + afterTop[:whitespace] + strconv.Itoa(newLimit) + closeParen + rest
}

// buildSyntheticSelect constructs a T-SQL SELECT that returns the given
// in-memory rows. Uses VALUES table constructor for rows, or a dummy
// SELECT with WHERE 1=0 for empty result sets.
func buildSyntheticSelect(columns []TDSColumnInfo, values [][]string, nulls [][]bool) string {
	if len(columns) == 0 {
		return "SELECT 1 WHERE 1=0"
	}

	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = quoteIdentMSSQL(col.Name)
	}

	if len(values) == 0 {
		// Empty result set: SELECT typed NULLs with column aliases WHERE 1=0.
		var parts []string
		for _, col := range columns {
			parts = append(parts, "CAST(NULL AS NVARCHAR(4000)) AS "+quoteIdentMSSQL(col.Name))
		}
		return "SELECT " + strings.Join(parts, ", ") + " WHERE 1=0"
	}

	// Build VALUES rows.
	var rowStrs []string
	for i, row := range values {
		var vals []string
		for j, val := range row {
			isNull := len(nulls) > i && len(nulls[i]) > j && nulls[i][j]
			if isNull {
				vals = append(vals, "NULL")
			} else {
				vals = append(vals, "N"+quoteLiteralMSSQL(val))
			}
		}
		rowStrs = append(rowStrs, "("+strings.Join(vals, ", ")+")")
	}

	return "SELECT " + strings.Join(colNames, ", ") +
		" FROM (VALUES " + strings.Join(rowStrs, ", ") +
		") AS _mori_merged(" + strings.Join(colNames, ", ") + ")"
}

func (rh *ReadHandler) logf(format string, args ...interface{}) {
	if rh.verbose {
		prefix := fmt.Sprintf("[conn %d] ", rh.connID)
		log.Printf(prefix+format, args...)
	}
}
