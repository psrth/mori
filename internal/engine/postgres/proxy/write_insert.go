package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
)

// handleInsert executes an INSERT on Shadow and relays the response to the client.
// Captures the CommandComplete tag to extract the insert count, then tracks it.
//
// If the INSERT has a RETURNING clause, the response DataRows are parsed to
// extract PK values for more precise delta tracking (individual PKs instead
// of just an insert count).
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawMsg []byte, cl *core.Classification) error {
	// Enforce FK constraints before executing the INSERT.
	if w.fkEnforcer != nil && len(cl.Tables) == 1 {
		if err := w.fkEnforcer.EnforceInsert(cl.Tables[0], cl.RawSQL); err != nil {
			if w.verbose {
				log.Printf("[conn %d] FK violation on INSERT: %v", w.connID, err)
			}
			return sendFKError(clientConn, err.Error())
		}
	}

	// If the INSERT has RETURNING and we know the table's PK, capture
	// the response to extract PK values from the returned rows.
	if cl.HasReturning && len(cl.Tables) == 1 {
		if meta, ok := w.tables[cl.Tables[0]]; ok && len(meta.PKColumns) > 0 {
			return w.handleInsertReturning(clientConn, rawMsg, cl)
		}
	}

	tag, err := forwardRelayAndCaptureTag(rawMsg, w.shadowConn, clientConn)
	if err != nil {
		return err
	}
	count := parseInsertCount(tag)
	for _, table := range cl.Tables {
		if w.inTxn() {
			w.deltaMap.StageInsertCount(table, count)
		} else {
			w.deltaMap.AddInsertCount(table, count)
		}
	}

	// Persist immediately in autocommit mode; txn commit handles persistence otherwise.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}
	return nil
}

// handleInsertReturning handles INSERT ... RETURNING by capturing the response,
// relaying it to the client, and extracting PK values from returned DataRows
// for precise delta tracking.
func (w *WriteHandler) handleInsertReturning(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	table := cl.Tables[0]
	meta := w.tables[table]

	// Capture the full response from Shadow.
	msgs, err := forwardAndCapture(rawMsg, w.shadowConn)
	if err != nil {
		return err
	}

	// Parse the response to find PK column values in RETURNING data.
	pkIndices := make(map[string]int, len(meta.PKColumns))
	var returnedPKs []string

	for _, msg := range msgs {
		switch msg.Type {
		case 'T': // RowDescription
			cols, parseErr := parseRowDescription(msg.Payload)
			if parseErr == nil {
				for _, pkc := range meta.PKColumns {
					for i, col := range cols {
						if col.Name == pkc {
							pkIndices[pkc] = i
							break
						}
					}
				}
			}
		case 'D': // DataRow
			if len(pkIndices) == len(meta.PKColumns) {
				vals, nulls, parseErr := parseDataRow(msg.Payload)
				if parseErr == nil {
					hasNull := false
					pkVals := make([]string, len(meta.PKColumns))
					for j, pkc := range meta.PKColumns {
						idx := pkIndices[pkc]
						if idx >= len(vals) || nulls[idx] {
							hasNull = true
							break
						}
						pkVals[j] = string(vals[idx])
					}
					if !hasNull {
						returnedPKs = append(returnedPKs, core.SerializeCompositePK(meta.PKColumns, pkVals))
					}
				}
			}
		}
	}

	// Relay all captured messages to the client.
	for _, msg := range msgs {
		if _, err := clientConn.Write(msg.Raw); err != nil {
			return fmt.Errorf("relaying to client: %w", err)
		}
	}

	// If we extracted specific PKs, track them individually.
	if len(returnedPKs) > 0 {
		for _, pk := range returnedPKs {
			if w.inTxn() {
				w.deltaMap.Stage(table, pk)
			} else {
				w.deltaMap.Add(table, pk)
			}
		}
		if w.verbose {
			log.Printf("[conn %d] INSERT RETURNING: tracked %d PKs for %s", w.connID, len(returnedPKs), table)
		}
	} else {
		// Fallback: track as insert count if PK extraction failed.
		tag := ""
		for _, msg := range msgs {
			if msg.Type == 'C' {
				tag = parseCommandTag(msg.Payload)
				break
			}
		}
		count := parseInsertCount(tag)
		if w.inTxn() {
			w.deltaMap.StageInsertCount(table, count)
		} else {
			w.deltaMap.AddInsertCount(table, count)
		}
	}

	// Persist immediately in autocommit mode.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// handleUpsert handles INSERT ... ON CONFLICT (upsert) statements.
//
// The conflict might not fire on Shadow if the conflicting row only exists in
// Prod. This method:
//  1. Parses the ON CONFLICT target columns and inserted values from the SQL.
//  2. For each inserted row, checks if the conflict key exists in Prod but not
//     already in Shadow (delta map).
//  3. Hydrates matching rows from Prod into Shadow.
//  4. Forwards the original INSERT ... ON CONFLICT to Shadow.
//  5. Tracks affected rows in the delta map.
//
// If parsing fails or the conflict target cannot be determined, falls back to
// forwarding directly to Shadow (same as a plain INSERT).
func (w *WriteHandler) handleUpsert(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
) error {
	// Must have exactly one table for upsert hydration.
	if len(cl.Tables) != 1 {
		if w.verbose {
			log.Printf("[conn %d] upsert: multi-table INSERT, falling back to Shadow-only", w.connID)
		}
		return w.handleInsert(clientConn, rawMsg, cl)
	}

	table := cl.Tables[0]
	meta, hasMeta := w.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		if w.verbose {
			log.Printf("[conn %d] upsert: no PK metadata for %s, falling back to Shadow-only", w.connID, table)
		}
		return w.handleInsert(clientConn, rawMsg, cl)
	}

	// Parse the INSERT to extract conflict target columns and inserted values.
	conflictCols, insertedRows, err := parseUpsertDetails(cl.RawSQL)
	if err != nil || len(conflictCols) == 0 {
		if w.verbose {
			log.Printf("[conn %d] upsert: could not parse conflict target: %v, falling back to Shadow-only", w.connID, err)
		}
		return w.handleInsert(clientConn, rawMsg, cl)
	}

	// For each inserted row, check if the conflict key exists in Prod.
	hydratedCount := 0
	var affectedPKs []string

	// Build SELECT list for all PK columns.
	pkSelectCols := make([]string, len(meta.PKColumns))
	for i, pkc := range meta.PKColumns {
		pkSelectCols[i] = quoteIdent(pkc)
	}
	pkSelectList := strings.Join(pkSelectCols, ", ")

	for _, row := range insertedRows {
		// Build a WHERE clause from the conflict target columns and values.
		whereClause := buildConflictWhere(conflictCols, row)
		if whereClause == "" {
			continue
		}

		// Check if this row is already tracked in delta map by querying
		// Shadow for the conflict key.
		checkSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1",
			pkSelectList, quoteIdent(table), whereClause)
		shadowResult, err := execQuery(w.shadowConn, checkSQL)
		if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
			// Row already exists in Shadow — conflict will fire correctly.
			// Track the PK for delta map.
			if len(shadowResult.RowValues[0]) >= len(meta.PKColumns) {
				pkVals := make([]string, len(meta.PKColumns))
				for j, pkc := range meta.PKColumns {
					for ci, col := range shadowResult.Columns {
						if col.Name == pkc {
							pkVals[j] = shadowResult.RowValues[0][ci]
							break
						}
					}
				}
				affectedPKs = append(affectedPKs, core.SerializeCompositePK(meta.PKColumns, pkVals))
			}
			continue
		}

		// Row not in Shadow — check Prod and hydrate if found.
		selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1",
			quoteIdent(table), whereClause)
		prodResult, err := execQuery(w.prodConn, selectSQL)
		if err != nil {
			if w.verbose {
				log.Printf("[conn %d] upsert: Prod query failed: %v", w.connID, err)
			}
			continue
		}
		if prodResult.Error != "" {
			if w.verbose {
				log.Printf("[conn %d] upsert: Prod query error: %s", w.connID, prodResult.Error)
			}
			continue
		}
		if len(prodResult.RowValues) == 0 {
			// Row doesn't exist in Prod either — INSERT will create a new row.
			continue
		}

		// Find PK values in the Prod result for delta tracking.
		pkIndices := make(map[string]int, len(meta.PKColumns))
		for _, pkc := range meta.PKColumns {
			for i, col := range prodResult.Columns {
				if col.Name == pkc {
					pkIndices[pkc] = i
					break
				}
			}
		}
		if len(pkIndices) == len(meta.PKColumns) {
			hasNull := false
			pkVals := make([]string, len(meta.PKColumns))
			for j, pkc := range meta.PKColumns {
				idx := pkIndices[pkc]
				if prodResult.RowNulls[0][idx] {
					hasNull = true
					break
				}
				pkVals[j] = prodResult.RowValues[0][idx]
			}
			if !hasNull {
				affectedPKs = append(affectedPKs, core.SerializeCompositePK(meta.PKColumns, pkVals))
			}
		}

		// Hydrate: insert the Prod row into Shadow, skipping generated columns.
		insertSQL := buildInsertSQL(table, prodResult.Columns, prodResult.RowValues[0], prodResult.RowNulls[0], toSkipSet(meta.GeneratedCols))
		shadowInsertResult, err := execQuery(w.shadowConn, insertSQL)
		if err != nil {
			if w.verbose {
				log.Printf("[conn %d] upsert: hydration INSERT failed: %v", w.connID, err)
			}
			continue
		}
		if shadowInsertResult.Error != "" && w.verbose {
			log.Printf("[conn %d] upsert: hydration INSERT: %s", w.connID, shadowInsertResult.Error)
		}
		hydratedCount++
	}

	if w.verbose && hydratedCount > 0 {
		log.Printf("[conn %d] upsert: hydrated %d rows from Prod for %s", w.connID, hydratedCount, table)
	}

	// Forward the original INSERT ... ON CONFLICT to Shadow.
	// If it has RETURNING, use the RETURNING-aware path for better tracking.
	if cl.HasReturning {
		if _, ok := w.tables[table]; ok && len(meta.PKColumns) > 0 {
			return w.handleUpsertReturning(clientConn, rawMsg, cl, affectedPKs)
		}
	}

	tag, err := forwardRelayAndCaptureTag(rawMsg, w.shadowConn, clientConn)
	if err != nil {
		return err
	}

	// Track deltas: for affected PKs (hydrated rows that got upserted), track individually.
	for _, pk := range affectedPKs {
		if w.inTxn() {
			w.deltaMap.Stage(table, pk)
		} else {
			w.deltaMap.Add(table, pk)
		}
	}

	// Also track the insert count for any newly inserted rows.
	count := parseInsertCount(tag)
	if count > 0 {
		if w.inTxn() {
			w.deltaMap.StageInsertCount(table, count)
		} else {
			w.deltaMap.AddInsertCount(table, count)
		}
	}

	// Persist immediately in autocommit mode.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// handleUpsertReturning handles INSERT ... ON CONFLICT ... RETURNING by
// capturing the full response to extract PKs from RETURNING data.
func (w *WriteHandler) handleUpsertReturning(
	clientConn net.Conn,
	rawMsg []byte,
	cl *core.Classification,
	preHydratedPKs []string,
) error {
	table := cl.Tables[0]
	meta := w.tables[table]

	// Capture the full response from Shadow.
	msgs, err := forwardAndCapture(rawMsg, w.shadowConn)
	if err != nil {
		return err
	}

	// Parse response for PK values.
	pkIndices := make(map[string]int, len(meta.PKColumns))
	var returnedPKs []string

	for _, msg := range msgs {
		switch msg.Type {
		case 'T':
			cols, parseErr := parseRowDescription(msg.Payload)
			if parseErr == nil {
				for _, pkc := range meta.PKColumns {
					for i, col := range cols {
						if col.Name == pkc {
							pkIndices[pkc] = i
							break
						}
					}
				}
			}
		case 'D':
			if len(pkIndices) == len(meta.PKColumns) {
				vals, nulls, parseErr := parseDataRow(msg.Payload)
				if parseErr == nil {
					hasNull := false
					pkVals := make([]string, len(meta.PKColumns))
					for j, pkc := range meta.PKColumns {
						idx := pkIndices[pkc]
						if idx >= len(vals) || nulls[idx] {
							hasNull = true
							break
						}
						pkVals[j] = string(vals[idx])
					}
					if !hasNull {
						returnedPKs = append(returnedPKs, core.SerializeCompositePK(meta.PKColumns, pkVals))
					}
				}
			}
		}
	}

	// Relay all messages to client.
	for _, msg := range msgs {
		if _, err := clientConn.Write(msg.Raw); err != nil {
			return fmt.Errorf("relaying to client: %w", err)
		}
	}

	// Track all returned PKs (covers both inserted and upserted rows).
	allPKs := mergeUniquePKs(preHydratedPKs, returnedPKs)
	for _, pk := range allPKs {
		if w.inTxn() {
			w.deltaMap.Stage(table, pk)
		} else {
			w.deltaMap.Add(table, pk)
		}
	}

	if w.verbose && len(allPKs) > 0 {
		log.Printf("[conn %d] upsert RETURNING: tracked %d PKs for %s", w.connID, len(allPKs), table)
	}

	// Persist immediately in autocommit mode.
	if !w.inTxn() {
		if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
			if w.verbose {
				log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
			}
		}
	}

	return nil
}

// upsertRow holds column name → value pairs for a single row being inserted.
type upsertRow map[string]string

// parseUpsertDetails extracts the ON CONFLICT target columns and the inserted
// row values from an INSERT ... ON CONFLICT statement using pg_query_go AST parsing.
//
// Returns:
//   - conflictCols: the column names from the ON CONFLICT target (e.g., ["email"])
//   - rows: one upsertRow per VALUES row, mapping conflict column names to their values
//
// Returns an error if the SQL cannot be parsed or is not an INSERT with ON CONFLICT.
func parseUpsertDetails(rawSQL string) (conflictCols []string, rows []upsertRow, err error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse: %w", err)
	}

	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return nil, nil, fmt.Errorf("no statements found")
	}

	ins := stmts[0].GetStmt().GetInsertStmt()
	if ins == nil {
		return nil, nil, fmt.Errorf("not an INSERT statement")
	}

	onConflict := ins.GetOnConflictClause()
	if onConflict == nil {
		return nil, nil, fmt.Errorf("no ON CONFLICT clause")
	}

	// Extract conflict target columns from the INFER clause.
	infer := onConflict.GetInfer()
	if infer == nil {
		return nil, nil, fmt.Errorf("no conflict target (INFER) clause")
	}

	for _, elem := range infer.GetIndexElems() {
		if ie := elem.GetIndexElem(); ie != nil {
			name := ie.GetName()
			if name != "" {
				conflictCols = append(conflictCols, name)
			}
		}
	}

	if len(conflictCols) == 0 {
		return nil, nil, fmt.Errorf("no conflict target columns found")
	}

	// Extract the target column list (the columns in INSERT INTO t (col1, col2, ...)).
	var insertCols []string
	for _, col := range ins.GetCols() {
		if rt := col.GetResTarget(); rt != nil {
			insertCols = append(insertCols, rt.GetName())
		}
	}

	// Extract VALUES rows from the SELECT statement.
	selStmt := ins.GetSelectStmt()
	if selStmt == nil {
		return conflictCols, nil, nil
	}

	sel := selStmt.GetSelectStmt()
	if sel == nil {
		return conflictCols, nil, nil
	}

	// VALUES clause appears as ValuesLists in the SelectStmt.
	for _, valList := range sel.GetValuesLists() {
		list := valList.GetList()
		if list == nil {
			continue
		}

		row := make(upsertRow)
		for i, item := range list.GetItems() {
			if i >= len(insertCols) {
				break
			}
			colName := insertCols[i]
			// Only extract values for conflict target columns.
			if !stringInSlice(colName, conflictCols) {
				continue
			}
			// Try to extract a constant value from the AST node.
			val, ok := extractConstValue(item)
			if ok {
				row[colName] = val
			}
		}

		if len(row) > 0 {
			rows = append(rows, row)
		}
	}

	return conflictCols, rows, nil
}

// extractConstValue attempts to extract a literal value from a pg_query AST node.
// Handles A_Const nodes with Integer, Float, Boolean, and String values.
// Also handles TypeCast wrapping a constant (e.g., 'value'::uuid).
func extractConstValue(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}

	// Direct A_Const
	if ac := node.GetAConst(); ac != nil {
		return aConstToString(ac)
	}

	// TypeCast wrapping a constant (e.g., '...'::uuid)
	if tc := node.GetTypeCast(); tc != nil {
		if arg := tc.GetArg(); arg != nil {
			if ac := arg.GetAConst(); ac != nil {
				return aConstToString(ac)
			}
		}
	}

	return "", false
}

// aConstToString converts a pg_query A_Const to its string representation.
func aConstToString(ac *pg_query.A_Const) (string, bool) {
	if iv := ac.GetIval(); iv != nil {
		return fmt.Sprintf("%d", iv.GetIval()), true
	}
	if fv := ac.GetFval(); fv != nil {
		return fv.GetFval(), true
	}
	if sv := ac.GetSval(); sv != nil {
		return sv.GetSval(), true
	}
	if bv := ac.GetBoolval(); bv != nil {
		if bv.GetBoolval() {
			return "true", true
		}
		return "false", true
	}
	return "", false
}

// buildConflictWhere builds a WHERE clause string from conflict column names
// and an upsertRow's values. Returns empty string if any conflict column is
// missing from the row.
func buildConflictWhere(conflictCols []string, row upsertRow) string {
	var parts []string
	for _, col := range conflictCols {
		val, ok := row[col]
		if !ok {
			return "" // Cannot build WHERE — missing conflict column value.
		}
		parts = append(parts, fmt.Sprintf("%s = %s", quoteIdent(col), quoteLiteral(val)))
	}
	return strings.Join(parts, " AND ")
}

// stringInSlice reports whether s is in the slice.
func stringInSlice(s string, slice []string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// mergeUniquePKs merges two PK slices, deduplicating by value.
func mergeUniquePKs(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	var result []string
	for _, pk := range a {
		if !seen[pk] {
			seen[pk] = true
			result = append(result, pk)
		}
	}
	for _, pk := range b {
		if !seen[pk] {
			seen[pk] = true
			result = append(result, pk)
		}
	}
	return result
}
