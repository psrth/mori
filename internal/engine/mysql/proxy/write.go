package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// WriteHandler encapsulates write path logic for a single MySQL connection.
type WriteHandler struct {
	prodConn       net.Conn
	shadowConn     net.Conn
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	fkEnforcer     *FKEnforcer
	moriDir        string
	connID         int64
	verbose        bool
	logger         *logging.Logger
	maxRowsHydrate int
}

// HandleWrite dispatches a write operation based on the routing strategy.
func (w *WriteHandler) HandleWrite(
	clientConn net.Conn,
	rawPkt []byte,
	cl *core.Classification,
	strategy core.RoutingStrategy,
) error {
	switch strategy {
	case core.StrategyShadowWrite:
		return w.handleInsert(clientConn, rawPkt, cl)
	case core.StrategyHydrateAndWrite:
		if cl.SubType == core.SubInsert && cl.HasOnConflict {
			return w.handleUpsert(clientConn, rawPkt, cl)
		}
		return w.handleUpdate(clientConn, rawPkt, cl)
	case core.StrategyShadowDelete:
		return w.handleDelete(clientConn, rawPkt, cl)
	default:
		return fmt.Errorf("unsupported write strategy: %s", strategy)
	}
}

// handleInsert executes an INSERT on Shadow and marks the table as having inserts.
func (w *WriteHandler) handleInsert(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	// FK enforcement: check parent rows exist before inserting.
	if w.fkEnforcer != nil && len(cl.Tables) > 0 {
		if err := w.fkEnforcer.EnforceInsert(cl.Tables[0], cl.RawSQL); err != nil {
			clientConn.Write(buildFKErrorPacket(err.Error()))
			return nil
		}
	}

	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}
	for _, table := range cl.Tables {
		w.deltaMap.MarkInserted(table)
	}
	return nil
}

// handleUpdate handles UPDATE statements with hydration from Prod when needed.
func (w *WriteHandler) handleUpdate(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	// FK enforcement: check parent rows exist for updated FK columns.
	if w.fkEnforcer != nil && len(cl.Tables) > 0 {
		if err := w.fkEnforcer.EnforceUpdate(cl.Tables[0], cl.RawSQL); err != nil {
			clientConn.Write(buildFKErrorPacket(err.Error()))
			return nil
		}
	}

	if len(cl.PKs) == 0 {
		return w.handleBulkUpdate(clientConn, rawPkt, cl)
	}

	// Point update: hydrate missing rows, then execute.
	for _, pk := range cl.PKs {
		if w.deltaMap.IsDelta(pk.Table, pk.PK) {
			continue // Already in Shadow.
		}
		meta := w.tables[pk.Table]
		if err := w.hydrateRow(pk.Table, pkValuesFromSingle(meta, pk.PK)); err != nil {
			if w.verbose {
				log.Printf("[conn %d] hydration failed for (%s, %s): %v", w.connID, pk.Table, pk.PK, err)
			}
		}
	}

	// Execute the UPDATE on Shadow, relay response to client.
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Update delta map for all PKs.
	for _, pk := range cl.PKs {
		w.deltaMap.Add(pk.Table, pk.PK)
	}

	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}

	return nil
}

// handleDelete handles DELETE statements by executing on Shadow and adding tombstones.
func (w *WriteHandler) handleDelete(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if len(cl.PKs) == 0 {
		return w.handleBulkDelete(clientConn, rawPkt, cl)
	}

	// FK enforcement: check RESTRICT/CASCADE before deleting.
	if w.fkEnforcer != nil && len(cl.Tables) > 0 {
		deletedPKs := make([]string, 0, len(cl.PKs))
		for _, pk := range cl.PKs {
			if pk.Table == cl.Tables[0] {
				deletedPKs = append(deletedPKs, pk.PK)
			}
		}
		if err := w.fkEnforcer.EnforceDeleteCascade(cl.Tables[0], deletedPKs); err != nil {
			clientConn.Write(buildFKDeleteErrorPacket(err.Error()))
			return nil
		}
	}

	// Forward DELETE to Shadow, relay response.
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Add tombstones for deleted PKs.
	for _, pk := range cl.PKs {
		w.tombstones.Add(pk.Table, pk.PK)
		w.deltaMap.Remove(pk.Table, pk.PK)
	}

	if err := delta.WriteTombstoneSet(w.moriDir, w.tombstones); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist tombstone set: %v", w.connID, err)
		}
	}
	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}

	return nil
}

// hydrateRow fetches a single row from Prod by PK and inserts it into Shadow.
// pkValues maps PK column names to their values.
// Handles schema diffs: adapts Prod results for column renames, drops, and additions.
func (w *WriteHandler) hydrateRow(table string, pkValues map[string]string) error {
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		return fmt.Errorf("no PK metadata for table %q", table)
	}

	// Build composite WHERE clause using Prod column names (may differ from Shadow).
	var diff *coreSchema.TableDiff
	if w.schemaRegistry != nil {
		diff = w.schemaRegistry.GetDiff(table)
	}

	// Map PK column names to Prod names for the WHERE clause.
	var whereParts []string
	for _, col := range meta.PKColumns {
		val, ok := pkValues[col]
		if !ok {
			return fmt.Errorf("missing PK column %q for table %q", col, table)
		}
		// Use Prod column name if column was renamed.
		prodCol := col
		if diff != nil {
			for oldName, newName := range diff.Renamed {
				if newName == col {
					prodCol = oldName
					break
				}
			}
		}
		whereParts = append(whereParts, fmt.Sprintf("`%s` = '%s'", prodCol, strings.ReplaceAll(val, "'", "''")))
	}
	selectSQL := capSQL(
		fmt.Sprintf("SELECT * FROM `%s` WHERE %s", table, strings.Join(whereParts, " AND ")),
		w.maxRowsHydrate,
	)

	result, err := execMySQLQuery(w.prodConn, selectSQL)
	if err != nil {
		return fmt.Errorf("prod SELECT: %w", err)
	}
	if result.Error != "" {
		return fmt.Errorf("prod SELECT error: %s", result.Error)
	}
	if len(result.RowValues) == 0 {
		return nil // Row doesn't exist in Prod.
	}

	// Build skip sets for generated columns and PK columns.
	skipCols := make(map[string]bool)
	for _, gc := range meta.GeneratedCols {
		skipCols[gc] = true
	}
	pkColSet := make(map[string]bool)
	for _, pk := range meta.PKColumns {
		pkColSet[pk] = true
	}

	// Adapt Prod result for schema diffs before building Shadow INSERT.
	columns := result.Columns
	rowValues := result.RowValues[0]
	rowNulls := result.RowNulls[0]
	if diff != nil {
		columns, rowValues, rowNulls = w.adaptProdRowForShadow(diff, columns, rowValues, rowNulls)
	}

	insertSQL := buildMySQLInsertSQL(table, columns, rowValues, rowNulls, skipCols, pkColSet)

	shadowResult, err := execMySQLQuery(w.shadowConn, insertSQL)
	if err != nil {
		return fmt.Errorf("shadow INSERT: %w", err)
	}
	if shadowResult.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] hydration INSERT for %s: %s", w.connID, table, shadowResult.Error)
		}
	}

	return nil
}

// adaptProdRowForShadow transforms a Prod row's columns and values to match
// Shadow schema when there are schema diffs (renamed/dropped/added columns).
func (w *WriteHandler) adaptProdRowForShadow(
	diff *coreSchema.TableDiff,
	columns []ColumnInfo,
	values []string,
	nulls []bool,
) ([]ColumnInfo, []string, []bool) {
	// Build sets for quick lookup.
	droppedSet := make(map[string]bool)
	for _, d := range diff.Dropped {
		droppedSet[strings.ToLower(d)] = true
	}

	// Build reverse rename map: old Prod name → new Shadow name.
	reverseRename := make(map[string]string)
	for oldName, newName := range diff.Renamed {
		reverseRename[strings.ToLower(oldName)] = newName
	}

	// Filter dropped columns and rename columns.
	var newCols []ColumnInfo
	var newVals []string
	var newNulls []bool
	for i, col := range columns {
		colLower := strings.ToLower(col.Name)
		// Skip columns that were dropped from Shadow.
		if droppedSet[colLower] {
			continue
		}
		// Rename if column was renamed.
		name := col.Name
		if newName, ok := reverseRename[colLower]; ok {
			name = newName
		}
		newCols = append(newCols, ColumnInfo{Name: name, RawDef: col.RawDef})
		if i < len(values) {
			newVals = append(newVals, values[i])
		} else {
			newVals = append(newVals, "")
		}
		if i < len(nulls) {
			newNulls = append(newNulls, nulls[i])
		} else {
			newNulls = append(newNulls, true)
		}
	}

	// Append added columns (exist in Shadow but not Prod) as NULL.
	for _, added := range diff.Added {
		newCols = append(newCols, ColumnInfo{Name: added.Name})
		newVals = append(newVals, "")
		newNulls = append(newNulls, true)
	}

	return newCols, newVals, newNulls
}

// buildMySQLInsertSQL constructs an INSERT ... ON DUPLICATE KEY UPDATE statement for hydration.
// skipCols contains column names to exclude (e.g., generated columns).
// pkCols contains primary key column names (excluded from ON DUPLICATE KEY UPDATE clause).
func buildMySQLInsertSQL(table string, columns []ColumnInfo, values []string, nulls []bool, skipCols map[string]bool, pkCols map[string]bool) string {
	var colNames []string
	var valParts []string
	var updateParts []string

	for i, col := range columns {
		if skipCols[col.Name] {
			continue
		}
		quotedName := "`" + col.Name + "`"
		colNames = append(colNames, quotedName)
		if nulls[i] {
			valParts = append(valParts, "NULL")
		} else {
			valParts = append(valParts, "'"+strings.ReplaceAll(values[i], "'", "''")+"'")
		}
		// Non-PK, non-skip columns go in ON DUPLICATE KEY UPDATE
		if !pkCols[col.Name] {
			updateParts = append(updateParts, fmt.Sprintf("%s=VALUES(%s)", quotedName, quotedName))
		}
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(valParts, ", "))

	if len(updateParts) > 0 {
		sql += " ON DUPLICATE KEY UPDATE " + strings.Join(updateParts, ", ")
	}

	return sql
}

// serializeCompositePK creates a deterministic string key from composite PK values.
// For single-column PKs, returns the value directly. For composite, returns JSON.
func serializeCompositePK(pkCols []string, values []string) string {
	if len(pkCols) == 1 {
		return values[0]
	}
	// JSON object for composite keys.
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		parts[i] = fmt.Sprintf("%q:%q", col, values[i])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// pkValuesFromSingle creates a pkValues map for single-PK tables.
func pkValuesFromSingle(meta schema.TableMeta, pk string) map[string]string {
	if len(meta.PKColumns) == 0 {
		return nil
	}
	return map[string]string{meta.PKColumns[0]: pk}
}

// handleBulkUpdate handles UPDATE without extractable PKs by querying Prod
// for matching rows, hydrating them, then executing on Shadow.
func (w *WriteHandler) handleBulkUpdate(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if len(cl.Tables) == 0 {
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}
	table := cl.Tables[0]
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE: no PK metadata for %s, forwarding to Shadow", w.connID, table)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Extract WHERE clause from the UPDATE using Vitess.
	whereSQL := extractWhereFromSQL(cl.RawSQL)

	// Build PK discovery query.
	pkColList := buildPKColumnList(meta.PKColumns)
	discoverSQL := fmt.Sprintf("SELECT %s FROM `%s`", pkColList, table)
	if whereSQL != "" {
		discoverSQL += " WHERE " + whereSQL
	}
	discoverSQL = capSQL(discoverSQL, w.maxRowsHydrate)

	// Execute on Prod.
	result, err := execMySQLQuery(w.prodConn, discoverSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE PK discovery failed: %v, forwarding to Shadow", w.connID, err)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}
	if result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk UPDATE PK discovery error: %s, forwarding to Shadow", w.connID, result.Error)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Hydrate matching Prod rows into Shadow.
	var affectedPKs []string
	for _, row := range result.RowValues {
		pkValues := make(map[string]string)
		pkVals := make([]string, 0, len(meta.PKColumns))
		for i, col := range meta.PKColumns {
			if i < len(row) {
				pkValues[col] = row[i]
				pkVals = append(pkVals, row[i])
			}
		}
		serialized := serializeCompositePK(meta.PKColumns, pkVals)
		affectedPKs = append(affectedPKs, serialized)

		if w.deltaMap.IsDelta(table, serialized) {
			continue
		}
		if err := w.hydrateRow(table, pkValues); err != nil {
			if w.verbose {
				log.Printf("[conn %d] bulk UPDATE hydration failed for %s: %v", w.connID, table, err)
			}
		}
	}

	// Execute the original UPDATE on Shadow.
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Track all affected PKs.
	for _, pk := range affectedPKs {
		w.deltaMap.Add(table, pk)
	}

	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}

	return nil
}

// handleBulkDelete handles DELETE without extractable PKs by querying Prod
// for matching rows, tombstoning them, executing on Shadow, and returning
// the corrected row count.
func (w *WriteHandler) handleBulkDelete(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if len(cl.Tables) == 0 {
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}
	table := cl.Tables[0]
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE: no PK metadata for %s, forwarding to Shadow", w.connID, table)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Extract WHERE clause from the DELETE.
	whereSQL := extractWhereFromSQL(cl.RawSQL)

	// Build PK discovery query.
	pkColList := buildPKColumnList(meta.PKColumns)
	discoverSQL := fmt.Sprintf("SELECT %s FROM `%s`", pkColList, table)
	if whereSQL != "" {
		discoverSQL += " WHERE " + whereSQL
	}
	discoverSQL = capSQL(discoverSQL, w.maxRowsHydrate)

	// Discover matching Prod rows.
	result, err := execMySQLQuery(w.prodConn, discoverSQL)
	if err != nil {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE PK discovery failed: %v, forwarding to Shadow", w.connID, err)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}
	if result.Error != "" {
		if w.verbose {
			log.Printf("[conn %d] bulk DELETE PK discovery error: %s, forwarding to Shadow", w.connID, result.Error)
		}
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Collect all discovered PKs for FK enforcement and tombstoning.
	var tombstonePKs []string
	prodOnlyCount := 0
	for _, row := range result.RowValues {
		pkVals := make([]string, 0, len(meta.PKColumns))
		for i := range meta.PKColumns {
			if i < len(row) {
				pkVals = append(pkVals, row[i])
			}
		}
		serialized := serializeCompositePK(meta.PKColumns, pkVals)
		tombstonePKs = append(tombstonePKs, serialized)
		if !w.deltaMap.IsDelta(table, serialized) {
			prodOnlyCount++
		}
	}

	// FK enforcement: check RESTRICT/CASCADE before deleting.
	if w.fkEnforcer != nil && len(tombstonePKs) > 0 {
		if err := w.fkEnforcer.EnforceDeleteCascade(table, tombstonePKs); err != nil {
			clientConn.Write(buildFKDeleteErrorPacket(err.Error()))
			return nil
		}
	}

	// Execute DELETE on Shadow, capture response.
	shadowResult, err := execMySQLQuery(w.shadowConn, cl.RawSQL)
	if err != nil {
		return fmt.Errorf("shadow DELETE: %w", err)
	}

	// Parse Shadow affected rows from the OK response.
	shadowCount := shadowResult.AffectedRows

	// Build corrected OK packet with total count.
	totalCount := shadowCount + uint64(prodOnlyCount)
	okPkt := buildOKPacketWithAffectedRows(totalCount)
	if _, err := clientConn.Write(okPkt); err != nil {
		return fmt.Errorf("writing corrected OK: %w", err)
	}

	// Add tombstones.
	for _, pk := range tombstonePKs {
		w.tombstones.Add(table, pk)
		w.deltaMap.Remove(table, pk)
	}

	if err := delta.WriteTombstoneSet(w.moriDir, w.tombstones); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist tombstone set: %v", w.connID, err)
		}
	}
	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}

	return nil
}

// extractWhereFromSQL extracts the WHERE clause text from a SQL statement
// using the Vitess parser.
func extractWhereFromSQL(sql string) string {
	parser, _ := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	stmt, err := parser.Parse(sql)
	if err != nil {
		// Fallback: regex extraction.
		return extractWhereRegex(sql)
	}
	switch s := stmt.(type) {
	case *sqlparser.Update:
		if s.Where != nil {
			return sqlparser.String(s.Where.Expr)
		}
	case *sqlparser.Delete:
		if s.Where != nil {
			return sqlparser.String(s.Where.Expr)
		}
	}
	return ""
}

// extractWhereRegex extracts WHERE clause text using simple string search.
func extractWhereRegex(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "WHERE ")
	if idx < 0 {
		return ""
	}
	rest := sql[idx+6:]
	// Stop at ORDER BY, LIMIT, or end.
	for _, kw := range []string{"ORDER BY", "LIMIT"} {
		if kwIdx := strings.Index(strings.ToUpper(rest), kw); kwIdx >= 0 {
			rest = rest[:kwIdx]
		}
	}
	return strings.TrimSpace(rest)
}

// buildPKColumnList builds a comma-separated backtick-quoted PK column list.
func buildPKColumnList(pkCols []string) string {
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		parts[i] = "`" + col + "`"
	}
	return strings.Join(parts, ", ")
}

// handleUpsert handles INSERT ... ON DUPLICATE KEY UPDATE by pre-hydrating
// conflicting rows from Prod before executing on Shadow.
func (w *WriteHandler) handleUpsert(clientConn net.Conn, rawPkt []byte, cl *core.Classification) error {
	if len(cl.Tables) == 0 {
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}
	table := cl.Tables[0]

	// FK enforcement: check parent rows exist for the INSERT part.
	if w.fkEnforcer != nil {
		if err := w.fkEnforcer.EnforceInsert(table, cl.RawSQL); err != nil {
			clientConn.Write(buildFKErrorPacket(err.Error()))
			return nil
		}
	}
	meta, ok := w.tables[table]
	if !ok || len(meta.PKColumns) == 0 {
		// No PK info — just forward to Shadow.
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Parse the INSERT to extract column list and VALUES using Vitess.
	parser, _ := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	stmt, err := parser.Parse(cl.RawSQL)
	if err != nil {
		// Can't parse — forward to Shadow.
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}
	ins, ok := stmt.(*sqlparser.Insert)
	if !ok || ins.Table == nil {
		return forwardAndRelay(rawPkt, w.shadowConn, clientConn)
	}

	// Extract column names from INSERT.
	var insertCols []string
	for _, col := range ins.Columns {
		insertCols = append(insertCols, col.String())
	}

	// Extract VALUES rows.
	valRows := extractInsertValues(ins)

	// Build unique key columns to check (use PK as the conflict target).
	// Find which insert columns correspond to PK columns.
	pkIndices := make(map[int]string) // index in insertCols -> PK col name
	for i, col := range insertCols {
		colLower := strings.ToLower(strings.Trim(col, "`"))
		for _, pkCol := range meta.PKColumns {
			if strings.EqualFold(colLower, pkCol) {
				pkIndices[i] = pkCol
			}
		}
	}

	// For each VALUES row, check if Prod has a matching row and hydrate if needed.
	for _, row := range valRows {
		pkValues := make(map[string]string)
		allPKsFound := true
		for idx, pkCol := range pkIndices {
			if idx < len(row) {
				pkValues[pkCol] = strings.Trim(row[idx], "'")
			} else {
				allPKsFound = false
				break
			}
		}
		if !allPKsFound || len(pkValues) == 0 {
			continue
		}

		// Build serialized PK for delta check.
		pkVals := make([]string, 0, len(meta.PKColumns))
		for _, col := range meta.PKColumns {
			pkVals = append(pkVals, pkValues[col])
		}
		serialized := serializeCompositePK(meta.PKColumns, pkVals)

		if w.deltaMap.IsDelta(table, serialized) {
			continue // Already in Shadow.
		}

		// Hydrate from Prod.
		if err := w.hydrateRow(table, pkValues); err != nil {
			if w.verbose {
				log.Printf("[conn %d] upsert hydration failed for %s: %v", w.connID, table, err)
			}
		}
	}

	// Execute the original INSERT ... ON DUPLICATE KEY UPDATE on Shadow.
	if err := forwardAndRelay(rawPkt, w.shadowConn, clientConn); err != nil {
		return err
	}

	// Track in delta map.
	for _, row := range valRows {
		pkVals := make([]string, 0, len(meta.PKColumns))
		for _, pkCol := range meta.PKColumns {
			for i, col := range insertCols {
				if strings.EqualFold(strings.Trim(col, "`"), pkCol) && i < len(row) {
					pkVals = append(pkVals, strings.Trim(row[i], "'"))
				}
			}
		}
		if len(pkVals) == len(meta.PKColumns) {
			serialized := serializeCompositePK(meta.PKColumns, pkVals)
			w.deltaMap.Add(table, serialized)
		}
	}
	w.deltaMap.MarkInserted(table)

	if err := delta.WriteDeltaMap(w.moriDir, w.deltaMap); err != nil {
		if w.verbose {
			log.Printf("[conn %d] failed to persist delta map: %v", w.connID, err)
		}
	}

	return nil
}

// extractInsertValues extracts the VALUES rows from an INSERT statement.
func extractInsertValues(ins *sqlparser.Insert) [][]string {
	var rows [][]string
	switch v := ins.Rows.(type) {
	case sqlparser.Values:
		for _, row := range v {
			var vals []string
			for _, expr := range row {
				vals = append(vals, sqlparser.String(expr))
			}
			rows = append(rows, vals)
		}
	}
	return rows
}

// capSQL appends a LIMIT clause to a SQL query if maxRows > 0 and the outer
// query doesn't already have one. Subquery LIMIT clauses are ignored.
func capSQL(sql string, maxRows int) string {
	if maxRows <= 0 {
		return sql
	}
	if hasOuterLimit(sql) {
		return sql
	}
	return fmt.Sprintf("%s LIMIT %d", sql, maxRows)
}

// hasOuterLimit reports whether sql contains a LIMIT keyword at the outermost
// level (not inside parenthesized subqueries). It handles any whitespace
// (spaces, tabs, newlines) around the keyword.
func hasOuterLimit(sql string) bool {
	upper := strings.ToUpper(sql)
	depth := 0
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && i+5 <= len(upper) && upper[i:i+5] == "LIMIT" {
				before := i == 0 || upper[i-1] == ' ' || upper[i-1] == '\t' || upper[i-1] == '\n' || upper[i-1] == '\r' || upper[i-1] == ')'
				after := i+5 == len(upper) || upper[i+5] == ' ' || upper[i+5] == '\t' || upper[i+5] == '\n' || upper[i+5] == '\r'
				if before && after {
					return true
				}
			}
		}
	}
	return false
}
