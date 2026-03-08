package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// FKEnforcer handles foreign key validation at the proxy layer.
// Because FK constraints are stripped from Shadow DDL, the proxy must enforce
// referential integrity checks before writes are executed.
type FKEnforcer struct {
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

// buildFKErrorResponse constructs a pgwire ErrorResponse + ReadyForQuery for
// a foreign key violation (SQLSTATE 23503).
func buildFKErrorResponse(message string) []byte {
	var errPayload []byte
	errPayload = append(errPayload, 'S')
	errPayload = append(errPayload, []byte("ERROR")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'V') // severity (non-localized)
	errPayload = append(errPayload, []byte("ERROR")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'C')
	errPayload = append(errPayload, []byte("23503")...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 'M')
	errPayload = append(errPayload, []byte(message)...)
	errPayload = append(errPayload, 0)
	errPayload = append(errPayload, 0) // terminator

	var result []byte
	result = append(result, buildPGMsg('E', errPayload)...)
	result = append(result, buildReadyForQueryMsg()...)
	return result
}

// sendFKError sends a foreign key violation error to the client connection.
func sendFKError(clientConn net.Conn, message string) error {
	_, err := clientConn.Write(buildFKErrorResponse(message))
	return err
}

// CheckParentExists verifies that a parent row exists for the given FK
// constraint and child column values. It checks shadow delta first, then
// tombstones, then falls back to querying prod.
//
// Returns nil if the parent row exists, or an error describing the violation.
func (fke *FKEnforcer) CheckParentExists(fk coreSchema.ForeignKey, childValues map[string]string) error {
	// Build the WHERE clause for the parent lookup.
	var whereParts []string
	for i, parentCol := range fk.ParentColumns {
		if i >= len(fk.ChildColumns) {
			break
		}
		childCol := fk.ChildColumns[i]
		val, ok := childValues[childCol]
		if !ok {
			// Value not provided for this FK column — skip enforcement.
			return nil
		}
		// NULL values satisfy FK constraints (NULL != any value).
		if strings.EqualFold(val, "NULL") {
			return nil
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", quoteIdent(parentCol), quoteLiteral(val)))
	}

	if len(whereParts) == 0 {
		return nil
	}

	parentTable := fk.ParentTable
	whereClause := strings.Join(whereParts, " AND ")

	// 1. Check if parent row exists in shadow delta.
	// If the parent table has PK metadata, we can check the delta map directly.
	if meta, ok := fke.tables[parentTable]; ok && len(meta.PKColumns) == 1 {
		pkCol := meta.PKColumns[0]
		// If the FK references the PK column, check delta and tombstone maps.
		if len(fk.ParentColumns) == 1 && fk.ParentColumns[0] == pkCol {
			val := childValues[fk.ChildColumns[0]]
			if fke.tombstones.IsTombstoned(parentTable, val) {
				return fmt.Errorf("insert or update on table %q violates foreign key constraint: "+
					"key (%s)=(%s) is not present in table %q",
					fk.ChildTable, fk.ChildColumns[0], val, parentTable)
			}
			if fke.deltaMap.IsDelta(parentTable, val) {
				// Row exists in shadow delta — parent exists.
				return nil
			}
		}
	}

	// 2. Query shadow for the parent row first (it may have been inserted in this session).
	shadowSQL := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1",
		quoteIdent(parentTable), whereClause)
	shadowResult, err := execQuery(fke.shadowConn, shadowSQL)
	if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
		return nil // Found in shadow.
	}

	// 3. Query prod for the parent row.
	prodSQL := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1",
		quoteIdent(parentTable), whereClause)
	prodResult, err := execQuery(fke.prodConn, prodSQL)
	if err != nil {
		// Best-effort: if prod query fails, log warning and allow the write.
		if fke.verbose {
			log.Printf("[conn %d] FK check: prod query failed for %s: %v", fke.connID, parentTable, err)
		}
		return nil
	}
	if prodResult.Error != "" {
		if fke.verbose {
			log.Printf("[conn %d] FK check: prod query error for %s: %s", fke.connID, parentTable, prodResult.Error)
		}
		return nil // Best-effort.
	}

	if len(prodResult.RowValues) > 0 {
		return nil // Parent row exists in prod.
	}

	// Parent not found anywhere.
	childColStr := strings.Join(fk.ChildColumns, ", ")
	valParts := make([]string, len(fk.ChildColumns))
	for i, col := range fk.ChildColumns {
		valParts[i] = childValues[col]
	}
	valStr := strings.Join(valParts, ", ")

	return fmt.Errorf("insert or update on table %q violates foreign key constraint: "+
		"key (%s)=(%s) is not present in table %q",
		fk.ChildTable, childColStr, valStr, parentTable)
}

// EnforceInsert checks FK constraints for an INSERT statement.
// Returns an error if any FK violation is detected.
func (fke *FKEnforcer) EnforceInsert(table string, rawSQL string) error {
	fks := fke.schemaRegistry.GetForeignKeys(table)
	if len(fks) == 0 {
		return nil
	}

	// Parse the INSERT to extract column names and values.
	rows, insertCols, err := parseInsertValues(rawSQL)
	if err != nil {
		if fke.verbose {
			log.Printf("[conn %d] FK insert: parse failed: %v — allowing write", fke.connID, err)
		}
		return nil // Best-effort.
	}

	for _, fk := range fks {
		for _, row := range rows {
			// Build a map of FK child column → value for this row.
			childValues := make(map[string]string)
			for _, childCol := range fk.ChildColumns {
				for i, col := range insertCols {
					if col == childCol && i < len(row) {
						childValues[childCol] = row[i]
					}
				}
			}

			if len(childValues) == 0 {
				continue
			}

			if err := fke.CheckParentExists(fk, childValues); err != nil {
				return err
			}
		}
	}

	return nil
}

// EnforceUpdate checks FK constraints for an UPDATE statement.
// Only checks FK columns that are being modified (appear in SET clause).
func (fke *FKEnforcer) EnforceUpdate(table string, rawSQL string) error {
	fks := fke.schemaRegistry.GetForeignKeys(table)
	if len(fks) == 0 {
		return nil
	}

	// Parse the UPDATE to extract SET clause column=value pairs.
	setValues, err := parseUpdateSetValues(rawSQL)
	if err != nil {
		if fke.verbose {
			log.Printf("[conn %d] FK update: parse failed: %v — allowing write", fke.connID, err)
		}
		return nil // Best-effort.
	}

	for _, fk := range fks {
		// Only check this FK if at least one of its child columns is being updated.
		childValues := make(map[string]string)
		relevant := false
		for _, childCol := range fk.ChildColumns {
			if val, ok := setValues[childCol]; ok {
				childValues[childCol] = val
				relevant = true
			}
		}
		if !relevant {
			continue
		}

		if err := fke.CheckParentExists(fk, childValues); err != nil {
			return err
		}
	}

	return nil
}

// EnforceDeleteCascade handles cascade/restrict logic when a parent row is deleted.
// For CASCADE: tombstones child rows referencing the deleted parent.
// For RESTRICT/NO ACTION: returns an error if child rows exist.
func (fke *FKEnforcer) EnforceDeleteCascade(table string, deletedPKs []string) error {
	refFKs := fke.schemaRegistry.GetReferencingFKs(table)
	if len(refFKs) == 0 {
		return nil
	}

	meta, hasMeta := fke.tables[table]
	if !hasMeta || len(meta.PKColumns) == 0 {
		return nil
	}
	pkCol := meta.PKColumns[0]

	for _, fk := range refFKs {
		// Only handle single-column FKs that reference the PK for now.
		if len(fk.ParentColumns) != 1 || fk.ParentColumns[0] != pkCol {
			continue
		}
		if len(fk.ChildColumns) != 1 {
			continue
		}

		childTable := fk.ChildTable
		childCol := fk.ChildColumns[0]
		childMeta, hasChildMeta := fke.tables[childTable]

		switch fk.OnDelete {
		case "CASCADE":
			if !hasChildMeta || len(childMeta.PKColumns) == 0 {
				continue
			}
			childPKCol := childMeta.PKColumns[0]

			// Find child rows referencing the deleted parent PKs.
			childPKs := fke.findChildRows(childTable, childCol, childPKCol, deletedPKs)
			if len(childPKs) == 0 {
				continue
			}

			// Tombstone each child row.
			for _, cpk := range childPKs {
				fke.tombstones.Add(childTable, cpk)
				fke.deltaMap.Remove(childTable, cpk)
			}

			// Delete from shadow as well.
			fke.deleteChildRowsFromShadow(childTable, childCol, deletedPKs)

			if fke.verbose {
				log.Printf("[conn %d] FK CASCADE: tombstoned %d rows in %s for deleted %s PKs",
					fke.connID, len(childPKs), childTable, table)
			}
			fke.logger.Event(fke.connID, "fk_cascade",
				fmt.Sprintf("CASCADE DELETE: %d rows in %s", len(childPKs), childTable))

			// Recurse: the cascaded deletes might trigger further cascades.
			if err := fke.EnforceDeleteCascade(childTable, childPKs); err != nil {
				return err
			}

		case "RESTRICT", "NO ACTION":
			// Check if any child rows exist referencing the deleted PKs.
			if fke.childRowsExist(childTable, childCol, deletedPKs) {
				return fmt.Errorf("update or delete on table %q violates foreign key constraint on table %q: "+
					"key is still referenced from table %q",
					table, childTable, childTable)
			}

		case "SET NULL":
			// Set the FK column to NULL in child rows.
			fke.setChildColumnNull(childTable, childCol, deletedPKs)

		case "SET DEFAULT":
			// Not commonly used — log and skip for now.
			if fke.verbose {
				log.Printf("[conn %d] FK SET DEFAULT not implemented for %s.%s", fke.connID, childTable, childCol)
			}
		}
	}

	return nil
}

// findChildRows queries both shadow and prod for child rows that reference
// any of the given parent PK values. Returns the child table's PK values.
func (fke *FKEnforcer) findChildRows(childTable, childCol, childPKCol string, parentPKs []string) []string {
	if len(parentPKs) == 0 {
		return nil
	}

	inClause := buildINClause(parentPKs)
	selectSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		quoteIdent(childPKCol), quoteIdent(childTable), quoteIdent(childCol), inClause)

	seen := make(map[string]bool)
	var result []string

	// Query shadow first.
	shadowResult, err := execQuery(fke.shadowConn, selectSQL)
	if err == nil && shadowResult.Error == "" {
		for i, row := range shadowResult.RowValues {
			if len(row) > 0 && !shadowResult.RowNulls[i][0] {
				pk := row[0]
				if !seen[pk] {
					seen[pk] = true
					result = append(result, pk)
				}
			}
		}
	}

	// Query prod.
	prodResult, err := execQuery(fke.prodConn, selectSQL)
	if err == nil && prodResult.Error == "" {
		for i, row := range prodResult.RowValues {
			if len(row) > 0 && !prodResult.RowNulls[i][0] {
				pk := row[0]
				if !seen[pk] && !fke.tombstones.IsTombstoned(childTable, pk) {
					seen[pk] = true
					result = append(result, pk)
				}
			}
		}
	}

	return result
}

// childRowsExist checks if any child rows reference any of the given parent PKs.
func (fke *FKEnforcer) childRowsExist(childTable, childCol string, parentPKs []string) bool {
	if len(parentPKs) == 0 {
		return false
	}

	inClause := buildINClause(parentPKs)
	selectSQL := fmt.Sprintf("SELECT 1 FROM %s WHERE %s IN (%s) LIMIT 1",
		quoteIdent(childTable), quoteIdent(childCol), inClause)

	// Check shadow first.
	shadowResult, err := execQuery(fke.shadowConn, selectSQL)
	if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
		return true
	}

	// Check prod.
	prodResult, err := execQuery(fke.prodConn, selectSQL)
	if err == nil && prodResult.Error == "" && len(prodResult.RowValues) > 0 {
		// Make sure the found row isn't tombstoned. Since we used LIMIT 1,
		// a full check would require more queries. For best-effort, this is sufficient.
		return true
	}

	return false
}

// deleteChildRowsFromShadow deletes child rows from shadow that reference
// the given parent PKs. This supports CASCADE deletes.
func (fke *FKEnforcer) deleteChildRowsFromShadow(childTable, childCol string, parentPKs []string) {
	if len(parentPKs) == 0 {
		return
	}

	inClause := buildINClause(parentPKs)
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		quoteIdent(childTable), quoteIdent(childCol), inClause)

	result, err := execQuery(fke.shadowConn, deleteSQL)
	if err != nil && fke.verbose {
		log.Printf("[conn %d] FK CASCADE: shadow DELETE failed for %s: %v", fke.connID, childTable, err)
	}
	if result != nil && result.Error != "" && fke.verbose {
		log.Printf("[conn %d] FK CASCADE: shadow DELETE error for %s: %s", fke.connID, childTable, result.Error)
	}
}

// setChildColumnNull sets the FK column to NULL in child rows that reference
// the given parent PKs. This supports SET NULL deletes.
func (fke *FKEnforcer) setChildColumnNull(childTable, childCol string, parentPKs []string) {
	if len(parentPKs) == 0 {
		return
	}

	inClause := buildINClause(parentPKs)
	updateSQL := fmt.Sprintf("UPDATE %s SET %s = NULL WHERE %s IN (%s)",
		quoteIdent(childTable), quoteIdent(childCol), quoteIdent(childCol), inClause)

	result, err := execQuery(fke.shadowConn, updateSQL)
	if err != nil && fke.verbose {
		log.Printf("[conn %d] FK SET NULL: shadow UPDATE failed for %s: %v", fke.connID, childTable, err)
	}
	if result != nil && result.Error != "" && fke.verbose {
		log.Printf("[conn %d] FK SET NULL: shadow UPDATE error for %s: %s", fke.connID, childTable, result.Error)
	}
}

// buildINClause builds a SQL IN clause values string from a list of PK values.
func buildINClause(pks []string) string {
	parts := make([]string, len(pks))
	for i, pk := range pks {
		parts[i] = quoteLiteral(pk)
	}
	return strings.Join(parts, ", ")
}

// parseInsertValues parses an INSERT statement to extract the column names
// and the values for each row. Returns the rows (each row is a slice of
// string values) and the column names.
func parseInsertValues(rawSQL string) (rows [][]string, columns []string, err error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse: %w", err)
	}

	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return nil, nil, fmt.Errorf("no statements")
	}

	ins := stmts[0].GetStmt().GetInsertStmt()
	if ins == nil {
		return nil, nil, fmt.Errorf("not an INSERT")
	}

	// Extract column names.
	for _, col := range ins.GetCols() {
		if rt := col.GetResTarget(); rt != nil {
			columns = append(columns, rt.GetName())
		}
	}

	// Extract VALUES rows.
	selStmt := ins.GetSelectStmt()
	if selStmt == nil {
		return nil, columns, nil
	}

	sel := selStmt.GetSelectStmt()
	if sel == nil {
		return nil, columns, nil
	}

	for _, valList := range sel.GetValuesLists() {
		list := valList.GetList()
		if list == nil {
			continue
		}

		var row []string
		for _, item := range list.GetItems() {
			val, ok := extractConstValue(item)
			if ok {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		rows = append(rows, row)
	}

	return rows, columns, nil
}

// parseUpdateSetValues parses an UPDATE statement and extracts the SET clause
// as a map of column name → value.
func parseUpdateSetValues(rawSQL string) (map[string]string, error) {
	parseResult, err := pg_query.Parse(rawSQL)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	stmts := parseResult.GetStmts()
	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements")
	}

	upd := stmts[0].GetStmt().GetUpdateStmt()
	if upd == nil {
		return nil, fmt.Errorf("not an UPDATE")
	}

	values := make(map[string]string)
	for _, target := range upd.GetTargetList() {
		rt := target.GetResTarget()
		if rt == nil {
			continue
		}
		colName := rt.GetName()
		val, ok := extractConstValue(rt.GetVal())
		if ok {
			values[colName] = val
		}
	}

	return values, nil
}
