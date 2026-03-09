package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/mysql/schema"
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
	fkDiscovered   map[string]bool // tracks tables where FK discovery from Prod has been attempted
}

// CheckParentExists verifies that a parent row exists for the given FK
// constraint and child column values.
// Returns nil if the parent row exists, or an error describing the violation.
func (fke *FKEnforcer) CheckParentExists(fk coreSchema.ForeignKey, childValues map[string]string) error {
	var whereParts []string
	for i, parentCol := range fk.ParentColumns {
		if i >= len(fk.ChildColumns) {
			break
		}
		childCol := fk.ChildColumns[i]
		val, ok := childValues[childCol]
		if !ok {
			return nil // Value not provided — skip enforcement.
		}
		// NULL values satisfy FK constraints.
		if strings.EqualFold(val, "NULL") {
			return nil
		}
		whereParts = append(whereParts, fmt.Sprintf("`%s` = %s", parentCol, quoteMySQLLiteral(val)))
	}

	if len(whereParts) == 0 {
		return nil
	}

	parentTable := fk.ParentTable
	whereClause := strings.Join(whereParts, " AND ")

	// Fast path: check delta/tombstone maps for single-column PK FKs.
	if meta, ok := fke.tables[parentTable]; ok && len(meta.PKColumns) == 1 {
		pkCol := meta.PKColumns[0]
		if len(fk.ParentColumns) == 1 && fk.ParentColumns[0] == pkCol {
			val := childValues[fk.ChildColumns[0]]
			cleanVal := strings.Trim(val, "'")
			if fke.tombstones.IsTombstoned(parentTable, cleanVal) {
				return fmt.Errorf("Cannot add or update a child row: a foreign key constraint fails "+
					"(`%s`, FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`))",
					fk.ChildTable, fk.ChildColumns[0], parentTable, pkCol)
			}
			if fke.deltaMap.IsDelta(parentTable, cleanVal) {
				return nil // Row exists in Shadow delta.
			}
		}
	}

	// Query Shadow first.
	shadowSQL := fmt.Sprintf("SELECT 1 FROM `%s` WHERE %s LIMIT 1", parentTable, whereClause)
	shadowResult, err := execMySQLQuery(fke.shadowConn, shadowSQL)
	if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
		return nil
	}

	// Query Prod.
	prodSQL := fmt.Sprintf("SELECT 1 FROM `%s` WHERE %s LIMIT 1", parentTable, whereClause)
	prodResult, err := execMySQLQuery(fke.prodConn, prodSQL)
	if err != nil {
		if fke.verbose {
			log.Printf("[conn %d] FK check: prod query failed for %s: %v", fke.connID, parentTable, err)
		}
		return fmt.Errorf("Cannot add or update a child row: foreign key check failed — "+
			"parent lookup in `%s` failed", parentTable)
	}
	if prodResult.Error != "" {
		if fke.verbose {
			log.Printf("[conn %d] FK check: prod query error for %s: %s", fke.connID, parentTable, prodResult.Error)
		}
		return fmt.Errorf("Cannot add or update a child row: foreign key check failed — "+
			"parent lookup in `%s` failed", parentTable)
	}

	if len(prodResult.RowValues) > 0 {
		return nil // Parent exists in Prod.
	}

	// Parent not found.
	childColStr := strings.Join(fk.ChildColumns, ", ")
	valParts := make([]string, len(fk.ChildColumns))
	for i, col := range fk.ChildColumns {
		valParts[i] = childValues[col]
	}
	valStr := strings.Join(valParts, ", ")

	return fmt.Errorf("Cannot add or update a child row: a foreign key constraint fails "+
		"(`%s`, FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)) — "+
		"key (%s)=(%s) is not present in table `%s`",
		fk.ChildTable, childColStr, fk.ParentTable,
		strings.Join(fk.ParentColumns, "`, `"),
		childColStr, valStr, fk.ParentTable)
}

// discoverFKsFromProd queries Prod's INFORMATION_SCHEMA to discover FK constraints
// for a table. Discovered FKs are cached in the schema registry.
func (fke *FKEnforcer) discoverFKsFromProd(table string) []coreSchema.ForeignKey {
	query := fmt.Sprintf(`SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME,
		kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
		rc.DELETE_RULE, rc.UPDATE_RULE
	FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
		ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
	WHERE kcu.TABLE_SCHEMA = DATABASE()
		AND kcu.TABLE_NAME = '%s'
		AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
	ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`,
		strings.ReplaceAll(table, "'", "''"))

	result, err := execMySQLQuery(fke.prodConn, query)
	if err != nil || (result != nil && result.Error != "") {
		if fke.verbose {
			errMsg := ""
			if result != nil {
				errMsg = result.Error
			}
			log.Printf("[conn %d] FK discovery: failed for %s: %v %s", fke.connID, table, err, errMsg)
		}
		return nil
	}

	type fkData struct {
		constraintName string
		childColumns   []string
		parentTable    string
		parentColumns  []string
		onDelete       string
		onUpdate       string
	}
	fkMap := make(map[string]*fkData)
	var fkOrder []string

	for i, row := range result.RowValues {
		if len(row) < 6 {
			continue
		}
		if result.RowNulls[i][0] {
			continue
		}
		constraintName := row[0]
		childCol := row[1]
		parentTable := row[2]
		parentCol := row[3]
		deleteRule := row[4]
		updateRule := row[5]

		fd, ok := fkMap[constraintName]
		if !ok {
			fd = &fkData{
				constraintName: constraintName,
				parentTable:    parentTable,
				onDelete:       deleteRule,
				onUpdate:       updateRule,
			}
			fkMap[constraintName] = fd
			fkOrder = append(fkOrder, constraintName)
		}
		fd.childColumns = append(fd.childColumns, childCol)
		fd.parentColumns = append(fd.parentColumns, parentCol)
	}

	var fks []coreSchema.ForeignKey
	for _, name := range fkOrder {
		fd := fkMap[name]
		fk := coreSchema.ForeignKey{
			ConstraintName: fd.constraintName,
			ChildTable:     table,
			ChildColumns:   fd.childColumns,
			ParentTable:    fd.parentTable,
			ParentColumns:  fd.parentColumns,
			OnDelete:       fd.onDelete,
			OnUpdate:       fd.onUpdate,
		}
		fks = append(fks, fk)
		fke.schemaRegistry.RecordForeignKey(table, fk)
	}

	if fke.verbose && len(fks) > 0 {
		log.Printf("[conn %d] FK discovery: found %d FK constraints for %s from Prod",
			fke.connID, len(fks), table)
	}

	return fks
}

// getFKsForTable returns FKs where the given table is the child, with lazy
// discovery from Prod on first access.
func (fke *FKEnforcer) getFKsForTable(table string) []coreSchema.ForeignKey {
	fks := fke.schemaRegistry.GetForeignKeys(table)
	if len(fks) == 0 {
		if _, known := fke.tables[table]; known {
			if fke.fkDiscovered == nil {
				fke.fkDiscovered = make(map[string]bool)
			}
			if !fke.fkDiscovered[table] {
				fke.fkDiscovered[table] = true
				fks = fke.discoverFKsFromProd(table)
			}
		}
	}
	return fks
}

// EnforceInsert checks FK constraints for an INSERT statement.
func (fke *FKEnforcer) EnforceInsert(table string, rawSQL string) error {
	fks := fke.getFKsForTable(table)
	if len(fks) == 0 {
		return nil
	}

	rows, insertCols, err := parseInsertValuesMySQL(rawSQL)
	if err != nil {
		if fke.verbose {
			log.Printf("[conn %d] FK insert: parse failed: %v", fke.connID, err)
		}
		return nil // Fail open on parse failure.
	}

	for _, fk := range fks {
		for _, row := range rows {
			childValues := make(map[string]string)
			for _, childCol := range fk.ChildColumns {
				for i, col := range insertCols {
					if strings.EqualFold(strings.Trim(col, "`"), childCol) && i < len(row) {
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
// Only checks FK columns that are being modified in the SET clause.
func (fke *FKEnforcer) EnforceUpdate(table string, rawSQL string) error {
	fks := fke.getFKsForTable(table)
	if len(fks) == 0 {
		return nil
	}

	setValues, err := parseUpdateSetValuesMySQL(rawSQL)
	if err != nil {
		if fke.verbose {
			log.Printf("[conn %d] FK update: parse failed: %v", fke.connID, err)
		}
		return nil // Fail open on parse failure.
	}

	for _, fk := range fks {
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

// EnforceDeleteCascade handles referential actions when a parent row is deleted.
// CASCADE: tombstones child rows. RESTRICT/NO ACTION: rejects if children exist.
// SET NULL: sets child FK column to NULL.
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
		// Only handle single-column FKs referencing the PK.
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

			childPKs := fke.findChildRows(childTable, childCol, childPKCol, deletedPKs)
			if len(childPKs) == 0 {
				continue
			}

			for _, cpk := range childPKs {
				fke.tombstones.Add(childTable, cpk)
				fke.deltaMap.Remove(childTable, cpk)
			}

			fke.deleteChildRowsFromShadow(childTable, childCol, deletedPKs)

			if fke.verbose {
				log.Printf("[conn %d] FK CASCADE: tombstoned %d rows in %s for deleted %s PKs",
					fke.connID, len(childPKs), childTable, table)
			}
			fke.logger.Event(fke.connID, "fk_cascade",
				fmt.Sprintf("CASCADE DELETE: %d rows in %s", len(childPKs), childTable))

			// Recurse for transitive cascades.
			if err := fke.EnforceDeleteCascade(childTable, childPKs); err != nil {
				return err
			}

		case "RESTRICT", "NO ACTION":
			if fke.childRowsExist(childTable, childCol, deletedPKs) {
				return fmt.Errorf("Cannot delete or update a parent row: a foreign key constraint fails "+
					"(`%s`, FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`))",
					childTable, childCol, table, pkCol)
			}

		case "SET NULL":
			fke.setChildColumnNull(childTable, childCol, deletedPKs)

		case "SET DEFAULT":
			if fke.verbose {
				log.Printf("[conn %d] FK SET DEFAULT not implemented for %s.%s", fke.connID, childTable, childCol)
			}
		}
	}

	return nil
}

// findChildRows queries both Shadow and Prod for child rows referencing
// the given parent PK values. Returns the child table's PK values.
func (fke *FKEnforcer) findChildRows(childTable, childCol, childPKCol string, parentPKs []string) []string {
	if len(parentPKs) == 0 {
		return nil
	}

	inClause := buildMySQLINClause(parentPKs)
	selectSQL := fmt.Sprintf("SELECT `%s` FROM `%s` WHERE `%s` IN (%s)",
		childPKCol, childTable, childCol, inClause)

	seen := make(map[string]bool)
	var result []string

	// Query Shadow first.
	shadowResult, err := execMySQLQuery(fke.shadowConn, selectSQL)
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

	// Query Prod.
	prodResult, err := execMySQLQuery(fke.prodConn, selectSQL)
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

// childRowsExist checks if any child rows reference the given parent PKs.
func (fke *FKEnforcer) childRowsExist(childTable, childCol string, parentPKs []string) bool {
	if len(parentPKs) == 0 {
		return false
	}

	inClause := buildMySQLINClause(parentPKs)
	selectSQL := fmt.Sprintf("SELECT 1 FROM `%s` WHERE `%s` IN (%s) LIMIT 1",
		childTable, childCol, inClause)

	// Check Shadow.
	shadowResult, err := execMySQLQuery(fke.shadowConn, selectSQL)
	if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
		return true
	}

	// Check Prod.
	prodResult, err := execMySQLQuery(fke.prodConn, selectSQL)
	if err == nil && prodResult.Error == "" && len(prodResult.RowValues) > 0 {
		return true
	}

	return false
}

// deleteChildRowsFromShadow deletes child rows from Shadow for CASCADE.
func (fke *FKEnforcer) deleteChildRowsFromShadow(childTable, childCol string, parentPKs []string) {
	if len(parentPKs) == 0 {
		return
	}

	inClause := buildMySQLINClause(parentPKs)
	deleteSQL := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` IN (%s)",
		childTable, childCol, inClause)

	result, err := execMySQLQuery(fke.shadowConn, deleteSQL)
	if err != nil && fke.verbose {
		log.Printf("[conn %d] FK CASCADE: shadow DELETE failed for %s: %v", fke.connID, childTable, err)
	}
	if result != nil && result.Error != "" && fke.verbose {
		log.Printf("[conn %d] FK CASCADE: shadow DELETE error for %s: %s", fke.connID, childTable, result.Error)
	}
}

// setChildColumnNull sets FK column to NULL in child rows for SET NULL action.
func (fke *FKEnforcer) setChildColumnNull(childTable, childCol string, parentPKs []string) {
	if len(parentPKs) == 0 {
		return
	}

	inClause := buildMySQLINClause(parentPKs)
	updateSQL := fmt.Sprintf("UPDATE `%s` SET `%s` = NULL WHERE `%s` IN (%s)",
		childTable, childCol, childCol, inClause)

	result, err := execMySQLQuery(fke.shadowConn, updateSQL)
	if err != nil && fke.verbose {
		log.Printf("[conn %d] FK SET NULL: shadow UPDATE failed for %s: %v", fke.connID, childTable, err)
	}
	if result != nil && result.Error != "" && fke.verbose {
		log.Printf("[conn %d] FK SET NULL: shadow UPDATE error for %s: %s", fke.connID, childTable, result.Error)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildMySQLINClause builds a SQL IN clause from PK values.
func buildMySQLINClause(pks []string) string {
	parts := make([]string, len(pks))
	for i, pk := range pks {
		parts[i] = quoteMySQLLiteral(pk)
	}
	return strings.Join(parts, ", ")
}

// quoteMySQLLiteral quotes a string value for MySQL SQL.
func quoteMySQLLiteral(val string) string {
	val = strings.Trim(val, "'")
	return "'" + strings.ReplaceAll(val, "'", "''") + "'"
}

// parseInsertValuesMySQL parses an INSERT using Vitess sqlparser.
func parseInsertValuesMySQL(rawSQL string) (rows [][]string, columns []string, err error) {
	parser, _ := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	stmt, parseErr := parser.Parse(rawSQL)
	if parseErr != nil {
		return nil, nil, fmt.Errorf("parse: %w", parseErr)
	}
	ins, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return nil, nil, fmt.Errorf("not an INSERT")
	}
	for _, col := range ins.Columns {
		columns = append(columns, col.String())
	}
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
	return rows, columns, nil
}

// parseUpdateSetValuesMySQL parses an UPDATE using Vitess sqlparser to
// extract the SET clause as a map of column name → value.
func parseUpdateSetValuesMySQL(rawSQL string) (map[string]string, error) {
	parser, _ := sqlparser.New(sqlparser.Options{MySQLServerVersion: "8.0.30"})
	stmt, parseErr := parser.Parse(rawSQL)
	if parseErr != nil {
		return nil, fmt.Errorf("parse: %w", parseErr)
	}
	upd, ok := stmt.(*sqlparser.Update)
	if !ok {
		return nil, fmt.Errorf("not an UPDATE")
	}
	values := make(map[string]string)
	for _, expr := range upd.Exprs {
		colName := expr.Name.Name.String()
		val := sqlparser.String(expr.Expr)
		values[colName] = val
	}
	return values, nil
}

// buildFKErrorPacket constructs a MySQL ERR packet for a FK violation.
// MySQL error code 1452 (ER_NO_REFERENCED_ROW_2), SQLSTATE 23000.
func buildFKErrorPacket(message string) []byte {
	return buildERRPacket(1, 1452, "23000", message)
}

// buildFKDeleteErrorPacket constructs a MySQL ERR packet for a FK delete violation.
// MySQL error code 1451 (ER_ROW_IS_REFERENCED_2), SQLSTATE 23000.
func buildFKDeleteErrorPacket(message string) []byte {
	return buildERRPacket(1, 1451, "23000", message)
}
