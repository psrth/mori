package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
	"github.com/mori-dev/mori/internal/logging"
)

// FKEnforcer handles foreign key validation at the proxy layer for MSSQL.
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

// EnforceInsert checks FK constraints for an INSERT statement.
// Returns an error if any FK violation is detected.
func (fke *FKEnforcer) EnforceInsert(table string, rawSQL string) error {
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
	if len(fks) == 0 {
		return nil
	}

	// Parse the INSERT to extract column names and values (regex-based for T-SQL).
	rows, insertCols := parseInsertValuesMSSQL(rawSQL)
	if len(rows) == 0 || len(insertCols) == 0 {
		return nil // Can't parse — skip enforcement.
	}

	for _, fk := range fks {
		for _, row := range rows {
			childValues := make(map[string]string)
			for _, childCol := range fk.ChildColumns {
				for i, col := range insertCols {
					if strings.EqualFold(col, childCol) && i < len(row) {
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
	if len(fks) == 0 {
		return nil
	}

	setValues := parseUpdateSetValuesMSSQL(rawSQL)
	if len(setValues) == 0 {
		return nil
	}

	for _, fk := range fks {
		childValues := make(map[string]string)
		relevant := false
		for _, childCol := range fk.ChildColumns {
			if val, ok := setValues[strings.ToLower(childCol)]; ok {
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

// CheckParentExists verifies that a parent row exists for the given FK
// constraint and child column values. Checks: tombstones → delta map → Shadow → Prod.
func (fke *FKEnforcer) CheckParentExists(fk coreSchema.ForeignKey, childValues map[string]string) error {
	var whereParts []string
	for i, parentCol := range fk.ParentColumns {
		if i >= len(fk.ChildColumns) {
			break
		}
		childCol := fk.ChildColumns[i]
		val, ok := childValues[childCol]
		if !ok {
			return nil
		}
		if strings.EqualFold(val, "NULL") {
			return nil
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", quoteIdentMSSQL(parentCol), quoteLiteralMSSQL(val)))
	}
	if len(whereParts) == 0 {
		return nil
	}

	parentTable := fk.ParentTable
	whereClause := strings.Join(whereParts, " AND ")

	// 1. Check delta map and tombstones for single-column PK references.
	if meta, ok := fke.tables[parentTable]; ok && len(meta.PKColumns) == 1 {
		pkCol := meta.PKColumns[0]
		if len(fk.ParentColumns) == 1 && strings.EqualFold(fk.ParentColumns[0], pkCol) {
			val := childValues[fk.ChildColumns[0]]
			if fke.tombstones.IsTombstoned(parentTable, val) {
				return fmt.Errorf("insert or update on table %q violates foreign key constraint: "+
					"key (%s)=(%s) is not present in table %q",
					fk.ChildTable, fk.ChildColumns[0], val, parentTable)
			}
			if fke.deltaMap.IsDelta(parentTable, val) {
				return nil
			}
		}
	}

	// 2. Query Shadow.
	shadowSQL := fmt.Sprintf("SELECT TOP 1 1 FROM %s WHERE %s",
		quoteIdentMSSQL(parentTable), whereClause)
	shadowResult, err := execTDSQuery(fke.shadowConn, shadowSQL)
	if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
		return nil
	}

	// 3. Query Prod.
	prodSQL := fmt.Sprintf("SELECT TOP 1 1 FROM %s WHERE %s",
		quoteIdentMSSQL(parentTable), whereClause)
	prodResult, err := execTDSQuery(fke.prodConn, prodSQL)
	if err != nil {
		if fke.verbose {
			log.Printf("[conn %d] FK check: prod query failed for %s: %v", fke.connID, parentTable, err)
		}
		return fmt.Errorf("insert or update on table %q: cannot verify foreign key constraint — "+
			"parent lookup in %q failed", fk.ChildTable, parentTable)
	}
	if prodResult.Error != "" {
		return fmt.Errorf("insert or update on table %q: cannot verify foreign key constraint — "+
			"parent lookup in %q failed: %s", fk.ChildTable, parentTable, prodResult.Error)
	}
	if len(prodResult.RowValues) > 0 {
		return nil
	}

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

// EnforceDeleteCascade handles cascade/restrict logic when a parent row is deleted.
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
		if len(fk.ParentColumns) != 1 || !strings.EqualFold(fk.ParentColumns[0], pkCol) {
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
			if err := fke.EnforceDeleteCascade(childTable, childPKs); err != nil {
				return err
			}

		case "RESTRICT", "NO ACTION":
			if fke.childRowsExist(childTable, childCol, deletedPKs) {
				return fmt.Errorf("update or delete on table %q violates foreign key constraint on table %q: "+
					"key is still referenced from table %q",
					table, childTable, childTable)
			}

		case "SET NULL":
			fke.setChildColumnNull(childTable, childCol, deletedPKs)
		}
	}
	return nil
}

// discoverFKsFromProd queries Prod's sys views to discover FK constraints for a table.
func (fke *FKEnforcer) discoverFKsFromProd(table string) []coreSchema.ForeignKey {
	sql := fmt.Sprintf(`
		SELECT
			fk.name AS constraint_name,
			cp.name AS child_column,
			tr.name AS parent_table,
			cr.name AS parent_column,
			fk.delete_referential_action_desc AS delete_rule,
			fk.update_referential_action_desc AS update_rule
		FROM sys.foreign_keys fk
		JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
		JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
		JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
		JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
		JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
		WHERE tp.name = %s AND SCHEMA_NAME(tp.schema_id) = 'dbo'
		ORDER BY fk.name, fkc.constraint_column_id`, quoteLiteralMSSQL(table))

	result, err := execTDSQuery(fke.prodConn, sql)
	if err != nil || result.Error != "" {
		if fke.verbose {
			log.Printf("[conn %d] FK discovery: failed to query Prod for %s", fke.connID, table)
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
	var orderedKeys []string

	for i, row := range result.RowValues {
		if len(row) < 6 || result.RowNulls[i][0] {
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
				parentTable:    strings.ToLower(parentTable),
				onDelete:       normalizeFKAction(deleteRule),
				onUpdate:       normalizeFKAction(updateRule),
			}
			fkMap[constraintName] = fd
			orderedKeys = append(orderedKeys, constraintName)
		}
		fd.childColumns = append(fd.childColumns, strings.ToLower(childCol))
		fd.parentColumns = append(fd.parentColumns, strings.ToLower(parentCol))
	}

	var fks []coreSchema.ForeignKey
	for _, key := range orderedKeys {
		fd := fkMap[key]
		fk := coreSchema.ForeignKey{
			ConstraintName: fd.constraintName,
			ChildTable:     strings.ToLower(table),
			ChildColumns:   fd.childColumns,
			ParentTable:    fd.parentTable,
			ParentColumns:  fd.parentColumns,
			OnDelete:       fd.onDelete,
			OnUpdate:       fd.onUpdate,
		}
		fks = append(fks, fk)
		fke.schemaRegistry.RecordForeignKey(strings.ToLower(table), fk)
	}

	if fke.verbose && len(fks) > 0 {
		log.Printf("[conn %d] FK discovery: found %d FK constraints for %s from Prod", fke.connID, len(fks), table)
	}
	return fks
}

// findChildRows queries both shadow and prod for child rows.
func (fke *FKEnforcer) findChildRows(childTable, childCol, childPKCol string, parentPKs []string) []string {
	if len(parentPKs) == 0 {
		return nil
	}
	inClause := buildINClauseMSSQL(parentPKs)
	selectSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		quoteIdentMSSQL(childPKCol), quoteIdentMSSQL(childTable), quoteIdentMSSQL(childCol), inClause)

	seen := make(map[string]bool)
	var result []string

	shadowResult, err := execTDSQuery(fke.shadowConn, selectSQL)
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

	prodResult, err := execTDSQuery(fke.prodConn, selectSQL)
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
	inClause := buildINClauseMSSQL(parentPKs)
	selectSQL := fmt.Sprintf("SELECT TOP 1 1 FROM %s WHERE %s IN (%s)",
		quoteIdentMSSQL(childTable), quoteIdentMSSQL(childCol), inClause)

	shadowResult, err := execTDSQuery(fke.shadowConn, selectSQL)
	if err == nil && shadowResult.Error == "" && len(shadowResult.RowValues) > 0 {
		return true
	}
	prodResult, err := execTDSQuery(fke.prodConn, selectSQL)
	if err == nil && prodResult.Error == "" && len(prodResult.RowValues) > 0 {
		return true
	}
	return false
}

// deleteChildRowsFromShadow deletes child rows from shadow (CASCADE support).
func (fke *FKEnforcer) deleteChildRowsFromShadow(childTable, childCol string, parentPKs []string) {
	if len(parentPKs) == 0 {
		return
	}
	inClause := buildINClauseMSSQL(parentPKs)
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		quoteIdentMSSQL(childTable), quoteIdentMSSQL(childCol), inClause)
	execTDSQuery(fke.shadowConn, deleteSQL) //nolint:errcheck
}

// setChildColumnNull sets FK column to NULL in child rows (SET NULL support).
func (fke *FKEnforcer) setChildColumnNull(childTable, childCol string, parentPKs []string) {
	if len(parentPKs) == 0 {
		return
	}
	inClause := buildINClauseMSSQL(parentPKs)
	updateSQL := fmt.Sprintf("UPDATE %s SET %s = NULL WHERE %s IN (%s)",
		quoteIdentMSSQL(childTable), quoteIdentMSSQL(childCol), quoteIdentMSSQL(childCol), inClause)
	execTDSQuery(fke.shadowConn, updateSQL) //nolint:errcheck
}

// buildINClauseMSSQL builds a SQL IN clause from PK values.
func buildINClauseMSSQL(pks []string) string {
	parts := make([]string, len(pks))
	for i, pk := range pks {
		parts[i] = quoteLiteralMSSQL(pk)
	}
	return strings.Join(parts, ", ")
}

// normalizeFKAction normalizes MSSQL FK action descriptions.
func normalizeFKAction(action string) string {
	switch strings.ToUpper(strings.TrimSpace(action)) {
	case "CASCADE":
		return "CASCADE"
	case "SET_NULL", "SET NULL":
		return "SET NULL"
	case "SET_DEFAULT", "SET DEFAULT":
		return "SET DEFAULT"
	case "NO_ACTION", "NO ACTION":
		return "NO ACTION"
	default:
		return "NO ACTION"
	}
}

// parseInsertValuesMSSQL extracts column names and value rows from an INSERT (regex-based).
func parseInsertValuesMSSQL(rawSQL string) (rows [][]string, columns []string) {
	upper := strings.ToUpper(rawSQL)
	colStart := strings.Index(upper, "(")
	valuesIdx := strings.Index(upper, "VALUES")
	if colStart < 0 || valuesIdx < 0 || colStart > valuesIdx {
		return nil, nil
	}
	colEnd := strings.Index(upper[colStart:], ")")
	if colEnd < 0 {
		return nil, nil
	}
	colEnd += colStart

	colList := rawSQL[colStart+1 : colEnd]
	for _, col := range strings.Split(colList, ",") {
		name := strings.TrimSpace(col)
		name = strings.Trim(name, "[]`\"")
		columns = append(columns, strings.ToLower(name))
	}

	// Find VALUES ( ... )
	valStart := strings.Index(rawSQL[valuesIdx:], "(")
	if valStart < 0 {
		return nil, columns
	}
	valStart += valuesIdx
	valEnd := strings.Index(rawSQL[valStart:], ")")
	if valEnd < 0 {
		return nil, columns
	}
	valEnd += valStart

	valList := rawSQL[valStart+1 : valEnd]
	vals := splitValuesRespectingQuotes(valList)
	var row []string
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = v[1 : len(v)-1]
		}
		row = append(row, v)
	}
	if len(row) > 0 {
		rows = append(rows, row)
	}

	return rows, columns
}

// parseUpdateSetValuesMSSQL extracts SET column=value pairs from UPDATE (regex-based).
func parseUpdateSetValuesMSSQL(rawSQL string) map[string]string {
	upper := strings.ToUpper(rawSQL)
	setIdx := strings.Index(upper, " SET ")
	if setIdx < 0 {
		return nil
	}
	rest := rawSQL[setIdx+5:]

	// Cut at WHERE.
	whereIdx := strings.Index(strings.ToUpper(rest), " WHERE ")
	if whereIdx >= 0 {
		rest = rest[:whereIdx]
	}

	values := make(map[string]string)
	parts := strings.Split(rest, ",")
	for _, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		col := strings.TrimSpace(part[:eqIdx])
		col = strings.Trim(col, "[]`\"")
		val := strings.TrimSpace(part[eqIdx+1:])
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			val = val[1 : len(val)-1]
		}
		values[strings.ToLower(col)] = val
	}
	return values
}
