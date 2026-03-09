package proxy

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/core/delta"
	coreSchema "github.com/mori-dev/mori/internal/core/schema"
	"github.com/mori-dev/mori/internal/engine/duckdb/schema"
)

// FKEnforcer handles foreign key validation at the proxy layer for DuckDB.
// Because FK constraints may not be enforced in the shadow database,
// the proxy checks referential integrity before writes.
type FKEnforcer struct {
	prodDB         *sql.DB
	shadowDB       *sql.DB
	deltaMap       *delta.Map
	tombstones     *delta.TombstoneSet
	tables         map[string]schema.TableMeta
	schemaRegistry *coreSchema.Registry
	connID         int64
	verbose        bool
}

// CheckParentExists verifies that a parent row exists for the given FK
// constraint and child column values.
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
		whereParts = append(whereParts, fmt.Sprintf(`"%s" = '%s'`, parentCol, strings.ReplaceAll(val, "'", "''")))
	}
	if len(whereParts) == 0 {
		return nil
	}

	parentTable := fk.ParentTable
	whereClause := strings.Join(whereParts, " AND ")

	// 1. Check delta/tombstone maps.
	if meta, ok := fke.tables[parentTable]; ok && len(meta.PKColumns) == 1 {
		pkCol := meta.PKColumns[0]
		if len(fk.ParentColumns) == 1 && fk.ParentColumns[0] == pkCol {
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

	// 2. Query shadow.
	shadowSQL := fmt.Sprintf(`SELECT 1 FROM "%s" WHERE %s LIMIT 1`, parentTable, whereClause)
	var one sql.NullString
	if err := fke.shadowDB.QueryRow(shadowSQL).Scan(&one); err == nil {
		return nil
	}

	// 3. Query prod.
	prodSQL := fmt.Sprintf(`SELECT 1 FROM "%s" WHERE %s LIMIT 1`, parentTable, whereClause)
	if err := fke.prodDB.QueryRow(prodSQL).Scan(&one); err == nil {
		return nil
	}

	return fmt.Errorf("insert or update on table %q violates foreign key constraint: "+
		"key (%s)=(%s) is not present in table %q",
		fk.ChildTable,
		strings.Join(fk.ChildColumns, ", "),
		formatChildValues(fk.ChildColumns, childValues),
		parentTable)
}

// EnforceInsert validates FK constraints for an INSERT statement.
func (fke *FKEnforcer) EnforceInsert(table, sqlStr string) error {
	fks := fke.schemaRegistry.GetForeignKeys(table)
	if len(fks) == 0 {
		return nil
	}

	// Extract column values from the INSERT statement.
	colVals := extractInsertColumnValues(sqlStr)
	if len(colVals) == 0 {
		return nil
	}

	for _, fk := range fks {
		if err := fke.CheckParentExists(fk, colVals); err != nil {
			return err
		}
	}
	return nil
}

// EnforceDeleteRestrict checks if deleting rows from a parent table would
// violate RESTRICT/NO ACTION FK constraints.
func (fke *FKEnforcer) EnforceDeleteRestrict(table string, deletedPKs []string) error {
	refFKs := fke.schemaRegistry.GetReferencingFKs(table)
	for _, fk := range refFKs {
		if fk.OnDelete != "RESTRICT" && fk.OnDelete != "NO ACTION" {
			continue
		}
		if len(fk.ChildColumns) == 0 || len(fk.ParentColumns) == 0 {
			continue
		}
		if fke.childRowsExist(fk.ChildTable, fk.ChildColumns[0], deletedPKs) {
			return fmt.Errorf("update or delete on table %q violates foreign key constraint on table %q",
				table, fk.ChildTable)
		}
	}
	return nil
}

// childRowsExist checks if any rows in the child table reference the given PKs.
func (fke *FKEnforcer) childRowsExist(childTable, childCol string, parentPKs []string) bool {
	if len(parentPKs) == 0 {
		return false
	}

	// Build IN clause.
	var quoted []string
	for _, pk := range parentPKs {
		quoted = append(quoted, "'"+strings.ReplaceAll(pk, "'", "''")+"'")
	}
	inClause := strings.Join(quoted, ", ")

	query := fmt.Sprintf(`SELECT 1 FROM "%s" WHERE "%s" IN (%s) LIMIT 1`,
		childTable, childCol, inClause)

	// Check shadow first.
	var one sql.NullString
	if err := fke.shadowDB.QueryRow(query).Scan(&one); err == nil {
		return true
	}
	// Check prod.
	if err := fke.prodDB.QueryRow(query).Scan(&one); err == nil {
		return true
	}
	return false
}

// extractInsertColumnValues extracts column->value pairs from an INSERT statement.
// Returns a map from column name to literal value.
func extractInsertColumnValues(sqlStr string) map[string]string {
	upper := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Find column list.
	parenStart := strings.Index(upper, "(")
	valuesIdx := strings.Index(upper, "VALUES")
	if parenStart < 0 || valuesIdx < 0 || parenStart > valuesIdx {
		return nil
	}

	parenEnd := strings.Index(upper[:valuesIdx], ")")
	if parenEnd < 0 {
		return nil
	}

	colList := sqlStr[parenStart+1 : parenEnd]
	cols := strings.Split(colList, ",")
	for i := range cols {
		cols[i] = strings.Trim(strings.TrimSpace(cols[i]), `"'`)
	}

	// Find values.
	valuesStart := strings.Index(upper[valuesIdx:], "(")
	if valuesStart < 0 {
		return nil
	}
	valuesEnd := strings.LastIndex(sqlStr[valuesIdx+valuesStart:], ")")
	if valuesEnd < 0 {
		return nil
	}
	valuePart := sqlStr[valuesIdx+valuesStart+1 : valuesIdx+valuesStart+valuesEnd]
	vals := splitValuesTuple(valuePart)

	result := make(map[string]string)
	for i, col := range cols {
		if i < len(vals) {
			val := strings.TrimSpace(vals[i])
			val = strings.Trim(val, "'")
			result[col] = val
		}
	}
	return result
}

func formatChildValues(childCols []string, vals map[string]string) string {
	var parts []string
	for _, col := range childCols {
		if v, ok := vals[col]; ok {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, ", ")
}

// discoverFKsFromProd queries Prod for FK constraints and records them in the schema registry.
func discoverFKsFromProd(prodDB *sql.DB, schemaRegistry *coreSchema.Registry, verbose bool, connID int64) {
	// Use DuckDB's information_schema to discover FKs.
	rows, err := prodDB.Query(
		`SELECT
			rc.constraint_name,
			kcu.table_name AS child_table,
			kcu.column_name AS child_column,
			ccu.table_name AS parent_table,
			ccu.column_name AS parent_column,
			rc.delete_rule,
			rc.update_rule
		 FROM information_schema.referential_constraints rc
		 JOIN information_schema.key_column_usage kcu
			ON rc.constraint_name = kcu.constraint_name
			AND rc.constraint_schema = kcu.table_schema
		 JOIN information_schema.constraint_column_usage ccu
			ON rc.unique_constraint_name = ccu.constraint_name
			AND rc.unique_constraint_schema = ccu.constraint_schema
		 WHERE rc.constraint_schema = 'main'
		 ORDER BY rc.constraint_name, kcu.ordinal_position`)
	if err != nil {
		if verbose {
			log.Printf("[conn %d] FK discovery failed: %v", connID, err)
		}
		return
	}
	defer rows.Close()

	fkMap := make(map[string]*coreSchema.ForeignKey)
	for rows.Next() {
		var constraintName, childTable, childCol, parentTable, parentCol, deleteRule, updateRule string
		if err := rows.Scan(&constraintName, &childTable, &childCol, &parentTable, &parentCol, &deleteRule, &updateRule); err != nil {
			continue
		}
		fk, ok := fkMap[constraintName]
		if !ok {
			fk = &coreSchema.ForeignKey{
				ConstraintName: constraintName,
				ChildTable:     childTable,
				ParentTable:    parentTable,
				OnDelete:       deleteRule,
				OnUpdate:       updateRule,
			}
			fkMap[constraintName] = fk
		}
		fk.ChildColumns = append(fk.ChildColumns, childCol)
		fk.ParentColumns = append(fk.ParentColumns, parentCol)
	}

	for _, fk := range fkMap {
		schemaRegistry.RecordForeignKey(fk.ChildTable, *fk)
		if verbose {
			log.Printf("[conn %d] FK discovered: %s.(%s) -> %s.(%s)",
				connID, fk.ChildTable, strings.Join(fk.ChildColumns, ","),
				fk.ParentTable, strings.Join(fk.ParentColumns, ","))
		}
	}
}

// extractFKFromDDL attempts to extract FK constraints from a CREATE TABLE DDL statement
// using regex-based parsing.
func extractFKFromDDL(sqlStr string) []coreSchema.ForeignKey {
	var fks []coreSchema.ForeignKey

	// Pattern: FOREIGN KEY (col1, col2) REFERENCES parent_table (col1, col2) [ON DELETE ...]  [ON UPDATE ...]
	reFKConstraint := regexp.MustCompile(`(?i)(?:CONSTRAINT\s+\w+\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+("?[\w.]+"?)\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?(?:\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?`)

	matches := reFKConstraint.FindAllStringSubmatch(sqlStr, -1)
	for _, m := range matches {
		if len(m) < 4 {
			continue
		}
		childCols := splitAndTrimCols(m[1])
		parentTable := strings.Trim(strings.TrimSpace(m[2]), `"`)
		parentCols := splitAndTrimCols(m[3])
		onDelete := "NO ACTION"
		onUpdate := "NO ACTION"
		if len(m) > 4 && m[4] != "" {
			onDelete = strings.ToUpper(strings.TrimSpace(m[4]))
		}
		if len(m) > 5 && m[5] != "" {
			onUpdate = strings.ToUpper(strings.TrimSpace(m[5]))
		}

		fks = append(fks, coreSchema.ForeignKey{
			ChildColumns:  childCols,
			ParentTable:   parentTable,
			ParentColumns: parentCols,
			OnDelete:      onDelete,
			OnUpdate:      onUpdate,
		})
	}

	return fks
}

// CheckWriteFK enforces FK constraints before a write operation.
// Returns an error message if the FK constraint would be violated, or "" if OK.
func (fke *FKEnforcer) CheckWriteFK(cl *core.Classification, sqlStr string) string {
	if fke == nil || cl == nil {
		return ""
	}
	if cl.SubType == core.SubInsert && len(cl.Tables) > 0 {
		if err := fke.EnforceInsert(cl.Tables[0], sqlStr); err != nil {
			return err.Error()
		}
	}
	if cl.SubType == core.SubDelete && len(cl.Tables) > 0 {
		var pkValues []string
		for _, pk := range cl.PKs {
			pkValues = append(pkValues, pk.PK)
		}
		if len(pkValues) > 0 {
			if err := fke.EnforceDeleteRestrict(cl.Tables[0], pkValues); err != nil {
				return err.Error()
			}
		}
	}
	return ""
}

func splitAndTrimCols(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		col := strings.Trim(strings.TrimSpace(p), `"'`)
		if col != "" {
			result = append(result, col)
		}
	}
	return result
}
