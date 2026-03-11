package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	coreSchema "github.com/psrth/mori/internal/core/schema"
)

// DetectForeignKeys discovers FK constraints for all tables in the SQLite database
// using PRAGMA foreign_key_list and stores them in the schema registry.
func DetectForeignKeys(ctx context.Context, db *sql.DB, registry *coreSchema.Registry, tableNames []string) error {
	if registry == nil {
		return nil
	}

	for _, tableName := range tableNames {
		fks, err := detectFKsForTable(ctx, db, tableName)
		if err != nil {
			// Non-fatal: log and continue.
			continue
		}
		for _, fk := range fks {
			registry.RecordForeignKey(tableName, fk)
		}
	}

	return nil
}

// detectFKsForTable queries PRAGMA foreign_key_list(table) to discover FKs.
// Returns: id, seq, table (parent), from (child col), to (parent col), on_update, on_delete, match
func detectFKsForTable(ctx context.Context, db *sql.DB, tableName string) ([]coreSchema.ForeignKey, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(%q)", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign_key_list for %q: %w", tableName, err)
	}
	defer rows.Close()

	// Group by FK id since composite FKs have multiple rows with the same id.
	type fkRow struct {
		id       int
		seq      int
		parent   string
		from     string
		to       string
		onUpdate string
		onDelete string
		match    string
	}
	var fkRows []fkRow

	for rows.Next() {
		var r fkRow
		if err := rows.Scan(&r.id, &r.seq, &r.parent, &r.from, &r.to, &r.onUpdate, &r.onDelete, &r.match); err != nil {
			return nil, fmt.Errorf("failed to scan FK for %q: %w", tableName, err)
		}
		fkRows = append(fkRows, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Group by id.
	grouped := make(map[int][]fkRow)
	for _, r := range fkRows {
		grouped[r.id] = append(grouped[r.id], r)
	}

	var fks []coreSchema.ForeignKey
	for id, group := range grouped {
		if len(group) == 0 {
			continue
		}
		fk := coreSchema.ForeignKey{
			ConstraintName: fmt.Sprintf("fk_%s_%d", tableName, id),
			ChildTable:     tableName,
			ParentTable:    group[0].parent,
			OnUpdate:       normalizeAction(group[0].onUpdate),
			OnDelete:       normalizeAction(group[0].onDelete),
		}
		for _, r := range group {
			fk.ChildColumns = append(fk.ChildColumns, r.from)
			fk.ParentColumns = append(fk.ParentColumns, r.to)
		}
		fks = append(fks, fk)
	}

	return fks, nil
}

func normalizeAction(action string) string {
	upper := strings.ToUpper(strings.TrimSpace(action))
	switch upper {
	case "CASCADE":
		return "CASCADE"
	case "SET NULL":
		return "SET NULL"
	case "SET DEFAULT":
		return "SET DEFAULT"
	case "RESTRICT":
		return "RESTRICT"
	case "NO ACTION", "":
		return "NO ACTION"
	default:
		return upper
	}
}

// FKEnforcer provides proxy-layer FK enforcement for the SQLite engine.
// This supplements PRAGMA foreign_keys=OFF on the shadow database.
type FKEnforcer struct {
	proxy          *Proxy
	schemaRegistry *coreSchema.Registry
	verbose        bool
	connID         int64
}

// NewFKEnforcer creates a new FKEnforcer.
func NewFKEnforcer(proxy *Proxy, connID int64) *FKEnforcer {
	if proxy.schemaRegistry == nil {
		return nil
	}
	return &FKEnforcer{
		proxy:          proxy,
		schemaRegistry: proxy.schemaRegistry,
		verbose:        proxy.verbose,
		connID:         connID,
	}
}

// ValidateParentExists checks that the parent row exists for a given FK value.
// Checks delta map, tombstones, shadow, and prod in that order.
func (fke *FKEnforcer) ValidateParentExists(parentTable string, parentCol string, value string) error {
	p := fke.proxy

	// Check tombstones first — if parent is tombstoned, it's deleted.
	if p.tombstones != nil && p.tombstones.IsTombstoned(parentTable, value) {
		return fmt.Errorf("insert or update on table violates foreign key constraint: key (%s)=(%s) is not present in table %q",
			parentCol, value, parentTable)
	}

	// Check delta map — if parent is in delta, it exists in shadow.
	if p.deltaMap != nil && p.deltaMap.IsDelta(parentTable, value) {
		return nil // Exists in shadow.
	}

	// Check shadow database.
	escapedVal := strings.ReplaceAll(value, "'", "''")
	checkSQL := fmt.Sprintf(`SELECT 1 FROM %q WHERE %q = '%s' LIMIT 1`, parentTable, parentCol, escapedVal)
	var dummy int
	if err := p.shadowDB.QueryRow(checkSQL).Scan(&dummy); err == nil {
		return nil // Exists in shadow.
	}

	// Check prod database.
	if err := p.prodDB.QueryRow(checkSQL).Scan(&dummy); err == nil {
		return nil // Exists in prod.
	}

	return fmt.Errorf("insert or update on table violates foreign key constraint: key (%s)=(%s) is not present in table %q",
		parentCol, value, parentTable)
}

// CheckDeleteRestrict checks if deleting from parentTable would violate
// RESTRICT/NO ACTION constraints on child tables.
func (fke *FKEnforcer) CheckDeleteRestrict(parentTable string, deletedPKs []string) error {
	if fke.schemaRegistry == nil {
		return nil
	}

	refFKs := fke.schemaRegistry.GetReferencingFKs(parentTable)
	for _, fk := range refFKs {
		if fk.OnDelete != "RESTRICT" && fk.OnDelete != "NO ACTION" {
			continue
		}
		if len(fk.ChildColumns) == 0 || len(fk.ParentColumns) == 0 {
			continue
		}
		childCol := fk.ChildColumns[0]
		if fke.childRowsExist(fk.ChildTable, childCol, deletedPKs) {
			return fmt.Errorf("update or delete on table %q violates foreign key constraint on table %q",
				parentTable, fk.ChildTable)
		}
	}
	return nil
}

// childRowsExist checks if any rows in childTable reference any of the given PK values.
func (fke *FKEnforcer) childRowsExist(childTable, childCol string, parentPKs []string) bool {
	if len(parentPKs) == 0 {
		return false
	}
	p := fke.proxy

	var quoted []string
	for _, pk := range parentPKs {
		quoted = append(quoted, "'"+strings.ReplaceAll(pk, "'", "''")+"'")
	}

	checkSQL := fmt.Sprintf(`SELECT 1 FROM %q WHERE %q IN (%s) LIMIT 1`,
		childTable, childCol, strings.Join(quoted, ", "))

	var dummy int
	// Check shadow first.
	if err := p.shadowDB.QueryRow(checkSQL).Scan(&dummy); err == nil {
		return true
	}
	// Check prod.
	if err := p.prodDB.QueryRow(checkSQL).Scan(&dummy); err == nil {
		return true
	}
	return false
}

// EnforceDeleteCascade handles ON DELETE CASCADE/SET NULL for child tables.
func (fke *FKEnforcer) EnforceDeleteCascade(parentTable string, deletedPKs []string) {
	if fke.schemaRegistry == nil || len(deletedPKs) == 0 {
		return
	}

	refFKs := fke.schemaRegistry.GetReferencingFKs(parentTable)
	for _, fk := range refFKs {
		if len(fk.ChildColumns) == 0 {
			continue
		}
		childCol := fk.ChildColumns[0]
		p := fke.proxy

		var quoted []string
		for _, pk := range deletedPKs {
			quoted = append(quoted, "'"+strings.ReplaceAll(pk, "'", "''")+"'")
		}
		inList := strings.Join(quoted, ", ")

		switch fk.OnDelete {
		case "CASCADE":
			// Delete child rows and tombstone them.
			deleteSQL := fmt.Sprintf(`DELETE FROM %q WHERE %q IN (%s)`, fk.ChildTable, childCol, inList)
			p.shadowDB.Exec(deleteSQL)
			// Tombstone the child rows.
			for _, pk := range deletedPKs {
				if p.tombstones != nil {
					p.tombstones.Add(fk.ChildTable, pk)
				}
			}
			if fke.verbose {
				log.Printf("[conn %d] FK CASCADE: deleted from %s where %s in (%d values)",
					fke.connID, fk.ChildTable, childCol, len(deletedPKs))
			}

		case "SET NULL":
			updateSQL := fmt.Sprintf(`UPDATE %q SET %q = NULL WHERE %q IN (%s)`,
				fk.ChildTable, childCol, childCol, inList)
			p.shadowDB.Exec(updateSQL)
			if fke.verbose {
				log.Printf("[conn %d] FK SET NULL: updated %s where %s in (%d values)",
					fke.connID, fk.ChildTable, childCol, len(deletedPKs))
			}
		}
	}
}
