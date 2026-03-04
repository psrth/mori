package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

const (
	// RegistryFile is the name of the schema registry persistence file.
	RegistryFile = "schema_registry.json"
)

// Column represents a column added via DDL.
type Column struct {
	Name    string  `json:"name"`
	Type    string  `json:"type"`
	Default *string `json:"default"`
}

// ForeignKey describes a foreign key constraint.
type ForeignKey struct {
	ConstraintName string   `json:"constraint_name"`
	ChildTable     string   `json:"child_table"`
	ChildColumns   []string `json:"child_columns"`
	ParentTable    string   `json:"parent_table"`
	ParentColumns  []string `json:"parent_columns"`
	OnDelete       string   `json:"on_delete"` // CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION
	OnUpdate       string   `json:"on_update"`
}

// TableDiff tracks schema differences between Prod and Shadow for a single table.
type TableDiff struct {
	Added           []Column            `json:"added"`
	Dropped         []string            `json:"dropped"`
	Renamed         map[string]string   `json:"renamed"`          // old_name -> new_name
	TypeChanged     map[string][2]string `json:"type_changed"`    // column -> [old_type, new_type]
	IsNewTable      bool                `json:"is_new_table"`     // true if table only exists in Shadow
	IsFullyShadowed bool                `json:"is_fully_shadowed"` // true after TRUNCATE — skip Prod reads
	ForeignKeys     []ForeignKey        `json:"foreign_keys,omitempty"` // FK constraints where this table is the child
}

// Registry tracks schema diffs for all modified tables.
type Registry struct {
	mu    sync.RWMutex
	diffs map[string]*TableDiff
}

// NewRegistry creates an empty schema registry.
func NewRegistry() *Registry {
	return &Registry{
		diffs: make(map[string]*TableDiff),
	}
}

func (r *Registry) getOrCreateDiff(table string) *TableDiff {
	d, ok := r.diffs[table]
	if !ok {
		d = &TableDiff{
			Renamed:     make(map[string]string),
			TypeChanged: make(map[string][2]string),
		}
		r.diffs[table] = d
	}
	return d
}

// RecordNewTable records that a table was created in Shadow (doesn't exist in Prod).
func (r *Registry) RecordNewTable(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(table)
	d.IsNewTable = true
}

// RemoveTable removes a table's diff from the registry (e.g., after DROP TABLE).
func (r *Registry) RemoveTable(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.diffs, table)
}

// RecordAddColumn records that a column was added to the table in Shadow.
func (r *Registry) RecordAddColumn(table string, col Column) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(table)
	d.Added = append(d.Added, col)
}

// RecordDropColumn records that a column was dropped from the table in Shadow.
func (r *Registry) RecordDropColumn(table, colName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(table)
	d.Dropped = append(d.Dropped, colName)
}

// RecordRenameColumn records that a column was renamed in the table in Shadow.
func (r *Registry) RecordRenameColumn(table, oldName, newName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(table)
	d.Renamed[oldName] = newName
}

// RecordTypeChange records that a column's type was changed in the table in Shadow.
func (r *Registry) RecordTypeChange(table, colName, oldType, newType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(table)
	d.TypeChanged[colName] = [2]string{oldType, newType}
}

// GetDiff returns a copy of the schema diff for a table, or nil if no diff exists.
func (r *Registry) GetDiff(table string) *TableDiff {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.diffs[table]
	if !ok {
		return nil
	}
	// Return a deep copy to avoid data races on the returned value.
	cp := &TableDiff{
		Added:           make([]Column, len(d.Added)),
		Dropped:         make([]string, len(d.Dropped)),
		Renamed:         make(map[string]string, len(d.Renamed)),
		TypeChanged:     make(map[string][2]string, len(d.TypeChanged)),
		IsNewTable:      d.IsNewTable,
		IsFullyShadowed: d.IsFullyShadowed,
	}
	copy(cp.Added, d.Added)
	copy(cp.Dropped, d.Dropped)
	for k, v := range d.Renamed {
		cp.Renamed[k] = v
	}
	for k, v := range d.TypeChanged {
		cp.TypeChanged[k] = v
	}
	if len(d.ForeignKeys) > 0 {
		cp.ForeignKeys = make([]ForeignKey, len(d.ForeignKeys))
		copy(cp.ForeignKeys, d.ForeignKeys)
	}
	return cp
}

// MarkFullyShadowed marks a table as fully shadowed (e.g., after TRUNCATE).
// All future reads for this table will skip Prod and query Shadow only.
func (r *Registry) MarkFullyShadowed(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(table)
	d.IsFullyShadowed = true
}

// RecordForeignKey stores a foreign key constraint in the child table's diff.
// If an FK with the same constraint name already exists, it is replaced.
func (r *Registry) RecordForeignKey(childTable string, fk ForeignKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.getOrCreateDiff(childTable)
	// Replace if constraint name already recorded.
	for i, existing := range d.ForeignKeys {
		if existing.ConstraintName != "" && existing.ConstraintName == fk.ConstraintName {
			d.ForeignKeys[i] = fk
			return
		}
	}
	d.ForeignKeys = append(d.ForeignKeys, fk)
}

// GetForeignKeys returns FKs where the given table is the child (i.e., the table
// that has the REFERENCES clause). Returns nil if no FKs are recorded.
func (r *Registry) GetForeignKeys(table string) []ForeignKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.diffs[table]
	if !ok || len(d.ForeignKeys) == 0 {
		return nil
	}
	cp := make([]ForeignKey, len(d.ForeignKeys))
	copy(cp, d.ForeignKeys)
	return cp
}

// GetReferencingFKs returns all FKs where the given table is the parent
// (i.e., some other table references it). This is needed for cascade/restrict
// checks on DELETE/UPDATE of parent rows.
func (r *Registry) GetReferencingFKs(table string) []ForeignKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []ForeignKey
	for _, d := range r.diffs {
		for _, fk := range d.ForeignKeys {
			if fk.ParentTable == table {
				result = append(result, fk)
			}
		}
	}
	return result
}

// IsFullyShadowed reports whether the table is fully shadowed (skip Prod reads).
func (r *Registry) IsFullyShadowed(table string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.diffs[table]
	if !ok {
		return false
	}
	return d.IsFullyShadowed
}

// HasDiff reports whether the table has any schema divergence.
func (r *Registry) HasDiff(table string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.diffs[table]
	return ok
}

// Tables returns all table names that have schema diffs, sorted.
func (r *Registry) Tables() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.diffs) == 0 {
		return nil
	}
	out := make([]string, 0, len(r.diffs))
	for t := range r.diffs {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// WriteRegistry persists the schema registry to .mori/schema_registry.json.
func WriteRegistry(moriDir string, r *Registry) error {
	r.mu.RLock()
	data, err := json.MarshalIndent(r.diffs, "", "  ")
	r.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to marshal schema registry: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, RegistryFile), data, 0644)
}

// ReadRegistry reads and deserializes the schema registry from .mori/schema_registry.json.
func ReadRegistry(moriDir string) (*Registry, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, RegistryFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read schema registry: %w", err)
	}
	var diffs map[string]*TableDiff
	if err := json.Unmarshal(data, &diffs); err != nil {
		return nil, fmt.Errorf("failed to parse schema registry: %w", err)
	}
	// Ensure nested maps are initialized for deserialized diffs.
	for _, d := range diffs {
		if d.Renamed == nil {
			d.Renamed = make(map[string]string)
		}
		if d.TypeChanged == nil {
			d.TypeChanged = make(map[string][2]string)
		}
	}
	return &Registry{diffs: diffs}, nil
}
