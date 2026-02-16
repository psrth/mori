package delta

import "sync"

// Map tracks which (table, pk) pairs have been modified in the Shadow database.
// It is the primary input to the Router for deciding read strategies.
type Map struct {
	s *pkSet

	// insertedTables tracks tables that have had rows inserted into Shadow.
	// Inserts don't need per-PK tracking — the Router just needs to know
	// "this table has Shadow-only rows" to trigger merged reads.
	insertMu       sync.RWMutex
	insertedTables map[string]bool
}

// NewMap creates an empty delta map.
func NewMap() *Map {
	return &Map{
		s:              newPKSet(),
		insertedTables: make(map[string]bool),
	}
}

// Add marks (table, pk) as having a local modification in Shadow.
func (m *Map) Add(table, pk string) { m.s.add(table, pk) }

// Remove unmarks (table, pk) as a delta.
func (m *Map) Remove(table, pk string) { m.s.remove(table, pk) }

// ClearTable removes all delta entries for the given table.
func (m *Map) ClearTable(table string) {
	m.s.clearTable(table)
	m.insertMu.Lock()
	delete(m.insertedTables, table)
	m.insertMu.Unlock()
}

// IsDelta reports whether (table, pk) has been modified in Shadow.
func (m *Map) IsDelta(table, pk string) bool { return m.s.has(table, pk) }

// DeltaPKs returns all modified primary keys for a table, sorted.
func (m *Map) DeltaPKs(table string) []string { return m.s.pks(table) }

// CountForTable returns the number of modified rows for a table.
func (m *Map) CountForTable(table string) int { return m.s.countForTable(table) }

// Tables returns all table names that have delta entries, sorted.
func (m *Map) Tables() []string { return m.s.tables() }

// MarkInserted records that a table has had rows inserted into Shadow.
func (m *Map) MarkInserted(table string) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	m.insertedTables[table] = true
}

// HasInserts reports whether a table has had rows inserted into Shadow.
func (m *Map) HasInserts(table string) bool {
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	return m.insertedTables[table]
}

// InsertedTablesList returns all tables with inserts, for persistence.
func (m *Map) InsertedTablesList() []string {
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	var out []string
	for t := range m.insertedTables {
		out = append(out, t)
	}
	return out
}

// LoadInsertedTables populates the inserted tables set from a persisted list.
func (m *Map) LoadInsertedTables(tables []string) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	m.insertedTables = make(map[string]bool, len(tables))
	for _, t := range tables {
		m.insertedTables[t] = true
	}
}

// HasAnyDelta reports whether any delta entries or inserts exist at all.
func (m *Map) HasAnyDelta() bool {
	if len(m.s.tables()) > 0 {
		return true
	}
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	return len(m.insertedTables) > 0
}

// AnyTableDelta reports whether any of the given tables have delta entries or inserts.
func (m *Map) AnyTableDelta(tables []string) bool {
	for _, t := range tables {
		if m.s.countForTable(t) > 0 {
			return true
		}
	}
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	for _, t := range tables {
		if m.insertedTables[t] {
			return true
		}
	}
	return false
}

// Stage adds (table, pk) to the per-transaction staging buffer.
func (m *Map) Stage(table, pk string) { m.s.stage(table, pk) }

// Commit promotes all staged entries into the committed state.
func (m *Map) Commit() { m.s.commit() }

// Rollback discards all staged entries.
func (m *Map) Rollback() { m.s.rollback() }

// Snapshot returns a serializable representation of the committed entries.
func (m *Map) Snapshot() map[string][]string { return m.s.snapshot() }

// Load populates the map from a deserialized representation.
func (m *Map) Load(data map[string][]string) { m.s.load(data) }
