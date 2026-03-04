package delta

import "sync"

// Map tracks which (table, pk) pairs have been modified in the Shadow database.
// It is the primary input to the Router for deciding read strategies.
type Map struct {
	s *pkSet

	// insertedTables tracks tables that have had rows inserted into Shadow.
	// The int value is the cumulative insert count (0 means "has inserts but count unknown").
	insertMu       sync.RWMutex
	insertedTables map[string]int

	// stagedInserts holds insert counts added during a transaction, promoted on Commit.
	stagedInserts map[string]int
}

// NewMap creates an empty delta map.
func NewMap() *Map {
	return &Map{
		s:              newPKSet(),
		insertedTables: make(map[string]int),
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

// AddInsertCount atomically adds count to the insert total for a table.
func (m *Map) AddInsertCount(table string, count int) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	m.insertedTables[table] += count
}

// MarkInserted records that a table has had rows inserted into Shadow (count unknown).
// Used by non-Postgres engines that don't track per-statement row counts.
func (m *Map) MarkInserted(table string) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	if _, ok := m.insertedTables[table]; !ok {
		m.insertedTables[table] = 0
	}
}

// HasInserts reports whether a table has had rows inserted into Shadow.
func (m *Map) HasInserts(table string) bool {
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	_, ok := m.insertedTables[table]
	return ok
}

// InsertCountForTable returns the cumulative insert count for a table.
// Returns 0 if unknown or no inserts.
func (m *Map) InsertCountForTable(table string) int {
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	return m.insertedTables[table]
}

// InsertedTablesList returns all tables with inserts and their counts.
func (m *Map) InsertedTablesList() map[string]int {
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	out := make(map[string]int, len(m.insertedTables))
	for t, c := range m.insertedTables {
		out[t] = c
	}
	return out
}

// LoadInsertedTables populates the inserted tables set from a persisted map.
func (m *Map) LoadInsertedTables(tables map[string]int) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	m.insertedTables = make(map[string]int, len(tables))
	for t, c := range tables {
		m.insertedTables[t] = c
	}
}

// StageInsertCount stages an insert count addition for transaction support.
func (m *Map) StageInsertCount(table string, count int) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	if m.stagedInserts == nil {
		m.stagedInserts = make(map[string]int)
	}
	m.stagedInserts[table] += count
}

// CommitInsertCounts promotes staged insert counts into committed state.
func (m *Map) CommitInsertCounts() {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	for t, c := range m.stagedInserts {
		m.insertedTables[t] += c
	}
	m.stagedInserts = nil
}

// RollbackInsertCounts discards staged insert counts.
func (m *Map) RollbackInsertCounts() {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	m.stagedInserts = nil
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
		if _, ok := m.insertedTables[t]; ok {
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

// SnapshotAll returns a merged snapshot of committed + staged PK entries.
// Used by savepoint support to capture the full in-flight delta state.
func (m *Map) SnapshotAll() map[string][]string { return m.s.snapshotAll() }

// RestoreAll replaces committed + staged PK entries with the given snapshot.
// Staged is cleared since the snapshot already includes everything up to the savepoint.
func (m *Map) RestoreAll(snap map[string][]string) { m.s.restoreAll(snap) }

// SnapshotInsertedTables returns a merged copy of insertedTables + stagedInserts.
// Used by savepoint support to capture the full in-flight insert count state.
func (m *Map) SnapshotInsertedTables() map[string]int {
	m.insertMu.RLock()
	defer m.insertMu.RUnlock()
	out := make(map[string]int, len(m.insertedTables))
	for t, c := range m.insertedTables {
		out[t] = c
	}
	for t, c := range m.stagedInserts {
		out[t] += c
	}
	return out
}

// RestoreInsertedTables replaces insertedTables with the given snapshot and clears stagedInserts.
// Used by savepoint support to restore insert count state on ROLLBACK TO.
func (m *Map) RestoreInsertedTables(snap map[string]int) {
	m.insertMu.Lock()
	defer m.insertMu.Unlock()
	m.insertedTables = make(map[string]int, len(snap))
	for t, c := range snap {
		m.insertedTables[t] = c
	}
	m.stagedInserts = nil
}
