package delta

// Map tracks which (table, pk) pairs have been modified in the Shadow database.
// It is the primary input to the Router for deciding read strategies.
type Map struct {
	s *pkSet
}

// NewMap creates an empty delta map.
func NewMap() *Map {
	return &Map{s: newPKSet()}
}

// Add marks (table, pk) as having a local modification in Shadow.
func (m *Map) Add(table, pk string) { m.s.add(table, pk) }

// Remove unmarks (table, pk) as a delta.
func (m *Map) Remove(table, pk string) { m.s.remove(table, pk) }

// IsDelta reports whether (table, pk) has been modified in Shadow.
func (m *Map) IsDelta(table, pk string) bool { return m.s.has(table, pk) }

// DeltaPKs returns all modified primary keys for a table, sorted.
func (m *Map) DeltaPKs(table string) []string { return m.s.pks(table) }

// CountForTable returns the number of modified rows for a table.
func (m *Map) CountForTable(table string) int { return m.s.countForTable(table) }

// Tables returns all table names that have delta entries, sorted.
func (m *Map) Tables() []string { return m.s.tables() }

// AnyTableDelta reports whether any of the given tables have at least one delta entry.
func (m *Map) AnyTableDelta(tables []string) bool {
	for _, t := range tables {
		if m.s.countForTable(t) > 0 {
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
