package delta

// TombstoneSet tracks which (table, pk) pairs have been deleted via the Shadow.
// Tombstoned rows are filtered out of Prod query results during merged reads.
type TombstoneSet struct {
	s *pkSet
}

// NewTombstoneSet creates an empty tombstone set.
func NewTombstoneSet() *TombstoneSet {
	return &TombstoneSet{s: newPKSet()}
}

// Add marks (table, pk) as deleted.
func (t *TombstoneSet) Add(table, pk string) { t.s.add(table, pk) }

// IsTombstoned reports whether (table, pk) has been deleted.
func (t *TombstoneSet) IsTombstoned(table, pk string) bool { return t.s.has(table, pk) }

// TombstonedPKs returns all tombstoned primary keys for a table, sorted.
func (t *TombstoneSet) TombstonedPKs(table string) []string { return t.s.pks(table) }

// CountForTable returns the number of tombstoned rows for a table.
func (t *TombstoneSet) CountForTable(table string) int { return t.s.countForTable(table) }

// Tables returns all table names that have tombstone entries, sorted.
func (t *TombstoneSet) Tables() []string { return t.s.tables() }

// Stage adds (table, pk) to the per-transaction staging buffer.
func (t *TombstoneSet) Stage(table, pk string) { t.s.stage(table, pk) }

// Commit promotes all staged entries into the committed state.
func (t *TombstoneSet) Commit() { t.s.commit() }

// Rollback discards all staged entries.
func (t *TombstoneSet) Rollback() { t.s.rollback() }

// Snapshot returns a serializable representation of the committed entries.
func (t *TombstoneSet) Snapshot() map[string][]string { return t.s.snapshot() }

// Load populates the set from a deserialized representation.
func (t *TombstoneSet) Load(data map[string][]string) { t.s.load(data) }
