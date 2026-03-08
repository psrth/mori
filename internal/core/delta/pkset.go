package delta

import (
	"slices"
	"sync"
)

// pkSet is the shared, unexported base for Map and TombstoneSet.
// It provides a thread-safe set of (table, pk) pairs with transaction staging.
type pkSet struct {
	mu      sync.RWMutex
	entries map[string]map[string]bool // table -> pk -> exists
	staged  map[string]map[string]bool // per-transaction staging buffer
}

func newPKSet() *pkSet {
	return &pkSet{
		entries: make(map[string]map[string]bool),
	}
}

func (s *pkSet) add(table, pk string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries[table] == nil {
		s.entries[table] = make(map[string]bool)
	}
	s.entries[table][pk] = true
}

func (s *pkSet) remove(table, pk string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pks := s.entries[table]
	if pks == nil {
		return
	}
	delete(pks, pk)
	if len(pks) == 0 {
		delete(s.entries, table)
	}
}

// has returns true if (table, pk) exists in entries or in the staging buffer.
func (s *pkSet) has(table, pk string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.entries[table][pk] {
		return true
	}
	return s.staged[table][pk]
}

// pks returns all primary keys for a table from the committed entries, sorted.
func (s *pkSet) pks(table string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.entries[table]
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for pk := range m {
		out = append(out, pk)
	}
	slices.Sort(out)
	return out
}

func (s *pkSet) countForTable(table string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries[table])
}

// clearTable removes all entries for a table from both committed and staged.
func (s *pkSet) clearTable(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, table)
	if s.staged != nil {
		delete(s.staged, table)
	}
}

// tables returns all table names that have entries, sorted.
func (s *pkSet) tables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.entries) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.entries))
	for t := range s.entries {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// stage adds (table, pk) to the staging buffer without affecting committed entries.
func (s *pkSet) stage(table, pk string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.staged == nil {
		s.staged = make(map[string]map[string]bool)
	}
	if s.staged[table] == nil {
		s.staged[table] = make(map[string]bool)
	}
	s.staged[table][pk] = true
}

// commit promotes all staged entries into the committed entries.
func (s *pkSet) commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for table, pks := range s.staged {
		if s.entries[table] == nil {
			s.entries[table] = make(map[string]bool)
		}
		for pk := range pks {
			s.entries[table][pk] = true
		}
	}
	s.staged = nil
}

// rollback discards all staged entries.
func (s *pkSet) rollback() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staged = nil
}

// snapshot returns a map[string][]string representation of the committed entries
// suitable for JSON serialization. PKs are sorted per-table.
func (s *pkSet) snapshot() map[string][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]string, len(s.entries))
	for table, pks := range s.entries {
		keys := make([]string, 0, len(pks))
		for pk := range pks {
			keys = append(keys, pk)
		}
		slices.Sort(keys)
		out[table] = keys
	}
	return out
}

// load populates entries from a deserialized map[string][]string, replacing any existing state.
func (s *pkSet) load(data map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]map[string]bool, len(data))
	for table, pks := range data {
		m := make(map[string]bool, len(pks))
		for _, pk := range pks {
			m[pk] = true
		}
		s.entries[table] = m
	}
}

// snapshotAll returns a merged snapshot of both committed entries and staged entries,
// suitable for savepoint snapshots where we need to capture the full in-flight state.
func (s *pkSet) snapshotAll() map[string][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Merge entries + staged into a single map.
	merged := make(map[string]map[string]bool, len(s.entries))
	for table, pks := range s.entries {
		m := make(map[string]bool, len(pks))
		for pk := range pks {
			m[pk] = true
		}
		merged[table] = m
	}
	for table, pks := range s.staged {
		if merged[table] == nil {
			merged[table] = make(map[string]bool, len(pks))
		}
		for pk := range pks {
			merged[table][pk] = true
		}
	}

	out := make(map[string][]string, len(merged))
	for table, pks := range merged {
		keys := make([]string, 0, len(pks))
		for pk := range pks {
			keys = append(keys, pk)
		}
		slices.Sort(keys)
		out[table] = keys
	}
	return out
}

// restoreAll replaces both committed entries and staged with the given snapshot.
// Staged is cleared because the snapshot already includes everything up to the savepoint.
func (s *pkSet) restoreAll(snap map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]map[string]bool, len(snap))
	for table, pks := range snap {
		m := make(map[string]bool, len(pks))
		for _, pk := range pks {
			m[pk] = true
		}
		s.entries[table] = m
	}
	s.staged = nil
}
