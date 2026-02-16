package delta

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	// DeltaFile is the name of the delta map persistence file.
	DeltaFile = "delta.json"
	// TombstoneFile is the name of the tombstone set persistence file.
	TombstoneFile = "tombstones.json"
)

// deltaFile is the on-disk format for the delta map, including inserted tables.
type deltaFile struct {
	Deltas         map[string][]string `json:"deltas"`
	InsertedTables map[string]int      `json:"inserted_tables,omitempty"`
}

// deltaFileLegacy is the old on-disk format where InsertedTables was []string.
type deltaFileLegacy struct {
	Deltas         map[string][]string `json:"deltas"`
	InsertedTables []string            `json:"inserted_tables,omitempty"`
}

// WriteDeltaMap persists the delta map to .mori/delta.json.
func WriteDeltaMap(moriDir string, m *Map) error {
	df := deltaFile{
		Deltas:         m.Snapshot(),
		InsertedTables: m.InsertedTablesList(),
	}
	data, err := json.MarshalIndent(df, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal delta map: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, DeltaFile), data, 0644)
}

// ReadDeltaMap reads and deserializes the delta map from .mori/delta.json.
func ReadDeltaMap(moriDir string) (*Map, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, DeltaFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read delta map: %w", err)
	}

	m := NewMap()

	// Try new format first (with inserted_tables as map[string]int).
	var df deltaFile
	if err := json.Unmarshal(data, &df); err == nil && df.Deltas != nil {
		m.Load(df.Deltas)
		if df.InsertedTables != nil {
			m.LoadInsertedTables(df.InsertedTables)
		}
		return m, nil
	}

	// Try legacy format (inserted_tables as []string).
	var legacy deltaFileLegacy
	if err := json.Unmarshal(data, &legacy); err == nil && legacy.Deltas != nil {
		m.Load(legacy.Deltas)
		if len(legacy.InsertedTables) > 0 {
			converted := make(map[string]int, len(legacy.InsertedTables))
			for _, t := range legacy.InsertedTables {
				converted[t] = 0 // count unknown from legacy format
			}
			m.LoadInsertedTables(converted)
		}
		return m, nil
	}

	// Fall back to bare format (plain map[string][]string, no inserted_tables).
	var raw map[string][]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse delta map: %w", err)
	}
	m.Load(raw)
	return m, nil
}

// WriteTombstoneSet persists the tombstone set to .mori/tombstones.json.
func WriteTombstoneSet(moriDir string, t *TombstoneSet) error {
	data, err := json.MarshalIndent(t.Snapshot(), "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tombstone set: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, TombstoneFile), data, 0644)
}

// ReadTombstoneSet reads and deserializes the tombstone set from .mori/tombstones.json.
func ReadTombstoneSet(moriDir string) (*TombstoneSet, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, TombstoneFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read tombstone set: %w", err)
	}
	var raw map[string][]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse tombstone set: %w", err)
	}
	t := NewTombstoneSet()
	t.Load(raw)
	return t, nil
}
