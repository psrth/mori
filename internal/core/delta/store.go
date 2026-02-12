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

// WriteDeltaMap persists the delta map to .mori/delta.json.
func WriteDeltaMap(moriDir string, m *Map) error {
	data, err := json.MarshalIndent(m.Snapshot(), "", "  ")
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
	var raw map[string][]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse delta map: %w", err)
	}
	m := NewMap()
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
