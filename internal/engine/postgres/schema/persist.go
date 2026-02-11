package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	SequencesFile = "sequences.json"
	TablesFile    = "tables.json"
)

// WriteSequences persists the sequence offset data to .mori/sequences.json.
func WriteSequences(moriDir string, offsets map[string]SequenceOffset) error {
	data, err := json.MarshalIndent(offsets, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal sequences: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, SequencesFile), data, 0644)
}

// ReadSequences reads the sequence offset data from .mori/sequences.json.
func ReadSequences(moriDir string) (map[string]SequenceOffset, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, SequencesFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read sequences: %w", err)
	}
	var offsets map[string]SequenceOffset
	if err := json.Unmarshal(data, &offsets); err != nil {
		return nil, fmt.Errorf("failed to parse sequences: %w", err)
	}
	return offsets, nil
}

// WriteTables persists the table metadata to .mori/tables.json.
func WriteTables(moriDir string, tables map[string]TableMeta) error {
	data, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tables: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, TablesFile), data, 0644)
}

// ReadTables reads the table metadata from .mori/tables.json.
func ReadTables(moriDir string) (map[string]TableMeta, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, TablesFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	var tables map[string]TableMeta
	if err := json.Unmarshal(data, &tables); err != nil {
		return nil, fmt.Errorf("failed to parse tables: %w", err)
	}
	return tables, nil
}
