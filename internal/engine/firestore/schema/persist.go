package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	TablesFile = "tables.json"
)

// CollectionMeta holds metadata for a Firestore collection.
type CollectionMeta struct {
	PKColumns []string `json:"pk_columns"`
	PKType    string   `json:"pk_type"` // "string" for Firestore document IDs (arbitrary strings)
}

// WriteTables persists the collection metadata to .mori/tables.json.
func WriteTables(moriDir string, collections map[string]CollectionMeta) error {
	data, err := json.MarshalIndent(collections, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal collections: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, TablesFile), data, 0644)
}

// ReadTables reads the collection metadata from .mori/tables.json.
func ReadTables(moriDir string) (map[string]CollectionMeta, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, TablesFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	var collections map[string]CollectionMeta
	if err := json.Unmarshal(data, &collections); err != nil {
		return nil, fmt.Errorf("failed to parse tables: %w", err)
	}
	return collections, nil
}
