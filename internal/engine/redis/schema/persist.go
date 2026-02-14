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

// KeyMeta holds metadata for a Redis key prefix.
type KeyMeta struct {
	Prefix    string `json:"prefix"`
	Type      string `json:"type"`       // "string", "hash", "list", "set", "zset", "stream", "mixed"
	Count     int    `json:"count"`      // approximate number of keys with this prefix
	PKColumns []string `json:"pk_columns"` // always ["key"] for Redis
	PKType    string `json:"pk_type"`    // always "uuid" for Redis (string keys)
}

// WriteTables persists the key metadata to .mori/tables.json.
func WriteTables(moriDir string, tables map[string]KeyMeta) error {
	data, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tables: %w", err)
	}
	return os.WriteFile(filepath.Join(moriDir, TablesFile), data, 0644)
}

// ReadTables reads the key metadata from .mori/tables.json.
func ReadTables(moriDir string) (map[string]KeyMeta, error) {
	data, err := os.ReadFile(filepath.Join(moriDir, TablesFile))
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	var tables map[string]KeyMeta
	if err := json.Unmarshal(data, &tables); err != nil {
		return nil, fmt.Errorf("failed to parse tables: %w", err)
	}
	return tables, nil
}
