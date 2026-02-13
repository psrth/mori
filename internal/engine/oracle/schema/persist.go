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

// TableMeta holds primary key metadata for an Oracle table.
type TableMeta struct {
	PKColumns []string `json:"pk_columns"`
	PKType    string   `json:"pk_type"` // "serial", "bigserial", "uuid", "composite", "none"
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
