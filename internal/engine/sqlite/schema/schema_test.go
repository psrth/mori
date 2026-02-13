package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadTables(t *testing.T) {
	dir := t.TempDir()

	tables := map[string]TableMeta{
		"users": {
			PKColumns: []string{"id"},
			PKType:    "serial",
		},
		"posts": {
			PKColumns: []string{"id"},
			PKType:    "serial",
		},
		"tags": {
			PKColumns: []string{"name"},
			PKType:    "uuid",
		},
	}

	if err := WriteTables(dir, tables); err != nil {
		t.Fatalf("WriteTables error: %v", err)
	}

	path := filepath.Join(dir, TablesFile)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("tables.json not created: %v", err)
	}

	got, err := ReadTables(dir)
	if err != nil {
		t.Fatalf("ReadTables error: %v", err)
	}

	if len(got) != len(tables) {
		t.Errorf("table count = %d, want %d", len(got), len(tables))
	}

	for name, want := range tables {
		g, ok := got[name]
		if !ok {
			t.Errorf("table %q not found", name)
			continue
		}
		if g.PKType != want.PKType {
			t.Errorf("table %q PKType = %q, want %q", name, g.PKType, want.PKType)
		}
		if len(g.PKColumns) != len(want.PKColumns) {
			t.Errorf("table %q PKColumns len = %d, want %d", name, len(g.PKColumns), len(want.PKColumns))
		}
	}
}

func TestReadTables_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadTables(dir)
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestClassifyPKType(t *testing.T) {
	tests := []struct {
		name      string
		dataTypes []string
		want      string
	}{
		{"integer pk", []string{"INTEGER"}, "serial"},
		{"text pk", []string{"TEXT"}, "uuid"},
		{"varchar pk", []string{"VARCHAR"}, "uuid"},
		{"bigint pk", []string{"BIGINT"}, "bigserial"},
		{"int pk", []string{"INT"}, "serial"},
		{"composite", []string{"INTEGER", "TEXT"}, "composite"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPKType(tt.dataTypes, "test")
			if got != tt.want {
				t.Errorf("classifyPKType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestComputeOffset(t *testing.T) {
	tests := []struct {
		prodMax int64
		want    int64
	}{
		{0, 10_000_000},
		{100, 10_000_100},
		{2_000_000, 20_000_000},
	}

	for _, tt := range tests {
		got := computeOffset(tt.prodMax)
		if got != tt.want {
			t.Errorf("computeOffset(%d) = %d, want %d", tt.prodMax, got, tt.want)
		}
	}
}

func TestSplitStatements(t *testing.T) {
	input := "CREATE TABLE a (id INTEGER); INSERT INTO a VALUES (1); SELECT 'hello;world'"
	stmts := splitStatements(input)
	if len(stmts) != 3 {
		t.Errorf("splitStatements returned %d statements, want 3", len(stmts))
	}
}
