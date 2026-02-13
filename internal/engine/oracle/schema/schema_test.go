package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadTables(t *testing.T) {
	dir := t.TempDir()

	tables := map[string]TableMeta{
		"USERS": {
			PKColumns: []string{"ID"},
			PKType:    "serial",
		},
		"POSTS": {
			PKColumns: []string{"ID"},
			PKType:    "serial",
		},
		"TAGS": {
			PKColumns: []string{"TAG_ID", "NAME"},
			PKType:    "composite",
		},
		"SESSIONS": {
			PKColumns: []string{"SESSION_ID"},
			PKType:    "uuid",
		},
	}

	if err := WriteTables(dir, tables); err != nil {
		t.Fatalf("WriteTables error: %v", err)
	}

	// Verify file exists.
	path := filepath.Join(dir, TablesFile)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("tables.json not created: %v", err)
	}

	// Read back.
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
		{"number pk", []string{"NUMBER"}, "serial"},
		{"varchar pk", []string{"VARCHAR2"}, "uuid"},
		{"raw pk", []string{"RAW"}, "uuid"},
		{"char pk", []string{"CHAR"}, "uuid"},
		{"composite", []string{"NUMBER", "NUMBER"}, "composite"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPKType(tt.dataTypes)
			if got != tt.want {
				t.Errorf("classifyPKType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripForeignKeys(t *testing.T) {
	input := `CREATE TABLE users (id NUMBER PRIMARY KEY);
ALTER TABLE posts ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);
CREATE TABLE posts (id NUMBER PRIMARY KEY);`

	got := StripForeignKeys(input)
	if got == input {
		t.Error("StripForeignKeys should have removed FK lines")
	}
	if !containsStr(got, "CREATE TABLE users") {
		t.Error("StripForeignKeys should keep non-FK lines")
	}
	if containsStr(got, "FOREIGN KEY") {
		t.Error("StripForeignKeys should remove FK lines")
	}
}

func TestSplitStatements(t *testing.T) {
	input := "CREATE TABLE a (id NUMBER); INSERT INTO a VALUES (1); SELECT 'hello;world' FROM DUAL"
	stmts := splitStatements(input)
	if len(stmts) != 3 {
		t.Errorf("splitStatements returned %d statements, want 3", len(stmts))
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

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
