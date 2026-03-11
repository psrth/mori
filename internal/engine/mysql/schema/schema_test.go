package schema

import (
	"fmt"
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
			PKType:    "bigserial",
		},
		"tags": {
			PKColumns: []string{"name"},
			PKType:    "none",
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
		extras    []string
		want      string
	}{
		{"auto_increment int", []string{"int"}, []string{"auto_increment"}, "serial"},
		{"auto_increment bigint", []string{"bigint"}, []string{"auto_increment"}, "bigserial"},
		{"varchar pk", []string{"varchar"}, []string{""}, "uuid"},
		{"int no auto", []string{"int"}, []string{""}, "serial"},
		{"composite", []string{"int", "int"}, []string{"", ""}, "composite"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPKType(tt.dataTypes, tt.extras)
			if got != tt.want {
				t.Errorf("classifyPKType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripForeignKeys(t *testing.T) {
	input := `CREATE TABLE users (id INT PRIMARY KEY);
ALTER TABLE posts ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);
CREATE TABLE posts (id INT PRIMARY KEY);`

	got := StripForeignKeys(input)
	if got == input {
		t.Error("StripForeignKeys should have removed FK lines")
	}
	if !contains(got, "CREATE TABLE users") {
		t.Error("StripForeignKeys should keep non-FK lines")
	}
	if contains(got, "FOREIGN KEY") {
		t.Error("StripForeignKeys should remove FK lines")
	}
}

func TestSplitStatements(t *testing.T) {
	input := "CREATE TABLE a (id INT); INSERT INTO a VALUES (1); SELECT 'hello;world'"
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

func TestIsLockConflict(t *testing.T) {
	tests := []struct {
		name string
		err  string
		want bool
	}{
		{"lock wait timeout 1205", "Error 1205: Lock wait timeout exceeded", true},
		{"deadlock 1213", "Error 1213: Deadlock found", true},
		{"lost connection 2013", "Error 2013: Lost connection to MySQL server during query", true},
		{"too many connections 1040", "Error 1040: Too many connections", true},
		{"lock text", "Lock wait timeout exceeded", true},
		{"deadlock text", "Deadlock found when trying to get lock", true},
		{"normal error", "Unknown column 'foo'", false},
		{"syntax error", "You have an error in your SQL syntax", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("%s", tt.err)
			if got := isLockConflict(err); got != tt.want {
				t.Errorf("isLockConflict(%q) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsLockConflict_Nil(t *testing.T) {
	if isLockConflict(nil) {
		t.Error("isLockConflict(nil) = true, want false")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
