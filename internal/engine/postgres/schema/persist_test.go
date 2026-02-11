package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadSequences(t *testing.T) {
	dir := t.TempDir()
	offsets := map[string]SequenceOffset{
		"users": {
			Column:      "id",
			Type:        "serial",
			ProdMax:     834219,
			ShadowStart: 10834219,
		},
		"orders": {
			Column:      "id",
			Type:        "bigserial",
			ProdMax:     612003,
			ShadowStart: 10612003,
		},
	}

	if err := WriteSequences(dir, offsets); err != nil {
		t.Fatalf("WriteSequences failed: %v", err)
	}

	got, err := ReadSequences(dir)
	if err != nil {
		t.Fatalf("ReadSequences failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got["users"].ShadowStart != 10834219 {
		t.Errorf("users.ShadowStart = %d, want %d", got["users"].ShadowStart, 10834219)
	}
	if got["orders"].ProdMax != 612003 {
		t.Errorf("orders.ProdMax = %d, want %d", got["orders"].ProdMax, 612003)
	}
}

func TestWriteAndReadTables(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]TableMeta{
		"users": {
			PKColumns: []string{"id"},
			PKType:    "serial",
		},
		"user_roles": {
			PKColumns: []string{"user_id", "role_id"},
			PKType:    "composite",
		},
	}

	if err := WriteTables(dir, tables); err != nil {
		t.Fatalf("WriteTables failed: %v", err)
	}

	got, err := ReadTables(dir)
	if err != nil {
		t.Fatalf("ReadTables failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got["users"].PKType != "serial" {
		t.Errorf("users.PKType = %q, want %q", got["users"].PKType, "serial")
	}
	if len(got["user_roles"].PKColumns) != 2 {
		t.Errorf("user_roles PK columns = %d, want 2", len(got["user_roles"].PKColumns))
	}
}

func TestReadSequencesMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadSequences(dir)
	if err == nil {
		t.Fatal("expected error for missing sequences file")
	}
}

func TestReadTablesMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadTables(dir)
	if err == nil {
		t.Fatal("expected error for missing tables file")
	}
}

func TestSequencesFileFormat(t *testing.T) {
	dir := t.TempDir()
	offsets := map[string]SequenceOffset{
		"users": {Column: "id", Type: "serial", ProdMax: 100, ShadowStart: 10000100},
	}
	if err := WriteSequences(dir, offsets); err != nil {
		t.Fatalf("WriteSequences failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, SequencesFile))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Verify it's indented JSON
	s := string(data)
	if s[0] != '{' {
		t.Error("JSON should start with {")
	}
	if !containsSubstr(s, "  ") {
		t.Error("JSON should be indented")
	}
}
