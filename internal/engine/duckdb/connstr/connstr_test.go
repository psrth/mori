package connstr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParse_PlainPath(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")
	os.WriteFile(dbPath, []byte{}, 0644)

	info, err := Parse(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.FilePath != dbPath {
		t.Errorf("FilePath = %q, want %q", info.FilePath, dbPath)
	}
}

func TestParse_DuckDBURI(t *testing.T) {
	info, err := Parse("duckdb:///tmp/test.duckdb")
	if err != nil {
		t.Fatal(err)
	}
	if info.FilePath != "/tmp/test.duckdb" {
		t.Errorf("FilePath = %q, want /tmp/test.duckdb", info.FilePath)
	}
}

func TestParse_InMemory(t *testing.T) {
	info, err := Parse(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if info.FilePath != ":memory:" {
		t.Errorf("FilePath = %q, want :memory:", info.FilePath)
	}
}

func TestParse_WithParams(t *testing.T) {
	info, err := Parse("/tmp/test.duckdb?threads=4")
	if err != nil {
		t.Fatal(err)
	}
	if info.Params["threads"] != "4" {
		t.Errorf("Params[threads] = %q, want 4", info.Params["threads"])
	}
}

func TestParse_Empty(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Error("expected error for empty connection string")
	}
}

func TestValidate_NonExistent(t *testing.T) {
	info := &ConnInfo{FilePath: "/nonexistent/path/db.duckdb"}
	err := info.Validate()
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestValidate_Directory(t *testing.T) {
	tmpDir := t.TempDir()
	info := &ConnInfo{FilePath: tmpDir}
	err := info.Validate()
	if err == nil {
		t.Error("expected error for directory path")
	}
}

func TestValidate_InMemory(t *testing.T) {
	info := &ConnInfo{FilePath: ":memory:"}
	err := info.Validate()
	if err != nil {
		t.Errorf("unexpected error for :memory:: %v", err)
	}
}

func TestDSN(t *testing.T) {
	info := &ConnInfo{
		FilePath: "/tmp/test.duckdb",
		Params:   map[string]string{},
	}
	if info.DSN() != "/tmp/test.duckdb" {
		t.Errorf("DSN() = %q, want /tmp/test.duckdb", info.DSN())
	}
}

func TestDSN_WithParams(t *testing.T) {
	info := &ConnInfo{
		FilePath: "/tmp/test.duckdb",
		Params:   map[string]string{"threads": "4"},
	}
	dsn := info.DSN()
	if !strings.HasPrefix(dsn, "/tmp/test.duckdb?") {
		t.Errorf("DSN() = %q, expected to start with /tmp/test.duckdb?", dsn)
	}
	if !strings.Contains(dsn, "threads=4") {
		t.Errorf("DSN() = %q, expected to contain threads=4", dsn)
	}
}

func TestReadOnlyDSN(t *testing.T) {
	info := &ConnInfo{
		FilePath: "/tmp/test.duckdb",
		Params:   map[string]string{},
	}
	dsn := info.ReadOnlyDSN()
	if !strings.Contains(dsn, "access_mode=READ_ONLY") {
		t.Errorf("ReadOnlyDSN() = %q, expected access_mode=READ_ONLY", dsn)
	}
}

func TestDSN_InMemory(t *testing.T) {
	info := &ConnInfo{
		FilePath: ":memory:",
		Params:   map[string]string{},
	}
	if info.DSN() != ":memory:" {
		t.Errorf("DSN() = %q, want :memory:", info.DSN())
	}
}
