package connstr

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParse_PlainPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFile string // just the base name, since absolute path depends on cwd
	}{
		{"absolute path", "/tmp/test.db", "/tmp/test.db"},
		{"relative path", "mydb.sqlite", "mydb.sqlite"},
		{"relative with dir", "./data/test.db", "data/test.db"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			if tt.input == "/tmp/test.db" {
				if info.FilePath != "/tmp/test.db" {
					t.Errorf("FilePath = %q, want %q", info.FilePath, "/tmp/test.db")
				}
			} else {
				// Relative paths get resolved to absolute.
				if !filepath.IsAbs(info.FilePath) {
					t.Errorf("FilePath = %q, want absolute path", info.FilePath)
				}
			}
		})
	}
}

func TestParse_FileURI(t *testing.T) {
	info, err := Parse("file:/tmp/test.db?mode=ro&cache=shared")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if info.FilePath != "/tmp/test.db" {
		t.Errorf("FilePath = %q, want %q", info.FilePath, "/tmp/test.db")
	}
	if info.Params["mode"] != "ro" {
		t.Errorf("Params[mode] = %q, want %q", info.Params["mode"], "ro")
	}
	if info.Params["cache"] != "shared" {
		t.Errorf("Params[cache] = %q, want %q", info.Params["cache"], "shared")
	}
}

func TestParse_FileURITripleSlash(t *testing.T) {
	info, err := Parse("file:///tmp/test.db")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if info.FilePath != "/tmp/test.db" {
		t.Errorf("FilePath = %q, want %q", info.FilePath, "/tmp/test.db")
	}
}

func TestParse_SQLiteURI(t *testing.T) {
	info, err := Parse("sqlite:///tmp/test.db?cache=shared")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if info.FilePath != "/tmp/test.db" {
		t.Errorf("FilePath = %q, want %q", info.FilePath, "/tmp/test.db")
	}
	if info.Params["cache"] != "shared" {
		t.Errorf("Params[cache] = %q, want %q", info.Params["cache"], "shared")
	}
}

func TestParse_EmptyString(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Error("expected error for empty string, got nil")
	}
}

func TestConnInfo_DSN(t *testing.T) {
	info := &ConnInfo{FilePath: "/tmp/test.db", Params: map[string]string{}}
	if got := info.DSN(); got != "/tmp/test.db" {
		t.Errorf("DSN() = %q, want %q", got, "/tmp/test.db")
	}
}

func TestConnInfo_ReadOnlyDSN(t *testing.T) {
	info := &ConnInfo{FilePath: "/tmp/test.db", Params: map[string]string{}}
	got := info.ReadOnlyDSN()
	if got != "/tmp/test.db?mode=ro" {
		t.Errorf("ReadOnlyDSN() = %q, want %q", got, "/tmp/test.db?mode=ro")
	}
}

func TestConnInfo_Validate(t *testing.T) {
	// Create a temp file.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	if err := os.WriteFile(dbPath, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	info := &ConnInfo{FilePath: dbPath}
	if err := info.Validate(); err != nil {
		t.Errorf("Validate() for existing file: %v", err)
	}

	info2 := &ConnInfo{FilePath: filepath.Join(dir, "nonexistent.db")}
	if err := info2.Validate(); err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}

	info3 := &ConnInfo{FilePath: dir}
	if err := info3.Validate(); err == nil {
		t.Error("expected error for directory path, got nil")
	}
}
