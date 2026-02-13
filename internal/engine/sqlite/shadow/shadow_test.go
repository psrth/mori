package shadow

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateShadow(t *testing.T) {
	dir := t.TempDir()

	// Create a fake prod database file.
	prodPath := filepath.Join(dir, "prod.db")
	content := []byte("SQLite format 3\x00test data")
	if err := os.WriteFile(prodPath, content, 0644); err != nil {
		t.Fatal(err)
	}

	moriDir := filepath.Join(dir, ".mori")
	if err := os.MkdirAll(moriDir, 0755); err != nil {
		t.Fatal(err)
	}

	shadowPath, err := CreateShadow(prodPath, moriDir)
	if err != nil {
		t.Fatalf("CreateShadow error: %v", err)
	}

	if shadowPath != ShadowPath(moriDir) {
		t.Errorf("shadowPath = %q, want %q", shadowPath, ShadowPath(moriDir))
	}

	// Verify content.
	got, err := os.ReadFile(shadowPath)
	if err != nil {
		t.Fatalf("reading shadow: %v", err)
	}
	if string(got) != string(content) {
		t.Error("shadow content doesn't match prod content")
	}
}

func TestCreateShadow_MissingProd(t *testing.T) {
	dir := t.TempDir()
	moriDir := filepath.Join(dir, ".mori")
	os.MkdirAll(moriDir, 0755)

	_, err := CreateShadow(filepath.Join(dir, "nonexistent.db"), moriDir)
	if err == nil {
		t.Error("expected error for missing prod file, got nil")
	}
}

func TestShadowExists(t *testing.T) {
	dir := t.TempDir()

	if ShadowExists(dir) {
		t.Error("ShadowExists should be false for empty dir")
	}

	// Create shadow file.
	if err := os.WriteFile(ShadowPath(dir), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}
	if !ShadowExists(dir) {
		t.Error("ShadowExists should be true after creating file")
	}
}

func TestShadowPath(t *testing.T) {
	got := ShadowPath("/tmp/.mori")
	want := "/tmp/.mori/shadow.db"
	if got != want {
		t.Errorf("ShadowPath() = %q, want %q", got, want)
	}
}

func TestRemoveShadow(t *testing.T) {
	dir := t.TempDir()
	shadowPath := ShadowPath(dir)
	os.WriteFile(shadowPath, []byte("test"), 0644)
	os.WriteFile(shadowPath+"-journal", []byte("test"), 0644)
	os.WriteFile(shadowPath+"-wal", []byte("test"), 0644)

	if err := RemoveShadow(dir); err != nil {
		t.Fatalf("RemoveShadow error: %v", err)
	}

	if ShadowExists(dir) {
		t.Error("shadow should be removed")
	}
	if _, err := os.Stat(shadowPath + "-journal"); err == nil {
		t.Error("journal should be removed")
	}
	if _, err := os.Stat(shadowPath + "-wal"); err == nil {
		t.Error("wal should be removed")
	}
}
