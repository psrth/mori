package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIsInitializedFalseForEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if IsInitialized(dir) {
		t.Error("IsInitialized() = true for empty dir, want false")
	}
}

func TestInitDirCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := InitDir(dir); err != nil {
		t.Fatalf("InitDir() error: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, MoriDir))
	if err != nil {
		t.Fatalf("Stat(.mori) error: %v", err)
	}
	if !info.IsDir() {
		t.Error(".mori is not a directory")
	}
}

func TestWriteAndReadConfig(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Truncate(time.Second)

	cfg := &Config{
		ProdConnection:  "postgres://prod:5432/mydb",
		ShadowPort:      54320,
		ShadowContainer: "mori-shadow-test123",
		ShadowImage:     "postgres:16.2",
		Engine:          "postgres",
		EngineVersion:   "16.2",
		ProxyPort:       5432,
		Extensions:      []string{"uuid-ossp", "hstore"},
		InitializedAt:   now,
	}

	if err := WriteConfig(dir, cfg); err != nil {
		t.Fatalf("WriteConfig() error: %v", err)
	}

	if !IsInitialized(dir) {
		t.Error("IsInitialized() = false after WriteConfig, want true")
	}

	got, err := ReadConfig(dir)
	if err != nil {
		t.Fatalf("ReadConfig() error: %v", err)
	}

	if got.ProdConnection != cfg.ProdConnection {
		t.Errorf("ProdConnection = %q, want %q", got.ProdConnection, cfg.ProdConnection)
	}
	if got.ShadowPort != cfg.ShadowPort {
		t.Errorf("ShadowPort = %d, want %d", got.ShadowPort, cfg.ShadowPort)
	}
	if got.Engine != cfg.Engine {
		t.Errorf("Engine = %q, want %q", got.Engine, cfg.Engine)
	}
	if got.EngineVersion != cfg.EngineVersion {
		t.Errorf("EngineVersion = %q, want %q", got.EngineVersion, cfg.EngineVersion)
	}
	if got.ProxyPort != cfg.ProxyPort {
		t.Errorf("ProxyPort = %d, want %d", got.ProxyPort, cfg.ProxyPort)
	}
	if len(got.Extensions) != len(cfg.Extensions) {
		t.Errorf("Extensions count = %d, want %d", len(got.Extensions), len(cfg.Extensions))
	}
}

func TestReadConfigNotInitialized(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadConfig(dir)
	if err == nil {
		t.Error("ReadConfig() on uninitialized dir should return error")
	}
}
