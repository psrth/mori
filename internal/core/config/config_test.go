package config

import (
	"os"
	"path/filepath"
	"strings"
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

func TestMySQLConnString_DatabaseInjection(t *testing.T) {
	c := &Connection{
		Engine:   "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Database: "mydb?allowAllFiles=true",
		SSLMode:  "verify-full",
	}
	dsn := c.ToConnString()
	// The injected ?allowAllFiles=true must be stripped.
	if strings.Contains(dsn, "allowAllFiles") {
		t.Errorf("DSN contains injected parameter: %s", dsn)
	}
	// ssl-mode should still be the first (and only) query param.
	if !strings.Contains(dsn, "?ssl-mode=verify-full") {
		t.Errorf("DSN missing ssl-mode param: %s", dsn)
	}
}

func TestMySQLConnString_CleanDatabase(t *testing.T) {
	c := &Connection{
		Engine:   "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Database: "mydb",
		SSLMode:  "verify-full",
	}
	dsn := c.ToConnString()
	if !strings.Contains(dsn, "/mydb?ssl-mode=verify-full") {
		t.Errorf("DSN format unexpected: %s", dsn)
	}
}

func TestSanitizeSSLMode(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"disable", "disable"},
		{"verify-full", "verify-full"},
		{"require", "require"},
		{"verify-full&allowAllFiles=true", ""},
		{"", ""},
		{"bogus", ""},
	}
	for _, tt := range tests {
		got := sanitizeSSLMode(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeSSLMode(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
