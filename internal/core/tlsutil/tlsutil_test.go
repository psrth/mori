package tlsutil

import (
	"testing"
)

func TestBuildConfig_Disable(t *testing.T) {
	cfg, err := BuildConfig(TLSParams{SSLMode: "disable"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil config for disable mode")
	}
}

func TestBuildConfig_EmptySSLMode(t *testing.T) {
	cfg, err := BuildConfig(TLSParams{SSLMode: ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil config for empty sslmode")
	}
}

func TestBuildConfig_Require(t *testing.T) {
	cfg, err := BuildConfig(TLSParams{
		SSLMode:    "require",
		ServerName: "db.example.com",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("require mode should set InsecureSkipVerify=true")
	}
	if cfg.ServerName != "db.example.com" {
		t.Errorf("ServerName = %q, want %q", cfg.ServerName, "db.example.com")
	}
}

func TestBuildConfig_VerifyCA_RequiresCACertPath(t *testing.T) {
	_, err := BuildConfig(TLSParams{
		SSLMode:    "verify-ca",
		ServerName: "db.example.com",
	})
	if err == nil {
		t.Fatal("expected error when verify-ca used without CACertPath")
	}
}

func TestBuildConfig_VerifyCA_WithBadPath(t *testing.T) {
	// With a CACertPath set, verify-ca should attempt to load it.
	// A nonexistent path should produce a file-read error, not the
	// "requires CACertPath" error — proving the guard passed.
	_, err := BuildConfig(TLSParams{
		SSLMode:    "verify-ca",
		ServerName: "db.example.com",
		CACertPath: "/nonexistent/ca.pem",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent CA cert path")
	}
}

func TestBuildConfig_VerifyFull(t *testing.T) {
	cfg, err := BuildConfig(TLSParams{
		SSLMode:    "verify-full",
		ServerName: "db.example.com",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.InsecureSkipVerify {
		t.Error("verify-full should not set InsecureSkipVerify")
	}
	if cfg.ServerName != "db.example.com" {
		t.Errorf("ServerName = %q, want %q", cfg.ServerName, "db.example.com")
	}
}

func TestBuildConfig_InvalidMode(t *testing.T) {
	_, err := BuildConfig(TLSParams{SSLMode: "bogus"})
	if err == nil {
		t.Fatal("expected error for invalid sslmode")
	}
}

func TestBuildConfig_ClientCertMissingKey(t *testing.T) {
	_, err := BuildConfig(TLSParams{
		SSLMode:  "require",
		CertPath: "/tmp/cert.pem",
	})
	if err == nil {
		t.Fatal("expected error when CertPath set but KeyPath empty")
	}
}

func TestBuildConfig_ClientCertMissingCert(t *testing.T) {
	_, err := BuildConfig(TLSParams{
		SSLMode: "require",
		KeyPath: "/tmp/key.pem",
	})
	if err == nil {
		t.Fatal("expected error when KeyPath set but CertPath empty")
	}
}
