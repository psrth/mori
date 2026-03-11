package tunnel

import (
	"context"
	"testing"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func TestNewManagerNilForNoTunnel(t *testing.T) {
	m, err := NewManager(nil)
	if err != nil {
		t.Fatalf("NewManager(nil) error: %v", err)
	}
	if m != nil {
		t.Error("NewManager(nil) returned non-nil manager")
	}
}

func TestNewManagerNilForNoneType(t *testing.T) {
	m, err := NewManager(&config.TunnelConfig{Type: "none"})
	if err != nil {
		t.Fatalf("NewManager(none) error: %v", err)
	}
	if m != nil {
		t.Error("NewManager(none) returned non-nil manager")
	}
}

func TestNewManagerNilForEmptyType(t *testing.T) {
	m, err := NewManager(&config.TunnelConfig{Type: ""})
	if err != nil {
		t.Fatalf("NewManager(empty) error: %v", err)
	}
	if m != nil {
		t.Error("NewManager(empty) returned non-nil manager")
	}
}

func TestNewManagerUnknownType(t *testing.T) {
	_, err := NewManager(&config.TunnelConfig{Type: "nonexistent-tunnel"})
	if err == nil {
		t.Error("NewManager(nonexistent) returned nil error; want error")
	}
}

func TestLookupUnregistered(t *testing.T) {
	_, ok := Lookup("nonexistent")
	if ok {
		t.Error("Lookup(nonexistent) returned ok=true; want false")
	}
}

func TestRegisterAndLookup(t *testing.T) {
	Register(&mockTunnel{id: "test-mock"})
	t.Cleanup(func() {
		mu.Lock()
		delete(handlers, "test-mock")
		mu.Unlock()
	})

	tun, ok := Lookup("test-mock")
	if !ok {
		t.Fatal("Lookup(test-mock) returned ok=false; want true")
	}
	if tun.ID() != "test-mock" {
		t.Errorf("Lookup(test-mock).ID() = %q; want %q", tun.ID(), "test-mock")
	}
}

func TestFindFreePort(t *testing.T) {
	port, err := FindFreePort()
	if err != nil {
		t.Fatalf("FindFreePort() error: %v", err)
	}
	if port < 1024 || port > 65535 {
		t.Errorf("FindFreePort() = %d; want port in range 1024-65535", port)
	}
	if !IsPortFree(port) {
		t.Errorf("IsPortFree(%d) = false; want true for freshly found port", port)
	}
}

func TestManagerMethodsOnNil(t *testing.T) {
	var m *Manager
	if m.LocalAddr() != "" {
		t.Error("nil Manager.LocalAddr() should return empty string")
	}
	if m.LocalPort() != 0 {
		t.Error("nil Manager.LocalPort() should return 0")
	}
	if m.PID() != 0 {
		t.Error("nil Manager.PID() should return 0")
	}
	if m.DisplayName() != "" {
		t.Error("nil Manager.DisplayName() should return empty string")
	}
	if err := m.Stop(); err != nil {
		t.Errorf("nil Manager.Stop() error: %v", err)
	}
}

// mockTunnel is a minimal Tunnel implementation for testing the registry.
type mockTunnel struct {
	id string
}

func (m *mockTunnel) ID() string          { return m.id }
func (m *mockTunnel) DisplayName() string { return "Mock" }
func (m *mockTunnel) BinaryName() string  { return "true" } // always exists
func (m *mockTunnel) ConnectivityOptions(_ registry.EngineID, _ registry.ProviderID) []ConnectivityOption {
	return nil
}
func (m *mockTunnel) Fields() []Field { return nil }
func (m *mockTunnel) Start(_ context.Context, _ *config.TunnelConfig) (*Process, error) {
	return nil, nil
}
