package engine

import (
	"context"
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/registry"
)

// mockEngine is a minimal Engine implementation for registry tests.
// Only ID() has a real implementation; other methods are unused in these tests.
type mockEngine struct {
	id registry.EngineID
}

func (m *mockEngine) ID() registry.EngineID { return m.id }

func (m *mockEngine) Init(_ context.Context, _ InitOptions) (*InitResult, error) {
	panic("not implemented")
}

func (m *mockEngine) ParseConnStr(_ string) (*ConnInfo, error) {
	panic("not implemented")
}

func (m *mockEngine) LoadTableMeta(_ string) (map[string]TableMeta, error) {
	panic("not implemented")
}

func (m *mockEngine) NewClassifier(_ map[string]TableMeta) core.Classifier {
	panic("not implemented")
}

func (m *mockEngine) NewProxy(_ ProxyDeps, _ map[string]TableMeta) Proxy {
	panic("not implemented")
}

func TestRegisterAndLookup(t *testing.T) {
	// Clean up the global registry for test isolation.
	mu.Lock()
	saved := engines
	engines = map[registry.EngineID]Engine{}
	mu.Unlock()
	defer func() {
		mu.Lock()
		engines = saved
		mu.Unlock()
	}()

	mock := &mockEngine{id: "test-engine"}
	Register(mock)

	got, err := Lookup("test-engine")
	if err != nil {
		t.Fatalf("Lookup returned unexpected error: %v", err)
	}
	if got.ID() != "test-engine" {
		t.Fatalf("expected ID %q, got %q", "test-engine", got.ID())
	}
}

func TestLookupUnregistered(t *testing.T) {
	mu.Lock()
	saved := engines
	engines = map[registry.EngineID]Engine{}
	mu.Unlock()
	defer func() {
		mu.Lock()
		engines = saved
		mu.Unlock()
	}()

	_, err := Lookup("nonexistent")
	if err == nil {
		t.Fatal("expected error for unregistered engine, got nil")
	}
}

func TestIsRegistered(t *testing.T) {
	mu.Lock()
	saved := engines
	engines = map[registry.EngineID]Engine{}
	mu.Unlock()
	defer func() {
		mu.Lock()
		engines = saved
		mu.Unlock()
	}()

	if IsRegistered("test-engine") {
		t.Fatal("expected IsRegistered to return false before registration")
	}

	Register(&mockEngine{id: "test-engine"})

	if !IsRegistered("test-engine") {
		t.Fatal("expected IsRegistered to return true after registration")
	}
}
