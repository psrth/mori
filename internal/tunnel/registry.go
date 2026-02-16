package tunnel

import (
	"sync"

	"github.com/mori-dev/mori/internal/registry"
)

var (
	mu       sync.RWMutex
	handlers = map[string]Tunnel{}
)

// Register adds a tunnel implementation to the global registry.
// Typically called from init() in each tunnel implementation.
func Register(t Tunnel) {
	mu.Lock()
	defer mu.Unlock()
	handlers[t.ID()] = t
}

// Lookup returns the tunnel implementation for the given type ID.
func Lookup(id string) (Tunnel, bool) {
	mu.RLock()
	defer mu.RUnlock()
	t, ok := handlers[id]
	return t, ok
}

// ConnectivityOptionsFor returns all tunnel options applicable to the given
// engine+provider combination, prepended with the "Public IP (direct)" option.
func ConnectivityOptionsFor(engine registry.EngineID, provider registry.ProviderID) []ConnectivityOption {
	opts := []ConnectivityOption{
		{Label: "Public IP (direct connection)", TunnelType: "none"},
	}
	mu.RLock()
	defer mu.RUnlock()
	for _, t := range handlers {
		tunnelOpts := t.ConnectivityOptions(engine, provider)
		opts = append(opts, tunnelOpts...)
	}
	return opts
}
