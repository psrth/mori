package engine

import (
	"fmt"
	"sync"

	"github.com/mori-dev/mori/internal/registry"
)

var (
	mu      sync.RWMutex
	engines = map[registry.EngineID]Engine{}
)

// Register adds an engine to the global engine registry. It is intended to be
// called from engine init() functions.
func Register(e Engine) {
	mu.Lock()
	defer mu.Unlock()
	engines[e.ID()] = e
}

// Lookup returns the registered Engine for the given ID. Returns an error if
// no engine has been registered with that ID.
func Lookup(id registry.EngineID) (Engine, error) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := engines[id]
	if !ok {
		return nil, fmt.Errorf("engine %q is not registered", id)
	}
	return e, nil
}

// IsRegistered reports whether an engine with the given ID has been registered.
func IsRegistered(id registry.EngineID) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := engines[id]
	return ok
}
