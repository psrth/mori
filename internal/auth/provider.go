// Package auth provides per-provider authentication and connection-string
// building for mori's supported database providers.
package auth

import (
	"context"
	"sync"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

// Provider defines how a hosting provider collects connection fields during
// `mori init` and builds a connection string at runtime for `mori start`.
type Provider interface {
	// ID returns the provider's unique identifier.
	ID() registry.ProviderID

	// Fields returns custom init prompts for this provider and engine
	// combination. Returning nil signals the caller to fall back to
	// registry.FieldsWithProviderDefaults().
	Fields(engine registry.EngineID) []registry.ConnectionField

	// ConnString builds the runtime connection string. It takes a context
	// so cloud providers can shell out to CLIs (gcloud, aws, az) for
	// token generation.
	ConnString(ctx context.Context, conn *config.Connection) (string, error)
}

var (
	mu       sync.RWMutex
	handlers = map[registry.ProviderID]Provider{}
)

// Register adds a provider to the global registry. It is intended to be
// called from provider init() functions.
func Register(p Provider) {
	mu.Lock()
	defer mu.Unlock()
	handlers[p.ID()] = p
}

// Lookup returns the registered Provider for the given ID. If no provider
// has been registered, it returns a defaultProvider that delegates to the
// existing Connection.ToConnString() method and returns nil fields.
func Lookup(id registry.ProviderID) Provider {
	mu.RLock()
	defer mu.RUnlock()
	if p, ok := handlers[id]; ok {
		return p
	}
	return defaultProv
}

// defaultProv is the fallback for unregistered providers.
var defaultProv Provider = &defaultProvider{}

type defaultProvider struct{}

func (d *defaultProvider) ID() registry.ProviderID { return "" }

func (d *defaultProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (d *defaultProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return conn.ToConnString(), nil
}
