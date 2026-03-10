// Package auth provides per-provider authentication and connection-string
// building for mori's supported database providers.
package auth

import (
	"context"
	"sync"
	"time"

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

// Refreshable is an optional interface that token-based providers implement
// to declare how long their generated tokens remain valid. Providers that do
// not implement this interface are assumed to have static credentials.
type Refreshable interface {
	TokenTTL() time.Duration
}

// ConnStringFunc returns a (possibly cached) connection string.
// It is safe for concurrent use.
type ConnStringFunc func(ctx context.Context) (string, error)

// NewConnStringFunc wraps a Provider's ConnString method with TTL-based
// caching. If the provider implements Refreshable, the cached value expires
// after 80% of TokenTTL() to avoid using a token right at expiry. Static
// providers are cached indefinitely after the first call.
//
// The mutex is NOT held during the (potentially slow) ConnString call to
// avoid serializing all callers behind a subprocess (aws/gcloud/az). Instead
// we use a refreshing flag to let one goroutine refresh while others return
// the stale cached value.
func NewConnStringFunc(p Provider, conn *config.Connection) ConnStringFunc {
	var (
		mu        sync.Mutex
		cached    string
		cachedErr error
		expiresAt time.Time
		refreshing bool
	)
	cond := sync.NewCond(&mu)

	ttl := time.Duration(0)
	if r, ok := p.(Refreshable); ok {
		ttl = r.TokenTTL()
	}

	return func(ctx context.Context) (string, error) {
		mu.Lock()

	retry:
		now := time.Now()
		valid := cached != "" && (ttl == 0 || now.Before(expiresAt))

		// Fast path: cache hit.
		if valid {
			s := cached
			mu.Unlock()
			return s, nil
		}

		if refreshing {
			// Another goroutine is already refreshing. If we have a stale
			// value, return it immediately. Otherwise wait for the result.
			if cached != "" {
				s := cached
				mu.Unlock()
				return s, nil
			}
			cond.Wait() // unlocks mu, sleeps, re-locks mu on wake
			// Re-check: the refresher may have succeeded or failed.
			if cached != "" {
				s := cached
				mu.Unlock()
				return s, nil
			}
			if cachedErr != nil {
				err := cachedErr
				mu.Unlock()
				return "", err
			}
			// Refresher failed with no cached value and error was cleared
			// by a new refresh attempt — loop back.
			goto retry
		}

		// We are the refreshing goroutine.
		refreshing = true
		cachedErr = nil
		stale := cached
		mu.Unlock()

		// Call ConnString WITHOUT holding the lock.
		s, err := p.ConnString(ctx, conn)

		mu.Lock()
		refreshing = false

		if err != nil {
			cachedErr = err
			cond.Broadcast()

			// If we have a stale cached value, return it — the token may
			// still be accepted for a few more minutes, avoiding a hard
			// failure from a transient IAM/metadata-service outage.
			if stale != "" {
				mu.Unlock()
				return stale, nil
			}
			mu.Unlock()
			return "", err
		}

		cached = s
		if ttl > 0 {
			expiresAt = now.Add(ttl * 4 / 5)
		}
		cond.Broadcast()
		mu.Unlock()
		return s, nil
	}
}
