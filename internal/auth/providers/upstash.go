package providers

import (
	"context"
	"fmt"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&upstashProvider{}) }

type upstashProvider struct{}

func (p *upstashProvider) ID() registry.ProviderID { return registry.Upstash }

func (p *upstashProvider) Fields(engine registry.EngineID) []registry.ConnectionField {
	return nil // use registry defaults with provider overrides
}

func (p *upstashProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	port := conn.Port
	if port == 0 {
		port = 6379
	}
	// rediss:// (double-s) for TLS-enabled Redis connections.
	return fmt.Sprintf("rediss://default:%s@%s:%d", conn.Password, conn.Host, port), nil
}
