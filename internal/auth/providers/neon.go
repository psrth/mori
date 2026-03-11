package providers

import (
	"context"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&neonProvider{}) }

type neonProvider struct{}

func (p *neonProvider) ID() registry.ProviderID { return registry.Neon }

func (p *neonProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *neonProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return connWithSSL(conn).ToConnString(), nil
}
