package providers

import (
	"context"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&planetscaleProvider{}) }

type planetscaleProvider struct{}

func (p *planetscaleProvider) ID() registry.ProviderID { return registry.PlanetScale }

func (p *planetscaleProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *planetscaleProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return connWithSSL(conn).ToConnString(), nil
}
