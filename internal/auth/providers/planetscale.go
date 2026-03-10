package providers

import (
	"context"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&planetscaleProvider{}) }

type planetscaleProvider struct{}

func (p *planetscaleProvider) ID() registry.ProviderID { return registry.PlanetScale }

func (p *planetscaleProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *planetscaleProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return connWithSSL(conn).ToConnString(), nil
}
