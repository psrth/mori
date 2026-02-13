package providers

import (
	"context"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&digitalOceanProvider{}) }

type digitalOceanProvider struct{}

func (p *digitalOceanProvider) ID() registry.ProviderID { return registry.DigitalOcean }

func (p *digitalOceanProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *digitalOceanProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return connWithSSL(conn).ToConnString(), nil
}
