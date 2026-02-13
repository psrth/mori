package providers

import (
	"context"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&directProvider{}) }

type directProvider struct{}

func (p *directProvider) ID() registry.ProviderID { return registry.Direct }

func (p *directProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *directProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return conn.ToConnString(), nil
}
