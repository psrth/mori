package providers

import (
	"context"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&supabaseProvider{}) }

type supabaseProvider struct{}

func (p *supabaseProvider) ID() registry.ProviderID { return registry.Supabase }

func (p *supabaseProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *supabaseProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	return connWithSSL(conn).ToConnString(), nil
}
