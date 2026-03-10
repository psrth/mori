package providers

import (
	"context"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&vercelPostgresProvider{}) }

type vercelPostgresProvider struct{}

func (p *vercelPostgresProvider) ID() registry.ProviderID { return registry.VercelPG }

func (p *vercelPostgresProvider) Fields(engine registry.EngineID) []registry.ConnectionField {
	return nil // use registry defaults; wizard's "paste connection string" handles URL paste
}

func (p *vercelPostgresProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	if u := conn.Extra["connection_url"]; u != "" {
		return enforceSSLInURL(u), nil
	}
	return connWithSSL(conn).ToConnString(), nil
}
