package providers

import (
	"context"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&railwayProvider{}) }

type railwayProvider struct{}

func (p *railwayProvider) ID() registry.ProviderID { return registry.Railway }

func (p *railwayProvider) Fields(engine registry.EngineID) []registry.ConnectionField {
	return nil // use registry defaults; wizard's "paste connection string" handles URL paste
}

func (p *railwayProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	if u := conn.Extra["connection_url"]; u != "" {
		return u, nil
	}
	return connWithSSL(conn).ToConnString(), nil
}
