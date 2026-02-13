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
	urlField := registry.ConnectionField{
		Key:         "connection_url",
		Label:       "Connection URL (optional, paste full URL or leave empty)",
		Placeholder: "postgres://user:pass@host:5432/db?sslmode=require",
	}
	defaults := registry.FieldsWithProviderDefaults(engine, registry.VercelPG)
	return append([]registry.ConnectionField{urlField}, defaults...)
}

func (p *vercelPostgresProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	if u := conn.Extra["connection_url"]; u != "" {
		return u, nil
	}
	return connWithSSL(conn).ToConnString(), nil
}
