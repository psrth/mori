package providers

import (
	"context"
	"fmt"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&upstashProvider{}) }

type upstashProvider struct{}

func (p *upstashProvider) ID() registry.ProviderID { return registry.Upstash }

func (p *upstashProvider) Fields(engine registry.EngineID) []registry.ConnectionField {
	return []registry.ConnectionField{
		{Key: "rest_url", Label: "REST URL (optional)", Placeholder: "https://us1-example.upstash.io"},
		{Key: "rest_token", Label: "REST Token (optional)", Sensitive: true, Placeholder: "AXxx..."},
		{Key: "host", Label: "Host", Required: true, Placeholder: "us1-example.upstash.io"},
		{Key: "port", Label: "Port", Default: "6379", Placeholder: "6379"},
		{Key: "password", Label: "Password", Sensitive: true, Placeholder: "enter password"},
	}
}

func (p *upstashProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	port := conn.Port
	if port == 0 {
		port = 6379
	}
	// rediss:// (double-s) for TLS-enabled Redis connections.
	return fmt.Sprintf("rediss://default:%s@%s:%d", conn.Password, conn.Host, port), nil
}
