package providers

import (
	"context"
	"fmt"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&planetscaleProvider{}) }

type planetscaleProvider struct{}

func (p *planetscaleProvider) ID() registry.ProviderID { return registry.PlanetScale }

func (p *planetscaleProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *planetscaleProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	port := conn.Port
	if port == 0 {
		port = 3306
	}
	// MySQL DSN format with TLS enabled for PlanetScale.
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=true",
		conn.User, conn.Password, conn.Host, port, conn.Database), nil
}
