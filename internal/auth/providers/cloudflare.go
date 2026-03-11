package providers

import (
	"context"
	"fmt"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&cloudflareProvider{}) }

type cloudflareProvider struct{}

func (p *cloudflareProvider) ID() registry.ProviderID { return registry.Cloudflare }

func (p *cloudflareProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *cloudflareProvider) ConnString(_ context.Context, conn *config.Connection) (string, error) {
	accountID := conn.Extra["cf_account_id"]
	if accountID == "" {
		return "", fmt.Errorf("cloudflare: cf_account_id is required")
	}
	databaseID := conn.Extra["cf_database_id"]
	if databaseID == "" {
		return "", fmt.Errorf("cloudflare: cf_database_id is required")
	}
	return fmt.Sprintf(
		"https://api.cloudflare.com/client/v4/accounts/%s/d1/databases/%s",
		accountID, databaseID,
	), nil
}
