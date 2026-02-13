package providers

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&azureProvider{}) }

type azureProvider struct{}

func (p *azureProvider) ID() registry.ProviderID { return registry.Azure }

func (p *azureProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *azureProvider) ConnString(ctx context.Context, conn *config.Connection) (string, error) {
	c := connWithSSL(conn)

	if c.Password != "" {
		return c.ToConnString(), nil
	}

	// No password supplied — obtain an access token from the Azure CLI.
	out, err := exec.CommandContext(ctx,
		"az", "account", "get-access-token",
		"--resource-type", "oss-rdbms",
		"--query", "accessToken",
		"-o", "tsv",
	).Output()
	if err != nil {
		return "", fmt.Errorf(
			"azure: could not obtain access token via az CLI: %w\n"+
				"Make sure the Azure CLI is installed and you are logged in (az login)",
			err,
		)
	}

	c.Password = strings.TrimSpace(string(out))
	return c.ToConnString(), nil
}
