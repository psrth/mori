package providers

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&azureProvider{}) }

type azureProvider struct{}

func (p *azureProvider) ID() registry.ProviderID { return registry.Azure }

func (p *azureProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

// TokenTTL implements auth.Refreshable. Azure access tokens are valid for ~1 hour.
func (p *azureProvider) TokenTTL() time.Duration { return 1 * time.Hour }

func (p *azureProvider) ConnString(ctx context.Context, conn *config.Connection) (string, error) {
	c := connWithSSL(conn)

	if c.Password != "" {
		return c.ToConnString(), nil
	}

	// No password supplied — obtain an access token from the Azure CLI.
	// MSSQL (Azure SQL Database) uses a different resource identifier than
	// the OSS RDBMS engines (PostgreSQL, MySQL, MariaDB).
	var azArgs []string
	if registry.EngineID(c.Engine) == registry.MSSQL {
		azArgs = []string{
			"account", "get-access-token",
			"--resource", "https://database.windows.net/",
			"--query", "accessToken",
			"-o", "tsv",
		}
	} else {
		azArgs = []string{
			"account", "get-access-token",
			"--resource-type", "oss-rdbms",
			"--query", "accessToken",
			"-o", "tsv",
		}
	}

	out, err := exec.CommandContext(ctx, "az", azArgs...).Output()
	if err != nil {
		return "", fmt.Errorf(
			"azure: could not obtain access token via az CLI: %s\n"+
				"Make sure the Azure CLI is installed and you are logged in (az login)",
			cliErrorDetail(err),
		)
	}

	c.Password = strings.TrimSpace(string(out))
	return c.ToConnString(), nil
}
