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

func init() { auth.Register(&gcpCloudSQLProvider{}) }

type gcpCloudSQLProvider struct{}

func (p *gcpCloudSQLProvider) ID() registry.ProviderID { return registry.GCPCloudSQL }

func (p *gcpCloudSQLProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

func (p *gcpCloudSQLProvider) ConnString(ctx context.Context, conn *config.Connection) (string, error) {
	c := connWithSSL(conn)

	if c.Password != "" {
		return c.ToConnString(), nil
	}

	if c.Host == "" {
		return "", fmt.Errorf("gcp-cloud-sql: host is required")
	}
	if c.User == "" {
		return "", fmt.Errorf("gcp-cloud-sql: user is required")
	}

	// No password supplied — obtain an access token from gcloud.
	out, err := exec.CommandContext(ctx, "gcloud", "auth", "print-access-token").Output()
	if err != nil {
		return "", fmt.Errorf(
			"gcp-cloud-sql: could not obtain access token via gcloud: %s\n"+
				"Make sure the Google Cloud SDK is installed and you are authenticated (gcloud auth login)",
			cliErrorDetail(err),
		)
	}

	c.Password = strings.TrimSpace(string(out))
	return c.ToConnString(), nil
}
