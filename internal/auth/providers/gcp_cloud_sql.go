package providers

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/psrth/mori/internal/auth"
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

func init() { auth.Register(&gcpCloudSQLProvider{}) }

type gcpCloudSQLProvider struct{}

func (p *gcpCloudSQLProvider) ID() registry.ProviderID { return registry.GCPCloudSQL }

func (p *gcpCloudSQLProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

// TokenTTL implements auth.Refreshable. GCP Cloud SQL login tokens are valid for ~1 hour.
func (p *gcpCloudSQLProvider) TokenTTL() time.Duration { return 1 * time.Hour }

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

	// No password supplied — obtain a Cloud SQL login token from gcloud.
	// gcloud sql generate-login-token is the officially documented command
	// for Cloud SQL IAM database authentication; it returns a token scoped
	// specifically to Cloud SQL (sqlservice.login) rather than the broader
	// cloud-platform scope returned by gcloud auth print-access-token.
	out, err := exec.CommandContext(ctx, "gcloud", "sql", "generate-login-token").Output()
	if err != nil {
		return "", fmt.Errorf(
			"gcp-cloud-sql: could not obtain login token via gcloud: %s\n"+
				"Make sure the Google Cloud SDK is installed and you are authenticated (gcloud auth login)",
			cliErrorDetail(err),
		)
	}

	c.Password = strings.TrimSpace(string(out))
	return c.ToConnString(), nil
}
