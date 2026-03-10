package providers

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

// cliErrorDetail extracts stderr from an exec.ExitError for better diagnostics.
func cliErrorDetail(err error) string {
	if exitErr, ok := err.(*exec.ExitError); ok && len(exitErr.Stderr) > 0 {
		return strings.TrimSpace(string(exitErr.Stderr))
	}
	return err.Error()
}

func init() { auth.Register(&awsRDSProvider{}) }

type awsRDSProvider struct{}

func (p *awsRDSProvider) ID() registry.ProviderID { return registry.AWSRDS }

func (p *awsRDSProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

// TokenTTL implements auth.Refreshable. AWS RDS IAM tokens are valid for 15 minutes.
func (p *awsRDSProvider) TokenTTL() time.Duration { return 15 * time.Minute }

func (p *awsRDSProvider) ConnString(ctx context.Context, conn *config.Connection) (string, error) {
	c := connWithSSL(conn)

	if c.Password != "" {
		return c.ToConnString(), nil
	}

	// Resolve region, hostname, port, and username for the token request.
	region := conn.Extra["aws_region"]
	if region == "" {
		return "", fmt.Errorf("aws-rds: aws_region is required when no password is set")
	}

	hostname := conn.Host
	if hostname == "" {
		return "", fmt.Errorf("aws-rds: host is required")
	}

	port := conn.Port
	if port == 0 {
		if eng, ok := registry.EngineByID(registry.EngineID(conn.Engine)); ok && eng.DefaultPort != 0 {
			port = eng.DefaultPort
		} else {
			return "", fmt.Errorf("aws-rds: port is required (no default for engine %q)", conn.Engine)
		}
	}

	username := conn.User
	if username == "" {
		return "", fmt.Errorf("aws-rds: user is required")
	}

	out, err := exec.CommandContext(ctx,
		"aws", "rds", "generate-db-auth-token",
		"--hostname", hostname,
		"--port", strconv.Itoa(port),
		"--region", region,
		"--username", username,
	).Output()
	if err != nil {
		return "", fmt.Errorf(
			"aws-rds: could not generate IAM auth token: %s\n"+
				"Make sure the AWS CLI is installed and configured (aws configure)",
			cliErrorDetail(err),
		)
	}

	c.Password = strings.TrimSpace(string(out))
	return c.ToConnString(), nil
}
