package providers

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

func init() { auth.Register(&awsRDSProvider{}) }

type awsRDSProvider struct{}

func (p *awsRDSProvider) ID() registry.ProviderID { return registry.AWSRDS }

func (p *awsRDSProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }

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
		port = 5432
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
			"aws-rds: could not generate IAM auth token: %w\n"+
				"Make sure the AWS CLI is installed and configured (aws configure)",
			err,
		)
	}

	c.Password = strings.TrimSpace(string(out))
	return c.ToConnString(), nil
}
