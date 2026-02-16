package tunnels

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
	"github.com/mori-dev/mori/internal/tunnel"
)

func init() { tunnel.Register(&awsSSMTunnel{}) }

type awsSSMTunnel struct{}

func (a *awsSSMTunnel) ID() string          { return "aws-ssm" }
func (a *awsSSMTunnel) DisplayName() string { return "AWS SSM Port Forwarding" }
func (a *awsSSMTunnel) BinaryName() string  { return "aws" }

func (a *awsSSMTunnel) ConnectivityOptions(_ registry.EngineID, provider registry.ProviderID) []tunnel.ConnectivityOption {
	if provider != registry.AWSRDS {
		return nil
	}
	return []tunnel.ConnectivityOption{
		{Label: "Private subnet (via AWS SSM)", TunnelType: "aws-ssm"},
	}
}

func (a *awsSSMTunnel) Fields() []tunnel.Field {
	return []tunnel.Field{
		{Key: "target_instance", Label: "EC2 instance ID (SSM target)", Required: true,
			Placeholder: "i-0abcd1234efgh5678"},
		{Key: "remote_host", Label: "Remote DB host (from target)", Required: true,
			Placeholder: "mydb.cluster-xxx.us-east-1.rds.amazonaws.com"},
		{Key: "remote_port", Label: "Remote DB port", Required: true, Default: "5432",
			Placeholder: "5432"},
		{Key: "region", Label: "AWS region", Placeholder: "us-east-1"},
	}
}

func (a *awsSSMTunnel) buildArgs(cfg *config.TunnelConfig) []string {
	p := cfg.Params
	args := []string{
		"ssm", "start-session",
		"--target", p["target_instance"],
		"--document-name", "AWS-StartPortForwardingSessionToRemoteHost",
		"--parameters", fmt.Sprintf(
			`{"host":["%s"],"portNumber":["%s"],"localPortNumber":["%d"]}`,
			p["remote_host"], p["remote_port"], cfg.LocalPort,
		),
	}
	if region := p["region"]; region != "" {
		args = append(args, "--region", region)
	}
	return args
}

func (a *awsSSMTunnel) Start(ctx context.Context, cfg *config.TunnelConfig) (*tunnel.Process, error) {
	args := a.buildArgs(cfg)
	cmd := exec.CommandContext(ctx, "aws", args...)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start AWS SSM tunnel: %w", err)
	}

	localAddr := fmt.Sprintf("127.0.0.1:%d", cfg.LocalPort)
	if err := tunnel.WaitForHealthy(localAddr, 30*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("AWS SSM tunnel failed to become healthy: %w", err)
	}

	doneCh := make(chan error, 1)
	go func() { doneCh <- cmd.Wait() }()

	return &tunnel.Process{
		Cmd:       cmd,
		PID:       cmd.Process.Pid,
		LocalAddr: localAddr,
		Done:      doneCh,
	}, nil
}
