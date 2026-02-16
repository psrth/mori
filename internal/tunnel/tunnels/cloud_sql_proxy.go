package tunnels

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
	"github.com/mori-dev/mori/internal/tunnel"
)

func init() { tunnel.Register(&cloudSQLProxyTunnel{}) }

type cloudSQLProxyTunnel struct{}

func (c *cloudSQLProxyTunnel) ID() string          { return "cloud-sql-proxy" }
func (c *cloudSQLProxyTunnel) DisplayName() string { return "Cloud SQL Auth Proxy" }
func (c *cloudSQLProxyTunnel) BinaryName() string  { return "cloud-sql-proxy" }

func (c *cloudSQLProxyTunnel) ConnectivityOptions(_ registry.EngineID, provider registry.ProviderID) []tunnel.ConnectivityOption {
	if provider != registry.GCPCloudSQL {
		return nil
	}
	return []tunnel.ConnectivityOption{
		{Label: "Private IP (via Cloud SQL Auth Proxy)", TunnelType: "cloud-sql-proxy"},
	}
}

func (c *cloudSQLProxyTunnel) Fields() []tunnel.Field {
	return []tunnel.Field{
		{Key: "instance", Label: "Instance connection name", Required: true,
			Placeholder: "project:region:instance"},
	}
}

func (c *cloudSQLProxyTunnel) buildArgs(cfg *config.TunnelConfig) []string {
	return []string{
		cfg.Params["instance"],
		"--port", strconv.Itoa(cfg.LocalPort),
		"--address", "127.0.0.1",
	}
}

func (c *cloudSQLProxyTunnel) Start(ctx context.Context, cfg *config.TunnelConfig) (*tunnel.Process, error) {
	if cfg.Params["instance"] == "" {
		return nil, fmt.Errorf("cloud-sql-proxy: required parameter \"instance\" is missing")
	}

	args := c.buildArgs(cfg)
	cmd := exec.CommandContext(ctx, "cloud-sql-proxy", args...)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start Cloud SQL Auth Proxy: %w", err)
	}

	localAddr := fmt.Sprintf("127.0.0.1:%d", cfg.LocalPort)
	if err := tunnel.WaitForHealthy(localAddr, 30*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("Cloud SQL Auth Proxy failed to become healthy: %w", err)
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
