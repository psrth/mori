package tunnels

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
	"github.com/psrth/mori/internal/tunnel"
)

func init() { tunnel.Register(&sshTunnel{}) }

type sshTunnel struct{}

func (s *sshTunnel) ID() string          { return "ssh" }
func (s *sshTunnel) DisplayName() string { return "SSH Tunnel" }
func (s *sshTunnel) BinaryName() string  { return "ssh" }

func (s *sshTunnel) ConnectivityOptions(_ registry.EngineID, _ registry.ProviderID) []tunnel.ConnectivityOption {
	return []tunnel.ConnectivityOption{
		{Label: "Private IP (via SSH tunnel)", TunnelType: "ssh"},
	}
}

func (s *sshTunnel) Fields() []tunnel.Field {
	return []tunnel.Field{
		{Key: "ssh_host", Label: "SSH bastion host", Required: true, Placeholder: "bastion.example.com"},
		{Key: "ssh_user", Label: "SSH user", Required: true, Default: "ec2-user", Placeholder: "ec2-user"},
		{Key: "ssh_port", Label: "SSH port", Default: "22", Placeholder: "22"},
		{Key: "ssh_key", Label: "SSH key path", Placeholder: "~/.ssh/id_rsa"},
		{Key: "remote_host", Label: "Remote DB host (from bastion)", Required: true, Placeholder: "10.0.0.5"},
		{Key: "remote_port", Label: "Remote DB port", Required: true, Placeholder: "5432"},
	}
}

func (s *sshTunnel) buildArgs(cfg *config.TunnelConfig) []string {
	p := cfg.Params
	sshPort := p["ssh_port"]
	if sshPort == "" {
		sshPort = "22"
	}

	args := []string{
		"-N", "-L",
		fmt.Sprintf("%d:%s:%s", cfg.LocalPort, p["remote_host"], p["remote_port"]),
		fmt.Sprintf("%s@%s", p["ssh_user"], p["ssh_host"]),
		"-p", sshPort,
		"-o", "StrictHostKeyChecking=accept-new",
		"-o", "ServerAliveInterval=30",
		"-o", "ServerAliveCountMax=3",
		"-o", "ExitOnForwardFailure=yes",
	}
	if key := p["ssh_key"]; key != "" {
		args = append(args, "-i", key)
	}
	return args
}

func (s *sshTunnel) Start(ctx context.Context, cfg *config.TunnelConfig) (*tunnel.Process, error) {
	args := s.buildArgs(cfg)
	cmd := exec.CommandContext(ctx, "ssh", args...)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start SSH tunnel: %w", err)
	}

	localAddr := fmt.Sprintf("127.0.0.1:%d", cfg.LocalPort)
	if err := tunnel.WaitForHealthy(localAddr, 15*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("SSH tunnel failed to become healthy: %w", err)
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
