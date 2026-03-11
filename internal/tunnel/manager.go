package tunnel

import (
	"fmt"
	"net"
	"os/exec"
	"syscall"
	"time"

	"context"

	"github.com/psrth/mori/internal/core/config"
)

// Manager handles tunnel subprocess lifecycle.
type Manager struct {
	process   *Process
	tunnelCfg *config.TunnelConfig
	tunnel    Tunnel
}

// NewManager creates a Manager for the given tunnel config.
// Returns nil, nil if the connection has no tunnel configured.
func NewManager(cfg *config.TunnelConfig) (*Manager, error) {
	if cfg == nil || cfg.Type == "" || cfg.Type == "none" {
		return nil, nil
	}
	t, ok := Lookup(cfg.Type)
	if !ok {
		return nil, fmt.Errorf("unknown tunnel type: %s", cfg.Type)
	}
	if _, err := exec.LookPath(t.BinaryName()); err != nil {
		return nil, fmt.Errorf("%s not found on PATH — is %s installed?",
			t.BinaryName(), t.DisplayName())
	}
	return &Manager{tunnelCfg: cfg, tunnel: t}, nil
}

// Start launches the tunnel and blocks until it is healthy.
func (m *Manager) Start(ctx context.Context) error {
	if m.tunnelCfg.LocalPort == 0 {
		port, err := FindFreePort()
		if err != nil {
			return fmt.Errorf("could not find free port for tunnel: %w", err)
		}
		m.tunnelCfg.LocalPort = port
	}

	if !IsPortFree(m.tunnelCfg.LocalPort) {
		return fmt.Errorf("tunnel local port %d is already in use", m.tunnelCfg.LocalPort)
	}

	process, err := m.tunnel.Start(ctx, m.tunnelCfg)
	if err != nil {
		return err
	}
	m.process = process
	return nil
}

// LocalAddr returns the local address the tunnel is listening on.
func (m *Manager) LocalAddr() string {
	if m == nil || m.process == nil {
		return ""
	}
	return m.process.LocalAddr
}

// LocalPort returns the local port the tunnel is listening on.
func (m *Manager) LocalPort() int {
	if m == nil || m.tunnelCfg == nil {
		return 0
	}
	return m.tunnelCfg.LocalPort
}

// PID returns the tunnel process PID.
func (m *Manager) PID() int {
	if m == nil || m.process == nil {
		return 0
	}
	return m.process.PID
}

// DisplayName returns the tunnel's human-readable name.
func (m *Manager) DisplayName() string {
	if m == nil || m.tunnel == nil {
		return ""
	}
	return m.tunnel.DisplayName()
}

// Done returns a channel that is closed when the tunnel process exits.
func (m *Manager) Done() <-chan error {
	if m == nil || m.process == nil {
		ch := make(chan error)
		return ch
	}
	return m.process.Done
}

// Stop kills the tunnel subprocess gracefully.
func (m *Manager) Stop() error {
	if m == nil || m.process == nil || m.process.Cmd == nil || m.process.Cmd.Process == nil {
		return nil
	}
	_ = m.process.Cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-m.process.Done:
		return nil
	case <-time.After(5 * time.Second):
		return m.process.Cmd.Process.Kill()
	}
}

// WaitForHealthy polls the local address until it accepts TCP connections.
func WaitForHealthy(addr string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("tunnel did not become healthy within %s", timeout)
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err == nil {
				conn.Close()
				return nil
			}
		}
	}
}

// FindFreePort finds an available TCP port on localhost.
func FindFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// IsPortFree checks whether the given port is available on localhost.
func IsPortFree(port int) bool {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	l.Close()
	return true
}
