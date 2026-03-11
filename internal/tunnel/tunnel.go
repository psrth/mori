// Package tunnel defines the interface and types for network tunnel
// implementations that provide connectivity to private-network databases.
package tunnel

import (
	"context"
	"os/exec"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

// Tunnel defines the contract for a tunnel type implementation.
type Tunnel interface {
	// ID returns the tunnel type's unique identifier (e.g. "ssh", "cloud-sql-proxy").
	ID() string

	// DisplayName returns a human-readable name (e.g. "SSH Tunnel").
	DisplayName() string

	// BinaryName returns the CLI binary needed (e.g. "ssh", "cloud-sql-proxy").
	BinaryName() string

	// ConnectivityOptions returns the options this tunnel contributes to the
	// "How do you connect?" picker for the given engine and provider.
	// Returns nil if this tunnel is not applicable for the combination.
	ConnectivityOptions(engine registry.EngineID, provider registry.ProviderID) []ConnectivityOption

	// Fields returns the prompt fields needed to configure this tunnel.
	Fields() []Field

	// Start launches the tunnel subprocess. Returns when the tunnel is healthy
	// or an error occurs.
	Start(ctx context.Context, cfg *config.TunnelConfig) (*Process, error)
}

// Process tracks a running tunnel subprocess.
type Process struct {
	Cmd       *exec.Cmd
	PID       int
	LocalAddr string      // "127.0.0.1:<local_port>"
	Done      <-chan error // closed when process exits
}

// ConnectivityOption describes one option in the "How do you connect?" picker.
type ConnectivityOption struct {
	Label      string
	TunnelType string
}

// Field describes a prompt field for tunnel configuration during init.
type Field struct {
	Key         string
	Label       string
	Required    bool
	Default     string
	Sensitive   bool
	Placeholder string
	Validate    func(string) error
}
