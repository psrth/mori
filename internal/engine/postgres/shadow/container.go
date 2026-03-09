package shadow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ContainerConfig holds the configuration for creating a Shadow container.
type ContainerConfig struct {
	Image    string // Docker image (e.g., "postgres:16.2")
	DBName   string // Database name to create
	Password string // Postgres password (default: "mori")
	HostPort int    // Host port to bind (default: 9001)
}

// ContainerInfo holds the result of creating a Shadow container.
type ContainerInfo struct {
	ContainerID   string
	ContainerName string
	HostPort      int
	Image         string
}

// Manager manages the Docker lifecycle for Shadow containers via the docker CLI.
type Manager struct{}

// NewManager creates a new Manager and verifies docker is available.
func NewManager() (*Manager, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("docker not found on PATH — is Docker installed?")
	}
	// Verify Docker daemon is reachable
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("cannot connect to Docker daemon — is Docker running? %s", strings.TrimSpace(string(out)))
	}
	return &Manager{}, nil
}

// Close is a no-op for the CLI-based manager.
func (m *Manager) Close() error {
	return nil
}

// ImageExists checks whether a Docker image is available locally.
func (m *Manager) ImageExists(ctx context.Context, imageName string) bool {
	cmd := exec.CommandContext(ctx, "docker", "image", "inspect", imageName)
	return cmd.Run() == nil
}

// Pull pulls the specified Docker image if not already present locally.
func (m *Manager) Pull(ctx context.Context, imageName string) error {
	cmd := exec.CommandContext(ctx, "docker", "pull", imageName)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to pull image %q: %s", imageName, strings.TrimSpace(string(out)))
	}
	return nil
}

// Create creates and starts a Shadow PostgreSQL container with a random port.
// It waits for PostgreSQL to be ready before returning.
func (m *Manager) Create(ctx context.Context, cfg ContainerConfig) (*ContainerInfo, error) {
	name, err := generateContainerName()
	if err != nil {
		return nil, fmt.Errorf("failed to generate container name: %w", err)
	}

	password := cfg.Password
	if password == "" {
		password = "mori"
	}

	// Determine host port (dynamic allocation if not specified).
	hostPort := cfg.HostPort
	if hostPort == 0 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("failed to find free port: %w", err)
		}
		hostPort = listener.Addr().(*net.TCPAddr).Port
		listener.Close()
	}
	portMapping := fmt.Sprintf("127.0.0.1:%d:5432", hostPort)

	// Create and start container with fixed port mapping.
	// Use trust auth since Shadow is a local throwaway database bound to 127.0.0.1.
	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-e", "POSTGRES_PASSWORD="+password,
		"-e", "POSTGRES_DB="+cfg.DBName,
		"-e", "POSTGRES_HOST_AUTH_METHOD=trust",
		"-p", portMapping,
		cfg.Image,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %s", strings.TrimSpace(string(out)))
	}
	containerID := strings.TrimSpace(string(out))

	// Wait for PostgreSQL to be ready
	if err := m.WaitReady(ctx, containerID); err != nil {
		m.StopAndRemove(ctx, containerID)
		return nil, fmt.Errorf("shadow container failed to become ready: %w", err)
	}

	return &ContainerInfo{
		ContainerID:   containerID,
		ContainerName: name,
		HostPort:      hostPort,
		Image:         cfg.Image,
	}, nil
}

func (m *Manager) getHostPort(ctx context.Context, containerID string) (int, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect", containerID,
		"--format", "{{json .NetworkSettings.Ports}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to inspect container: %s", strings.TrimSpace(string(out)))
	}

	var ports map[string][]struct {
		HostIP   string `json:"HostIp"`
		HostPort string `json:"HostPort"`
	}
	if err := json.Unmarshal(out, &ports); err != nil {
		return 0, fmt.Errorf("failed to parse port mapping: %w", err)
	}

	bindings, ok := ports["5432/tcp"]
	if !ok || len(bindings) == 0 {
		return 0, fmt.Errorf("no port binding found for 5432/tcp")
	}

	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return 0, fmt.Errorf("invalid host port %q: %w", bindings[0].HostPort, err)
	}
	return port, nil
}

// WaitReady polls the container until PostgreSQL is accepting connections.
// Polls every 500ms, times out after 30 seconds.
func (m *Manager) WaitReady(ctx context.Context, containerID string) error {
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for PostgreSQL to start (30s)")
		case <-ticker.C:
			if m.isPostgresReady(ctx, containerID) {
				return nil
			}
		}
	}
}

func (m *Manager) isPostgresReady(ctx context.Context, containerID string) bool {
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID, "pg_isready", "-U", "postgres")
	return cmd.Run() == nil
}

// Stop stops a running Shadow container.
func (m *Manager) Stop(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "stop", "-t", "10", containerID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to stop container: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// Remove removes a stopped Shadow container.
func (m *Manager) Remove(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "rm", "-v", containerID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to remove container: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// StopAndRemove stops and removes a Shadow container.
func (m *Manager) StopAndRemove(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "rm", "-f", "-v", containerID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to remove container: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// IsRunning checks whether a container is currently running.
func (m *Manager) IsRunning(ctx context.Context, containerIDOrName string) (bool, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect", containerIDOrName,
		"--format", "{{.State.Running}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "No such") {
			return false, nil
		}
		return false, fmt.Errorf("failed to inspect container: %s", strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)) == "true", nil
}

func generateContainerName() (string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "mori-shadow-" + hex.EncodeToString(b), nil
}
