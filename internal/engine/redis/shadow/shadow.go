package shadow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const (
	DefaultImage    = "redis:7"
	DefaultHostPort = 9001
)

// ContainerConfig holds the configuration for creating a Shadow Redis container.
type ContainerConfig struct {
	Image    string
	HostPort int
}

// ContainerInfo holds the result of creating a Shadow container.
type ContainerInfo struct {
	ContainerID   string
	ContainerName string
	HostPort      int
	Image         string
}

// Manager manages the Docker lifecycle for Shadow Redis containers.
type Manager struct{}

// NewManager creates a new Manager and verifies docker is available.
func NewManager() (*Manager, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("docker not found on PATH — is Docker installed?")
	}
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

// Pull pulls the specified Docker image.
func (m *Manager) Pull(ctx context.Context, imageName string) error {
	cmd := exec.CommandContext(ctx, "docker", "pull", imageName)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to pull image %q: %s", imageName, strings.TrimSpace(string(out)))
	}
	return nil
}

// Create creates and starts a Shadow Redis container.
func (m *Manager) Create(ctx context.Context, cfg ContainerConfig) (*ContainerInfo, error) {
	name, err := generateContainerName()
	if err != nil {
		return nil, fmt.Errorf("failed to generate container name: %w", err)
	}

	image := cfg.Image
	if image == "" {
		image = DefaultImage
	}

	hostPort := cfg.HostPort
	if hostPort == 0 {
		hostPort = DefaultHostPort
	}

	portMapping := fmt.Sprintf("127.0.0.1:%d:6379", hostPort)

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-p", portMapping,
		image,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %s", strings.TrimSpace(string(out)))
	}
	containerID := strings.TrimSpace(string(out))

	// Wait for Redis to be ready.
	if err := m.WaitReady(ctx, containerID); err != nil {
		m.StopAndRemove(ctx, containerID)
		return nil, fmt.Errorf("shadow container failed to become ready: %w", err)
	}

	return &ContainerInfo{
		ContainerID:   containerID,
		ContainerName: name,
		HostPort:      hostPort,
		Image:         image,
	}, nil
}

// WaitReady polls the container until Redis is accepting connections.
func (m *Manager) WaitReady(ctx context.Context, containerID string) error {
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for Redis to start (30s)")
		case <-ticker.C:
			if m.isRedisReady(ctx, containerID) {
				return nil
			}
		}
	}
}

func (m *Manager) isRedisReady(ctx context.Context, containerID string) bool {
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID, "redis-cli", "ping")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "PONG"
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
	return "mori-shadow-redis-" + hex.EncodeToString(b), nil
}
