package shadow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ContainerConfig holds the configuration for creating a Shadow container.
type ContainerConfig struct {
	Image       string // Docker image (e.g., "gvenzl/oracle-xe:21-slim")
	ServiceName string // Oracle service name
	Password    string // System/app password (default: "mori")
	HostPort    int    // Host port to bind (default: 9001)
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

// Pull pulls the specified Docker image if not already present locally.
func (m *Manager) Pull(ctx context.Context, imageName string) error {
	cmd := exec.CommandContext(ctx, "docker", "pull", imageName)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to pull image %q: %s", imageName, strings.TrimSpace(string(out)))
	}
	return nil
}

// Create creates and starts a Shadow Oracle container with a fixed port.
// It waits for Oracle to be ready before returning.
func (m *Manager) Create(ctx context.Context, cfg ContainerConfig) (*ContainerInfo, error) {
	name, err := generateContainerName()
	if err != nil {
		return nil, fmt.Errorf("failed to generate container name: %w", err)
	}

	password := cfg.Password
	if password == "" {
		password = "mori"
	}

	hostPort := cfg.HostPort
	if hostPort == 0 {
		hostPort = 9001
	}
	portMapping := fmt.Sprintf("127.0.0.1:%d:1521", hostPort)

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-e", "ORACLE_PASSWORD="+password,
		"-e", "APP_USER=mori",
		"-e", "APP_USER_PASSWORD="+password,
		"-p", portMapping,
		cfg.Image,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %s", strings.TrimSpace(string(out)))
	}
	containerID := strings.TrimSpace(string(out))

	if err := m.WaitReady(ctx, containerID, password); err != nil {
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

// GetHostPort inspects a container and returns the host port mapped to 1521.
func (m *Manager) GetHostPort(ctx context.Context, containerID string) (int, error) {
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

	bindings, ok := ports["1521/tcp"]
	if !ok || len(bindings) == 0 {
		return 0, fmt.Errorf("no port binding found for 1521/tcp")
	}

	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return 0, fmt.Errorf("invalid host port %q: %w", bindings[0].HostPort, err)
	}
	return port, nil
}

// WaitReady polls the container until Oracle is accepting connections.
// Polls every 3s, times out after 300 seconds (Oracle XE under ARM emulation is very slow).
func (m *Manager) WaitReady(ctx context.Context, containerID, password string) error {
	deadline := time.After(300 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for Oracle to start (300s)")
		case <-ticker.C:
			if m.isOracleReady(ctx, containerID, password) {
				return nil
			}
		}
	}
}

func (m *Manager) isOracleReady(ctx context.Context, containerID, password string) bool {
	// Use sqlplus to test connectivity to the XEPDB1 pluggable database.
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID,
		"sqlplus", "-s", fmt.Sprintf("system/%s@localhost:1521/XEPDB1", password))
	cmd.Stdin = strings.NewReader("SELECT 1 FROM DUAL;\nEXIT;\n")
	return cmd.Run() == nil
}

// Stop stops a running Shadow container.
func (m *Manager) Stop(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "stop", "-t", "30", containerID)
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
	return "mori-oracle-shadow-" + hex.EncodeToString(b), nil
}
