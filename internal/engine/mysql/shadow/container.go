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
	Image    string // Docker image (e.g., "mysql:8.0")
	DBName   string // Database name to create
	Password string // MySQL root password (default: "mori")
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

// Create creates and starts a Shadow MySQL container with a fixed port.
// It waits for MySQL to be ready before returning.
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
	portMapping := fmt.Sprintf("127.0.0.1:%d:3306", hostPort)

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-e", "MYSQL_ROOT_PASSWORD="+password,
		"-e", "MYSQL_DATABASE="+cfg.DBName,
		"-p", portMapping,
		cfg.Image,
		"--default-authentication-plugin=mysql_native_password",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %s", strings.TrimSpace(string(out)))
	}
	// Extract only the last non-empty line as the container ID.
	// CombinedOutput may include Docker warnings (e.g., platform mismatch)
	// before the actual container ID on stdout.
	containerID := extractContainerID(string(out))

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

// GetHostPort inspects a container and returns the host port mapped to 3306.
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

	bindings, ok := ports["3306/tcp"]
	if !ok || len(bindings) == 0 {
		return 0, fmt.Errorf("no port binding found for 3306/tcp")
	}

	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return 0, fmt.Errorf("invalid host port %q: %w", bindings[0].HostPort, err)
	}
	return port, nil
}

// WaitReady polls the container until MySQL is accepting connections.
// Polls every 1s, times out after 60 seconds (MySQL takes longer than PG).
func (m *Manager) WaitReady(ctx context.Context, containerID, password string) error {
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for MySQL to start (60s)")
		case <-ticker.C:
			if m.isMySQLReady(ctx, containerID, password) {
				return nil
			}
		}
	}
}

func (m *Manager) isMySQLReady(ctx context.Context, containerID, password string) bool {
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID,
		"mysqladmin", "ping", "-h", "localhost", "-uroot", "-p"+password, "--silent")
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

// extractContainerID extracts the container ID from docker run output.
// On ARM hosts, docker may prepend a platform mismatch WARNING before the ID.
func extractContainerID(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return line
		}
	}
	return strings.TrimSpace(output)
}

func generateContainerName() (string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "mori-mysql-shadow-" + hex.EncodeToString(b), nil
}
