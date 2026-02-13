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
	Image    string // Docker image (e.g., "mcr.microsoft.com/mssql/server:2022-latest")
	DBName   string // Database name to create
	Password string // SA password (default: "Mori_P@ss1")
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

// Create creates and starts a Shadow MSSQL container with a fixed port.
// It waits for MSSQL to be ready and then creates the database.
func (m *Manager) Create(ctx context.Context, cfg ContainerConfig) (*ContainerInfo, error) {
	name, err := generateContainerName()
	if err != nil {
		return nil, fmt.Errorf("failed to generate container name: %w", err)
	}

	password := cfg.Password
	if password == "" {
		password = "Mori_P@ss1"
	}

	hostPort := cfg.HostPort
	if hostPort == 0 {
		hostPort = 9001
	}
	portMapping := fmt.Sprintf("127.0.0.1:%d:1433", hostPort)

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-e", "ACCEPT_EULA=Y",
		"-e", "SA_PASSWORD="+password,
		"-e", "MSSQL_PID=Express",
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

	// Create the database.
	if err := m.CreateDatabase(ctx, containerID, password, cfg.DBName); err != nil {
		m.StopAndRemove(ctx, containerID)
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return &ContainerInfo{
		ContainerID:   containerID,
		ContainerName: name,
		HostPort:      hostPort,
		Image:         cfg.Image,
	}, nil
}

// GetHostPort inspects a container and returns the host port mapped to 1433.
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

	bindings, ok := ports["1433/tcp"]
	if !ok || len(bindings) == 0 {
		return 0, fmt.Errorf("no port binding found for 1433/tcp")
	}

	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return 0, fmt.Errorf("invalid host port %q: %w", bindings[0].HostPort, err)
	}
	return port, nil
}

// WaitReady polls the container until MSSQL is accepting connections.
// Polls every 2s, times out after 180 seconds (MSSQL under ARM emulation is very slow).
func (m *Manager) WaitReady(ctx context.Context, containerID, password string) error {
	deadline := time.After(180 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for MSSQL to start (180s)")
		case <-ticker.C:
			if m.isMSSQLReady(ctx, containerID, password) {
				return nil
			}
		}
	}
}

func (m *Manager) isMSSQLReady(ctx context.Context, containerID, password string) bool {
	// Try using /opt/mssql-tools18/bin/sqlcmd (newer images) or /opt/mssql-tools/bin/sqlcmd (older images).
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID,
		"/opt/mssql-tools18/bin/sqlcmd",
		"-S", "localhost",
		"-U", "sa",
		"-P", password,
		"-C",
		"-Q", "SELECT 1")
	if cmd.Run() == nil {
		return true
	}

	// Fallback to older path.
	cmd = exec.CommandContext(ctx, "docker", "exec", containerID,
		"/opt/mssql-tools/bin/sqlcmd",
		"-S", "localhost",
		"-U", "sa",
		"-P", password,
		"-Q", "SELECT 1")
	return cmd.Run() == nil
}

// CreateDatabase creates the target database in the MSSQL container.
func (m *Manager) CreateDatabase(ctx context.Context, containerID, password, dbName string) error {
	query := fmt.Sprintf("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '%s') CREATE DATABASE [%s]", dbName, dbName)

	// Try newer sqlcmd path first.
	cmd := exec.CommandContext(ctx, "docker", "exec", containerID,
		"/opt/mssql-tools18/bin/sqlcmd",
		"-S", "localhost",
		"-U", "sa",
		"-P", password,
		"-C",
		"-Q", query)
	if err := cmd.Run(); err == nil {
		return nil
	}

	// Fallback to older path.
	cmd = exec.CommandContext(ctx, "docker", "exec", containerID,
		"/opt/mssql-tools/bin/sqlcmd",
		"-S", "localhost",
		"-U", "sa",
		"-P", password,
		"-Q", query)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create database: %s", strings.TrimSpace(string(out)))
	}
	return nil
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
	return "mori-mssql-shadow-" + hex.EncodeToString(b), nil
}
