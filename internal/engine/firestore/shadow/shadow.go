package shadow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"
)

const (
	// EmulatorImage is the Docker image for the Firestore emulator.
	EmulatorImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"
	// EmulatorPort is the default port the emulator listens on inside the container.
	EmulatorPort = 8080
	// DefaultHostPort is the default host port for the shadow emulator.
	DefaultHostPort = 9010
)

// ContainerConfig holds configuration for the Firestore emulator container.
type ContainerConfig struct {
	ProjectID string
	HostPort  int // Host port to bind (default: 9010)
}

// ContainerInfo holds the result of creating the emulator container.
type ContainerInfo struct {
	ContainerID   string
	ContainerName string
	HostPort      int
	Image         string
}

// Manager manages the Docker lifecycle for the Firestore emulator.
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

// Pull pulls the Firestore emulator image if not already present.
// Skips pull if the image is already cached locally (P3 4.5).
func (m *Manager) Pull(ctx context.Context) error {
	// Check if image is already cached.
	checkCmd := exec.CommandContext(ctx, "docker", "image", "inspect", EmulatorImage)
	if err := checkCmd.Run(); err == nil {
		// Image exists locally — skip pull.
		return nil
	}

	cmd := exec.CommandContext(ctx, "docker", "pull", EmulatorImage)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to pull image %q: %s", EmulatorImage, strings.TrimSpace(string(out)))
	}
	return nil
}

// FindExisting checks if a matching emulator container already exists for the project.
// Returns the container info if found, nil otherwise.
func (m *Manager) FindExisting(ctx context.Context, projectID string) (*ContainerInfo, error) {
	// Look for containers with the mori-firestore prefix.
	cmd := exec.CommandContext(ctx, "docker", "ps", "-a",
		"--filter", fmt.Sprintf("name=mori-firestore-%s", sanitizeProjectID(projectID)),
		"--format", "{{.ID}}|{{.Names}}|{{.Status}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, nil // Not found or error — will create new.
	}

	output := strings.TrimSpace(string(out))
	if output == "" {
		return nil, nil
	}

	// Parse the first line.
	lines := strings.Split(output, "\n")
	parts := strings.SplitN(lines[0], "|", 3)
	if len(parts) < 3 {
		return nil, nil
	}

	containerID := parts[0]
	containerName := parts[1]
	containerStatus := parts[2]

	// Check if running.
	isRunning := strings.Contains(strings.ToLower(containerStatus), "up")

	if !isRunning {
		// Try to restart.
		restartCmd := exec.CommandContext(ctx, "docker", "start", containerID)
		if restartOut, err := restartCmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("failed to restart existing container: %s", strings.TrimSpace(string(restartOut)))
		}
	}

	// Get the host port.
	portCmd := exec.CommandContext(ctx, "docker", "port", containerID, fmt.Sprintf("%d", EmulatorPort))
	portOut, err := portCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to get container port: %s", strings.TrimSpace(string(portOut)))
	}
	portStr := strings.TrimSpace(string(portOut))
	// Parse "0.0.0.0:9010" or "127.0.0.1:9010"
	hostPort := DefaultHostPort
	if idx := strings.LastIndex(portStr, ":"); idx >= 0 {
		portPart := portStr[idx+1:]
		if p, err := net.LookupPort("tcp", portPart); err == nil {
			hostPort = p
		} else {
			// Try direct parsing.
			for _, c := range portPart {
				if c < '0' || c > '9' {
					goto useDefault
				}
			}
			if n := 0; true {
				for _, c := range portPart {
					n = n*10 + int(c-'0')
				}
				hostPort = n
			}
		useDefault:
		}
	}

	// Verify health.
	if err := m.WaitReady(ctx, hostPort); err != nil {
		return nil, fmt.Errorf("existing container not healthy: %w", err)
	}

	return &ContainerInfo{
		ContainerID:   containerID,
		ContainerName: containerName,
		HostPort:      hostPort,
		Image:         EmulatorImage,
	}, nil
}

// sanitizeProjectID sanitizes a project ID for Docker container name matching.
func sanitizeProjectID(projectID string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(projectID))
	if len(safe) > 20 {
		safe = safe[:20]
	}
	return safe
}

// Create creates and starts a Firestore emulator container.
func (m *Manager) Create(ctx context.Context, cfg ContainerConfig) (*ContainerInfo, error) {
	name, err := generateContainerName(cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate container name: %w", err)
	}

	hostPort := cfg.HostPort
	if hostPort == 0 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("failed to find free port: %w", err)
		}
		hostPort = listener.Addr().(*net.TCPAddr).Port
		listener.Close()
	}

	portMapping := fmt.Sprintf("127.0.0.1:%d:%d", hostPort, EmulatorPort)

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-p", portMapping,
		EmulatorImage,
		"gcloud", "emulators", "firestore", "start",
		"--host-port", fmt.Sprintf("0.0.0.0:%d", EmulatorPort),
		"--project", cfg.ProjectID,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to create emulator container: %s", strings.TrimSpace(string(out)))
	}
	containerID := strings.TrimSpace(string(out))

	if err := m.WaitReady(ctx, hostPort); err != nil {
		m.StopAndRemove(ctx, containerID)
		return nil, fmt.Errorf("emulator failed to become ready: %w", err)
	}

	return &ContainerInfo{
		ContainerID:   containerID,
		ContainerName: name,
		HostPort:      hostPort,
		Image:         EmulatorImage,
	}, nil
}

// WaitReady polls the emulator until it accepts TCP connections.
func (m *Manager) WaitReady(ctx context.Context, hostPort int) error {
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	addr := fmt.Sprintf("127.0.0.1:%d", hostPort)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for Firestore emulator to start (60s)")
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err == nil {
				conn.Close()
				return nil
			}
		}
	}
}

// StopAndRemove stops and removes the emulator container.
func (m *Manager) StopAndRemove(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "rm", "-f", "-v", containerID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to remove container: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

// IsRunning checks whether the container is currently running.
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

// EmulatorAddr returns the gRPC address for the emulator on the given host port.
func EmulatorAddr(hostPort int) string {
	return fmt.Sprintf("127.0.0.1:%d", hostPort)
}

func generateContainerName(projectID string) (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// Sanitize project ID for Docker container name.
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(projectID))
	if len(safe) > 20 {
		safe = safe[:20]
	}
	return fmt.Sprintf("mori-firestore-%s-%s", safe, hex.EncodeToString(b)), nil
}
