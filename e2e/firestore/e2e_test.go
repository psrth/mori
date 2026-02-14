//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/option"
)

// Global test state set by TestMain, used by all tests.
var (
	proxyAddr      string
	prodEmulatorPort  = 18080
	shadowEmulatorPort = 18081
	proxyPort      = 19010
	projectDir     string
	moriBin        string
	moriProcess    *exec.Cmd
	repoRoot       string
	testProjectID  = "mori-e2e-firestore"
)

func TestMain(m *testing.M) {
	code := runTests(m)
	os.Exit(code)
}

func runTests(m *testing.M) int {
	_, filename, _, _ := runtime.Caller(0)
	repoRoot = filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Create a temp project directory.
	var err error
	projectDir, err = os.MkdirTemp("", "mori-e2e-firestore-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to create temp dir: %v\n", err)
		return 1
	}
	defer os.RemoveAll(projectDir)

	// 2. Build mori binary.
	fmt.Println("SETUP: Building mori binary...")
	moriBin = filepath.Join(projectDir, "mori")
	if err := buildMori(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to build mori: %v\n", err)
		return 1
	}

	// 3. Start prod emulator.
	fmt.Println("SETUP: Starting prod Firestore emulator...")
	if err := startEmulator(ctx, "mori-e2e-prod-firestore", prodEmulatorPort); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to start prod emulator: %v\n", err)
		return 1
	}
	defer stopEmulator("mori-e2e-prod-firestore")

	// 4. Wait for prod emulator to be ready.
	fmt.Println("SETUP: Waiting for prod emulator...")
	if err := waitForEmulator(ctx, prodEmulatorPort, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: prod emulator not ready: %v\n", err)
		return 1
	}

	// 5. Seed prod emulator with test data.
	fmt.Println("SETUP: Seeding prod emulator...")
	if err := seedProdEmulator(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to seed: %v\n", err)
		return 1
	}

	// 6. Write mori.yaml.
	fmt.Println("SETUP: Writing mori.yaml...")
	if err := writeMoriYAML(); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: failed to write mori.yaml: %v\n", err)
		return 1
	}

	// 7. Start mori proxy.
	proxyAddr = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	fmt.Println("SETUP: Starting mori proxy...")
	if err := startMoriProxy(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: mori start failed: %v\n", err)
		return 1
	}
	defer stopMoriProxy()

	// 8. Wait for proxy to be ready.
	fmt.Println("SETUP: Waiting for proxy...")
	if err := waitForEmulator(ctx, proxyPort, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "SETUP: proxy not ready: %v\n", err)
		return 1
	}

	fmt.Println("SETUP: Ready! Running tests...")
	fmt.Println()
	return m.Run()
}

func buildMori(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", moriBin, "./cmd/mori")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func startEmulator(ctx context.Context, name string, port int) error {
	// Remove any existing container with the same name.
	exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()

	cmd := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-p", fmt.Sprintf("127.0.0.1:%d:8080", port),
		"gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators",
		"gcloud", "emulators", "firestore", "start",
		"--host-port", "0.0.0.0:8080",
		"--project", testProjectID,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func stopEmulator(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()
}

func waitForEmulator(ctx context.Context, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(ctx, "tcp", addr)
		if err == nil {
			conn.Close()
			// Give the emulator a moment to fully start.
			time.Sleep(2 * time.Second)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for port %d", port)
}

func seedProdEmulator(ctx context.Context) error {
	emulatorHost := fmt.Sprintf("127.0.0.1:%d", prodEmulatorPort)
	os.Setenv("FIRESTORE_EMULATOR_HOST", emulatorHost)
	client, err := firestore.NewClient(ctx, testProjectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(emulatorHost),
	)
	if err != nil {
		return fmt.Errorf("connect to emulator: %w", err)
	}
	defer client.Close()

	// Seed "users" collection.
	for i := 1; i <= 10; i++ {
		_, err := client.Collection("users").Doc(fmt.Sprintf("user_%d", i)).Set(ctx, map[string]interface{}{
			"name":  fmt.Sprintf("User %d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"age":   20 + i,
		})
		if err != nil {
			return fmt.Errorf("seed user %d: %w", i, err)
		}
	}

	// Seed "products" collection.
	for i := 1; i <= 5; i++ {
		_, err := client.Collection("products").Doc(fmt.Sprintf("prod_%d", i)).Set(ctx, map[string]interface{}{
			"name":  fmt.Sprintf("Product %d", i),
			"price": 10.0 + float64(i)*5.0,
		})
		if err != nil {
			return fmt.Errorf("seed product %d: %w", i, err)
		}
	}

	os.Unsetenv("FIRESTORE_EMULATOR_HOST")
	return nil
}

func writeMoriYAML() error {
	yaml := fmt.Sprintf(`version: 1
connections:
  e2e:
    engine: firestore
    provider: direct
    extra:
      project_id: %s
      emulator_host: "127.0.0.1:%d"
`, testProjectID, prodEmulatorPort)
	return os.WriteFile(filepath.Join(projectDir, "mori.yaml"), []byte(yaml), 0644)
}

func startMoriProxy(ctx context.Context) error {
	moriProcess = exec.CommandContext(ctx, moriBin, "start", "e2e",
		"--port", fmt.Sprintf("%d", proxyPort),
		"--verbose",
	)
	moriProcess.Dir = projectDir
	moriProcess.Stdout = os.Stdout
	moriProcess.Stderr = os.Stderr
	return moriProcess.Start()
}

func stopMoriProxy() {
	if moriProcess != nil && moriProcess.Process != nil {
		moriProcess.Process.Kill()
		moriProcess.Wait()
	}
}

func runMoriCommand(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, moriBin, args...)
	cmd.Dir = projectDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("mori %s failed: %v\nOutput: %s", strings.Join(args, " "), err, string(out))
	}
	return string(out)
}
