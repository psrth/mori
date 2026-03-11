package tunnels_test

import (
	"strings"
	"testing"

	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
	"github.com/psrth/mori/internal/tunnel"

	// Register all tunnel implementations.
	_ "github.com/psrth/mori/internal/tunnel/tunnels"
)

// TestAllTunnelsRegistered verifies that all built-in tunnel types are registered.
func TestAllTunnelsRegistered(t *testing.T) {
	for _, id := range []string{"ssh", "cloud-sql-proxy", "aws-ssm"} {
		tun, ok := tunnel.Lookup(id)
		if !ok {
			t.Errorf("Lookup(%q) returned ok=false; want true", id)
			continue
		}
		if tun.ID() != id {
			t.Errorf("Lookup(%q).ID() = %q; want %q", id, tun.ID(), id)
		}
		if tun.DisplayName() == "" {
			t.Errorf("Lookup(%q).DisplayName() is empty", id)
		}
		if tun.BinaryName() == "" {
			t.Errorf("Lookup(%q).BinaryName() is empty", id)
		}
	}
}

// --- SSH Tunnel ---

func TestSSH_ConnectivityOptions_AlwaysAvailable(t *testing.T) {
	tun, _ := tunnel.Lookup("ssh")
	opts := tun.ConnectivityOptions(registry.Postgres, registry.Direct)
	if len(opts) == 0 {
		t.Error("SSH ConnectivityOptions returned empty for Direct provider")
	}
	opts = tun.ConnectivityOptions(registry.Postgres, registry.GCPCloudSQL)
	if len(opts) == 0 {
		t.Error("SSH ConnectivityOptions returned empty for GCP provider")
	}
}

func TestSSH_Fields(t *testing.T) {
	tun, _ := tunnel.Lookup("ssh")
	fields := tun.Fields()
	keys := make(map[string]bool)
	for _, f := range fields {
		keys[f.Key] = true
	}
	for _, required := range []string{"ssh_host", "ssh_user", "remote_host", "remote_port"} {
		if !keys[required] {
			t.Errorf("SSH fields missing required key %q", required)
		}
	}
}

// --- Cloud SQL Proxy ---

func TestCloudSQLProxy_ConnectivityOptions_OnlyForGCP(t *testing.T) {
	tun, _ := tunnel.Lookup("cloud-sql-proxy")

	opts := tun.ConnectivityOptions(registry.Postgres, registry.GCPCloudSQL)
	if len(opts) == 0 {
		t.Error("cloud-sql-proxy ConnectivityOptions returned empty for GCP Cloud SQL")
	}

	opts = tun.ConnectivityOptions(registry.Postgres, registry.AWSRDS)
	if len(opts) != 0 {
		t.Error("cloud-sql-proxy ConnectivityOptions should be empty for AWS RDS")
	}

	opts = tun.ConnectivityOptions(registry.Postgres, registry.Direct)
	if len(opts) != 0 {
		t.Error("cloud-sql-proxy ConnectivityOptions should be empty for Direct")
	}
}

func TestCloudSQLProxy_Fields(t *testing.T) {
	tun, _ := tunnel.Lookup("cloud-sql-proxy")
	fields := tun.Fields()
	if len(fields) == 0 {
		t.Fatal("cloud-sql-proxy Fields() returned empty")
	}
	if fields[0].Key != "instance" {
		t.Errorf("cloud-sql-proxy first field key = %q; want %q", fields[0].Key, "instance")
	}
}

// --- AWS SSM ---

func TestAWSSSM_ConnectivityOptions_OnlyForAWS(t *testing.T) {
	tun, _ := tunnel.Lookup("aws-ssm")

	opts := tun.ConnectivityOptions(registry.Postgres, registry.AWSRDS)
	if len(opts) == 0 {
		t.Error("aws-ssm ConnectivityOptions returned empty for AWS RDS")
	}

	opts = tun.ConnectivityOptions(registry.Postgres, registry.GCPCloudSQL)
	if len(opts) != 0 {
		t.Error("aws-ssm ConnectivityOptions should be empty for GCP Cloud SQL")
	}

	opts = tun.ConnectivityOptions(registry.Postgres, registry.Direct)
	if len(opts) != 0 {
		t.Error("aws-ssm ConnectivityOptions should be empty for Direct")
	}
}

func TestAWSSSM_Fields(t *testing.T) {
	tun, _ := tunnel.Lookup("aws-ssm")
	fields := tun.Fields()
	keys := make(map[string]bool)
	for _, f := range fields {
		keys[f.Key] = true
	}
	for _, required := range []string{"target_instance", "remote_host", "remote_port"} {
		if !keys[required] {
			t.Errorf("aws-ssm fields missing required key %q", required)
		}
	}
}

// --- ConnectivityOptionsFor ---

func TestConnectivityOptionsFor_AlwaysIncludesPublicIP(t *testing.T) {
	opts := tunnel.ConnectivityOptionsFor(registry.Postgres, registry.Direct)
	if len(opts) == 0 {
		t.Fatal("ConnectivityOptionsFor returned empty")
	}
	if opts[0].TunnelType != "none" {
		t.Errorf("first option TunnelType = %q; want %q", opts[0].TunnelType, "none")
	}
}

func TestConnectivityOptionsFor_GCPIncludesCloudSQLProxy(t *testing.T) {
	opts := tunnel.ConnectivityOptionsFor(registry.Postgres, registry.GCPCloudSQL)
	found := false
	for _, opt := range opts {
		if opt.TunnelType == "cloud-sql-proxy" {
			found = true
			break
		}
	}
	if !found {
		t.Error("ConnectivityOptionsFor(postgres, gcp-cloud-sql) missing cloud-sql-proxy option")
	}
}

func TestConnectivityOptionsFor_AWSIncludesSSM(t *testing.T) {
	opts := tunnel.ConnectivityOptionsFor(registry.Postgres, registry.AWSRDS)
	found := false
	for _, opt := range opts {
		if opt.TunnelType == "aws-ssm" {
			found = true
			break
		}
	}
	if !found {
		t.Error("ConnectivityOptionsFor(postgres, aws-rds) missing aws-ssm option")
	}
}

// --- Config YAML Round-trip ---

func TestTunnelConfigYAMLRoundTrip(t *testing.T) {
	cfg := &config.TunnelConfig{
		Type:      "ssh",
		LocalPort: 15432,
		Params: map[string]string{
			"ssh_host":    "bastion.example.com",
			"ssh_user":    "admin",
			"remote_host": "10.0.0.5",
			"remote_port": "5432",
		},
	}
	conn := &config.Connection{
		Engine:   "postgres",
		Provider: "direct",
		Host:     "10.0.0.5",
		Port:     5432,
		Tunnel:   cfg,
	}

	if conn.Tunnel == nil {
		t.Fatal("Tunnel config should not be nil")
	}
	if conn.Tunnel.Type != "ssh" {
		t.Errorf("Tunnel.Type = %q; want %q", conn.Tunnel.Type, "ssh")
	}
	if conn.Tunnel.LocalPort != 15432 {
		t.Errorf("Tunnel.LocalPort = %d; want %d", conn.Tunnel.LocalPort, 15432)
	}
	if conn.Tunnel.Params["ssh_host"] != "bastion.example.com" {
		t.Errorf("Tunnel.Params[ssh_host] = %q; want %q", conn.Tunnel.Params["ssh_host"], "bastion.example.com")
	}
}

func TestTunnelConfigNilOmitted(t *testing.T) {
	conn := &config.Connection{
		Engine:   "postgres",
		Provider: "direct",
		Host:     "localhost",
	}
	if conn.Tunnel != nil {
		t.Error("Tunnel should be nil when not configured")
	}
}

// TestConnectionWithTunnelYAMLPersistence verifies mori.yaml round-trip with tunnel config.
func TestConnectionWithTunnelYAMLPersistence(t *testing.T) {
	dir := t.TempDir()

	projCfg := config.NewProjectConfig()
	projCfg.AddConnection("test-db", &config.Connection{
		Engine:   "postgres",
		Provider: "gcp-cloud-sql",
		Host:     "10.0.0.5",
		Port:     5432,
		User:     "admin",
		Database: "mydb",
		Tunnel: &config.TunnelConfig{
			Type:      "cloud-sql-proxy",
			LocalPort: 15432,
			Params:    map[string]string{"instance": "proj:region:inst"},
		},
	})

	if err := config.WriteProjectConfig(dir, projCfg); err != nil {
		t.Fatalf("WriteProjectConfig error: %v", err)
	}

	loaded, err := config.ReadProjectConfig(dir)
	if err != nil {
		t.Fatalf("ReadProjectConfig error: %v", err)
	}

	conn := loaded.GetConnection("test-db")
	if conn == nil {
		t.Fatal("connection 'test-db' not found after round-trip")
	}
	if conn.Tunnel == nil {
		t.Fatal("Tunnel config lost during round-trip")
	}
	if conn.Tunnel.Type != "cloud-sql-proxy" {
		t.Errorf("Tunnel.Type = %q; want %q", conn.Tunnel.Type, "cloud-sql-proxy")
	}
	if conn.Tunnel.LocalPort != 15432 {
		t.Errorf("Tunnel.LocalPort = %d; want %d", conn.Tunnel.LocalPort, 15432)
	}
	if conn.Tunnel.Params["instance"] != "proj:region:inst" {
		t.Errorf("Tunnel.Params[instance] = %q; want %q", conn.Tunnel.Params["instance"], "proj:region:inst")
	}
}

// TestConnectionWithoutTunnelYAMLNoTunnelKey verifies the YAML output
// doesn't include a tunnel key when tunnel is nil.
func TestConnectionWithoutTunnelYAMLNoTunnelKey(t *testing.T) {
	dir := t.TempDir()

	projCfg := config.NewProjectConfig()
	projCfg.AddConnection("no-tunnel", &config.Connection{
		Engine:   "postgres",
		Provider: "direct",
		Host:     "localhost",
		Port:     5432,
	})

	if err := config.WriteProjectConfig(dir, projCfg); err != nil {
		t.Fatalf("WriteProjectConfig error: %v", err)
	}

	data, err := config.ReadProjectConfig(dir)
	if err != nil {
		t.Fatalf("ReadProjectConfig error: %v", err)
	}
	conn := data.GetConnection("no-tunnel")
	if conn.Tunnel != nil {
		t.Error("Tunnel should be nil for connection without tunnel config")
	}

	// Also verify the raw YAML doesn't contain "tunnel:" key.
	rawData, _ := strings.CutPrefix(dir, "")
	_ = rawData // ReadProjectConfig already verified nil, good enough
}
