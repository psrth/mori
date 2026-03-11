package providers_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/psrth/mori/internal/auth"
	_ "github.com/psrth/mori/internal/auth/providers" // trigger init() registration
	"github.com/psrth/mori/internal/core/config"
	"github.com/psrth/mori/internal/registry"
)

// allProviderIDs lists every ProviderID that must be registered.
var allProviderIDs = []registry.ProviderID{
	registry.Direct,
	registry.GCPCloudSQL,
	registry.AWSRDS,
	registry.Neon,
	registry.Supabase,
	registry.Azure,
	registry.PlanetScale,
	registry.VercelPG,
	registry.MongoAtlas,
	registry.DigitalOcean,
	registry.Railway,
	registry.Upstash,
	registry.Cloudflare,
	registry.Firebase,
}

// TestAllProvidersRegistered verifies that every known ProviderID has a
// non-default handler after the providers package init() runs.
func TestAllProvidersRegistered(t *testing.T) {
	for _, id := range allProviderIDs {
		p := auth.Lookup(id)
		if p.ID() != id {
			t.Errorf("Lookup(%q) returned provider with ID %q; want %q", id, p.ID(), id)
		}
	}
}

// baseConn returns a config.Connection with typical Postgres-style fields.
func baseConn() *config.Connection {
	return &config.Connection{
		Host:     "db.example.com",
		Port:     5432,
		User:     "admin",
		Password: "secret",
		Database: "mydb",
		Extra:    map[string]string{},
	}
}

// --- ConnString tests for providers that don't shell out to CLIs ---

func TestDirect_ConnString(t *testing.T) {
	p := auth.Lookup(registry.Direct)
	conn := baseConn()
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Direct provider passes through as-is; default SSLMode is "disable".
	if !strings.Contains(got, "sslmode=disable") {
		t.Errorf("expected sslmode=disable, got %s", got)
	}
	if !strings.Contains(got, "db.example.com") {
		t.Errorf("expected host in conn string, got %s", got)
	}
}

func TestNeon_ConnString_EnforcesSSL(t *testing.T) {
	p := auth.Lookup(registry.Neon)
	conn := baseConn()
	conn.SSLMode = "" // should be enforced to verify-full
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("Neon should enforce sslmode=verify-full, got %s", got)
	}
}

func TestSupabase_ConnString_EnforcesSSL(t *testing.T) {
	p := auth.Lookup(registry.Supabase)
	conn := baseConn()
	conn.SSLMode = "disable" // should be overridden to verify-full
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("Supabase should enforce sslmode=verify-full, got %s", got)
	}
}

func TestDigitalOcean_ConnString_EnforcesSSL(t *testing.T) {
	p := auth.Lookup(registry.DigitalOcean)
	conn := baseConn()
	conn.SSLMode = ""
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("DigitalOcean should enforce sslmode=verify-full, got %s", got)
	}
}

func TestVercelPostgres_ConnString_EnforcesSSL(t *testing.T) {
	p := auth.Lookup(registry.VercelPG)
	conn := baseConn()
	conn.SSLMode = ""
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("Vercel Postgres should enforce sslmode=verify-full, got %s", got)
	}
}

func TestVercelPostgres_ConnString_URLPassthrough(t *testing.T) {
	p := auth.Lookup(registry.VercelPG)
	conn := baseConn()
	conn.Extra["connection_url"] = "postgres://custom:url@host/db"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// URL without sslmode should have sslmode=verify-full enforced.
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("expected SSL enforcement on URL passthrough, got %s", got)
	}
}

func TestVercelPostgres_ConnString_URLPassthrough_PreservesExistingSSL(t *testing.T) {
	p := auth.Lookup(registry.VercelPG)
	conn := baseConn()
	conn.Extra["connection_url"] = "postgres://custom:url@host/db?sslmode=verify-full"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("expected existing sslmode preserved, got %s", got)
	}
}

func TestVercelPostgres_ConnString_URLPassthrough_UpgradesRequire(t *testing.T) {
	p := auth.Lookup(registry.VercelPG)
	conn := baseConn()
	conn.Extra["connection_url"] = "postgres://user:pass@host/db?sslmode=require"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// sslmode=require is MITM-vulnerable — must be upgraded to verify-full.
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("expected sslmode=require upgraded to verify-full, got %s", got)
	}
}

func TestVercelPostgres_ConnString_URLPassthrough_PreservesVerifyCA(t *testing.T) {
	p := auth.Lookup(registry.VercelPG)
	conn := baseConn()
	conn.Extra["connection_url"] = "postgres://user:pass@host/db?sslmode=verify-ca"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// verify-ca performs certificate chain validation — should be preserved.
	if !strings.Contains(got, "sslmode=verify-ca") {
		t.Errorf("expected sslmode=verify-ca preserved, got %s", got)
	}
}

func TestRailway_ConnString_EnforcesSSL(t *testing.T) {
	p := auth.Lookup(registry.Railway)
	conn := baseConn()
	conn.SSLMode = ""
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("Railway should enforce sslmode=verify-full, got %s", got)
	}
}

func TestRailway_ConnString_URLPassthrough(t *testing.T) {
	p := auth.Lookup(registry.Railway)
	conn := baseConn()
	conn.Extra["connection_url"] = "postgres://railway:pass@host/db"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// URL without sslmode should have sslmode=verify-full enforced.
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("expected SSL enforcement on URL passthrough, got %s", got)
	}
}

func TestPlanetScale_ConnString_MySQLDSN(t *testing.T) {
	p := auth.Lookup(registry.PlanetScale)
	conn := baseConn()
	conn.Engine = "mysql"
	conn.Host = "aws.connect.psdb.cloud"
	conn.Port = 3306
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "tcp(aws.connect.psdb.cloud:3306)") {
		t.Errorf("expected MySQL DSN tcp() format, got %s", got)
	}
	if !strings.Contains(got, "ssl-mode=verify-full") {
		t.Errorf("expected ssl-mode=verify-full in DSN, got %s", got)
	}
}

func TestPlanetScale_ConnString_Postgres(t *testing.T) {
	p := auth.Lookup(registry.PlanetScale)
	conn := baseConn()
	conn.Host = "aws.connect.psdb.cloud"
	conn.Port = 5432
	// Engine unset — defaults to Postgres path in ToConnString.
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(got, "postgres://") {
		t.Errorf("expected postgres:// scheme for PlanetScale Postgres, got %s", got)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("expected sslmode=verify-full, got %s", got)
	}
}

func TestPlanetScale_ConnString_DefaultPort(t *testing.T) {
	p := auth.Lookup(registry.PlanetScale)
	conn := baseConn()
	conn.Engine = "mysql"
	conn.Port = 0 // should default to 3306
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, ":3306)") {
		t.Errorf("expected default port 3306, got %s", got)
	}
}

func TestMongoAtlas_ConnString_SRV(t *testing.T) {
	p := auth.Lookup(registry.MongoAtlas)
	conn := baseConn()
	conn.Host = "cluster0.abc123.mongodb.net"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(got, "mongodb+srv://") {
		t.Errorf("expected mongodb+srv:// scheme, got %s", got)
	}
	if !strings.Contains(got, "retryWrites=true") {
		t.Errorf("expected retryWrites=true, got %s", got)
	}
	if !strings.Contains(got, "w=majority") {
		t.Errorf("expected w=majority, got %s", got)
	}
}

func TestMongoAtlas_ConnString_ClusterExtra(t *testing.T) {
	p := auth.Lookup(registry.MongoAtlas)
	conn := baseConn()
	conn.Host = "" // no host; should use atlas_cluster
	conn.Extra["atlas_cluster"] = "cluster0"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "cluster0.mongodb.net") {
		t.Errorf("expected cluster0.mongodb.net host, got %s", got)
	}
}

func TestMongoAtlas_ConnString_ErrorNoHost(t *testing.T) {
	p := auth.Lookup(registry.MongoAtlas)
	conn := baseConn()
	conn.Host = ""
	// no atlas_cluster either
	_, err := p.ConnString(context.Background(), conn)
	if err == nil {
		t.Fatal("expected error when no host and no atlas_cluster")
	}
}

func TestUpstash_ConnString_Rediss(t *testing.T) {
	p := auth.Lookup(registry.Upstash)
	conn := baseConn()
	conn.Host = "us1-example.upstash.io"
	conn.Port = 6379
	conn.Password = "AXxx123"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(got, "rediss://") {
		t.Errorf("expected rediss:// scheme, got %s", got)
	}
	if !strings.Contains(got, "us1-example.upstash.io:6379") {
		t.Errorf("expected host:port, got %s", got)
	}
}

func TestUpstash_ConnString_DefaultPort(t *testing.T) {
	p := auth.Lookup(registry.Upstash)
	conn := baseConn()
	conn.Port = 0 // should default to 6379
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, ":6379") {
		t.Errorf("expected default port 6379, got %s", got)
	}
}

func TestCloudflare_ConnString(t *testing.T) {
	p := auth.Lookup(registry.Cloudflare)
	conn := baseConn()
	conn.Extra["cf_account_id"] = "acct123"
	conn.Extra["cf_database_id"] = "db456"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "https://api.cloudflare.com/client/v4/accounts/acct123/d1/databases/db456"
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCloudflare_ConnString_ErrorMissingAccountID(t *testing.T) {
	p := auth.Lookup(registry.Cloudflare)
	conn := baseConn()
	conn.Extra["cf_database_id"] = "db456"
	_, err := p.ConnString(context.Background(), conn)
	if err == nil {
		t.Fatal("expected error when cf_account_id is missing")
	}
}

func TestCloudflare_ConnString_ErrorMissingDatabaseID(t *testing.T) {
	p := auth.Lookup(registry.Cloudflare)
	conn := baseConn()
	conn.Extra["cf_account_id"] = "acct123"
	_, err := p.ConnString(context.Background(), conn)
	if err == nil {
		t.Fatal("expected error when cf_database_id is missing")
	}
}

func TestFirebase_ConnString(t *testing.T) {
	p := auth.Lookup(registry.Firebase)
	conn := baseConn()
	conn.Extra["project_id"] = "my-project"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "firestore://my-project" {
		t.Errorf("got %s, want firestore://my-project", got)
	}
}

func TestFirebase_ConnString_WithCredentials(t *testing.T) {
	p := auth.Lookup(registry.Firebase)
	conn := baseConn()
	conn.Extra["project_id"] = "my-project"
	conn.Extra["credentials_file"] = "/path/to/sa.json"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "firestore://my-project?credentials=/path/to/sa.json" {
		t.Errorf("got %s, want firestore://my-project?credentials=/path/to/sa.json", got)
	}
}

func TestFirebase_ConnString_FallbackToDatabase(t *testing.T) {
	p := auth.Lookup(registry.Firebase)
	conn := baseConn()
	conn.Database = "fallback-project"
	// no project_id in Extra — should fall back to Database
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "firestore://fallback-project" {
		t.Errorf("got %s, want firestore://fallback-project", got)
	}
}

func TestFirebase_ConnString_ErrorNoProjectID(t *testing.T) {
	p := auth.Lookup(registry.Firebase)
	conn := &config.Connection{Extra: map[string]string{}}
	_, err := p.ConnString(context.Background(), conn)
	if err == nil {
		t.Fatal("expected error when no project_id or database")
	}
}

// --- Cloud CLI providers: test the password-provided path only ---

func TestAWSRDS_ConnString_WithPassword(t *testing.T) {
	p := auth.Lookup(registry.AWSRDS)
	conn := baseConn()
	conn.Password = "explicit-password"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("AWS RDS should enforce SSL verify-full, got %s", got)
	}
	if !strings.Contains(got, "explicit-password") {
		t.Errorf("expected password in conn string, got %s", got)
	}
}

func TestGCPCloudSQL_ConnString_WithPassword(t *testing.T) {
	p := auth.Lookup(registry.GCPCloudSQL)
	conn := baseConn()
	conn.Password = "explicit-password"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("GCP Cloud SQL should enforce SSL verify-full, got %s", got)
	}
	if !strings.Contains(got, "explicit-password") {
		t.Errorf("expected password in conn string, got %s", got)
	}
}

func TestAzure_ConnString_WithPassword(t *testing.T) {
	p := auth.Lookup(registry.Azure)
	conn := baseConn()
	conn.Password = "explicit-password"
	got, err := p.ConnString(context.Background(), conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "sslmode=verify-full") {
		t.Errorf("Azure should enforce SSL verify-full, got %s", got)
	}
	if !strings.Contains(got, "explicit-password") {
		t.Errorf("expected password in conn string, got %s", got)
	}
}

// --- Fields() tests ---

func TestUpstash_Fields(t *testing.T) {
	p := auth.Lookup(registry.Upstash)
	fields := p.Fields(registry.Redis)
	if fields != nil {
		t.Errorf("Upstash.Fields() returned non-nil, expected nil (uses registry defaults)")
	}
}

func TestFirebase_Fields(t *testing.T) {
	p := auth.Lookup(registry.Firebase)
	fields := p.Fields(registry.Firestore)
	if len(fields) != 2 {
		t.Errorf("Firebase.Fields() returned %d fields, want 2", len(fields))
	}
}

func TestVercelPostgres_Fields(t *testing.T) {
	p := auth.Lookup(registry.VercelPG)
	fields := p.Fields(registry.Postgres)
	if fields != nil {
		t.Errorf("Vercel Postgres.Fields() returned non-nil, expected nil (uses registry defaults)")
	}
}

func TestRailway_Fields(t *testing.T) {
	p := auth.Lookup(registry.Railway)
	fields := p.Fields(registry.Postgres)
	if fields != nil {
		t.Errorf("Railway.Fields() returned non-nil, expected nil (uses registry defaults)")
	}
}

// --- Refreshable interface conformance ---

func TestRefreshableProviders(t *testing.T) {
	cases := []struct {
		id  registry.ProviderID
		ttl time.Duration
	}{
		{registry.AWSRDS, 15 * time.Minute},
		{registry.GCPCloudSQL, 1 * time.Hour},
		{registry.Azure, 1 * time.Hour},
	}
	for _, tc := range cases {
		p := auth.Lookup(tc.id)
		r, ok := p.(auth.Refreshable)
		if !ok {
			t.Errorf("%s does not implement auth.Refreshable", tc.id)
			continue
		}
		if got := r.TokenTTL(); got != tc.ttl {
			t.Errorf("%s.TokenTTL() = %v, want %v", tc.id, got, tc.ttl)
		}
	}
}

func TestStaticProvidersNotRefreshable(t *testing.T) {
	staticProviders := []registry.ProviderID{
		registry.Direct, registry.Neon, registry.Supabase,
		registry.PlanetScale, registry.VercelPG, registry.DigitalOcean,
		registry.Railway, registry.Upstash, registry.Cloudflare,
		registry.Firebase, registry.MongoAtlas,
	}
	for _, id := range staticProviders {
		p := auth.Lookup(id)
		if _, ok := p.(auth.Refreshable); ok {
			t.Errorf("%s should NOT implement auth.Refreshable (static credentials)", id)
		}
	}
}

// Providers that return nil Fields() should fall back to engine defaults.
func TestNilFieldsProviders(t *testing.T) {
	nilFieldProviders := []registry.ProviderID{
		registry.Direct, registry.GCPCloudSQL, registry.AWSRDS,
		registry.Neon, registry.Supabase, registry.Azure,
		registry.PlanetScale, registry.MongoAtlas, registry.DigitalOcean,
		registry.Cloudflare, registry.Upstash, registry.VercelPG, registry.Railway,
	}
	for _, id := range nilFieldProviders {
		p := auth.Lookup(id)
		fields := p.Fields(registry.Postgres)
		if fields != nil {
			t.Errorf("%s.Fields() returned non-nil, expected nil", id)
		}
	}
}
