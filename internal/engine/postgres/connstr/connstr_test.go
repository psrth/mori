package connstr

import (
	"testing"
)

func TestParseURI(t *testing.T) {
	dsn, err := Parse("postgres://alice:secret@prod-host:5433/mydb?sslmode=require")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dsn.Host != "prod-host" {
		t.Errorf("host = %q, want %q", dsn.Host, "prod-host")
	}
	if dsn.Port != 5433 {
		t.Errorf("port = %d, want %d", dsn.Port, 5433)
	}
	if dsn.DBName != "mydb" {
		t.Errorf("dbname = %q, want %q", dsn.DBName, "mydb")
	}
	if dsn.User != "alice" {
		t.Errorf("user = %q, want %q", dsn.User, "alice")
	}
	if dsn.Password != "secret" {
		t.Errorf("password = %q, want %q", dsn.Password, "secret")
	}
	if dsn.SSLMode != "require" {
		t.Errorf("sslmode = %q, want %q", dsn.SSLMode, "require")
	}
}

func TestParseURIDefaults(t *testing.T) {
	dsn, err := Parse("postgres://prod-host/mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dsn.Port != 5432 {
		t.Errorf("port = %d, want default %d", dsn.Port, 5432)
	}
	if dsn.User != "postgres" {
		t.Errorf("user = %q, want default %q", dsn.User, "postgres")
	}
	if dsn.SSLMode != "disable" {
		t.Errorf("sslmode = %q, want default %q", dsn.SSLMode, "disable")
	}
}

func TestParsePostgresqlScheme(t *testing.T) {
	dsn, err := Parse("postgresql://alice@prod-host/mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dsn.Host != "prod-host" {
		t.Errorf("host = %q, want %q", dsn.Host, "prod-host")
	}
}

func TestParseKeyValue(t *testing.T) {
	dsn, err := Parse("host=prod-host port=5433 dbname=mydb user=alice password=secret sslmode=require")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dsn.Host != "prod-host" {
		t.Errorf("host = %q, want %q", dsn.Host, "prod-host")
	}
	if dsn.Port != 5433 {
		t.Errorf("port = %d, want %d", dsn.Port, 5433)
	}
	if dsn.DBName != "mydb" {
		t.Errorf("dbname = %q, want %q", dsn.DBName, "mydb")
	}
	if dsn.User != "alice" {
		t.Errorf("user = %q, want %q", dsn.User, "alice")
	}
	if dsn.Password != "secret" {
		t.Errorf("password = %q, want %q", dsn.Password, "secret")
	}
}

func TestParseMissingHost(t *testing.T) {
	_, err := Parse("postgres:///mydb")
	if err == nil {
		t.Fatal("expected error for missing host")
	}
}

func TestParseMissingDBName(t *testing.T) {
	_, err := Parse("postgres://prod-host")
	if err == nil {
		t.Fatal("expected error for missing dbname")
	}
}

func TestParseEmpty(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Fatal("expected error for empty string")
	}
}

func TestRedactedWithPassword(t *testing.T) {
	dsn, _ := Parse("postgres://alice:secret@prod-host:5432/mydb")
	r := dsn.Redacted()
	if r != "postgres://alice:***@prod-host:5432/mydb" {
		t.Errorf("redacted = %q, want password replaced with ***", r)
	}
}

func TestRedactedWithoutPassword(t *testing.T) {
	dsn, _ := Parse("postgres://alice@prod-host/mydb")
	r := dsn.Redacted()
	if r != "postgres://alice@prod-host:5432/mydb" {
		t.Errorf("redacted = %q", r)
	}
}

func TestShadowDSN(t *testing.T) {
	s := ShadowDSN(54320, "mydb")
	want := "postgres://postgres:mori@localhost:54320/mydb?sslmode=disable"
	if s != want {
		t.Errorf("ShadowDSN = %q, want %q", s, want)
	}
}

func TestPgDumpArgs(t *testing.T) {
	dsn, _ := Parse("postgres://alice:secret@prod-host:5433/mydb")
	args := dsn.PgDumpArgs()
	expected := []string{"-h", "prod-host", "-p", "5433", "-U", "alice", "-d", "mydb", "--schema-only", "--no-owner", "--no-privileges"}
	if len(args) != len(expected) {
		t.Fatalf("PgDumpArgs length = %d, want %d", len(args), len(expected))
	}
	for i := range args {
		if args[i] != expected[i] {
			t.Errorf("PgDumpArgs[%d] = %q, want %q", i, args[i], expected[i])
		}
	}
}

func TestConnString(t *testing.T) {
	dsn, _ := Parse("postgres://alice:secret@prod-host:5433/mydb?sslmode=require")
	s := dsn.ConnString()
	// Parse it back to verify it's valid
	dsn2, err := Parse(s)
	if err != nil {
		t.Fatalf("ConnString produced unparseable result: %v", err)
	}
	if dsn2.Host != dsn.Host || dsn2.Port != dsn.Port || dsn2.DBName != dsn.DBName {
		t.Errorf("round-trip mismatch: got host=%s port=%d db=%s", dsn2.Host, dsn2.Port, dsn2.DBName)
	}
}
