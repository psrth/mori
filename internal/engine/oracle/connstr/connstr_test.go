package connstr

import (
	"testing"
)

func TestParse_URIFormat(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantUser    string
		wantPass    string
		wantHost    string
		wantPort    int
		wantService string
	}{
		{
			name:        "full uri",
			input:       "oracle://myuser:mypass@dbhost:1522/ORCL",
			wantUser:    "myuser",
			wantPass:    "mypass",
			wantHost:    "dbhost",
			wantPort:    1522,
			wantService: "ORCL",
		},
		{
			name:        "default port",
			input:       "oracle://system:secret@localhost/XEPDB1",
			wantUser:    "system",
			wantPass:    "secret",
			wantHost:    "localhost",
			wantPort:    1521,
			wantService: "XEPDB1",
		},
		{
			name:        "no password",
			input:       "oracle://admin@10.0.0.1:1521/MYDB",
			wantUser:    "admin",
			wantPass:    "",
			wantHost:    "10.0.0.1",
			wantPort:    1521,
			wantService: "MYDB",
		},
		{
			name:        "with params",
			input:       "oracle://user:pass@host:1521/SVC?ssl=true&timeout=30",
			wantUser:    "user",
			wantPass:    "pass",
			wantHost:    "host",
			wantPort:    1521,
			wantService: "SVC",
		},
		{
			name:        "default user",
			input:       "oracle://:pass@host:1521/SVC",
			wantUser:    "system",
			wantPass:    "pass",
			wantHost:    "host",
			wantPort:    1521,
			wantService: "SVC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			if dsn.User != tt.wantUser {
				t.Errorf("User = %q, want %q", dsn.User, tt.wantUser)
			}
			if dsn.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", dsn.Password, tt.wantPass)
			}
			if dsn.Host != tt.wantHost {
				t.Errorf("Host = %q, want %q", dsn.Host, tt.wantHost)
			}
			if dsn.Port != tt.wantPort {
				t.Errorf("Port = %d, want %d", dsn.Port, tt.wantPort)
			}
			if dsn.ServiceName != tt.wantService {
				t.Errorf("ServiceName = %q, want %q", dsn.ServiceName, tt.wantService)
			}
		})
	}
}

func TestParse_EmptyString(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Error("expected error for empty string, got nil")
	}
}

func TestParse_MissingServiceName(t *testing.T) {
	_, err := Parse("oracle://user:pass@localhost:1521/")
	if err == nil {
		t.Error("expected error for missing service name, got nil")
	}
}

func TestParse_InvalidScheme(t *testing.T) {
	_, err := Parse("postgres://user:pass@localhost/db")
	if err == nil {
		t.Error("expected error for non-oracle scheme, got nil")
	}
}

func TestDSN_Address(t *testing.T) {
	dsn := &DSN{Host: "example.com", Port: 1522}
	if got := dsn.Address(); got != "example.com:1522" {
		t.Errorf("Address() = %q, want %q", got, "example.com:1522")
	}
}

func TestDSN_GoOraDSN(t *testing.T) {
	dsn := &DSN{
		User:        "admin",
		Password:    "secret",
		Host:        "db.example.com",
		Port:        1521,
		ServiceName: "ORCL",
	}
	got := dsn.GoOraDSN()
	if got != "oracle://admin:secret@db.example.com:1521/ORCL" {
		t.Errorf("GoOraDSN() = %q", got)
	}
}

func TestDSN_DockerHost(t *testing.T) {
	tests := []struct {
		host string
		want string
	}{
		{"localhost", "host.docker.internal"},
		{"127.0.0.1", "host.docker.internal"},
		{"db.example.com", "db.example.com"},
	}
	for _, tt := range tests {
		dsn := &DSN{Host: tt.host}
		if got := dsn.DockerHost(); got != tt.want {
			t.Errorf("DockerHost() for %q = %q, want %q", tt.host, got, tt.want)
		}
	}
}

func TestShadowDSN(t *testing.T) {
	got := ShadowDSN(9001, "XEPDB1")
	want := "oracle://mori:mori@127.0.0.1:9001/XEPDB1"
	if got != want {
		t.Errorf("ShadowDSN() = %q, want %q", got, want)
	}
}

func TestDSN_Redacted(t *testing.T) {
	dsn := &DSN{User: "admin", Password: "secret", Host: "db.example.com", Port: 1521, ServiceName: "ORCL"}
	got := dsn.Redacted()
	if got != "oracle://admin:***@db.example.com:1521/ORCL" {
		t.Errorf("Redacted() = %q", got)
	}
}

func TestDSN_Redacted_NoPassword(t *testing.T) {
	dsn := &DSN{User: "admin", Host: "db.example.com", Port: 1521, ServiceName: "ORCL"}
	got := dsn.Redacted()
	if got != "oracle://admin@db.example.com:1521/ORCL" {
		t.Errorf("Redacted() = %q", got)
	}
}
