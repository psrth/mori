package connstr

import (
	"testing"
)

func TestParse_URIFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantUser string
		wantPass string
		wantHost string
		wantPort int
		wantDB   string
	}{
		{
			name:     "full uri with database param",
			input:    "sqlserver://myuser:mypass@dbhost:1434?database=mydb",
			wantUser: "myuser",
			wantPass: "mypass",
			wantHost: "dbhost",
			wantPort: 1434,
			wantDB:   "mydb",
		},
		{
			name:     "default port",
			input:    "sqlserver://sa:secret@localhost?database=testdb",
			wantUser: "sa",
			wantPass: "secret",
			wantHost: "localhost",
			wantPort: 1433,
			wantDB:   "testdb",
		},
		{
			name:     "database in path",
			input:    "sqlserver://admin:pass@10.0.0.1:1433/myapp",
			wantUser: "admin",
			wantPass: "pass",
			wantHost: "10.0.0.1",
			wantPort: 1433,
			wantDB:   "myapp",
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
			if dsn.DBName != tt.wantDB {
				t.Errorf("DBName = %q, want %q", dsn.DBName, tt.wantDB)
			}
		})
	}
}

func TestParse_KeyValueFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantUser string
		wantPass string
		wantHost string
		wantPort int
		wantDB   string
	}{
		{
			name:     "standard key-value",
			input:    "server=dbhost;user id=myuser;password=mypass;database=mydb",
			wantUser: "myuser",
			wantPass: "mypass",
			wantHost: "dbhost",
			wantPort: 1433,
			wantDB:   "mydb",
		},
		{
			name:     "with comma port",
			input:    "server=dbhost,1434;user id=sa;password=secret;database=testdb",
			wantUser: "sa",
			wantPass: "secret",
			wantHost: "dbhost",
			wantPort: 1434,
			wantDB:   "testdb",
		},
		{
			name:     "uid and pwd aliases",
			input:    "server=10.0.0.1;uid=admin;pwd=pass;database=app",
			wantUser: "admin",
			wantPass: "pass",
			wantHost: "10.0.0.1",
			wantPort: 1433,
			wantDB:   "app",
		},
		{
			name:     "initial catalog alias",
			input:    "server=host;user id=sa;password=pass;initial catalog=mydb",
			wantUser: "sa",
			wantPass: "pass",
			wantHost: "host",
			wantPort: 1433,
			wantDB:   "mydb",
		},
		{
			name:     "data source alias",
			input:    "data source=host,1433;user id=sa;password=pass;database=db",
			wantUser: "sa",
			wantPass: "pass",
			wantHost: "host",
			wantPort: 1433,
			wantDB:   "db",
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
			if dsn.DBName != tt.wantDB {
				t.Errorf("DBName = %q, want %q", dsn.DBName, tt.wantDB)
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

func TestParse_MissingDBName(t *testing.T) {
	_, err := Parse("sqlserver://sa:pass@localhost:1433")
	if err == nil {
		t.Error("expected error for missing dbname, got nil")
	}
}

func TestDSN_Address(t *testing.T) {
	dsn := &DSN{Host: "example.com", Port: 1434}
	if got := dsn.Address(); got != "example.com:1434" {
		t.Errorf("Address() = %q, want %q", got, "example.com:1434")
	}
}

func TestDSN_GoDSN(t *testing.T) {
	dsn := &DSN{
		User:     "admin",
		Password: "secret",
		Host:     "db.example.com",
		Port:     1433,
		DBName:   "myapp",
		Params:   make(map[string]string),
	}
	got := dsn.GoDSN()
	// Verify it starts with sqlserver:// and contains the key components.
	if got == "" {
		t.Fatal("GoDSN() returned empty string")
	}
	if !contains(got, "sqlserver://") {
		t.Errorf("GoDSN() missing sqlserver:// prefix: %q", got)
	}
	if !contains(got, "db.example.com:1433") {
		t.Errorf("GoDSN() missing host:port: %q", got)
	}
	if !contains(got, "database=myapp") {
		t.Errorf("GoDSN() missing database param: %q", got)
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
	got := ShadowDSN(9001, "testdb")
	if got == "" {
		t.Fatal("ShadowDSN() returned empty string")
	}
	if !contains(got, "127.0.0.1:9001") {
		t.Errorf("ShadowDSN() missing host:port: %q", got)
	}
	if !contains(got, "database=testdb") {
		t.Errorf("ShadowDSN() missing database: %q", got)
	}
}

func TestDSN_Redacted(t *testing.T) {
	dsn := &DSN{User: "admin", Password: "secret", Host: "db.example.com", Port: 1433, DBName: "app"}
	got := dsn.Redacted()
	if contains(got, "secret") {
		t.Errorf("Redacted() should not contain password: %q", got)
	}
	if !contains(got, "***") {
		t.Errorf("Redacted() should contain ***: %q", got)
	}
}

func TestDSN_Redacted_NoPassword(t *testing.T) {
	dsn := &DSN{User: "admin", Host: "db.example.com", Port: 1433, DBName: "app"}
	got := dsn.Redacted()
	if contains(got, "***") {
		t.Errorf("Redacted() should not contain *** when no password: %q", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
