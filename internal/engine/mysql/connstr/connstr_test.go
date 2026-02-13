package connstr

import (
	"testing"
)

func TestParse_GoDSNFormat(t *testing.T) {
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
			name:     "full dsn",
			input:    "myuser:mypass@tcp(dbhost:3307)/mydb",
			wantUser: "myuser",
			wantPass: "mypass",
			wantHost: "dbhost",
			wantPort: 3307,
			wantDB:   "mydb",
		},
		{
			name:     "default port",
			input:    "root:secret@tcp(localhost)/testdb",
			wantUser: "root",
			wantPass: "secret",
			wantHost: "localhost",
			wantPort: 3306,
			wantDB:   "testdb",
		},
		{
			name:     "no password",
			input:    "admin@tcp(10.0.0.1:3306)/app",
			wantUser: "admin",
			wantPass: "",
			wantHost: "10.0.0.1",
			wantPort: 3306,
			wantDB:   "app",
		},
		{
			name:     "with params",
			input:    "user:pass@tcp(host:3306)/db?charset=utf8mb4&parseTime=true",
			wantUser: "user",
			wantPass: "pass",
			wantHost: "host",
			wantPort: 3306,
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
			name:     "full uri",
			input:    "mysql://myuser:mypass@dbhost:3307/mydb",
			wantUser: "myuser",
			wantPass: "mypass",
			wantHost: "dbhost",
			wantPort: 3307,
			wantDB:   "mydb",
		},
		{
			name:     "default port",
			input:    "mysql://root:secret@localhost/testdb",
			wantUser: "root",
			wantPass: "secret",
			wantHost: "localhost",
			wantPort: 3306,
			wantDB:   "testdb",
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
	_, err := Parse("root:pass@tcp(localhost:3306)/")
	if err == nil {
		t.Error("expected error for missing dbname, got nil")
	}
}

func TestDSN_Address(t *testing.T) {
	dsn := &DSN{Host: "example.com", Port: 3307}
	if got := dsn.Address(); got != "example.com:3307" {
		t.Errorf("Address() = %q, want %q", got, "example.com:3307")
	}
}

func TestDSN_GoDSN(t *testing.T) {
	dsn := &DSN{
		User:     "admin",
		Password: "secret",
		Host:     "db.example.com",
		Port:     3306,
		DBName:   "myapp",
	}
	got := dsn.GoDSN()
	if got != "admin:secret@tcp(db.example.com:3306)/myapp" {
		t.Errorf("GoDSN() = %q", got)
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
	want := "root:mori@tcp(127.0.0.1:9001)/testdb"
	if got != want {
		t.Errorf("ShadowDSN() = %q, want %q", got, want)
	}
}

func TestDSN_Redacted(t *testing.T) {
	dsn := &DSN{User: "admin", Password: "secret", Host: "db.example.com", Port: 3306, DBName: "app"}
	got := dsn.Redacted()
	if got != "admin:***@tcp(db.example.com:3306)/app" {
		t.Errorf("Redacted() = %q", got)
	}
}
