package connstr

import (
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantHost string
		wantPort int
		wantUser string
		wantPW   string
		wantDB   int
		wantSSL  bool
		wantErr  bool
	}{
		{
			name:     "full redis URI",
			input:    "redis://:secret@myhost:6380/2",
			wantHost: "myhost",
			wantPort: 6380,
			wantPW:   "secret",
			wantDB:   2,
		},
		{
			name:     "redis URI defaults",
			input:    "redis://localhost",
			wantHost: "localhost",
			wantPort: 6379,
			wantDB:   0,
		},
		{
			name:     "redis URI with password no db",
			input:    "redis://:mypass@redis.example.com",
			wantHost: "redis.example.com",
			wantPort: 6379,
			wantPW:   "mypass",
		},
		{
			name:     "rediss SSL",
			input:    "rediss://host.com:6380/1",
			wantHost: "host.com",
			wantPort: 6380,
			wantDB:   1,
			wantSSL:  true,
		},
		{
			name:     "bare host:port",
			input:    "10.0.0.1:6380",
			wantHost: "10.0.0.1",
			wantPort: 6380,
		},
		{
			name:     "bare host only",
			input:    "redis.local",
			wantHost: "redis.local",
			wantPort: 6379,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid db number",
			input:   "redis://localhost/99",
			wantErr: true,
		},
		{
			name:     "password in username position",
			input:    "redis://mypassword@localhost:6379/0",
			wantHost: "localhost",
			wantPort: 6379,
			wantPW:   "mypassword",
			wantDB:   0,
		},
		{
			name:     "ACL user and password",
			input:    "redis://admin:s3cret@myhost:6380/0",
			wantHost: "myhost",
			wantPort: 6380,
			wantUser: "admin",
			wantPW:   "s3cret",
			wantDB:   0,
		},
		{
			name:     "ACL empty user with password",
			input:    "redis://:topSecret@myhost:6379",
			wantHost: "myhost",
			wantPort: 6379,
			wantUser: "",
			wantPW:   "topSecret",
		},
		{
			name:     "ACL user with password and SSL",
			input:    "rediss://myuser:mypass@secure.redis.io:6380/3",
			wantHost: "secure.redis.io",
			wantPort: 6380,
			wantUser: "myuser",
			wantPW:   "mypass",
			wantDB:   3,
			wantSSL:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := Parse(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if info.Host != tt.wantHost {
				t.Errorf("Host = %q, want %q", info.Host, tt.wantHost)
			}
			if info.Port != tt.wantPort {
				t.Errorf("Port = %d, want %d", info.Port, tt.wantPort)
			}
			if info.Username != tt.wantUser {
				t.Errorf("Username = %q, want %q", info.Username, tt.wantUser)
			}
			if info.Password != tt.wantPW {
				t.Errorf("Password = %q, want %q", info.Password, tt.wantPW)
			}
			if info.DB != tt.wantDB {
				t.Errorf("DB = %d, want %d", info.DB, tt.wantDB)
			}
			if info.SSL != tt.wantSSL {
				t.Errorf("SSL = %v, want %v", info.SSL, tt.wantSSL)
			}
		})
	}
}

func TestAddr(t *testing.T) {
	info := &ConnInfo{Host: "localhost", Port: 6379}
	if got := info.Addr(); got != "localhost:6379" {
		t.Errorf("Addr() = %q, want %q", got, "localhost:6379")
	}
}
