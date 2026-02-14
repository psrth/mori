package connstr

import (
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantProj  string
		wantCreds string
		wantDB    string
		wantEmu   string
		wantErr   bool
	}{
		{
			name:     "plain project ID",
			input:    "my-project",
			wantProj: "my-project",
			wantDB:   "(default)",
		},
		{
			name:      "project ID with credentials",
			input:     "my-project?credentials=/path/to/creds.json",
			wantProj:  "my-project",
			wantCreds: "/path/to/creds.json",
			wantDB:    "(default)",
		},
		{
			name:     "firestore URI",
			input:    "firestore://my-project",
			wantProj: "my-project",
			wantDB:   "(default)",
		},
		{
			name:      "firestore URI with credentials",
			input:     "firestore://my-project?credentials=/path/to/creds.json",
			wantProj:  "my-project",
			wantCreds: "/path/to/creds.json",
			wantDB:    "(default)",
		},
		{
			name:     "firestore URI with database",
			input:    "firestore://my-project?database=my-db",
			wantProj: "my-project",
			wantDB:   "my-db",
		},
		{
			name:     "firestore URI with emulator",
			input:    "firestore://my-project?emulator_host=localhost:8080",
			wantProj: "my-project",
			wantEmu:  "localhost:8080",
			wantDB:   "(default)",
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:     "whitespace trimmed",
			input:    "  my-project  ",
			wantProj: "my-project",
			wantDB:   "(default)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := Parse(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if info.ProjectID != tt.wantProj {
				t.Errorf("ProjectID = %q, want %q", info.ProjectID, tt.wantProj)
			}
			if info.CredentialsFile != tt.wantCreds {
				t.Errorf("CredentialsFile = %q, want %q", info.CredentialsFile, tt.wantCreds)
			}
			if info.DatabaseID != tt.wantDB {
				t.Errorf("DatabaseID = %q, want %q", info.DatabaseID, tt.wantDB)
			}
			if info.EmulatorHost != tt.wantEmu {
				t.Errorf("EmulatorHost = %q, want %q", info.EmulatorHost, tt.wantEmu)
			}
		})
	}
}

func TestResourceName(t *testing.T) {
	info := &ConnInfo{ProjectID: "my-project", DatabaseID: "(default)"}
	want := "projects/my-project/databases/(default)"
	if got := info.ResourceName(); got != want {
		t.Errorf("ResourceName() = %q, want %q", got, want)
	}
}

func TestProdAddr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		info := &ConnInfo{ProjectID: "my-project"}
		if got := info.ProdAddr(); got != "firestore.googleapis.com:443" {
			t.Errorf("ProdAddr() = %q, want firestore.googleapis.com:443", got)
		}
	})
	t.Run("emulator", func(t *testing.T) {
		info := &ConnInfo{ProjectID: "my-project", EmulatorHost: "localhost:8080"}
		if got := info.ProdAddr(); got != "localhost:8080" {
			t.Errorf("ProdAddr() = %q, want localhost:8080", got)
		}
	})
}

func TestIsEmulator(t *testing.T) {
	t.Run("not emulator", func(t *testing.T) {
		info := &ConnInfo{ProjectID: "my-project"}
		if info.IsEmulator() {
			t.Error("IsEmulator() = true, want false")
		}
	})
	t.Run("emulator", func(t *testing.T) {
		info := &ConnInfo{ProjectID: "my-project", EmulatorHost: "localhost:8080"}
		if !info.IsEmulator() {
			t.Error("IsEmulator() = false, want true")
		}
	})
}
