package schema

import (
	"testing"
)

func TestParseVersion(t *testing.T) {
	tests := []struct {
		input     string
		wantMajor int
		wantMinor int
		wantFull  string
		wantImage string
	}{
		{"16.2", 16, 2, "16.2", "postgres:16.2"},
		{"16.2 (Debian 16.2-1.pgdg120+2)", 16, 2, "16.2", "postgres:16.2"},
		{"15.0", 15, 0, "15.0", "postgres:15.0"},
		{"17.1", 17, 1, "17.1", "postgres:17.1"},
		{"14.10", 14, 10, "14.10", "postgres:14.10"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			v, err := ParseVersion(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Major != tt.wantMajor {
				t.Errorf("Major = %d, want %d", v.Major, tt.wantMajor)
			}
			if v.Minor != tt.wantMinor {
				t.Errorf("Minor = %d, want %d", v.Minor, tt.wantMinor)
			}
			if v.Full != tt.wantFull {
				t.Errorf("Full = %q, want %q", v.Full, tt.wantFull)
			}
			if v.ImageTag != tt.wantImage {
				t.Errorf("ImageTag = %q, want %q", v.ImageTag, tt.wantImage)
			}
		})
	}
}

func TestParseVersionEmpty(t *testing.T) {
	_, err := ParseVersion("")
	if err == nil {
		t.Fatal("expected error for empty version string")
	}
}
