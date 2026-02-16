package shadow

import "testing"

func TestImageForVersion(t *testing.T) {
	tests := []struct {
		version string
		want    string
	}{
		{"7.2.4", "redis:7.2"},
		{"6.2.14", "redis:6.2"},
		{"7.0.0", "redis:7.0"},
		{"5.0.14", "redis:5.0"},
		{"7", "redis:7"},
		{"", DefaultImage},
		{"unknown", DefaultImage},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			got := ImageForVersion(tt.version)
			if got != tt.want {
				t.Errorf("ImageForVersion(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}
