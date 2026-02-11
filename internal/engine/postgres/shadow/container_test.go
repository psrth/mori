package shadow

import (
	"strings"
	"testing"
)

func TestGenerateContainerName(t *testing.T) {
	name, err := generateContainerName()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(name, "mori-shadow-") {
		t.Errorf("name %q does not have prefix 'mori-shadow-'", name)
	}
	// "mori-shadow-" (12 chars) + 12 hex chars = 24 total
	if len(name) != 24 {
		t.Errorf("name length = %d, want 24", len(name))
	}
}

func TestGenerateContainerNameUnique(t *testing.T) {
	names := make(map[string]bool)
	for i := 0; i < 100; i++ {
		name, err := generateContainerName()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if names[name] {
			t.Errorf("duplicate name generated: %s", name)
		}
		names[name] = true
	}
}
