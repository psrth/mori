package shadow

import (
	"strings"
	"testing"
)

func TestGenerateContainerName(t *testing.T) {
	name, err := generateContainerName()
	if err != nil {
		t.Fatalf("generateContainerName() error: %v", err)
	}
	if !strings.HasPrefix(name, "mori-mysql-shadow-") {
		t.Errorf("container name %q does not have expected prefix", name)
	}
	if len(name) != len("mori-mysql-shadow-")+12 {
		t.Errorf("container name %q has unexpected length %d", name, len(name))
	}
}

func TestGenerateContainerName_Unique(t *testing.T) {
	name1, _ := generateContainerName()
	name2, _ := generateContainerName()
	if name1 == name2 {
		t.Error("two generated names should be different")
	}
}
