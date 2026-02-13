package shadow

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// ShadowFile is the name of the shadow database file within .mori/.
	ShadowFile = "shadow.db"
)

// ShadowPath returns the path to the shadow database file.
func ShadowPath(moriDir string) string {
	return filepath.Join(moriDir, ShadowFile)
}

// CreateShadow copies the production SQLite file to the shadow location.
func CreateShadow(prodPath, moriDir string) (string, error) {
	shadowPath := ShadowPath(moriDir)

	src, err := os.Open(prodPath)
	if err != nil {
		return "", fmt.Errorf("failed to open prod database %q: %w", prodPath, err)
	}
	defer src.Close()

	dst, err := os.Create(shadowPath)
	if err != nil {
		return "", fmt.Errorf("failed to create shadow database %q: %w", shadowPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		os.Remove(shadowPath)
		return "", fmt.Errorf("failed to copy database: %w", err)
	}

	if err := dst.Sync(); err != nil {
		os.Remove(shadowPath)
		return "", fmt.Errorf("failed to sync shadow database: %w", err)
	}

	return shadowPath, nil
}

// ShadowExists checks if the shadow database file exists.
func ShadowExists(moriDir string) bool {
	_, err := os.Stat(ShadowPath(moriDir))
	return err == nil
}

// RemoveShadow deletes the shadow database file and its journal/WAL files.
func RemoveShadow(moriDir string) error {
	shadowPath := ShadowPath(moriDir)
	os.Remove(shadowPath + "-journal")
	os.Remove(shadowPath + "-wal")
	os.Remove(shadowPath + "-shm")
	return os.Remove(shadowPath)
}
