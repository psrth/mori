package schema

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ProdVersion holds the detected PostgreSQL version info.
type ProdVersion struct {
	Major    int
	Minor    int
	Full     string // e.g., "16.2"
	ImageTag string // e.g., "postgres:16.2"
}

// DetectVersion connects to Prod and queries SHOW server_version.
func DetectVersion(ctx context.Context, connStr string) (*ProdVersion, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to production database: %w", err)
	}
	defer conn.Close(ctx)

	var versionStr string
	if err := conn.QueryRow(ctx, "SHOW server_version").Scan(&versionStr); err != nil {
		return nil, fmt.Errorf("failed to detect PostgreSQL version: %w", err)
	}

	return ParseVersion(versionStr)
}

// ParseVersion extracts major.minor from a PostgreSQL version string.
// Handles formats like "16.2", "16.2 (Debian ...)", "17beta1".
func ParseVersion(versionStr string) (*ProdVersion, error) {
	// Take only the first space-separated token
	parts := strings.Fields(versionStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty version string")
	}
	versionNum := parts[0]

	// Split on "."
	segments := strings.SplitN(versionNum, ".", 3)
	if len(segments) < 1 {
		return nil, fmt.Errorf("cannot parse version %q", versionStr)
	}

	// Parse major — may contain non-numeric suffix (e.g., "17beta1")
	majorStr := segments[0]
	for i, c := range majorStr {
		if c < '0' || c > '9' {
			majorStr = majorStr[:i]
			break
		}
	}
	major, err := strconv.Atoi(majorStr)
	if err != nil {
		return nil, fmt.Errorf("cannot parse major version from %q: %w", versionStr, err)
	}

	minor := 0
	if len(segments) >= 2 {
		minorStr := segments[1]
		// Strip non-numeric suffixes
		for i, c := range minorStr {
			if c < '0' || c > '9' {
				minorStr = minorStr[:i]
				break
			}
		}
		if minorStr != "" {
			minor, err = strconv.Atoi(minorStr)
			if err != nil {
				return nil, fmt.Errorf("cannot parse minor version from %q: %w", versionStr, err)
			}
		}
	}

	full := fmt.Sprintf("%d.%d", major, minor)
	return &ProdVersion{
		Major:    major,
		Minor:    minor,
		Full:     full,
		ImageTag: "postgres:" + full,
	}, nil
}
