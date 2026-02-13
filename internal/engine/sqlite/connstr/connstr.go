package connstr

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ConnInfo holds the parsed components of a SQLite connection string.
type ConnInfo struct {
	FilePath string            // Absolute path to the SQLite database file
	Params   map[string]string // Query parameters (e.g., mode=ro, cache=shared)
	Raw      string            // Original connection string
}

// Parse accepts a SQLite connection string and returns a validated ConnInfo.
// Supported formats:
//   - Plain file path: /path/to/db.sqlite, ./data.db, mydb.sqlite
//   - URI format:      file:path/to/db.sqlite?mode=ro&cache=shared
//   - sqlite:// prefix: sqlite:///path/to/db.sqlite
func Parse(connStr string) (*ConnInfo, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	info := &ConnInfo{
		Raw:    connStr,
		Params: make(map[string]string),
	}

	switch {
	case strings.HasPrefix(connStr, "sqlite://"):
		if err := parseSQLiteURI(connStr, info); err != nil {
			return nil, err
		}
	case strings.HasPrefix(connStr, "file:"):
		if err := parseFileURI(connStr, info); err != nil {
			return nil, err
		}
	default:
		info.FilePath = connStr
	}

	// Resolve to absolute path.
	absPath, err := filepath.Abs(info.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve path %q: %w", info.FilePath, err)
	}
	info.FilePath = absPath

	return info, nil
}

// parseSQLiteURI parses sqlite:///path/to/db or sqlite://path/to/db.
func parseSQLiteURI(connStr string, info *ConnInfo) error {
	// Replace sqlite:// with file:// for standard URL parsing.
	u, err := url.Parse(strings.Replace(connStr, "sqlite://", "file://", 1))
	if err != nil {
		return fmt.Errorf("invalid sqlite URI: %w", err)
	}
	info.FilePath = u.Host + u.Path
	if info.FilePath == "" {
		return fmt.Errorf("sqlite URI missing file path")
	}
	for k, vals := range u.Query() {
		if len(vals) > 0 {
			info.Params[k] = vals[0]
		}
	}
	return nil
}

// parseFileURI parses file:path?params or file:///path?params.
func parseFileURI(connStr string, info *ConnInfo) error {
	u, err := url.Parse(connStr)
	if err != nil {
		return fmt.Errorf("invalid file URI: %w", err)
	}

	// file: URIs can be file:path (opaque) or file:///path (hierarchical).
	if u.Opaque != "" {
		// file:path/to/db?mode=ro
		info.FilePath = u.Opaque
	} else {
		// file:///path/to/db?mode=ro
		info.FilePath = u.Host + u.Path
	}

	if info.FilePath == "" {
		return fmt.Errorf("file URI missing path")
	}

	for k, vals := range u.Query() {
		if len(vals) > 0 {
			info.Params[k] = vals[0]
		}
	}
	return nil
}

// Validate checks that the SQLite database file exists.
func (c *ConnInfo) Validate() error {
	fi, err := os.Stat(c.FilePath)
	if err != nil {
		return fmt.Errorf("SQLite database file not found: %w", err)
	}
	if fi.IsDir() {
		return fmt.Errorf("path %q is a directory, not a file", c.FilePath)
	}
	return nil
}

// DSN returns a database/sql-compatible connection string for modernc.org/sqlite.
func (c *ConnInfo) DSN() string {
	if len(c.Params) == 0 {
		return c.FilePath
	}
	params := url.Values{}
	for k, v := range c.Params {
		params.Set(k, v)
	}
	return c.FilePath + "?" + params.Encode()
}

// ReadOnlyDSN returns a DSN with mode=ro for read-only access.
func (c *ConnInfo) ReadOnlyDSN() string {
	params := url.Values{}
	for k, v := range c.Params {
		params.Set(k, v)
	}
	params.Set("mode", "ro")
	return c.FilePath + "?" + params.Encode()
}
