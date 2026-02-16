package connstr

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ConnInfo holds the parsed components of a DuckDB connection string.
type ConnInfo struct {
	FilePath string            // Absolute path to the DuckDB database file
	Params   map[string]string // Query parameters
	Raw      string            // Original connection string
}

// Parse accepts a DuckDB connection string and returns a validated ConnInfo.
// Supported formats:
//   - Plain file path: /path/to/db.duckdb, ./data.duckdb, mydb.db
//   - URI format:      duckdb:///path/to/db.duckdb
//   - In-memory:       :memory: (for testing only)
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
	case connStr == ":memory:":
		info.FilePath = ":memory:"
		return info, nil
	case strings.HasPrefix(connStr, "duckdb://"):
		if err := parseDuckDBURI(connStr, info); err != nil {
			return nil, err
		}
	default:
		// Check for query params: path?key=val
		if idx := strings.IndexByte(connStr, '?'); idx >= 0 {
			info.FilePath = connStr[:idx]
			q, err := url.ParseQuery(connStr[idx+1:])
			if err == nil {
				for k, vals := range q {
					if len(vals) > 0 {
						info.Params[k] = vals[0]
					}
				}
			}
		} else {
			info.FilePath = connStr
		}
	}

	if info.FilePath != ":memory:" {
		absPath, err := filepath.Abs(info.FilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve path %q: %w", info.FilePath, err)
		}
		info.FilePath = absPath
	}

	return info, nil
}

// parseDuckDBURI parses duckdb:///path/to/db or duckdb://path/to/db.
func parseDuckDBURI(connStr string, info *ConnInfo) error {
	u, err := url.Parse(strings.Replace(connStr, "duckdb://", "file://", 1))
	if err != nil {
		return fmt.Errorf("invalid duckdb URI: %w", err)
	}
	info.FilePath = u.Host + u.Path
	if info.FilePath == "" {
		return fmt.Errorf("duckdb URI missing file path")
	}
	for k, vals := range u.Query() {
		if len(vals) > 0 {
			info.Params[k] = vals[0]
		}
	}
	return nil
}

// Validate checks that the DuckDB database file exists.
func (c *ConnInfo) Validate() error {
	if c.FilePath == ":memory:" {
		return nil
	}
	fi, err := os.Stat(c.FilePath)
	if err != nil {
		return fmt.Errorf("DuckDB database file not found: %w", err)
	}
	if fi.IsDir() {
		return fmt.Errorf("path %q is a directory, not a file", c.FilePath)
	}
	return nil
}

// DSN returns a database/sql-compatible connection string for go-duckdb.
func (c *ConnInfo) DSN() string {
	if c.FilePath == ":memory:" {
		return ":memory:"
	}
	if len(c.Params) == 0 {
		return c.FilePath
	}
	params := url.Values{}
	for k, v := range c.Params {
		params.Set(k, v)
	}
	return c.FilePath + "?" + params.Encode()
}

// ReadOnlyDSN returns a DSN with access_mode=READ_ONLY for read-only access.
func (c *ConnInfo) ReadOnlyDSN() string {
	params := url.Values{}
	for k, v := range c.Params {
		params.Set(k, v)
	}
	params.Set("access_mode", "READ_ONLY")
	return c.FilePath + "?" + params.Encode()
}
