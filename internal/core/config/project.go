package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

const (
	// ProjectConfigFile is the name of the user-facing config file.
	ProjectConfigFile = "mori.yaml"
)

// ProjectConfig is the top-level mori.yaml structure.
type ProjectConfig struct {
	Version     int                    `yaml:"version"`
	Connections map[string]*Connection `yaml:"connections"`
}

// Connection is a single named database connection definition.
type Connection struct {
	Engine   string `yaml:"engine"`
	Provider string `yaml:"provider"`

	// Standard connection fields (not all engines use every field).
	Host     string `yaml:"host,omitempty"`
	Port     int    `yaml:"port,omitempty"`
	User     string `yaml:"user,omitempty"`
	Password string `yaml:"password,omitempty"`
	Database string `yaml:"database,omitempty"`
	SSLMode  string `yaml:"ssl_mode,omitempty"`

	// Extra holds engine- or provider-specific fields that don't have
	// dedicated struct fields (e.g. service_name, region, project_ref).
	Extra map[string]string `yaml:"extra,omitempty"`
}

// ProjectConfigPath returns the absolute path to mori.yaml
// relative to the given project root.
func ProjectConfigPath(projectRoot string) string {
	return filepath.Join(projectRoot, ProjectConfigFile)
}

// HasProjectConfig checks if mori.yaml exists at the project root.
func HasProjectConfig(projectRoot string) bool {
	_, err := os.Stat(ProjectConfigPath(projectRoot))
	return err == nil
}

// ReadProjectConfig reads and parses mori.yaml from the project root.
func ReadProjectConfig(projectRoot string) (*ProjectConfig, error) {
	path := ProjectConfigPath(projectRoot)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read %s: %w", path, err)
	}
	var cfg ProjectConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("could not parse %s: %w", path, err)
	}
	if cfg.Connections == nil {
		cfg.Connections = make(map[string]*Connection)
	}
	return &cfg, nil
}

// WriteProjectConfig serializes and writes the project config to mori.yaml.
func WriteProjectConfig(projectRoot string, cfg *ProjectConfig) error {
	if cfg.Version == 0 {
		cfg.Version = 1
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("could not serialize config: %w", err)
	}
	return os.WriteFile(ProjectConfigPath(projectRoot), data, 0600)
}

// NewProjectConfig creates a new empty project config.
func NewProjectConfig() *ProjectConfig {
	return &ProjectConfig{
		Version:     1,
		Connections: make(map[string]*Connection),
	}
}

// AddConnection adds or replaces a named connection.
func (pc *ProjectConfig) AddConnection(name string, conn *Connection) {
	if pc.Connections == nil {
		pc.Connections = make(map[string]*Connection)
	}
	pc.Connections[name] = conn
}

// RemoveConnection removes a named connection. Returns true if it existed.
func (pc *ProjectConfig) RemoveConnection(name string) bool {
	if _, ok := pc.Connections[name]; !ok {
		return false
	}
	delete(pc.Connections, name)
	return true
}

// ConnectionNames returns sorted connection names.
func (pc *ProjectConfig) ConnectionNames() []string {
	names := make([]string, 0, len(pc.Connections))
	for name := range pc.Connections {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GetConnection returns the named connection, or nil if not found.
func (pc *ProjectConfig) GetConnection(name string) *Connection {
	return pc.Connections[name]
}

// ToConnString builds a postgres:// connection string from the connection fields.
// Only valid for postgres-compatible engines (postgres, cockroachdb).
func (c *Connection) ToConnString() string {
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	port := c.Port
	if port == 0 {
		port = 5432
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, port),
		Path:   c.Database,
	}
	q := u.Query()
	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()
	return u.String()
}

// RedactedPassword returns the password masked for display.
func (c *Connection) RedactedPassword() string {
	if c.Password == "" {
		return "(none)"
	}
	return "***"
}
