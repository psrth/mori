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

	// Tunnel holds optional tunnel configuration for private-network databases.
	Tunnel *TunnelConfig `yaml:"tunnel,omitempty"`
}

// TunnelConfig is the tunnel section of a connection in mori.yaml.
type TunnelConfig struct {
	Type      string            `yaml:"type"`
	LocalPort int               `yaml:"local_port,omitempty"`
	Params    map[string]string `yaml:"params,omitempty"`
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

// ToConnString builds a connection string from the connection fields.
// Dispatches based on the Engine field to produce the correct format.
func (c *Connection) ToConnString() string {
	switch c.Engine {
	case "mssql":
		return c.toMSSQLConnString()
	case "mysql", "mariadb":
		return c.toMySQLConnString()
	case "oracle":
		return c.toOracleConnString()
	case "sqlite":
		return c.toSQLiteConnString()
	case "firestore":
		return c.toFirestoreConnString()
	case "redis":
		return c.toRedisConnString()
	default:
		// postgres, cockroachdb, and any other pg-compatible engine.
		return c.toPostgresConnString()
	}
}

func (c *Connection) toPostgresConnString() string {
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

func (c *Connection) toMSSQLConnString() string {
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 1433
	}
	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", host, port),
	}
	q := u.Query()
	if c.Database != "" {
		q.Set("database", c.Database)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Connection) toMySQLConnString() string {
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 3306
	}
	// Format: user:pass@tcp(host:port)/database
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		url.PathEscape(c.User), url.PathEscape(c.Password),
		host, port, c.Database)
}

func (c *Connection) toOracleConnString() string {
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 1521
	}
	svcName := c.Extra["service_name"]
	if svcName == "" {
		svcName = c.Database
	}
	u := &url.URL{
		Scheme: "oracle",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   svcName,
	}
	return u.String()
}

func (c *Connection) toSQLiteConnString() string {
	// SQLite connection string is just the file path.
	if p, ok := c.Extra["path"]; ok {
		return p
	}
	return c.Database
}

func (c *Connection) toRedisConnString() string {
	// If the caller stored a full connection string in Extra, use it directly.
	if cs, ok := c.Extra["connection_string"]; ok && cs != "" {
		return cs
	}
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 6379
	}
	db := "0"
	if d, ok := c.Extra["db"]; ok && d != "" {
		db = d
	}
	scheme := "redis"
	if c.SSLMode == "require" || c.SSLMode == "enabled" {
		scheme = "rediss"
	}
	if c.Password != "" {
		return fmt.Sprintf("%s://:%s@%s:%d/%s", scheme, url.PathEscape(c.Password), host, port, db)
	}
	return fmt.Sprintf("%s://%s:%d/%s", scheme, host, port, db)
}

func (c *Connection) toFirestoreConnString() string {
	projectID := c.Extra["project_id"]
	if projectID == "" {
		projectID = c.Database
	}
	if creds, ok := c.Extra["credentials_file"]; ok && creds != "" {
		return projectID + "?credentials_file=" + creds
	}
	return projectID
}

// RedactedPassword returns the password masked for display.
func (c *Connection) RedactedPassword() string {
	if c.Password == "" {
		return "(none)"
	}
	return "***"
}
