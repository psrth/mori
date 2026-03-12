package config

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	// MoriDir is the name of the Mori configuration directory.
	MoriDir = ".mori"
	// ConfigFile is the name of the main configuration file.
	ConfigFile = "config.json"
	// PidFile is the name of the proxy PID file.
	PidFile = "proxy.pid"
	// TunnelPidFile is the name of the tunnel PID file.
	TunnelPidFile = "tunnel.pid"
	// LogDir is the subdirectory for log files within .mori/.
	LogDir = "log"
	// LogFile is the name of the structured query log file.
	LogFile = "mori.log"
)

// Config holds all Mori project configuration.
type Config struct {
	ProdConnection   string    `json:"prod_connection"`
	ShadowPort       int       `json:"shadow_port"`
	ShadowContainer  string    `json:"shadow_container"`
	ShadowImage      string    `json:"shadow_image"`
	Engine           string    `json:"engine"`
	EngineVersion    string    `json:"engine_version"`
	ProxyPort        int       `json:"proxy_port"`
	Extensions       []string  `json:"extensions"`
	InitializedAt    time.Time `json:"initialized_at"`
	ActiveConnection string    `json:"active_connection,omitempty"` // name from mori.yaml
	MaxRowsHydrate   int       `json:"max_rows_hydrate,omitempty"` // cap on rows hydrated from Prod (0 = unlimited)
}

// ──────────────────────────────────────────────────────────────────
// Legacy global paths (kept for backward compat with TUI and old state).
// ──────────────────────────────────────────────────────────────────

// MoriDirPath returns the absolute path to the .mori/ directory
// relative to the given project root.
func MoriDirPath(projectRoot string) string {
	return filepath.Join(projectRoot, MoriDir)
}

// ConfigFilePath returns the absolute path to the config.json file.
func ConfigFilePath(projectRoot string) string {
	return filepath.Join(projectRoot, MoriDir, ConfigFile)
}

// PidFilePath returns the absolute path to the proxy PID file.
func PidFilePath(projectRoot string) string {
	return filepath.Join(projectRoot, MoriDir, PidFile)
}

// TunnelPidFilePath returns the absolute path to the tunnel PID file.
func TunnelPidFilePath(projectRoot string) string {
	return filepath.Join(projectRoot, MoriDir, TunnelPidFile)
}

// LogFilePath returns the absolute path to the structured query log file.
func LogFilePath(projectRoot string) string {
	return filepath.Join(projectRoot, MoriDir, LogDir, LogFile)
}

// IsInitialized checks whether a .mori/config.json file exists
// in the given project root directory.
func IsInitialized(projectRoot string) bool {
	_, err := os.Stat(ConfigFilePath(projectRoot))
	return err == nil
}

// InitDir creates the .mori/ directory structure at the given project root.
func InitDir(projectRoot string) error {
	dir := MoriDirPath(projectRoot)
	return os.MkdirAll(dir, 0755)
}

// WriteConfig serializes and writes the config to .mori/config.json.
func WriteConfig(projectRoot string, cfg *Config) error {
	if err := InitDir(projectRoot); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ConfigFilePath(projectRoot), data, 0644)
}

// ReadConfig reads and deserializes the config from .mori/config.json.
func ReadConfig(projectRoot string) (*Config, error) {
	if !IsInitialized(projectRoot) {
		return nil, errors.New("mori is not initialized (no .mori/config.json found)")
	}
	data, err := os.ReadFile(ConfigFilePath(projectRoot))
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ──────────────────────────────────────────────────────────────────
// Per-connection paths: .mori/<connName>/
// ──────────────────────────────────────────────────────────────────

// ConnDir returns the per-connection state directory: .mori/<connName>/
func ConnDir(projectRoot, connName string) string {
	return filepath.Join(projectRoot, MoriDir, connName)
}

// ConnConfigFilePath returns the per-connection config file path.
func ConnConfigFilePath(projectRoot, connName string) string {
	return filepath.Join(ConnDir(projectRoot, connName), ConfigFile)
}

// ConnPidFilePath returns the per-connection PID file path.
func ConnPidFilePath(projectRoot, connName string) string {
	return filepath.Join(ConnDir(projectRoot, connName), PidFile)
}

// ConnTunnelPidFilePath returns the per-connection tunnel PID file path.
func ConnTunnelPidFilePath(projectRoot, connName string) string {
	return filepath.Join(ConnDir(projectRoot, connName), TunnelPidFile)
}

// ConnLogFilePath returns the per-connection log file path.
func ConnLogFilePath(projectRoot, connName string) string {
	return filepath.Join(ConnDir(projectRoot, connName), LogDir, LogFile)
}


// IsConnInitialized checks whether config.json exists for a specific connection.
func IsConnInitialized(projectRoot, connName string) bool {
	_, err := os.Stat(ConnConfigFilePath(projectRoot, connName))
	return err == nil
}

// InitConnDir creates the per-connection state directory.
func InitConnDir(projectRoot, connName string) error {
	return os.MkdirAll(ConnDir(projectRoot, connName), 0755)
}

// WriteConnConfig serializes and writes the config to .mori/<connName>/config.json.
func WriteConnConfig(projectRoot, connName string, cfg *Config) error {
	if err := InitConnDir(projectRoot, connName); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ConnConfigFilePath(projectRoot, connName), data, 0644)
}

// ReadConnConfig reads and deserializes the config for a specific connection.
func ReadConnConfig(projectRoot, connName string) (*Config, error) {
	if !IsConnInitialized(projectRoot, connName) {
		return nil, errors.New("connection is not initialized (no config.json found)")
	}
	data, err := os.ReadFile(ConnConfigFilePath(projectRoot, connName))
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// InitializedConnections returns the names of all connections that have been
// initialized (have a config.json in .mori/<name>/).
func InitializedConnections(projectRoot string) []string {
	moriDir := MoriDirPath(projectRoot)
	entries, err := os.ReadDir(moriDir)
	if err != nil {
		return nil
	}
	var conns []string
	for _, e := range entries {
		if e.IsDir() {
			cfgPath := filepath.Join(moriDir, e.Name(), ConfigFile)
			if _, err := os.Stat(cfgPath); err == nil {
				conns = append(conns, e.Name())
			}
		}
	}
	sort.Strings(conns)
	return conns
}

// NextAvailablePort returns the next available proxy port starting from basePort.
// It checks existing per-connection configs to avoid collisions.
func NextAvailablePort(projectRoot string, basePort int) int {
	used := make(map[int]bool)
	for _, name := range InitializedConnections(projectRoot) {
		if cfg, err := ReadConnConfig(projectRoot, name); err == nil && cfg.ProxyPort > 0 {
			used[cfg.ProxyPort] = true
		}
	}
	port := basePort
	for used[port] {
		port++
	}
	return port
}

// MigrateToConnDir migrates legacy .mori/ state into .mori/<connName>/ if
// an old-style .mori/config.json exists at the project root. This enables
// seamless transition to per-connection directories.
func MigrateToConnDir(projectRoot, connName string) error {
	oldCfgPath := ConfigFilePath(projectRoot)
	if _, err := os.Stat(oldCfgPath); err != nil {
		return nil // nothing to migrate
	}

	connDir := ConnDir(projectRoot, connName)
	if _, err := os.Stat(filepath.Join(connDir, ConfigFile)); err == nil {
		return nil // already migrated
	}

	if err := os.MkdirAll(connDir, 0755); err != nil {
		return err
	}

	moriDir := MoriDirPath(projectRoot)
	// Files to move into the connection subdirectory.
	filesToMove := []string{
		ConfigFile, PidFile, TunnelPidFile,
		"tables.json", "sequences.json",
		"delta.json", "tombstones.json", "schema_registry.json",
	}
	for _, f := range filesToMove {
		src := filepath.Join(moriDir, f)
		dst := filepath.Join(connDir, f)
		if _, err := os.Stat(src); err == nil {
			os.Rename(src, dst)
		}
	}
	// Move log directory.
	oldLog := filepath.Join(moriDir, LogDir)
	newLog := filepath.Join(connDir, LogDir)
	if _, err := os.Stat(oldLog); err == nil {
		os.Rename(oldLog, newLog)
	}

	return nil
}

// ──────────────────────────────────────────────────────────────────
// Common helpers.
// ──────────────────────────────────────────────────────────────────

// RedactedProdConnection returns ProdConnection with the password masked.
// If ProdConnection cannot be parsed, returns it as-is.
func (c *Config) RedactedProdConnection() string {
	u, err := url.Parse(c.ProdConnection)
	if err != nil {
		return c.ProdConnection
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	return u.String()
}

// FindProjectRoot walks up the directory tree from the current working directory
// looking for a .mori/ directory or mori.yaml file. Returns the path to the
// project root, or the current working directory if neither is found.
func FindProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		// Check for .mori/ directory (runtime state).
		if _, err := os.Stat(filepath.Join(dir, MoriDir)); err == nil {
			return dir, nil
		}
		// Check for mori.yaml (project config).
		if _, err := os.Stat(filepath.Join(dir, ProjectConfigFile)); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root without finding either marker.
			wd, err := os.Getwd()
			if err != nil {
				return "", err
			}
			return wd, nil
		}
		dir = parent
	}
}
