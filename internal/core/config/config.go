package config

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const (
	// MoriDir is the name of the Mori configuration directory.
	MoriDir = ".mori"
	// ConfigFile is the name of the main configuration file.
	ConfigFile = "config.json"
	// PidFile is the name of the proxy PID file.
	PidFile = "proxy.pid"
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
}

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
