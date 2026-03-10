package connstr

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DSN holds the parsed components of an MSSQL connection string.
type DSN struct {
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
	SSLMode  string
	Params   map[string]string
	Raw      string
}

// Parse accepts an MSSQL connection string in URI format
// (sqlserver://user:pass@host:port?database=dbname) or key-value format
// (server=host;user id=user;password=pass;database=dbname) and returns a validated DSN.
func Parse(connStr string) (*DSN, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	var dsn *DSN
	var err error

	if strings.HasPrefix(connStr, "sqlserver://") {
		dsn, err = parseURI(connStr)
	} else {
		dsn, err = parseKeyValue(connStr)
	}
	if err != nil {
		return nil, err
	}

	dsn.Raw = connStr

	if dsn.Host == "" {
		dsn.Host = "127.0.0.1"
	}
	if dsn.Port == 0 {
		dsn.Port = 1433
	}
	if dsn.DBName == "" {
		return nil, fmt.Errorf("connection string missing database name")
	}
	if dsn.User == "" {
		dsn.User = "sa"
	}
	if dsn.Params == nil {
		dsn.Params = make(map[string]string)
	}

	// Map MSSQL encrypt/trust params to canonical SSLMode.
	// Default is "disable" because TDS-wrapped TLS is not yet implemented.
	// When TDS TLS support is added, change the default to "verify-full".
	encrypt := strings.ToLower(dsn.Params["encrypt"])
	trust := strings.ToLower(dsn.Params["trustservercertificate"])
	switch {
	case encrypt == "false" || encrypt == "no" || encrypt == "":
		dsn.SSLMode = "disable"
	case trust == "true" || trust == "yes":
		dsn.SSLMode = "require"
	default:
		dsn.SSLMode = "verify-full"
	}

	return dsn, nil
}

// parseURI parses sqlserver://user:pass@host:port?database=dbname
func parseURI(connStr string) (*DSN, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection URI: %w", err)
	}

	dsn := &DSN{
		Host: u.Hostname(),
	}

	if portStr := u.Port(); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
		}
		dsn.Port = p
	}

	if u.User != nil {
		dsn.User = u.User.Username()
		dsn.Password, _ = u.User.Password()
	}

	dsn.Params = make(map[string]string)
	for k, vals := range u.Query() {
		if len(vals) > 0 {
			dsn.Params[strings.ToLower(k)] = vals[0]
		}
	}

	// Database can be in path or query param.
	if path := strings.TrimPrefix(u.Path, "/"); path != "" {
		dsn.DBName = path
	} else if db, ok := dsn.Params["database"]; ok {
		dsn.DBName = db
	}

	return dsn, nil
}

// parseKeyValue parses key-value format:
// server=host,port;user id=user;password=pass;database=dbname
func parseKeyValue(connStr string) (*DSN, error) {
	dsn := &DSN{
		Params: make(map[string]string),
	}

	pairs := strings.Split(connStr, ";")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		idx := strings.Index(pair, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(strings.ToLower(pair[:idx]))
		val := strings.TrimSpace(pair[idx+1:])
		dsn.Params[key] = val

		switch key {
		case "server", "data source", "addr", "address":
			host, port := parseServerValue(val)
			dsn.Host = host
			if port > 0 {
				dsn.Port = port
			}
		case "user id", "uid", "user":
			dsn.User = val
		case "password", "pwd":
			dsn.Password = val
		case "database", "initial catalog":
			dsn.DBName = val
		case "port":
			if p, err := strconv.Atoi(val); err == nil {
				dsn.Port = p
			}
		}
	}

	return dsn, nil
}

// parseServerValue parses "host,port" or "host:port" or just "host".
func parseServerValue(val string) (string, int) {
	// MSSQL uses comma as host,port separator.
	if idx := strings.LastIndex(val, ","); idx >= 0 {
		host := strings.TrimSpace(val[:idx])
		portStr := strings.TrimSpace(val[idx+1:])
		if p, err := strconv.Atoi(portStr); err == nil {
			return host, p
		}
		return host, 0
	}
	// Also support colon separator.
	if idx := strings.LastIndex(val, ":"); idx >= 0 {
		host := strings.TrimSpace(val[:idx])
		portStr := strings.TrimSpace(val[idx+1:])
		if p, err := strconv.Atoi(portStr); err == nil {
			return host, p
		}
	}
	return val, 0
}

// Address returns the "host:port" string for TCP dialing.
func (d *DSN) Address() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

// GoDSN returns the connection string in sqlserver:// URI format for the Go MSSQL driver.
func (d *DSN) GoDSN() string {
	q := url.Values{}
	q.Set("database", d.DBName)
	for k, v := range d.Params {
		if k != "database" && k != "server" && k != "user id" && k != "uid" &&
			k != "user" && k != "password" && k != "pwd" && k != "port" &&
			k != "data source" && k != "addr" && k != "address" && k != "initial catalog" {
			q.Set(k, v)
		}
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(d.User, d.Password),
		Host:     fmt.Sprintf("%s:%d", d.Host, d.Port),
		RawQuery: q.Encode(),
	}
	return u.String()
}

// ShadowDSN constructs a Go MSSQL driver DSN for the Shadow database.
func ShadowDSN(port int, dbname string) string {
	q := url.Values{}
	q.Set("database", dbname)
	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("sa", "Mori_P@ss1"),
		Host:     fmt.Sprintf("127.0.0.1:%d", port),
		RawQuery: q.Encode(),
	}
	return u.String()
}

// ShadowAddr returns the host:port string for the Shadow database.
func ShadowAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// DockerHost returns the hostname to use when connecting from inside a Docker
// container. Translates localhost/127.0.0.1 to host.docker.internal.
func (d *DSN) DockerHost() string {
	if d.Host == "localhost" || d.Host == "127.0.0.1" {
		return "host.docker.internal"
	}
	return d.Host
}

// Redacted returns the connection string with the password replaced by "***".
func (d *DSN) Redacted() string {
	if d.Password == "" {
		return fmt.Sprintf("sqlserver://%s@%s:%d?database=%s", d.User, d.Host, d.Port, d.DBName)
	}
	return fmt.Sprintf("sqlserver://%s:***@%s:%d?database=%s", d.User, d.Host, d.Port, d.DBName)
}
