package connstr

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DSN holds the parsed components of a MySQL connection string.
type DSN struct {
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
	Params   map[string]string
	Raw      string
}

// Parse accepts a MySQL connection string in Go DSN format
// (user:password@tcp(host:port)/dbname?param=value) or URI format
// (mysql://user:pass@host:port/db?param=value) and returns a validated DSN.
func Parse(connStr string) (*DSN, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	var dsn *DSN
	var err error

	if strings.HasPrefix(connStr, "mysql://") {
		dsn, err = parseURI(connStr)
	} else {
		dsn, err = parseGoDSN(connStr)
	}
	if err != nil {
		return nil, err
	}

	dsn.Raw = connStr

	if dsn.Host == "" {
		dsn.Host = "127.0.0.1"
	}
	if dsn.Port == 0 {
		dsn.Port = 3306
	}
	if dsn.DBName == "" {
		return nil, fmt.Errorf("connection string missing database name")
	}
	if dsn.User == "" {
		dsn.User = "root"
	}
	if dsn.Params == nil {
		dsn.Params = make(map[string]string)
	}

	return dsn, nil
}

// parseURI parses mysql://user:pass@host:port/dbname?params
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

	dsn.DBName = strings.TrimPrefix(u.Path, "/")
	if u.User != nil {
		dsn.User = u.User.Username()
		dsn.Password, _ = u.User.Password()
	}

	dsn.Params = make(map[string]string)
	for k, vals := range u.Query() {
		if len(vals) > 0 {
			dsn.Params[k] = vals[0]
		}
	}

	return dsn, nil
}

// parseGoDSN parses the Go MySQL driver DSN format:
// [user[:password]@][protocol[(address)]]/dbname[?param=value]
func parseGoDSN(connStr string) (*DSN, error) {
	dsn := &DSN{
		Params: make(map[string]string),
	}

	// Split off query parameters.
	base := connStr
	if idx := strings.Index(base, "?"); idx >= 0 {
		paramStr := base[idx+1:]
		base = base[:idx]
		for _, pair := range strings.Split(paramStr, "&") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				dsn.Params[kv[0]] = kv[1]
			}
		}
	}

	// Split off dbname after the last "/".
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		dsn.DBName = base[idx+1:]
		base = base[:idx]
	}

	// Split user info from address at "@".
	if idx := strings.Index(base, "@"); idx >= 0 {
		userInfo := base[:idx]
		base = base[idx+1:]

		if cidx := strings.Index(userInfo, ":"); cidx >= 0 {
			dsn.User = userInfo[:cidx]
			dsn.Password = userInfo[cidx+1:]
		} else {
			dsn.User = userInfo
		}
	}

	// Parse protocol and address: tcp(host:port)
	if strings.HasPrefix(base, "tcp(") && strings.HasSuffix(base, ")") {
		addr := base[4 : len(base)-1]
		host, portStr, err := splitHostPort(addr)
		if err != nil {
			return nil, err
		}
		dsn.Host = host
		if portStr != "" {
			p, err := strconv.Atoi(portStr)
			if err != nil {
				return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
			}
			dsn.Port = p
		}
	} else if base != "" {
		// Bare host:port or just host.
		host, portStr, err := splitHostPort(base)
		if err != nil {
			dsn.Host = base
		} else {
			dsn.Host = host
			if portStr != "" {
				p, err := strconv.Atoi(portStr)
				if err != nil {
					return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
				}
				dsn.Port = p
			}
		}
	}

	return dsn, nil
}

func splitHostPort(addr string) (string, string, error) {
	// Handle IPv6 addresses.
	if strings.HasPrefix(addr, "[") {
		end := strings.Index(addr, "]")
		if end < 0 {
			return "", "", fmt.Errorf("invalid address %q", addr)
		}
		host := addr[1:end]
		rest := addr[end+1:]
		if rest == "" {
			return host, "", nil
		}
		if rest[0] == ':' {
			return host, rest[1:], nil
		}
		return "", "", fmt.Errorf("invalid address %q", addr)
	}

	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, "", nil
	}
	return addr[:idx], addr[idx+1:], nil
}

// Address returns the "host:port" string for TCP dialing.
func (d *DSN) Address() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

// GoDSN returns the connection string in Go MySQL driver format.
func (d *DSN) GoDSN() string {
	var buf strings.Builder
	if d.User != "" {
		buf.WriteString(d.User)
		if d.Password != "" {
			buf.WriteByte(':')
			buf.WriteString(d.Password)
		}
		buf.WriteByte('@')
	}
	buf.WriteString(fmt.Sprintf("tcp(%s:%d)", d.Host, d.Port))
	buf.WriteByte('/')
	buf.WriteString(d.DBName)
	if len(d.Params) > 0 {
		buf.WriteByte('?')
		first := true
		for k, v := range d.Params {
			if !first {
				buf.WriteByte('&')
			}
			buf.WriteString(k)
			buf.WriteByte('=')
			buf.WriteString(v)
			first = false
		}
	}
	return buf.String()
}

// ShadowDSN constructs a Go MySQL driver DSN for the Shadow database.
func ShadowDSN(port int, dbname string) string {
	return fmt.Sprintf("root:mori@tcp(127.0.0.1:%d)/%s", port, dbname)
}

// DockerHost returns the hostname to use when connecting from inside a Docker
// container. Translates localhost/127.0.0.1 to host.docker.internal.
func (d *DSN) DockerHost() string {
	if d.Host == "localhost" || d.Host == "127.0.0.1" {
		return "host.docker.internal"
	}
	return d.Host
}

// MysqldumpArgs returns arguments for mysqldump based on the parsed DSN.
func (d *DSN) MysqldumpArgs() []string {
	return []string{
		"-h", d.Host,
		"-P", strconv.Itoa(d.Port),
		"-u", d.User,
		"--no-data",
		"--routines",
		"--triggers",
		"--skip-lock-tables",
		d.DBName,
	}
}

// MysqldumpDockerArgs returns mysqldump arguments for running inside a Docker container.
func (d *DSN) MysqldumpDockerArgs() []string {
	return []string{
		"-h", d.DockerHost(),
		"-P", strconv.Itoa(d.Port),
		"-u", d.User,
		"--no-data",
		"--routines",
		"--triggers",
		"--skip-lock-tables",
		d.DBName,
	}
}

// MysqldumpEnv returns the environment variable for mysqldump password.
func (d *DSN) MysqldumpEnv() string {
	return "MYSQL_PWD=" + d.Password
}

// Redacted returns the connection string with the password replaced by "***".
func (d *DSN) Redacted() string {
	if d.Password == "" {
		return fmt.Sprintf("%s@tcp(%s:%d)/%s", d.User, d.Host, d.Port, d.DBName)
	}
	return fmt.Sprintf("%s:***@tcp(%s:%d)/%s", d.User, d.Host, d.Port, d.DBName)
}
