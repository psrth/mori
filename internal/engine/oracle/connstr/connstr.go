package connstr

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DSN holds the parsed components of an Oracle connection string.
type DSN struct {
	Host        string
	Port        int
	ServiceName string
	User        string
	Password    string
	Params      map[string]string
	Raw         string
}

// Parse accepts an Oracle connection string in URI format
// (oracle://user:pass@host:port/service_name?param=value) and returns a validated DSN.
func Parse(connStr string) (*DSN, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	dsn, err := parseURI(connStr)
	if err != nil {
		return nil, err
	}

	dsn.Raw = connStr

	if dsn.Host == "" {
		dsn.Host = "127.0.0.1"
	}
	if dsn.Port == 0 {
		dsn.Port = 1521
	}
	if dsn.ServiceName == "" {
		return nil, fmt.Errorf("connection string missing service name")
	}
	if dsn.User == "" {
		dsn.User = "system"
	}
	if dsn.Params == nil {
		dsn.Params = make(map[string]string)
	}

	return dsn, nil
}

// parseURI parses oracle://user:pass@host:port/service_name?params
func parseURI(connStr string) (*DSN, error) {
	// Ensure the scheme is present for url.Parse to work correctly.
	if !strings.HasPrefix(connStr, "oracle://") {
		return nil, fmt.Errorf("connection string must start with oracle:// scheme")
	}

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

	dsn.ServiceName = strings.TrimPrefix(u.Path, "/")
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

// Address returns the "host:port" string for TCP dialing.
func (d *DSN) Address() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

// GoOraDSN returns the connection string in go-ora driver format.
func (d *DSN) GoOraDSN() string {
	var buf strings.Builder
	buf.WriteString("oracle://")
	if d.User != "" {
		buf.WriteString(url.PathEscape(d.User))
		if d.Password != "" {
			buf.WriteByte(':')
			buf.WriteString(url.PathEscape(d.Password))
		}
		buf.WriteByte('@')
	}
	buf.WriteString(fmt.Sprintf("%s:%d", d.Host, d.Port))
	buf.WriteByte('/')
	buf.WriteString(d.ServiceName)
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

// ShadowDSN constructs a go-ora driver connection string for the Shadow database.
func ShadowDSN(port int, serviceName string) string {
	return fmt.Sprintf("oracle://mori:mori@127.0.0.1:%d/%s", port, serviceName)
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
		return fmt.Sprintf("oracle://%s@%s:%d/%s", d.User, d.Host, d.Port, d.ServiceName)
	}
	return fmt.Sprintf("oracle://%s:***@%s:%d/%s", d.User, d.Host, d.Port, d.ServiceName)
}
