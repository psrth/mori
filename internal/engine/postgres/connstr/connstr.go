package connstr

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ProdDSN holds the parsed components of a PostgreSQL connection string.
type ProdDSN struct {
	Host     string
	Port     int
	DBName   string
	User     string
	Password string
	SSLMode  string
	Raw      string
}

// Parse accepts a PostgreSQL connection string in URI format
// (postgres://user:pass@host:port/dbname?sslmode=disable) or
// keyword=value format (host=... port=... dbname=... user=... password=...)
// and returns a validated ProdDSN.
func Parse(connStr string) (*ProdDSN, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	var dsn *ProdDSN
	var err error

	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		dsn, err = parseURI(connStr)
	} else {
		dsn, err = parseKeyValue(connStr)
	}
	if err != nil {
		return nil, err
	}

	dsn.Raw = connStr

	if dsn.Host == "" {
		return nil, fmt.Errorf("connection string missing host")
	}
	if dsn.DBName == "" {
		return nil, fmt.Errorf("connection string missing database name")
	}
	if dsn.Port == 0 {
		dsn.Port = 5432
	}
	if dsn.SSLMode == "" {
		dsn.SSLMode = "disable"
	}
	if dsn.User == "" {
		dsn.User = "postgres"
	}

	return dsn, nil
}

func parseURI(connStr string) (*ProdDSN, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection URI: %w", err)
	}

	dsn := &ProdDSN{
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
	dsn.SSLMode = u.Query().Get("sslmode")

	return dsn, nil
}

func parseKeyValue(connStr string) (*ProdDSN, error) {
	dsn := &ProdDSN{}
	parts := strings.Fields(connStr)
	for _, part := range parts {
		idx := strings.Index(part, "=")
		if idx < 0 {
			continue
		}
		key := part[:idx]
		val := part[idx+1:]
		switch key {
		case "host":
			dsn.Host = val
		case "port":
			p, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("invalid port %q: %w", val, err)
			}
			dsn.Port = p
		case "dbname":
			dsn.DBName = val
		case "user":
			dsn.User = val
		case "password":
			dsn.Password = val
		case "sslmode":
			dsn.SSLMode = val
		}
	}
	return dsn, nil
}

// ShadowDSN constructs a connection string for the Shadow database
// running on localhost at the given port, using the same dbname.
func ShadowDSN(port int, dbname string) string {
	return fmt.Sprintf("postgres://postgres:mori@localhost:%d/%s?sslmode=disable", port, dbname)
}

// PgDumpArgs returns the arguments for pg_dump based on the parsed DSN.
func (d *ProdDSN) PgDumpArgs() []string {
	return []string{
		"-h", d.Host,
		"-p", strconv.Itoa(d.Port),
		"-U", d.User,
		"-d", d.DBName,
		"--schema-only",
		"--no-owner",
		"--no-privileges",
	}
}

// DockerHost returns the hostname to use when connecting from inside a Docker
// container. Translates localhost/127.0.0.1 to host.docker.internal.
func (d *ProdDSN) DockerHost() string {
	if d.Host == "localhost" || d.Host == "127.0.0.1" {
		return "host.docker.internal"
	}
	return d.Host
}

// PgDumpDockerArgs returns pg_dump arguments suitable for running inside a
// Docker container, using DockerHost() for the host.
func (d *ProdDSN) PgDumpDockerArgs() []string {
	return []string{
		"-h", d.DockerHost(),
		"-p", strconv.Itoa(d.Port),
		"-U", d.User,
		"-d", d.DBName,
		"--schema-only",
		"--no-owner",
		"--no-privileges",
	}
}

// PgDumpEnv returns the environment variable for pg_dump password.
func (d *ProdDSN) PgDumpEnv() string {
	return "PGPASSWORD=" + d.Password
}

// Redacted returns the connection string with the password replaced by "***".
func (d *ProdDSN) Redacted() string {
	if d.Password == "" {
		return fmt.Sprintf("postgres://%s@%s:%d/%s", d.User, d.Host, d.Port, d.DBName)
	}
	return fmt.Sprintf("postgres://%s:***@%s:%d/%s", d.User, d.Host, d.Port, d.DBName)
}

// Address returns the "host:port" string for TCP dialing.
func (d *ProdDSN) Address() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

// ConnString returns a full connection string suitable for pgx.
func (d *ProdDSN) ConnString() string {
	u := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", d.Host, d.Port),
		Path:   d.DBName,
	}
	if d.Password != "" {
		u.User = url.UserPassword(d.User, d.Password)
	} else {
		u.User = url.User(d.User)
	}
	q := u.Query()
	q.Set("sslmode", d.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}
