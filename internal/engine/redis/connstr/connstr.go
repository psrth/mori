package connstr

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ConnInfo holds the parsed components of a Redis connection string.
type ConnInfo struct {
	Host     string
	Port     int
	Username string // ACL username (Redis 6+); empty = default user
	Password string
	DB       int    // database number (0-15)
	SSL      bool
	Raw      string // original connection string
}

// Addr returns "host:port" for TCP dialing.
func (c *ConnInfo) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Parse accepts a Redis connection string and returns a ConnInfo.
// Supported formats:
//   - redis://[:password@]host[:port][/db]
//   - rediss://[:password@]host[:port][/db]  (SSL)
//   - host:port
//   - host
func Parse(connStr string) (*ConnInfo, error) {
	connStr = strings.TrimSpace(connStr)
	if connStr == "" {
		return nil, fmt.Errorf("connection string is empty")
	}

	info := &ConnInfo{
		Raw:  connStr,
		Host: "127.0.0.1",
		Port: 6379,
		DB:   0,
	}

	switch {
	case strings.HasPrefix(connStr, "rediss://"):
		info.SSL = true
		if err := parseRedisURI(connStr, info); err != nil {
			return nil, err
		}
	case strings.HasPrefix(connStr, "redis://"):
		if err := parseRedisURI(connStr, info); err != nil {
			return nil, err
		}
	default:
		// Bare host or host:port.
		if err := parseBare(connStr, info); err != nil {
			return nil, err
		}
	}

	return info, nil
}

func parseRedisURI(connStr string, info *ConnInfo) error {
	// Replace rediss:// with https:// for standard URL parsing.
	normalized := connStr
	if strings.HasPrefix(normalized, "rediss://") {
		normalized = "https://" + normalized[len("rediss://"):]
	} else {
		normalized = "http://" + normalized[len("redis://"):]
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return fmt.Errorf("invalid redis URI: %w", err)
	}

	// Host.
	if u.Hostname() != "" {
		info.Host = u.Hostname()
	}

	// Port.
	if u.Port() != "" {
		p, err := strconv.Atoi(u.Port())
		if err != nil || p < 1 || p > 65535 {
			return fmt.Errorf("invalid port %q", u.Port())
		}
		info.Port = p
	}

	// Username and password from userinfo.
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			// redis://user:password@host → username="user", password="password"
			// redis://:password@host    → username="" (empty), password="password"
			info.Username = u.User.Username()
			info.Password = pw
		} else {
			// redis://password@host — bare value with no colon; treat as password
			// for backward compatibility (legacy single-arg AUTH).
			bare := u.User.Username()
			if bare != "" {
				info.Password = bare
			}
		}
	}

	// DB number from path.
	path := strings.TrimPrefix(u.Path, "/")
	if path != "" {
		db, err := strconv.Atoi(path)
		if err != nil {
			return fmt.Errorf("invalid db number %q", path)
		}
		if db < 0 || db > 15 {
			return fmt.Errorf("db number must be 0-15, got %d", db)
		}
		info.DB = db
	}

	return nil
}

func parseBare(connStr string, info *ConnInfo) error {
	host, portStr, err := splitHostPort(connStr)
	if err != nil {
		// No port — treat entire string as host.
		info.Host = connStr
		return nil
	}
	info.Host = host
	p, err := strconv.Atoi(portStr)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("invalid port %q", portStr)
	}
	info.Port = p
	return nil
}

// splitHostPort splits host:port, handling IPv6 brackets.
func splitHostPort(s string) (host, port string, err error) {
	// Use net-style parsing: check for last colon.
	if strings.HasPrefix(s, "[") {
		// IPv6.
		end := strings.Index(s, "]")
		if end < 0 {
			return "", "", fmt.Errorf("missing closing bracket")
		}
		host = s[1:end]
		rest := s[end+1:]
		if rest == "" {
			return host, "", fmt.Errorf("no port")
		}
		if rest[0] != ':' {
			return "", "", fmt.Errorf("expected colon after bracket")
		}
		return host, rest[1:], nil
	}
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("no port")
	}
	return s[:idx], s[idx+1:], nil
}
