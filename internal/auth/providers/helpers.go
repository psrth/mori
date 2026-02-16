package providers

import (
	"maps"

	"github.com/mori-dev/mori/internal/core/config"
)

// connWithSSL returns a copy of conn with SSLMode enforced to "require"
// when it is empty or explicitly set to "disable". When a tunnel is
// configured the tunnel already handles TLS, so SSL enforcement is skipped.
// The Extra map is deep-copied so mutations on the returned value cannot
// affect the original.
func connWithSSL(conn *config.Connection) *config.Connection {
	c := *conn
	if conn.Tunnel == nil && (c.SSLMode == "" || c.SSLMode == "disable") {
		c.SSLMode = "require"
	}
	if conn.Extra != nil {
		c.Extra = make(map[string]string, len(conn.Extra))
		maps.Copy(c.Extra, conn.Extra)
	}
	return &c
}
