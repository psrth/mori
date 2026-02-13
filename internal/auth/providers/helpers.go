package providers

import "github.com/mori-dev/mori/internal/core/config"

// connWithSSL returns a shallow copy of conn with SSLMode enforced to
// "require" when it is empty or explicitly set to "disable".
func connWithSSL(conn *config.Connection) *config.Connection {
	c := *conn
	if c.SSLMode == "" || c.SSLMode == "disable" {
		c.SSLMode = "require"
	}
	return &c
}
