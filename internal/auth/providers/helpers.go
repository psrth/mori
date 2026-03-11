package providers

import (
	"maps"
	"net/url"
	"strings"

	"github.com/psrth/mori/internal/core/config"
)

// connWithSSL returns a copy of conn with SSLMode enforced to "verify-full"
// for any mode weaker than certificate validation. Cloud providers (AWS RDS,
// GCP Cloud SQL, Azure, Neon, etc.) all recommend full certificate verification.
// Modes upgraded: "", "disable", "allow", "prefer", "require".
// "require" only encrypts the connection without verifying the server identity,
// leaving the connection vulnerable to MITM attacks.
// Only "verify-ca" and "verify-full" are left as-is.
// When a tunnel is configured the tunnel already handles TLS, so SSL
// enforcement is skipped. The Extra map is deep-copied so mutations on the
// returned value cannot affect the original.
func connWithSSL(conn *config.Connection) *config.Connection {
	c := *conn
	if conn.Tunnel == nil {
		switch c.SSLMode {
		case "verify-ca", "verify-full":
			// Already strong — keep as-is.
		default:
			// Upgrade "", "disable", "allow", "prefer", "require", or any
			// unrecognised value to verify-full.
			c.SSLMode = "verify-full"
		}
	}
	if conn.Extra != nil {
		c.Extra = make(map[string]string, len(conn.Extra))
		maps.Copy(c.Extra, conn.Extra)
	}
	return &c
}

// enforceSSLInURL ensures a connection URL has sslmode=verify-full.
// Modes that are weaker than verify-full are upgraded:
//   - "" / "disable" / "allow" / "prefer" → no server verification at all
//   - "require" → encrypts but does NOT verify the server identity (MITM-vulnerable)
//
// Only "verify-ca" and "verify-full" are left as-is, since both perform
// certificate chain validation. Non-Postgres URLs (no sslmode concept) are
// returned unmodified.
func enforceSSLInURL(raw string) string {
	if !strings.HasPrefix(raw, "postgres://") && !strings.HasPrefix(raw, "postgresql://") {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	mode := q.Get("sslmode")
	switch mode {
	case "verify-ca", "verify-full":
		return raw
	default:
		// Upgrade "", "disable", "allow", "prefer", "require", or any
		// unrecognised value to verify-full.
		q.Set("sslmode", "verify-full")
		u.RawQuery = q.Encode()
		return u.String()
	}
}
