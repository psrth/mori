// Package tlsutil provides a shared TLS configuration builder for all database
// engine proxies. It translates user-facing SSL mode parameters into Go
// tls.Config values.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSParams holds the user-specified TLS configuration for a prod connection.
type TLSParams struct {
	ServerName string // hostname for SNI and certificate verification
	SSLMode    string // "disable", "prefer", "require", "verify-ca", "verify-full"
	CACertPath string // path to CA certificate file (sslrootcert)
	CertPath   string // path to client certificate file (sslcert)
	KeyPath    string // path to client private key file (sslkey)
}

// BuildConfig translates TLSParams into a *tls.Config.
// Returns (nil, nil) when SSLMode is "disable" — the caller should skip TLS.
func BuildConfig(p TLSParams) (*tls.Config, error) {
	switch p.SSLMode {
	case "disable", "":
		return nil, nil

	case "prefer", "require":
		// Encrypt traffic but do not verify the server certificate.
		// "prefer" and "require" produce the same TLS config; the difference
		// is in the caller: "prefer" falls back to plaintext if the server
		// does not support TLS, while "require" errors out.
		cfg := &tls.Config{
			ServerName:         p.ServerName,
			InsecureSkipVerify: true,
		}
		if err := loadClientCert(cfg, p.CertPath, p.KeyPath); err != nil {
			return nil, err
		}
		return cfg, nil

	case "verify-ca":
		// Verify the server certificate chain but not the hostname.
		// A CA cert path is required — without it, verify-ca has no meaning
		// distinct from verify-full (which already verifies against system CAs).
		if p.CACertPath == "" {
			return nil, fmt.Errorf("verify-ca requires CACertPath (sslrootcert) to be set")
		}
		cfg := &tls.Config{
			ServerName:         p.ServerName,
			InsecureSkipVerify: true, // we do our own chain verification below
		}
		rootCAs, err := loadCACert(p.CACertPath)
		if err != nil {
			return nil, err
		}
		// Use VerifyConnection instead of VerifyPeerCertificate — Go
		// guarantees it runs on all connections including TLS session
		// resumptions (VerifyPeerCertificate is skipped on resumption).
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("server sent no certificates")
			}
			opts := x509.VerifyOptions{
				Roots:         rootCAs,
				Intermediates: x509.NewCertPool(),
			}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
		if err := loadClientCert(cfg, p.CertPath, p.KeyPath); err != nil {
			return nil, err
		}
		return cfg, nil

	case "verify-full":
		// Full verification: chain + hostname (Go's default TLS behavior).
		cfg := &tls.Config{
			ServerName: p.ServerName,
		}
		if p.CACertPath != "" {
			rootCAs, err := loadCACert(p.CACertPath)
			if err != nil {
				return nil, err
			}
			cfg.RootCAs = rootCAs
		}
		if err := loadClientCert(cfg, p.CertPath, p.KeyPath); err != nil {
			return nil, err
		}
		return cfg, nil

	default:
		return nil, fmt.Errorf("unsupported sslmode %q (valid: disable, prefer, require, verify-ca, verify-full)", p.SSLMode)
	}
}

// loadCACert loads a CA certificate pool from a PEM file, or returns the
// system pool if path is empty.
func loadCACert(path string) (*x509.CertPool, error) {
	if path == "" {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("loading system CA pool: %w", err)
		}
		return pool, nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading CA cert %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid certificates found in %s", path)
	}
	return pool, nil
}

// loadClientCert loads a client certificate and key for mTLS, if both paths
// are provided.
func loadClientCert(cfg *tls.Config, certPath, keyPath string) error {
	if certPath == "" && keyPath == "" {
		return nil
	}
	if certPath == "" || keyPath == "" {
		return fmt.Errorf("both sslcert and sslkey must be provided for client certificate authentication")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return fmt.Errorf("loading client certificate: %w", err)
	}
	cfg.Certificates = []tls.Certificate{cert}
	return nil
}
