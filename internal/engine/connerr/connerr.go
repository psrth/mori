// Package connerr turns low-level production connection failures into
// errors that tell the user what went wrong and what to do about it.
package connerr

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

// ipv6Literal matches a bracketed IPv6 address as printed by Go's net
// package in dial errors, e.g. "dial tcp [2600:1f13::1]:5432: ...".
var ipv6Literal = regexp.MustCompile(`\[[0-9a-fA-F:.]*:[0-9a-fA-F:.]*\]`)

// Wrap wraps a production connect/ping error with the target host and port.
// When the failure looks like an IPv6 routing problem it appends an
// actionable hint, and a Supabase-specific one when the host is a Supabase
// direct-connection hostname. The original error is always wrapped with %w.
func Wrap(host string, port int, err error) error {
	if err == nil {
		return nil
	}
	base := fmt.Errorf("cannot connect to production database at %s:%d: %w", host, port, err)
	hint := Hint(host, err)
	if hint == "" {
		return base
	}
	return fmt.Errorf("%w\n\nHint: %s", base, hint)
}

// Hint returns guidance for err when it is an IPv6 routing failure, or ""
// when the error is unrelated and should be surfaced unchanged.
func Hint(host string, err error) string {
	if err == nil || !IsIPv6RoutingError(host, err) {
		return ""
	}
	msg := "the host resolved to an IPv6 address, but this machine or network has no IPv6 route to it. " +
		"This usually means the database host is IPv6-only and you are on an IPv4-only network " +
		"(common inside Docker containers, CI runners, and some home or corporate networks)."
	if IsSupabaseDirectHost(host) {
		msg += " Supabase direct connections (db.<project-ref>.supabase.co) are IPv6-only. " +
			"Use the Session or Transaction Pooler connection string instead, which works over IPv4: " +
			"Supabase dashboard → Project Settings → Database → Connection pooling."
	}
	return msg
}

// IsIPv6RoutingError reports whether err is an unreachable-host failure
// against an IPv6 address. It matches on the error text rather than doing a
// DNS lookup, so it is cheap and works on the already-failed error.
func IsIPv6RoutingError(host string, err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	unreachable := strings.Contains(text, "no route to host") ||
		strings.Contains(text, "network is unreachable")
	if !unreachable {
		return false
	}
	return isIPv6Host(host) || ipv6Literal.MatchString(err.Error())
}

// IsSupabaseDirectHost reports whether host is a Supabase direct-connection
// hostname (db.<ref>.supabase.co) as opposed to a pooler endpoint.
func IsSupabaseDirectHost(host string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if strings.Contains(h, ".pooler.supabase.com") {
		return false
	}
	return strings.HasSuffix(h, ".supabase.co")
}

func isIPv6Host(host string) bool {
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.To4() == nil
}
