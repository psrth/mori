package connerr

import (
	"errors"
	"strings"
	"testing"
)

var noRoute = errors.New("failed to connect to `user=postgres database=postgres`. dial error: dial tcp [2600:1f13:838:6e1d:b15f:d282:ed29:1206]:5432: connect: no route to host")

func TestWrapSupabaseDirectHost(t *testing.T) {
	err := Wrap("db.abcdupq.supabase.co", 5432, noRoute)
	msg := err.Error()

	if !strings.HasPrefix(msg, "cannot connect to production database at db.abcdupq.supabase.co:5432: ") {
		t.Errorf("unexpected prefix: %q", msg)
	}
	if !strings.Contains(msg, "no route to host") {
		t.Errorf("original error text missing: %q", msg)
	}
	if !strings.Contains(msg, "IPv6") {
		t.Errorf("generic IPv6 hint missing: %q", msg)
	}
	if !strings.Contains(msg, "Pooler") || !strings.Contains(msg, "IPv4") {
		t.Errorf("Supabase pooler hint missing: %q", msg)
	}
	if !errors.Is(err, noRoute) {
		t.Error("original error not reachable via errors.Is")
	}
}

func TestWrapNonSupabaseHostGetsGenericHintOnly(t *testing.T) {
	err := Wrap("db.example.com", 5432, noRoute)
	msg := err.Error()

	if !strings.Contains(msg, "IPv6") {
		t.Errorf("generic IPv6 hint missing: %q", msg)
	}
	if strings.Contains(msg, "Supabase") || strings.Contains(msg, "Pooler") {
		t.Errorf("Supabase hint should not appear for non-Supabase host: %q", msg)
	}
	if !errors.Is(err, noRoute) {
		t.Error("original error not reachable via errors.Is")
	}
}

func TestWrapIPv6LiteralHost(t *testing.T) {
	err := Wrap("2600:1f13::1", 5432, errors.New("connect: network is unreachable"))
	if !strings.Contains(err.Error(), "IPv6") {
		t.Errorf("expected IPv6 hint for IPv6 literal host: %q", err.Error())
	}
}

func TestWrapSupabasePoolerHostNotTreatedAsDirect(t *testing.T) {
	err := Wrap("aws-0-us-east-1.pooler.supabase.com", 6543, noRoute)
	if strings.Contains(err.Error(), "Pooler connection string") {
		t.Errorf("pooler host should not get the switch-to-pooler hint: %q", err.Error())
	}
}

func TestWrapUnrelatedErrorsUnchanged(t *testing.T) {
	cases := []error{
		errors.New("dial tcp 10.0.0.5:5432: connect: connection refused"),
		errors.New("failed SASL auth (FATAL: password authentication failed for user \"postgres\")"),
		errors.New("dial tcp: lookup db.abcdupq.supabase.co: no such host"),
		// no-route error without any IPv6 evidence
		errors.New("dial tcp 10.0.0.5:5432: connect: no route to host"),
	}
	for _, orig := range cases {
		err := Wrap("db.abcdupq.supabase.co", 5432, orig)
		want := "cannot connect to production database at db.abcdupq.supabase.co:5432: " + orig.Error()
		if err.Error() != want {
			t.Errorf("message changed for unrelated error:\n got: %q\nwant: %q", err.Error(), want)
		}
		if errors.Unwrap(err) != orig {
			t.Errorf("errors.Unwrap did not return original error for %q", orig)
		}
	}
}

func TestWrapNil(t *testing.T) {
	if Wrap("h", 1, nil) != nil {
		t.Error("Wrap(nil) should return nil")
	}
}

func TestIsSupabaseDirectHost(t *testing.T) {
	tests := map[string]bool{
		"db.abcdupq.supabase.co":              true,
		"DB.ABCDUPQ.SUPABASE.CO":              true,
		"db.abcdupq.supabase.co.":             true,
		"aws-0-us-east-1.pooler.supabase.com": false,
		"supabase.co":                         false,
		"db.example.com":                      false,
		"notsupabase.co":                      false,
	}
	for host, want := range tests {
		if got := IsSupabaseDirectHost(host); got != want {
			t.Errorf("IsSupabaseDirectHost(%q) = %v, want %v", host, got, want)
		}
	}
}
