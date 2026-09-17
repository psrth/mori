# Prompt: Clarify the "no route to host" connection error for Supabase (and other IPv6-only hosts)

## Issue

Fixes: https://github.com/psrth/mori/issues/34

A user connecting to a Supabase production database saw this raw, unhelpful error during `mori init` / `mori start`:

```
Error: cannot connect to production database at db.string.supabase.co:5432: failed to connect to `user=postgres database=postgres`. dial error: dial tcp [2600:1f13:838:6e1d:b15f:d282:ed29:1206]:5432: connect: no route to host
```

The ask (from the issue): make this error message clearer.

## Root cause

Supabase's **direct connection** hostname (`db.<project-ref>.supabase.co`) resolves to an **IPv6-only** address (note the `[2600:...]` literal in the dial error above). Many environments — Docker containers, IPv4-only CI runners, some corporate/home networks — have no IPv6 route, so the low-level TCP dial fails with `connect: no route to host`. Mori currently just wraps this raw Go `net` error with `%w` and surfaces it verbatim, giving the user no indication of *why* it failed or what to do about it.

Supabase's own documented fix is to use the **Session Pooler** or **Transaction Pooler** connection string instead of the direct connection — the pooler endpoints are IPv4-compatible.

## Where this lives today

The exact error format `"cannot connect to production database at %s:%d: %w"` is wrapped in three places (all follow the same shape, all worth fixing for consistency):

- `internal/engine/postgres/init.go:48` — inside `Init()`, wrapping `pgx.Connect(ctx, dsn.ConnString())`
- `internal/engine/mysql/init.go:52` — wrapping `prodDB.PingContext(ctx)`
- `internal/engine/mssql/init.go:50` — wrapping `prodDB.PingContext(ctx)`

There's also a bare `"cannot connect to production database: %w"` (no host/port) in `internal/engine/postgres/schema/version.go:24` inside `DetectVersion()` — lower priority, but same class of problem if it's reachable with a similarly unhelpful underlying error.

This error propagates unmodified up through `cmd/mori/start.go` (`eng.Init(...)` → `initErr` → returned straight to Cobra), which is why the user sees it printed as `Error: <the raw wrapped error>` at the CLI.

Note: by the time `Init()` builds this error, only the resolved DSN (host/port/connection string) is available — the original provider (e.g. "supabase") is *not* plumbed through as a separate field. That's fine: Supabase hostnames are distinctive (`*.supabase.co`, and the pooler is `*.pooler.supabase.com`), so provider-specific hinting can key off `dsn.Host` directly without needing to thread a `Provider` field through `InitOptions`.

## Requested change

In each of the three `init.go` files above, when the connect/ping fails, inspect the underlying error and add an actionable hint before wrapping it, instead of (or in addition to) the current opaque message:

1. **Generic IPv6 routing hint** — if the underlying error indicates `no route to host` (or similar: `network is unreachable`) and the dial target was an IPv6 literal/address, explain that this usually means the resolved host is IPv6-only and the current network/environment can't route to it.

2. **Supabase-specific hint** — if, in addition to the above, `dsn.Host` matches a Supabase direct-connection hostname (ends with `.supabase.co`, and does **not** already look like a pooler host, e.g. does not contain `.pooler.supabase.com`), append a concrete suggestion: switch to the Session or Transaction Pooler connection string from the Supabase dashboard (Project Settings → Database → Connection pooling), which is IPv4-compatible.

3. Keep the original error wrapped via `%w` in all cases, so `errors.Is`/`errors.Unwrap` and existing tests relying on the wrapped error keep working — only the message text should gain a trailing hint, not replace the underlying diagnostic.

Example shape (illustrative, not prescriptive — match existing code style and error-message conventions used elsewhere in the codebase, e.g. how other actionable errors are phrased):

```go
if err != nil {
    return nil, wrapConnectError(dsn.Host, dsn.Port, err)
}
```

with a small shared helper (could live in a common location if duplicated across postgres/mysql/mssql, or be implemented per-package if the engines don't already share a helper package — check for an existing shared "engine util" package before adding a new one) that:
- checks `errors.Is`/string-matching on the dial error for `no route to host` / `network is unreachable`,
- checks whether the host looks like a bare IPv6 literal or resolves to IPv6-only (matching on the presence of `no route to host` plus the host string is a reasonable, cheap heuristic — no need for an actual DNS lookup),
- checks whether `host` matches `*.supabase.co` and not `*.pooler.supabase.com`,
- returns a wrapped error whose message includes the original error plus, when applicable, one or two extra sentences of guidance.

## Tests

Add a unit test (e.g. `internal/engine/postgres/init_test.go`, mirrored for mysql/mssql if those packages already have engine-level unit tests — check first) asserting that:
- an error containing `no route to host` for a host like `db.abcdupq.supabase.co` produces a message mentioning the Supabase pooler / IPv4 guidance,
- an error containing `no route to host` for a non-Supabase host still gets the generic IPv6-routing hint (but not the Supabase-specific one),
- a normal, unrelated connect error (e.g. `connection refused`, wrong password) is unchanged in behavior — no misleading hint added — and the original error remains accessible via `errors.Unwrap`.

## Out of scope

- No changes to actual connection/retry logic — this is purely about the error message shown to the user.
- No changes to the Supabase auth provider (`internal/auth/providers/supabase.go`) — it already produces a correct connection string; the problem is purely that failures against it are hard to diagnose.
