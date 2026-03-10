package auth_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mori-dev/mori/internal/auth"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/registry"
)

// --- mock providers ---

type staticMockProvider struct {
	callCount atomic.Int32
}

func (m *staticMockProvider) ID() registry.ProviderID                          { return "mock-static" }
func (m *staticMockProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }
func (m *staticMockProvider) ConnString(_ context.Context, _ *config.Connection) (string, error) {
	m.callCount.Add(1)
	return "postgres://user:pass@host:5432/db", nil
}

type refreshableMockProvider struct {
	callCount atomic.Int32
	ttl       time.Duration
}

func (m *refreshableMockProvider) ID() registry.ProviderID                          { return "mock-refresh" }
func (m *refreshableMockProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }
func (m *refreshableMockProvider) ConnString(_ context.Context, _ *config.Connection) (string, error) {
	n := m.callCount.Add(1)
	return "postgres://user:token" + string(rune('0'+n)) + "@host:5432/db", nil
}
func (m *refreshableMockProvider) TokenTTL() time.Duration { return m.ttl }

type failOnceMockProvider struct {
	callCount atomic.Int32
}

func (m *failOnceMockProvider) ID() registry.ProviderID                          { return "mock-failonce" }
func (m *failOnceMockProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }
func (m *failOnceMockProvider) ConnString(_ context.Context, _ *config.Connection) (string, error) {
	n := m.callCount.Add(1)
	if n == 1 {
		return "", context.DeadlineExceeded
	}
	return "postgres://user:pass@host:5432/db", nil
}
func (m *failOnceMockProvider) TokenTTL() time.Duration { return 50 * time.Millisecond }

// --- tests ---

func TestNewConnStringFunc_StaticProvider_CachesForever(t *testing.T) {
	mock := &staticMockProvider{}
	conn := &config.Connection{}
	fn := auth.NewConnStringFunc(mock, conn)

	// Call multiple times — should only invoke ConnString once.
	for i := 0; i < 5; i++ {
		s, err := fn(context.Background())
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if s != "postgres://user:pass@host:5432/db" {
			t.Fatalf("call %d: got %q", i, s)
		}
	}
	if got := mock.callCount.Load(); got != 1 {
		t.Errorf("static provider called %d times, want 1", got)
	}
}

func TestNewConnStringFunc_RefreshableProvider_RefreshesAfterTTL(t *testing.T) {
	mock := &refreshableMockProvider{ttl: 50 * time.Millisecond}
	conn := &config.Connection{}
	fn := auth.NewConnStringFunc(mock, conn)

	// First call.
	s1, err := fn(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Immediate second call — should return cached value.
	s2, err := fn(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s1 != s2 {
		t.Errorf("expected cached value %q, got %q", s1, s2)
	}
	if got := mock.callCount.Load(); got != 1 {
		t.Errorf("expected 1 call before TTL, got %d", got)
	}

	// Wait for TTL to expire (80% of 50ms = 40ms, wait 60ms to be safe).
	time.Sleep(60 * time.Millisecond)

	s3, err := fn(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s3 == s1 {
		t.Errorf("expected refreshed value after TTL, got same %q", s3)
	}
	if got := mock.callCount.Load(); got != 2 {
		t.Errorf("expected 2 calls after TTL, got %d", got)
	}
}

func TestNewConnStringFunc_ErrorRetries(t *testing.T) {
	mock := &failOnceMockProvider{}
	conn := &config.Connection{}
	fn := auth.NewConnStringFunc(mock, conn)

	// First call fails.
	_, err := fn(context.Background())
	if err == nil {
		t.Fatal("expected error on first call")
	}

	// Second call should retry (not return cached error).
	s, err := fn(context.Background())
	if err != nil {
		t.Fatalf("expected success on retry, got: %v", err)
	}
	if s != "postgres://user:pass@host:5432/db" {
		t.Errorf("unexpected value: %q", s)
	}
}

// failOnRefreshMockProvider succeeds on the first call, then fails on
// subsequent calls — simulating a transient IAM outage during token refresh.
type failOnRefreshMockProvider struct {
	callCount atomic.Int32
}

func (m *failOnRefreshMockProvider) ID() registry.ProviderID                          { return "mock-fail-refresh" }
func (m *failOnRefreshMockProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }
func (m *failOnRefreshMockProvider) ConnString(_ context.Context, _ *config.Connection) (string, error) {
	n := m.callCount.Add(1)
	if n == 1 {
		return "postgres://user:token1@host:5432/db", nil
	}
	return "", context.DeadlineExceeded
}
func (m *failOnRefreshMockProvider) TokenTTL() time.Duration { return 50 * time.Millisecond }

func TestNewConnStringFunc_StaleTokenOnRefreshFailure(t *testing.T) {
	mock := &failOnRefreshMockProvider{}
	conn := &config.Connection{}
	fn := auth.NewConnStringFunc(mock, conn)

	// First call succeeds and caches.
	s1, err := fn(context.Background())
	if err != nil {
		t.Fatalf("expected success on first call, got: %v", err)
	}
	if s1 != "postgres://user:token1@host:5432/db" {
		t.Fatalf("unexpected first value: %q", s1)
	}

	// Wait for TTL to expire.
	time.Sleep(60 * time.Millisecond)

	// Second call triggers refresh which fails — should return stale cached value.
	s2, err := fn(context.Background())
	if err != nil {
		t.Fatalf("expected stale cached value on refresh failure, got error: %v", err)
	}
	if s2 != s1 {
		t.Errorf("expected stale value %q, got %q", s1, s2)
	}
}

func TestNewConnStringFunc_ConcurrentSafety(t *testing.T) {
	mock := &refreshableMockProvider{ttl: 100 * time.Millisecond}
	conn := &config.Connection{}
	fn := auth.NewConnStringFunc(mock, conn)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := fn(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	// With mutex protection, ConnString should only be called once
	// (all goroutines hit the cache after the first call).
	if got := mock.callCount.Load(); got != 1 {
		t.Errorf("expected 1 call with concurrent access, got %d", got)
	}
}

// slowMockProvider simulates a slow CLI subprocess (aws/gcloud/az) that takes
// time to generate a token, exercising the cold-cache wait path.
type slowMockProvider struct {
	callCount atomic.Int32
	delay     time.Duration
}

func (m *slowMockProvider) ID() registry.ProviderID                            { return "mock-slow" }
func (m *slowMockProvider) Fields(registry.EngineID) []registry.ConnectionField { return nil }
func (m *slowMockProvider) ConnString(_ context.Context, _ *config.Connection) (string, error) {
	m.callCount.Add(1)
	time.Sleep(m.delay)
	return "postgres://user:token@host:5432/db", nil
}
func (m *slowMockProvider) TokenTTL() time.Duration { return time.Minute }

func TestNewConnStringFunc_ColdCacheConcurrency(t *testing.T) {
	mock := &slowMockProvider{delay: 50 * time.Millisecond}
	conn := &config.Connection{}
	fn := auth.NewConnStringFunc(mock, conn)

	// Launch 20 goroutines on a cold cache — only one should call ConnString,
	// the rest should wait and receive the result.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := fn(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if s != "postgres://user:token@host:5432/db" {
				t.Errorf("unexpected value: %q", s)
			}
		}()
	}
	wg.Wait()

	if got := mock.callCount.Load(); got != 1 {
		t.Errorf("cold-cache concurrency: expected 1 ConnString call, got %d", got)
	}
}
