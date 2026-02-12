package logging

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mori-dev/mori/internal/core"
)

func TestNew_CreatesDirectoryAndFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "sub", "dir", "test.log")

	l, err := New(logPath)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer l.Close()

	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("log file not created: %v", err)
	}
}

func TestLog_WritesValidJSONLines(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	l, err := New(logPath)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	l.Log(LogEntry{
		Level:  "info",
		ConnID: 1,
		Event:  "query",
		SQL:    "SELECT 1",
	})
	l.Log(LogEntry{
		Level:  "error",
		ConnID: 2,
		Event:  "error",
		Error:  "something failed",
	})
	l.Close()

	f, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("line %d: invalid JSON: %v\nraw: %s", count, err, scanner.Text())
		}
		if entry.Timestamp.IsZero() {
			t.Errorf("line %d: timestamp not set", count)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 lines, got %d", count)
	}
}

func TestQuery_PopulatesFields(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	l, err := New(logPath)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users"},
	}
	l.Query(3, "SELECT * FROM users", cl, core.StrategyMergedRead, 2500*time.Microsecond)
	l.Close()

	data, _ := os.ReadFile(logPath)
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if entry.ConnID != 3 {
		t.Errorf("ConnID = %d, want 3", entry.ConnID)
	}
	if entry.OpType != "READ" {
		t.Errorf("OpType = %q, want READ", entry.OpType)
	}
	if entry.SubType != "SELECT" {
		t.Errorf("SubType = %q, want SELECT", entry.SubType)
	}
	if entry.Strategy != "MERGED_READ" {
		t.Errorf("Strategy = %q, want MERGED_READ", entry.Strategy)
	}
	if entry.DurationMs < 2.0 || entry.DurationMs > 3.0 {
		t.Errorf("DurationMs = %f, want ~2.5", entry.DurationMs)
	}
	if len(entry.Tables) != 1 || entry.Tables[0] != "users" {
		t.Errorf("Tables = %v, want [users]", entry.Tables)
	}
}

func TestConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	l, err := New(logPath)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			l.Log(LogEntry{
				Level:  "info",
				ConnID: int64(id),
				Event:  "test",
			})
		}(i)
	}
	wg.Wait()
	l.Close()

	f, _ := os.Open(logPath)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("line %d: invalid JSON: %v", count, err)
		}
		count++
	}
	if count != 50 {
		t.Errorf("expected 50 lines, got %d", count)
	}
}

func TestNilLogger(t *testing.T) {
	var l *Logger

	// All methods should be no-ops without panic.
	l.Log(LogEntry{Event: "test"})
	l.Query(1, "SELECT 1", nil, core.StrategyProdDirect, time.Millisecond)
	l.Event(1, "test", "detail")
	l.Error(1, "test", &testErr{})
	if err := l.Close(); err != nil {
		t.Errorf("nil Close() returned error: %v", err)
	}
}

type testErr struct{}

func (e *testErr) Error() string { return "test error" }
