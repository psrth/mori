package logging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mori-dev/mori/internal/core"
)

// LogEntry is a single structured log record written as one JSON line.
type LogEntry struct {
	Timestamp  time.Time `json:"ts"`
	Level      string    `json:"level"`
	ConnID     int64     `json:"conn_id,omitempty"`
	Event      string    `json:"event"`
	SQL        string    `json:"sql,omitempty"`
	OpType     string    `json:"op_type,omitempty"`
	SubType    string    `json:"sub_type,omitempty"`
	Tables     []string  `json:"tables,omitempty"`
	Strategy   string    `json:"strategy,omitempty"`
	DurationMs float64   `json:"duration_ms,omitempty"`
	Detail     string    `json:"detail,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Logger writes structured JSON log entries to a file.
// All methods are nil-safe — calling any method on a nil Logger is a no-op.
type Logger struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

// New creates a Logger that writes to the given path.
// Creates the parent directory if it doesn't exist.
func New(logPath string) (*Logger, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{
		file: f,
		enc:  json.NewEncoder(f),
	}, nil
}

// Log writes a single entry. Timestamp is set automatically if zero.
func (l *Logger) Log(entry LogEntry) {
	if l == nil {
		return
	}
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enc.Encode(entry) //nolint:errcheck
}

// Query logs a query classification and routing decision.
func (l *Logger) Query(connID int64, sql string, cl *core.Classification, strategy core.RoutingStrategy, duration time.Duration) {
	if l == nil {
		return
	}
	entry := LogEntry{
		Level:      "info",
		ConnID:     connID,
		Event:      "query",
		SQL:        sql,
		Strategy:   strategy.String(),
		DurationMs: float64(duration.Microseconds()) / 1000.0,
	}
	if cl != nil {
		entry.OpType = cl.OpType.String()
		entry.SubType = cl.SubType.String()
		entry.Tables = cl.Tables
	}
	l.Log(entry)
}

// Event logs a generic event with a detail message.
func (l *Logger) Event(connID int64, event, detail string) {
	if l == nil {
		return
	}
	l.Log(LogEntry{
		Level:  "info",
		ConnID: connID,
		Event:  event,
		Detail: detail,
	})
}

// Error logs an error event.
func (l *Logger) Error(connID int64, event string, err error) {
	if l == nil {
		return
	}
	l.Log(LogEntry{
		Level:  "error",
		ConnID: connID,
		Event:  event,
		Error:  err.Error(),
	})
}

// Close flushes and closes the log file.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
