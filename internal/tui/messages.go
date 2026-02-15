package tui

import (
	"github.com/mori-dev/mori/internal/logging"
)

// StateRefreshedMsg is sent when the state poller has read all .mori/ files.
type StateRefreshedMsg struct {
	Snap Snapshot
	Err  error
}

// LogEntriesMsg carries new log entries from the tailer.
type LogEntriesMsg struct {
	Entries []logging.LogEntry
}

// stateTickMsg triggers a state file re-read.
type stateTickMsg struct{}

// logTickMsg triggers a log file tail check.
type logTickMsg struct{}
