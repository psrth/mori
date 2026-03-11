package tui

import (
	"github.com/psrth/mori/internal/logging"
)

// StateRefreshedMsg is sent when the state poller has read all .mori/ files.
type StateRefreshedMsg struct {
	Snap Snapshot
	Err  error
}

// LogEntriesMsg carries new log entries from the tailer.
type LogEntriesMsg struct {
	Entries      []logging.LogEntry
	Reset        bool   // true when the log file was deleted (e.g. mori reset)
	NewTailerPath string // non-empty when a new log file appeared and needs a tailer
}

// stateTickMsg triggers a state file re-read.
type stateTickMsg struct{}

// logTickMsg triggers a log file tail check.
type logTickMsg struct{}
