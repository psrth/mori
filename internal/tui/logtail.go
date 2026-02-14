package tui

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/mori-dev/mori/internal/logging"
)

// Tailer tracks a file position and reads new JSON log lines.
type Tailer struct {
	path   string
	offset int64
}

// NewTailer creates a tailer positioned at the end of the file,
// after reading the last initialCount entries.
func NewTailer(path string, initialCount int) (*Tailer, []logging.LogEntry) {
	t := &Tailer{path: path}

	// Read initial entries from the end of the file.
	entries := t.readLast(initialCount)

	// Position offset at the current end of the file.
	if info, err := os.Stat(path); err == nil {
		t.offset = info.Size()
	}

	return t, entries
}

// ReadNew reads any new lines appended since the last call.
func (t *Tailer) ReadNew() []logging.LogEntry {
	f, err := os.Open(t.path)
	if err != nil {
		return nil
	}
	defer f.Close()

	// Detect truncation (e.g., after mori reset).
	info, err := f.Stat()
	if err != nil {
		return nil
	}
	if info.Size() < t.offset {
		t.offset = 0
	}

	if info.Size() == t.offset {
		return nil // no new data
	}

	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		return nil
	}

	var entries []logging.LogEntry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry logging.LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err == nil {
			entries = append(entries, entry)
		}
	}

	// Update offset to current position.
	if pos, err := f.Seek(0, io.SeekCurrent); err == nil {
		t.offset = pos
	}

	return entries
}

// readLast reads the last n log entries from the file.
func (t *Tailer) readLast(n int) []logging.LogEntry {
	f, err := os.Open(t.path)
	if err != nil {
		return nil
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil || stat.Size() == 0 {
		return nil
	}

	// Read backward in chunks to find the last N newlines.
	const chunkSize = 4096
	var buf []byte
	newlineCount := 0
	offset := stat.Size()

	for offset > 0 && newlineCount <= n {
		readSize := int64(chunkSize)
		if readSize > offset {
			readSize = offset
		}
		offset -= readSize

		chunk := make([]byte, readSize)
		if _, err := f.ReadAt(chunk, offset); err != nil {
			break
		}
		buf = append(chunk, buf...)

		for _, b := range chunk {
			if b == '\n' {
				newlineCount++
			}
		}
	}

	// Parse lines and take the last N.
	var entries []logging.LogEntry
	for _, line := range strings.Split(string(buf), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry logging.LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err == nil {
			entries = append(entries, entry)
		}
	}

	if len(entries) > n {
		entries = entries[len(entries)-n:]
	}
	return entries
}
