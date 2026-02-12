package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	coreSchema "github.com/mori-dev/mori/internal/core/schema"
)

// pluralize returns singular when n == 1, plural otherwise.
func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}

// formatNumber formats an int64 with comma separators (e.g. 10000001 → "10,000,001").
func formatNumber(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// formatSchemaDiff formats a TableDiff as a compact summary string
// (e.g. "+phone (TEXT), -fax").
func formatSchemaDiff(diff *coreSchema.TableDiff) string {
	var parts []string
	for _, col := range diff.Added {
		parts = append(parts, fmt.Sprintf("+%s (%s)", col.Name, col.Type))
	}
	for _, col := range diff.Dropped {
		parts = append(parts, "-"+col)
	}
	for old, newName := range diff.Renamed {
		parts = append(parts, fmt.Sprintf("%s->%s", old, newName))
	}
	for col, types := range diff.TypeChanged {
		parts = append(parts, fmt.Sprintf("%s: %s->%s", col, types[0], types[1]))
	}
	return strings.Join(parts, ", ")
}

// isProxyRunning checks the PID file and returns the PID and whether the
// process is still alive.
func isProxyRunning(pidPath string) (int, bool) {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return 0, false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return pid, false
	}
	return pid, true
}

// quoteIdent quotes a PostgreSQL identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
