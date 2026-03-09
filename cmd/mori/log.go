package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/logging"
	"github.com/mori-dev/mori/internal/ui"
	"github.com/spf13/cobra"
)

var logCmd = &cobra.Command{
	Use:   "log [connection-name]",
	Short: "Show proxy activity log",
	Long: `Stream or dump recent proxy activity: queries, routing decisions,
hydration events, and timing information. Useful for debugging and
understanding how Mori routes your application's queries.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runLog,
}

func init() {
	logCmd.Flags().IntP("tail", "n", 50, "Number of recent log entries to show")
	logCmd.Flags().BoolP("follow", "f", false, "Follow log output (like tail -f)")
}

func runLog(cmd *cobra.Command, args []string) error {
	tailN, _ := cmd.Flags().GetInt("tail")
	follow, _ := cmd.Flags().GetBool("follow")

	projectRoot, err := config.FindProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to determine project root: %w", err)
	}

	connName, err := resolveInitializedConnection(projectRoot, args)
	if err != nil {
		return err
	}

	logPath := config.ConnLogFilePath(projectRoot, connName)
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		fmt.Println("No log file found. Start the proxy with 'mori start' to generate logs.")
		return nil
	}

	// Read last N lines.
	lines, err := readLastNLines(logPath, tailN)
	if err != nil {
		return fmt.Errorf("failed to read log file: %w", err)
	}

	for _, line := range lines {
		printLogEntry(line)
	}

	if !follow {
		return nil
	}

	// Follow mode: watch for new lines.
	return followLog(logPath)
}

// readLastNLines reads the last n lines from a file efficiently by seeking backward.
func readLastNLines(path string, n int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()
	if size == 0 {
		return nil, nil
	}

	// Read backward in chunks to find the last N newlines.
	const chunkSize = 4096
	var buf []byte
	newlineCount := 0
	offset := size

	for offset > 0 && newlineCount <= n {
		readSize := int64(chunkSize)
		if readSize > offset {
			readSize = offset
		}
		offset -= readSize

		chunk := make([]byte, readSize)
		if _, err := f.ReadAt(chunk, offset); err != nil {
			return nil, err
		}
		buf = append(chunk, buf...)

		for _, b := range chunk {
			if b == '\n' {
				newlineCount++
			}
		}
	}

	// Split into lines and take the last N.
	allLines := strings.Split(string(buf), "\n")
	// Filter empty lines (common at start/end).
	var lines []string
	for _, l := range allLines {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines, nil
}

// printLogEntry parses a JSON log line and prints it in a human-readable format.
func printLogEntry(line string) {
	var entry logging.LogEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		// Not valid JSON — print raw.
		fmt.Println(line)
		return
	}

	ts := entry.Timestamp.Local().Format("15:04:05.000")

	var parts []string
	parts = append(parts, ui.Dim(ts))

	if entry.ConnID > 0 {
		parts = append(parts, ui.Dim(fmt.Sprintf("[conn %d]", entry.ConnID)))
	}

	parts = append(parts, ui.Bold(strings.ToUpper(entry.Event)))

	if entry.OpType != "" {
		routeInfo := entry.OpType
		if entry.SubType != "" {
			routeInfo += "/" + entry.SubType
		}
		if len(entry.Tables) > 0 {
			routeInfo += " " + strings.Join(entry.Tables, ",")
		}
		if entry.Strategy != "" {
			routeInfo += " -> " + entry.Strategy
		}
		if strings.Contains(routeInfo, "prod") {
			parts = append(parts, ui.Green(routeInfo))
		} else if strings.Contains(routeInfo, "shadow") {
			parts = append(parts, ui.Yellow(routeInfo))
		} else {
			parts = append(parts, routeInfo)
		}
	}

	if entry.DurationMs > 0 {
		parts = append(parts, ui.Cyan(fmt.Sprintf("%.1fms", entry.DurationMs)))
	}

	if entry.Detail != "" {
		parts = append(parts, entry.Detail)
	}

	if entry.Error != "" {
		parts = append(parts, ui.Red("error="+entry.Error))
	}

	fmt.Println(strings.Join(parts, "  "))

	// Show SQL on a continuation line if present.
	if entry.SQL != "" {
		sql := entry.SQL
		if len(sql) > 120 {
			sql = sql[:120] + "..."
		}
		fmt.Printf("  %s\n", ui.Dim(sql))
	}
}

// followLog tails the log file, printing new lines as they appear.
func followLog(logPath string) error {
	f, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	reader := bufio.NewReader(f)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		// Fall back to polling if fsnotify fails.
		return followLogPoll(f, reader)
	}
	defer watcher.Close()

	if err := watcher.Add(logPath); err != nil {
		return followLogPoll(f, reader)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("--- following log (Ctrl+C to stop) ---")

	for {
		select {
		case <-sigCh:
			fmt.Println()
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Write) {
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						break
					}
					line = strings.TrimSpace(line)
					if line != "" {
						printLogEntry(line)
					}
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			fmt.Fprintf(os.Stderr, "watcher error: %v\n", err)
		}
	}
}

// followLogPoll is the fallback polling implementation.
func followLogPoll(_ *os.File, reader *bufio.Reader) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("--- following log (Ctrl+C to stop) ---")

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			fmt.Println()
			return nil
		case <-ticker.C:
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					break
				}
				line = strings.TrimSpace(line)
				if line != "" {
					printLogEntry(line)
				}
			}
		}
	}
}
