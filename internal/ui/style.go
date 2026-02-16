package ui

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// Colors — matches the TUI theme palette in internal/tui/theme.go.
var (
	ColorGreen  = lipgloss.Color("#22C55E")
	ColorRed    = lipgloss.Color("#EF4444")
	ColorYellow = lipgloss.Color("#EAB308")
	ColorCyan   = lipgloss.Color("#06B6D4")
	ColorDim    = lipgloss.Color("#6B7280")
	ColorWhite  = lipgloss.Color("#F9FAFB")
	ColorPurple = lipgloss.Color("#A855F7")
)

// Styles.
var (
	StyleSuccess   = lipgloss.NewStyle().Foreground(ColorGreen)
	StyleError     = lipgloss.NewStyle().Foreground(ColorRed)
	StyleWarn      = lipgloss.NewStyle().Foreground(ColorYellow)
	StyleDim       = lipgloss.NewStyle().Foreground(ColorDim)
	StyleBold      = lipgloss.NewStyle().Bold(true).Foreground(ColorWhite)
	StyleHighlight = lipgloss.NewStyle().Foreground(ColorCyan)
)

// Unicode indicators.
const (
	IconDone     = "✓"
	IconFail     = "✗"
	IconActive   = "●"
	IconInactive = "○"
	IconArrow    = "→"
	IconWarn     = "!"
)

// Inline style wrappers.
func Bold(s string) string    { return StyleBold.Render(s) }
func Dim(s string) string     { return StyleDim.Render(s) }
func Green(s string) string   { return StyleSuccess.Render(s) }
func Red(s string) string     { return StyleError.Render(s) }
func Yellow(s string) string  { return StyleWarn.Render(s) }
func Cyan(s string) string    { return StyleHighlight.Render(s) }
func Purple(s string) string  { return lipgloss.NewStyle().Foreground(ColorPurple).Render(s) }

// StepDone prints a green checkmark with a message.
func StepDone(msg string) {
	fmt.Println(Green(IconDone) + " " + msg)
}

// StepFail prints a red cross with a message.
func StepFail(msg string) {
	fmt.Println(Red(IconFail) + " " + msg)
}

// StepWarn prints a yellow warning indicator with a message.
func StepWarn(msg string) {
	fmt.Println(Yellow(IconWarn) + " " + msg)
}

// Info prints an indented dim message.
func Info(msg string) {
	fmt.Println("  " + Dim(msg))
}

// Braille spinner frames.
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// Spinner runs fn() with an animated braille spinner, clears the line
// on completion, and returns the error from fn.
func Spinner(msg string, fn func() error) error {
	var result error
	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		result = fn()
		close(done)
	}()

	frame := 0
	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			// Clear the spinner line.
			fmt.Fprintf(os.Stderr, "\r\033[K")
			wg.Wait()
			return result
		case <-ticker.C:
			fmt.Fprintf(os.Stderr, "\r%s %s", Green(spinnerFrames[frame%len(spinnerFrames)]), msg)
			frame++
		}
	}
}
