package tui

import "github.com/charmbracelet/bubbles/key"

// KeyMap defines all keyboard shortcuts for the TUI.
type KeyMap struct {
	Quit    key.Binding
	Help    key.Binding
	Inspect key.Binding
	Back    key.Binding
	Up      key.Binding
	Down    key.Binding
	Enter   key.Binding
	Search  key.Binding
}

// DefaultKeyMap is the default set of key bindings.
var DefaultKeyMap = KeyMap{
	Quit:    key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	Help:    key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help")),
	Inspect: key.NewBinding(key.WithKeys("i"), key.WithHelp("i", "inspect")),
	Back:    key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "back")),
	Up:      key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("up/k", "up")),
	Down:    key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("down/j", "down")),
	Enter:   key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "select")),
	Search:  key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search")),
}
