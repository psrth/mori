package tui

import (
	"os"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/mori-dev/mori/internal/core/config"
	"github.com/mori-dev/mori/internal/logging"
)

// Model is the top-level bubbletea model.
type Model struct {
	// Terminal dimensions.
	width  int
	height int

	// Paths.
	projectRoot string
	moriDir     string
	logPath     string

	// Cached state.
	snap Snapshot

	// Log stream.
	logEntries   []logging.LogEntry
	tailer       *Tailer
	totalQueries int

	// Sub-models.
	tableList TableListModel

	// UI state.
	inspecting   bool
	inspectTable string
	showHelp     bool
	searching    bool
	searchBuf    string
	searchQuery  string
}

// NewModel creates a new TUI model.
func NewModel(projectRoot string, initialTail int) Model {
	moriDir := config.MoriDirPath(projectRoot)
	logPath := config.LogFilePath(projectRoot)

	// Read initial state.
	snap := ReadSnapshot(projectRoot, moriDir)

	// Initialize log tailer.
	var tailer *Tailer
	var initialEntries []logging.LogEntry
	if _, err := os.Stat(logPath); err == nil {
		tailer, initialEntries = NewTailer(logPath, initialTail)
	}

	return Model{
		projectRoot:  projectRoot,
		moriDir:      moriDir,
		logPath:      logPath,
		snap:         snap,
		logEntries:   initialEntries,
		tailer:       tailer,
		totalQueries: len(initialEntries),
	}
}

// Init starts the periodic polling commands.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		scheduleStateTick(),
		scheduleLogTick(),
	)
}

// Update handles all messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case stateTickMsg:
		return m, readStateCmd(m.projectRoot, m.moriDir)

	case StateRefreshedMsg:
		if msg.Err == nil {
			m.snap = msg.Snap
			m.tableList.Refresh(m.snap)
		}
		return m, scheduleStateTick()

	case logTickMsg:
		return m, readLogCmd(m.tailer, m.logPath)

	case LogEntriesMsg:
		if len(msg.Entries) > 0 {
			m.logEntries = append(m.logEntries, msg.Entries...)
			if len(m.logEntries) > 2000 {
				m.logEntries = m.logEntries[len(m.logEntries)-2000:]
			}
			for _, e := range msg.Entries {
				if e.Event == "query" {
					m.totalQueries++
				}
			}
		}
		return m, scheduleLogTick()

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Handle search input mode.
	if m.searching {
		return m.handleSearchInput(msg)
	}

	switch {
	case key.Matches(msg, DefaultKeyMap.Quit):
		return m, tea.Quit

	case key.Matches(msg, DefaultKeyMap.Help):
		m.showHelp = !m.showHelp
		return m, nil

	case key.Matches(msg, DefaultKeyMap.Back):
		if m.showHelp {
			m.showHelp = false
		} else if m.searching {
			m.searching = false
		} else if m.inspecting {
			m.inspecting = false
		}
		return m, nil

	case key.Matches(msg, DefaultKeyMap.Search):
		if !m.showHelp {
			m.searching = true
			m.searchBuf = m.searchQuery
		}
		return m, nil

	case key.Matches(msg, DefaultKeyMap.Up):
		if !m.showHelp && m.tableList.Cursor > 0 {
			m.tableList.Cursor--
			if m.inspecting && len(m.tableList.Tables) > 0 {
				m.inspectTable = m.tableList.Tables[m.tableList.Cursor]
			}
		}
		return m, nil

	case key.Matches(msg, DefaultKeyMap.Down):
		if !m.showHelp && m.tableList.Cursor < len(m.tableList.Tables)-1 {
			m.tableList.Cursor++
			if m.inspecting && len(m.tableList.Tables) > 0 {
				m.inspectTable = m.tableList.Tables[m.tableList.Cursor]
			}
		}
		return m, nil

	case key.Matches(msg, DefaultKeyMap.Enter), key.Matches(msg, DefaultKeyMap.Inspect):
		if !m.showHelp && len(m.tableList.Tables) > 0 {
			if m.inspecting {
				m.inspecting = false
			} else {
				m.inspecting = true
				m.inspectTable = m.tableList.Tables[m.tableList.Cursor]
			}
		}
		return m, nil
	}

	return m, nil
}

func (m Model) handleSearchInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEscape:
		m.searching = false
		return m, nil
	case tea.KeyEnter:
		m.searchQuery = m.searchBuf
		m.searching = false
		return m, nil
	case tea.KeyBackspace:
		if len(m.searchBuf) > 0 {
			m.searchBuf = m.searchBuf[:len(m.searchBuf)-1]
		}
		return m, nil
	default:
		if len(msg.Runes) > 0 {
			m.searchBuf += string(msg.Runes)
		}
		return m, nil
	}
}

// View renders the current screen.
func (m Model) View() string {
	if m.width == 0 || m.height == 0 {
		return ""
	}

	if m.showHelp {
		return RenderHelp(m.width, m.height)
	}

	layout := ComputeLayout(m.width, m.height)

	// Use searchBuf for live filtering during search, searchQuery otherwise.
	activeQuery := m.searchQuery
	if m.searching {
		activeQuery = m.searchBuf
	}

	return RenderDashboard(
		layout, m.snap, &m.tableList, m.logEntries, m.totalQueries,
		m.inspecting, m.inspectTable, activeQuery, m.searching,
	)
}

// Commands.

func scheduleStateTick() tea.Cmd {
	return tea.Tick(1*time.Second, func(time.Time) tea.Msg {
		return stateTickMsg{}
	})
}

func scheduleLogTick() tea.Cmd {
	return tea.Tick(200*time.Millisecond, func(time.Time) tea.Msg {
		return logTickMsg{}
	})
}

func readStateCmd(projectRoot, moriDir string) tea.Cmd {
	return func() tea.Msg {
		snap := ReadSnapshot(projectRoot, moriDir)
		return StateRefreshedMsg{Snap: snap}
	}
}

func readLogCmd(tailer *Tailer, logPath string) tea.Cmd {
	return func() tea.Msg {
		if tailer == nil {
			if _, err := os.Stat(logPath); err == nil {
				return LogEntriesMsg{}
			}
			return LogEntriesMsg{}
		}
		entries := tailer.ReadNew()
		return LogEntriesMsg{Entries: entries}
	}
}
