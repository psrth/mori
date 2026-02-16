package tui

import (
	"fmt"
	"sort"
	"strings"
)

// TableListModel manages the table list panel state.
type TableListModel struct {
	Cursor int
	Tables []string // sorted table names
}

// Refresh rebuilds the table list from the current snapshot.
// Called in Update() so the list is available for key handling.
func (m *TableListModel) Refresh(snap Snapshot) {
	tableSet := make(map[string]bool)
	if snap.Tables != nil {
		for t := range snap.Tables {
			tableSet[t] = true
		}
	}
	if snap.DeltaMap != nil {
		for _, t := range snap.DeltaMap.Tables() {
			tableSet[t] = true
		}
	}
	if snap.Tombstones != nil {
		for _, t := range snap.Tombstones.Tables() {
			tableSet[t] = true
		}
	}

	m.Tables = make([]string, 0, len(tableSet))
	for t := range tableSet {
		m.Tables = append(m.Tables, t)
	}
	sort.Strings(m.Tables)

	// Clamp cursor.
	if len(m.Tables) == 0 {
		m.Cursor = 0
	} else if m.Cursor >= len(m.Tables) {
		m.Cursor = len(m.Tables) - 1
	}
}

// RenderTableList renders the table list content (without box — box is added by dashboard).
func RenderTableList(innerW, innerH int, m *TableListModel, snap Snapshot) string {
	if len(m.Tables) == 0 {
		return DimText.Render(" (no tables)")
	}

	// Column widths: name gets the rest, indicators are right-aligned.
	nameW := innerW - 14 // reserve space for "▸ " + indicators
	if nameW < 8 {
		nameW = 8
	}

	// Scrolling window.
	startIdx := 0
	if m.Cursor >= innerH {
		startIdx = m.Cursor - innerH + 1
	}

	var rows []string
	for i := startIdx; i < len(m.Tables) && len(rows) < innerH; i++ {
		t := m.Tables[i]
		row := renderTableRow(t, i == m.Cursor, nameW, innerW, snap)
		rows = append(rows, row)
	}

	return strings.Join(rows, "\n")
}

func renderTableRow(table string, selected bool, nameW, totalW int, snap Snapshot) string {
	// Build indicator string.
	var indicators []string

	hasInserts := false
	hasDeltaCount := false
	if snap.DeltaMap != nil {
		editCount := snap.DeltaMap.CountForTable(table)
		hasInserts = snap.DeltaMap.HasInserts(table)
		insertCount := snap.DeltaMap.InsertCountForTable(table)
		if editCount > 0 {
			hasDeltaCount = true
			indicators = append(indicators, EditStyle.Render(fmt.Sprintf("~%d", editCount)))
		}
		if hasInserts {
			if insertCount > 0 {
				indicators = append(indicators, InsertIndicator.Render(fmt.Sprintf("+%d", insertCount)))
			} else {
				indicators = append(indicators, InsertIndicator.Render("+"))
			}
		}
	}

	hasTombstones := false
	if snap.Tombstones != nil {
		count := snap.Tombstones.CountForTable(table)
		if count > 0 {
			hasTombstones = true
			indicators = append(indicators, DeleteIndicator.Render(fmt.Sprintf("-%d", count)))
		}
	}

	if snap.SchemaReg != nil && snap.SchemaReg.HasDiff(table) {
		indicators = append(indicators, SchemaDiffIndicator.Render("▲"))
	}

	prefix := " "
	if selected {
		prefix = "▸"
	}

	// Determine table name style.
	_, inProd := snap.Tables[table]
	isNew := !inProd && (hasInserts || hasDeltaCount)
	isDead := !inProd && hasTombstones && !hasInserts && !hasDeltaCount

	nameStr := Ellipsis(table, nameW)
	var styledName string
	switch {
	case isNew:
		styledName = NewTableName.Render(nameStr)
	case isDead:
		styledName = DeadTableName.Render(nameStr)
	default:
		styledName = nameStr
	}

	indicatorStr := strings.Join(indicators, " ")

	// Build: prefix + space + name + padding + indicators
	// Pad based on raw name width to keep alignment.
	namePad := nameW - len([]rune(nameStr))
	if namePad < 0 {
		namePad = 0
	}
	left := prefix + " " + styledName + strings.Repeat(" ", namePad)
	line := left + " " + indicatorStr

	if selected {
		return SelectedRow.Render(PadRight(line, totalW))
	}
	return line
}

func isHot(table string, snap Snapshot) bool {
	if snap.DeltaMap != nil && (snap.DeltaMap.CountForTable(table) > 0 || snap.DeltaMap.HasInserts(table)) {
		return true
	}
	if snap.Tombstones != nil && snap.Tombstones.CountForTable(table) > 0 {
		return true
	}
	if snap.SchemaReg != nil && snap.SchemaReg.HasDiff(table) {
		return true
	}
	return false
}
