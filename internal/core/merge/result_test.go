package merge

import (
	"testing"

	"github.com/psrth/mori/internal/core"
)

func TestEmptyNil(t *testing.T) {
	if !Empty(nil) {
		t.Error("Empty(nil) = false, want true")
	}
}

func TestEmptyNoRows(t *testing.T) {
	rs := &core.ResultSet{Columns: []string{"id"}, Rows: nil}
	if !Empty(rs) {
		t.Error("Empty(no rows) = false, want true")
	}
}

func TestEmptyWithRows(t *testing.T) {
	rs := &core.ResultSet{
		Columns: []string{"id"},
		Rows:    []core.Row{{"id": 1}},
	}
	if Empty(rs) {
		t.Error("Empty(with rows) = true, want false")
	}
}

func TestRowCountNil(t *testing.T) {
	if got := RowCount(nil); got != 0 {
		t.Errorf("RowCount(nil) = %d, want 0", got)
	}
}

func TestRowCountPopulated(t *testing.T) {
	rs := &core.ResultSet{
		Columns: []string{"id", "name"},
		Rows:    []core.Row{{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}},
	}
	if got := RowCount(rs); got != 2 {
		t.Errorf("RowCount() = %d, want 2", got)
	}
}
