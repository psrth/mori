package core

import (
	"testing"
)

func TestSerializeCompositePK_SingleColumn(t *testing.T) {
	got := SerializeCompositePK([]string{"id"}, []string{"42"})
	if got != "42" {
		t.Errorf("SerializeCompositePK single = %q, want %q", got, "42")
	}
}

func TestSerializeCompositePK_MultiColumn(t *testing.T) {
	got := SerializeCompositePK([]string{"org_id", "user_id"}, []string{"1", "42"})
	// Should produce deterministic JSON.
	if got != `{"org_id":"1","user_id":"42"}` {
		t.Errorf("SerializeCompositePK composite = %q, want %q", got, `{"org_id":"1","user_id":"42"}`)
	}
}

func TestSerializeCompositePK_ThreeColumns(t *testing.T) {
	got := SerializeCompositePK(
		[]string{"a", "b", "c"},
		[]string{"x", "y", "z"},
	)
	want := `{"a":"x","b":"y","c":"z"}`
	if got != want {
		t.Errorf("SerializeCompositePK 3-col = %q, want %q", got, want)
	}
}

func TestDeserializeCompositePK_SingleColumn(t *testing.T) {
	got := DeserializeCompositePK("42", []string{"id"})
	if got["id"] != "42" {
		t.Errorf("DeserializeCompositePK single: id = %q, want %q", got["id"], "42")
	}
}

func TestDeserializeCompositePK_MultiColumn(t *testing.T) {
	serialized := SerializeCompositePK([]string{"org_id", "user_id"}, []string{"1", "42"})
	got := DeserializeCompositePK(serialized, []string{"org_id", "user_id"})

	if got["org_id"] != "1" {
		t.Errorf("org_id = %q, want %q", got["org_id"], "1")
	}
	if got["user_id"] != "42" {
		t.Errorf("user_id = %q, want %q", got["user_id"], "42")
	}
}

func TestDeserializeCompositePK_Roundtrip(t *testing.T) {
	cols := []string{"a", "b", "c"}
	vals := []string{"hello", "world", "123"}

	serialized := SerializeCompositePK(cols, vals)
	deserialized := DeserializeCompositePK(serialized, cols)

	for i, col := range cols {
		if deserialized[col] != vals[i] {
			t.Errorf("roundtrip: %s = %q, want %q", col, deserialized[col], vals[i])
		}
	}
}

func TestDeserializeCompositePK_InvalidJSON(t *testing.T) {
	// If JSON parsing fails, should fall back to treating as single value.
	got := DeserializeCompositePK("not-json", []string{"a", "b"})
	if got["a"] != "not-json" {
		t.Errorf("fallback: a = %q, want %q", got["a"], "not-json")
	}
}

func TestBuildCompositeWHERE_SingleColumn(t *testing.T) {
	quote := func(s string) string { return `"` + s + `"` }
	literal := func(s string) string { return `'` + s + `'` }

	got := BuildCompositeWHERE(
		[]string{"id"},
		map[string]string{"id": "42"},
		quote, literal,
	)
	want := `"id" = '42'`
	if got != want {
		t.Errorf("BuildCompositeWHERE single = %q, want %q", got, want)
	}
}

func TestBuildCompositeWHERE_MultiColumn(t *testing.T) {
	quote := func(s string) string { return `"` + s + `"` }
	literal := func(s string) string { return `'` + s + `'` }

	got := BuildCompositeWHERE(
		[]string{"org_id", "user_id"},
		map[string]string{"org_id": "1", "user_id": "42"},
		quote, literal,
	)
	want := `"org_id" = '1' AND "user_id" = '42'`
	if got != want {
		t.Errorf("BuildCompositeWHERE composite = %q, want %q", got, want)
	}
}
