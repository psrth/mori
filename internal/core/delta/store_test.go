package delta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadDeltaMap(t *testing.T) {
	dir := t.TempDir()

	m := NewMap()
	m.Add("users", "1")
	m.Add("users", "2")
	m.Add("orders", "10")

	if err := WriteDeltaMap(dir, m); err != nil {
		t.Fatalf("WriteDeltaMap() error: %v", err)
	}

	got, err := ReadDeltaMap(dir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}

	if !got.IsDelta("users", "1") || !got.IsDelta("users", "2") {
		t.Error("read map missing users entries")
	}
	if !got.IsDelta("orders", "10") {
		t.Error("read map missing orders entry")
	}
	if got.IsDelta("users", "99") {
		t.Error("read map has phantom entry")
	}
}

func TestWriteAndReadTombstoneSet(t *testing.T) {
	dir := t.TempDir()

	ts := NewTombstoneSet()
	ts.Add("users", "5")
	ts.Add("products", "20")

	if err := WriteTombstoneSet(dir, ts); err != nil {
		t.Fatalf("WriteTombstoneSet() error: %v", err)
	}

	got, err := ReadTombstoneSet(dir)
	if err != nil {
		t.Fatalf("ReadTombstoneSet() error: %v", err)
	}

	if !got.IsTombstoned("users", "5") {
		t.Error("read set missing users entry")
	}
	if !got.IsTombstoned("products", "20") {
		t.Error("read set missing products entry")
	}
}

func TestReadDeltaMapMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadDeltaMap(dir)
	if err == nil {
		t.Error("ReadDeltaMap() on missing file should return error")
	}
}

func TestReadTombstoneSetMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadTombstoneSet(dir)
	if err == nil {
		t.Error("ReadTombstoneSet() on missing file should return error")
	}
}

func TestDeltaMapFileFormat(t *testing.T) {
	dir := t.TempDir()

	m := NewMap()
	m.Add("users", "42")
	m.AddInsertCount("orders", 5)

	if err := WriteDeltaMap(dir, m); err != nil {
		t.Fatalf("WriteDeltaMap() error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, DeltaFile))
	if err != nil {
		t.Fatalf("ReadFile() error: %v", err)
	}

	// Should be valid indented JSON with deltas and inserted_tables as map[string]int.
	var raw deltaFile
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("file is not valid JSON: %v", err)
	}
	if len(raw.Deltas["users"]) != 1 || raw.Deltas["users"][0] != "42" {
		t.Errorf("deltas = %v, want {users: [42]}", raw.Deltas)
	}
	if raw.InsertedTables["orders"] != 5 {
		t.Errorf("inserted_tables = %v, want {orders: 5}", raw.InsertedTables)
	}
}

func TestDeltaMapLegacyFormat(t *testing.T) {
	dir := t.TempDir()

	// Write old format: inserted_tables as []string.
	legacy := `{"deltas":{"users":["1"]},"inserted_tables":["orders","products"]}`
	if err := os.WriteFile(filepath.Join(dir, DeltaFile), []byte(legacy), 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	got, err := ReadDeltaMap(dir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}

	if !got.IsDelta("users", "1") {
		t.Error("read map missing users entry")
	}
	if !got.HasInserts("orders") {
		t.Error("read map missing orders inserts")
	}
	if !got.HasInserts("products") {
		t.Error("read map missing products inserts")
	}
	// Legacy format should have count 0 (unknown).
	if got.InsertCountForTable("orders") != 0 {
		t.Errorf("InsertCountForTable(orders) = %d, want 0 (unknown)", got.InsertCountForTable("orders"))
	}
}

func TestDeltaMapInsertCounts(t *testing.T) {
	dir := t.TempDir()

	m := NewMap()
	m.AddInsertCount("users", 3)
	m.AddInsertCount("users", 2)
	m.MarkInserted("orders") // unknown count

	if err := WriteDeltaMap(dir, m); err != nil {
		t.Fatalf("WriteDeltaMap() error: %v", err)
	}

	got, err := ReadDeltaMap(dir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}

	if got.InsertCountForTable("users") != 5 {
		t.Errorf("InsertCountForTable(users) = %d, want 5", got.InsertCountForTable("users"))
	}
	if got.InsertCountForTable("orders") != 0 {
		t.Errorf("InsertCountForTable(orders) = %d, want 0", got.InsertCountForTable("orders"))
	}
	if !got.HasInserts("users") {
		t.Error("HasInserts(users) = false, want true")
	}
	if !got.HasInserts("orders") {
		t.Error("HasInserts(orders) = false, want true")
	}
}

func TestDeltaMapEmptyPersistence(t *testing.T) {
	dir := t.TempDir()

	m := NewMap()
	if err := WriteDeltaMap(dir, m); err != nil {
		t.Fatalf("WriteDeltaMap() error: %v", err)
	}

	got, err := ReadDeltaMap(dir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}

	if tables := got.Tables(); tables != nil {
		t.Errorf("Tables() = %v after loading empty map, want nil", tables)
	}
}

func TestDeltaMapPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()

	m := NewMap()
	m.Add("users", "1")
	m.Add("users", "2")
	m.Add("users", "3")
	m.Add("orders", "100")
	m.Add("orders", "200")
	m.Add("products", "42")

	if err := WriteDeltaMap(dir, m); err != nil {
		t.Fatalf("WriteDeltaMap() error: %v", err)
	}

	got, err := ReadDeltaMap(dir)
	if err != nil {
		t.Fatalf("ReadDeltaMap() error: %v", err)
	}

	// Verify all tables present.
	tables := got.Tables()
	wantTables := []string{"orders", "products", "users"}
	if len(tables) != len(wantTables) {
		t.Fatalf("Tables() = %v, want %v", tables, wantTables)
	}
	for i := range wantTables {
		if tables[i] != wantTables[i] {
			t.Errorf("Tables()[%d] = %q, want %q", i, tables[i], wantTables[i])
		}
	}

	// Verify counts.
	if got.CountForTable("users") != 3 {
		t.Errorf("CountForTable(users) = %d, want 3", got.CountForTable("users"))
	}
	if got.CountForTable("orders") != 2 {
		t.Errorf("CountForTable(orders) = %d, want 2", got.CountForTable("orders"))
	}
	if got.CountForTable("products") != 1 {
		t.Errorf("CountForTable(products) = %d, want 1", got.CountForTable("products"))
	}

	// Verify specific entries.
	for _, pk := range []string{"1", "2", "3"} {
		if !got.IsDelta("users", pk) {
			t.Errorf("IsDelta(users, %s) = false, want true", pk)
		}
	}
}
