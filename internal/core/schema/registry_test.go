package schema

import (
	"sync"
	"testing"
)

func TestNewRegistryEmpty(t *testing.T) {
	r := NewRegistry()
	if got := r.Tables(); got != nil {
		t.Errorf("Tables() = %v, want nil", got)
	}
}

func TestRecordAddColumn(t *testing.T) {
	r := NewRegistry()
	r.RecordAddColumn("users", Column{Name: "phone", Type: "TEXT"})

	d := r.GetDiff("users")
	if d == nil {
		t.Fatal("GetDiff(users) = nil, want non-nil")
	}
	if len(d.Added) != 1 {
		t.Fatalf("Added len = %d, want 1", len(d.Added))
	}
	if d.Added[0].Name != "phone" || d.Added[0].Type != "TEXT" {
		t.Errorf("Added[0] = %+v, want {Name:phone Type:TEXT}", d.Added[0])
	}
	if d.Added[0].Default != nil {
		t.Errorf("Added[0].Default = %v, want nil", d.Added[0].Default)
	}
}

func TestRecordAddColumnWithDefault(t *testing.T) {
	r := NewRegistry()
	def := "unknown"
	r.RecordAddColumn("users", Column{Name: "status", Type: "VARCHAR(20)", Default: &def})

	d := r.GetDiff("users")
	if d.Added[0].Default == nil || *d.Added[0].Default != "unknown" {
		t.Errorf("Added[0].Default = %v, want 'unknown'", d.Added[0].Default)
	}
}

func TestRecordAddMultipleColumns(t *testing.T) {
	r := NewRegistry()
	r.RecordAddColumn("users", Column{Name: "phone", Type: "TEXT"})
	r.RecordAddColumn("users", Column{Name: "email", Type: "VARCHAR(255)"})

	d := r.GetDiff("users")
	if len(d.Added) != 2 {
		t.Fatalf("Added len = %d, want 2", len(d.Added))
	}
}

func TestRecordDropColumn(t *testing.T) {
	r := NewRegistry()
	r.RecordDropColumn("users", "legacy_field")

	d := r.GetDiff("users")
	if d == nil {
		t.Fatal("GetDiff(users) = nil")
	}
	if len(d.Dropped) != 1 || d.Dropped[0] != "legacy_field" {
		t.Errorf("Dropped = %v, want [legacy_field]", d.Dropped)
	}
}

func TestRecordRenameColumn(t *testing.T) {
	r := NewRegistry()
	r.RecordRenameColumn("users", "fname", "first_name")

	d := r.GetDiff("users")
	if d == nil {
		t.Fatal("GetDiff(users) = nil")
	}
	if d.Renamed["fname"] != "first_name" {
		t.Errorf("Renamed[fname] = %q, want first_name", d.Renamed["fname"])
	}
}

func TestRecordTypeChange(t *testing.T) {
	r := NewRegistry()
	r.RecordTypeChange("users", "age", "INTEGER", "BIGINT")

	d := r.GetDiff("users")
	if d == nil {
		t.Fatal("GetDiff(users) = nil")
	}
	tc := d.TypeChanged["age"]
	if tc[0] != "INTEGER" || tc[1] != "BIGINT" {
		t.Errorf("TypeChanged[age] = %v, want [INTEGER BIGINT]", tc)
	}
}

func TestHasDiffTrue(t *testing.T) {
	r := NewRegistry()
	r.RecordDropColumn("users", "old_col")
	if !r.HasDiff("users") {
		t.Error("HasDiff(users) = false, want true")
	}
}

func TestHasDiffFalseForCleanTable(t *testing.T) {
	r := NewRegistry()
	if r.HasDiff("users") {
		t.Error("HasDiff(users) = true for clean registry, want false")
	}
}

func TestGetDiffReturnsNilForCleanTable(t *testing.T) {
	r := NewRegistry()
	if d := r.GetDiff("users"); d != nil {
		t.Errorf("GetDiff(users) = %+v, want nil", d)
	}
}

func TestGetDiffReturnsCopy(t *testing.T) {
	r := NewRegistry()
	r.RecordAddColumn("users", Column{Name: "phone", Type: "TEXT"})

	d := r.GetDiff("users")
	d.Added = append(d.Added, Column{Name: "injected", Type: "TEXT"})

	// Original should be unchanged.
	d2 := r.GetDiff("users")
	if len(d2.Added) != 1 {
		t.Errorf("GetDiff returned mutable reference: Added len = %d, want 1", len(d2.Added))
	}
}

func TestRegistryTables(t *testing.T) {
	r := NewRegistry()
	r.RecordAddColumn("orders", Column{Name: "total", Type: "DECIMAL"})
	r.RecordDropColumn("users", "old_col")

	got := r.Tables()
	want := []string{"orders", "users"}
	if len(got) != len(want) {
		t.Fatalf("Tables() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Tables()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestWriteAndReadRegistry(t *testing.T) {
	dir := t.TempDir()

	r := NewRegistry()
	def := "0"
	r.RecordAddColumn("users", Column{Name: "phone", Type: "TEXT"})
	r.RecordAddColumn("users", Column{Name: "score", Type: "INT", Default: &def})
	r.RecordDropColumn("users", "legacy")
	r.RecordRenameColumn("orders", "amt", "amount")
	r.RecordTypeChange("orders", "total", "INTEGER", "DECIMAL(10,2)")

	if err := WriteRegistry(dir, r); err != nil {
		t.Fatalf("WriteRegistry() error: %v", err)
	}

	got, err := ReadRegistry(dir)
	if err != nil {
		t.Fatalf("ReadRegistry() error: %v", err)
	}

	// Verify users diff.
	ud := got.GetDiff("users")
	if ud == nil {
		t.Fatal("GetDiff(users) = nil after read")
	}
	if len(ud.Added) != 2 {
		t.Errorf("users.Added len = %d, want 2", len(ud.Added))
	}
	if len(ud.Dropped) != 1 || ud.Dropped[0] != "legacy" {
		t.Errorf("users.Dropped = %v, want [legacy]", ud.Dropped)
	}

	// Verify orders diff.
	od := got.GetDiff("orders")
	if od == nil {
		t.Fatal("GetDiff(orders) = nil after read")
	}
	if od.Renamed["amt"] != "amount" {
		t.Errorf("orders.Renamed[amt] = %q, want amount", od.Renamed["amt"])
	}
	tc := od.TypeChanged["total"]
	if tc[0] != "INTEGER" || tc[1] != "DECIMAL(10,2)" {
		t.Errorf("orders.TypeChanged[total] = %v, want [INTEGER DECIMAL(10,2)]", tc)
	}
}

func TestReadRegistryMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadRegistry(dir)
	if err == nil {
		t.Error("ReadRegistry() on missing file should return error")
	}
}

func TestRecordForeignKey(t *testing.T) {
	r := NewRegistry()
	fk := ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "CASCADE",
		OnUpdate:       "NO ACTION",
	}
	r.RecordForeignKey("orders", fk)

	fks := r.GetForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("GetForeignKeys(orders) len = %d, want 1", len(fks))
	}
	if fks[0].ConstraintName != "fk_orders_user" {
		t.Errorf("FK name = %q, want fk_orders_user", fks[0].ConstraintName)
	}
	if fks[0].ParentTable != "users" {
		t.Errorf("FK parent = %q, want users", fks[0].ParentTable)
	}
	if fks[0].OnDelete != "CASCADE" {
		t.Errorf("FK OnDelete = %q, want CASCADE", fks[0].OnDelete)
	}
}

func TestRecordForeignKeyReplaceDuplicate(t *testing.T) {
	r := NewRegistry()
	fk1 := ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	}
	r.RecordForeignKey("orders", fk1)

	fk2 := ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "CASCADE",
		OnUpdate:       "CASCADE",
	}
	r.RecordForeignKey("orders", fk2)

	fks := r.GetForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("GetForeignKeys(orders) len = %d, want 1 (deduped)", len(fks))
	}
	if fks[0].OnDelete != "CASCADE" {
		t.Errorf("FK OnDelete = %q, want CASCADE (updated)", fks[0].OnDelete)
	}
}

func TestGetForeignKeysNone(t *testing.T) {
	r := NewRegistry()
	fks := r.GetForeignKeys("orders")
	if fks != nil {
		t.Errorf("GetForeignKeys(orders) = %v, want nil", fks)
	}
}

func TestGetReferencingFKs(t *testing.T) {
	r := NewRegistry()
	fk := ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "CASCADE",
		OnUpdate:       "NO ACTION",
	}
	r.RecordForeignKey("orders", fk)

	// Query by parent table name.
	refs := r.GetReferencingFKs("users")
	if len(refs) != 1 {
		t.Fatalf("GetReferencingFKs(users) len = %d, want 1", len(refs))
	}
	if refs[0].ChildTable != "orders" {
		t.Errorf("FK child = %q, want orders", refs[0].ChildTable)
	}

	// No referencing FKs for a table that is not a parent.
	refs2 := r.GetReferencingFKs("orders")
	if len(refs2) != 0 {
		t.Errorf("GetReferencingFKs(orders) len = %d, want 0", len(refs2))
	}
}

func TestForeignKeyPersistence(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry()
	fk := ForeignKey{
		ConstraintName: "fk_orders_user",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "CASCADE",
		OnUpdate:       "SET NULL",
	}
	r.RecordForeignKey("orders", fk)

	if err := WriteRegistry(dir, r); err != nil {
		t.Fatalf("WriteRegistry() error: %v", err)
	}

	loaded, err := ReadRegistry(dir)
	if err != nil {
		t.Fatalf("ReadRegistry() error: %v", err)
	}

	fks := loaded.GetForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("loaded FK count = %d, want 1", len(fks))
	}
	if fks[0].ConstraintName != "fk_orders_user" {
		t.Errorf("loaded FK name = %q, want fk_orders_user", fks[0].ConstraintName)
	}
	if fks[0].OnDelete != "CASCADE" {
		t.Errorf("loaded FK OnDelete = %q, want CASCADE", fks[0].OnDelete)
	}
	if fks[0].OnUpdate != "SET NULL" {
		t.Errorf("loaded FK OnUpdate = %q, want SET NULL", fks[0].OnUpdate)
	}
}

func TestGetDiffCopiesForeignKeys(t *testing.T) {
	r := NewRegistry()
	fk := ForeignKey{
		ConstraintName: "fk_test",
		ChildTable:     "orders",
		ChildColumns:   []string{"user_id"},
		ParentTable:    "users",
		ParentColumns:  []string{"id"},
		OnDelete:       "NO ACTION",
		OnUpdate:       "NO ACTION",
	}
	r.RecordForeignKey("orders", fk)

	d := r.GetDiff("orders")
	if len(d.ForeignKeys) != 1 {
		t.Fatalf("ForeignKeys len = %d, want 1", len(d.ForeignKeys))
	}

	// Mutate the copy — original should be unchanged.
	d.ForeignKeys = append(d.ForeignKeys, ForeignKey{ConstraintName: "injected"})
	d2 := r.GetDiff("orders")
	if len(d2.ForeignKeys) != 1 {
		t.Errorf("GetDiff returned mutable FK reference: len = %d, want 1", len(d2.ForeignKeys))
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup

	// Writers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r.RecordAddColumn("users", Column{Name: "col", Type: "TEXT"})
				r.RecordDropColumn("users", "old")
				r.RecordRenameColumn("orders", "a", "b")
				r.RecordTypeChange("orders", "x", "INT", "BIGINT")
			}
		}(i)
	}

	// Readers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r.GetDiff("users")
				r.HasDiff("orders")
				r.Tables()
			}
		}()
	}

	wg.Wait()
}
