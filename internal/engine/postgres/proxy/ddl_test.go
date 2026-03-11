package proxy

import (
	"net"
	"sync"
	"testing"

	"github.com/psrth/mori/internal/core"
	coreSchema "github.com/psrth/mori/internal/core/schema"
)

// --- Parsing tests (pure, no I/O) ---

func TestParseDDLChangesAddColumn(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users ADD COLUMN phone TEXT")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlAddColumn {
		t.Errorf("Kind = %d, want ddlAddColumn", ch.Kind)
	}
	if ch.Table != "users" {
		t.Errorf("Table = %q, want %q", ch.Table, "users")
	}
	if ch.Column != "phone" {
		t.Errorf("Column = %q, want %q", ch.Column, "phone")
	}
	if ch.ColType != "TEXT" {
		t.Errorf("ColType = %q, want %q", ch.ColType, "TEXT")
	}
	if ch.Default != nil {
		t.Errorf("Default = %v, want nil", ch.Default)
	}
}

func TestParseDDLChangesAddColumnWithDefault(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active'")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlAddColumn {
		t.Errorf("Kind = %d, want ddlAddColumn", ch.Kind)
	}
	if ch.Column != "status" {
		t.Errorf("Column = %q, want %q", ch.Column, "status")
	}
	if ch.ColType != "VARCHAR(20)" {
		t.Errorf("ColType = %q, want %q", ch.ColType, "VARCHAR(20)")
	}
	if ch.Default == nil {
		t.Fatal("Default is nil, want non-nil")
	}
	if *ch.Default != "active" {
		t.Errorf("Default = %q, want %q", *ch.Default, "active")
	}
}

func TestParseDDLChangesDropColumn(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users DROP COLUMN fax")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlDropColumn {
		t.Errorf("Kind = %d, want ddlDropColumn", ch.Kind)
	}
	if ch.Table != "users" {
		t.Errorf("Table = %q, want %q", ch.Table, "users")
	}
	if ch.Column != "fax" {
		t.Errorf("Column = %q, want %q", ch.Column, "fax")
	}
}

func TestParseDDLChangesAlterColumnType(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users ALTER COLUMN age TYPE BIGINT")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlAlterType {
		t.Errorf("Kind = %d, want ddlAlterType", ch.Kind)
	}
	if ch.Column != "age" {
		t.Errorf("Column = %q, want %q", ch.Column, "age")
	}
	if ch.NewType != "BIGINT" {
		t.Errorf("NewType = %q, want %q", ch.NewType, "BIGINT")
	}
	if ch.OldType != "unknown" {
		t.Errorf("OldType = %q, want %q", ch.OldType, "unknown")
	}
}

func TestParseDDLChangesRenameColumn(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users RENAME COLUMN fname TO first_name")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlRenameColumn {
		t.Errorf("Kind = %d, want ddlRenameColumn", ch.Kind)
	}
	if ch.Table != "users" {
		t.Errorf("Table = %q, want %q", ch.Table, "users")
	}
	if ch.OldName != "fname" {
		t.Errorf("OldName = %q, want %q", ch.OldName, "fname")
	}
	if ch.NewName != "first_name" {
		t.Errorf("NewName = %q, want %q", ch.NewName, "first_name")
	}
}

func TestParseDDLChangesRenameTable(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users RENAME TO customers")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlRenameTable {
		t.Errorf("Kind = %d, want ddlRenameTable", ch.Kind)
	}
	if ch.OldName != "users" {
		t.Errorf("OldName = %q, want %q", ch.OldName, "users")
	}
	if ch.NewName != "customers" {
		t.Errorf("NewName = %q, want %q", ch.NewName, "customers")
	}
}

func TestParseDDLChangesRenameTableSchemaQualified(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE public.users RENAME TO customers")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	ch := changes[0]
	if ch.Kind != ddlRenameTable {
		t.Errorf("Kind = %d, want ddlRenameTable", ch.Kind)
	}
	if ch.NewName != "customers" {
		t.Errorf("NewName = %q, want %q", ch.NewName, "customers")
	}
}

func TestParseDDLChangesMultipleCommands(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users ADD COLUMN phone TEXT, DROP COLUMN fax")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 2 {
		t.Fatalf("got %d changes, want 2", len(changes))
	}
	if changes[0].Kind != ddlAddColumn || changes[0].Column != "phone" {
		t.Errorf("change[0]: got Kind=%d Column=%q, want ddlAddColumn/phone", changes[0].Kind, changes[0].Column)
	}
	if changes[1].Kind != ddlDropColumn || changes[1].Column != "fax" {
		t.Errorf("change[1]: got Kind=%d Column=%q, want ddlDropColumn/fax", changes[1].Kind, changes[1].Column)
	}
}

func TestParseDDLChangesCreateTable(t *testing.T) {
	changes, err := parseDDLChanges("CREATE TABLE new_table (id SERIAL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes for CREATE TABLE, want 1", len(changes))
	}
	if changes[0].Kind != ddlCreateTable {
		t.Errorf("got kind %v, want ddlCreateTable", changes[0].Kind)
	}
	if changes[0].Table != "new_table" {
		t.Errorf("got table %q, want %q", changes[0].Table, "new_table")
	}
}

func TestParseDDLChangesCreateIndex(t *testing.T) {
	changes, err := parseDDLChanges("CREATE INDEX idx_name ON users (name)")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("got %d changes for CREATE INDEX, want 0", len(changes))
	}
}

func TestParseDDLChangesDropTable(t *testing.T) {
	changes, err := parseDDLChanges("DROP TABLE old_table")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("got %d changes, want 1", len(changes))
	}
	if changes[0].Kind != ddlDropTable {
		t.Errorf("Kind = %d, want ddlDropTable", changes[0].Kind)
	}
	if changes[0].Table != "old_table" {
		t.Errorf("Table = %q, want %q", changes[0].Table, "old_table")
	}
}

func TestParseDDLChangesAddConstraint(t *testing.T) {
	changes, err := parseDDLChanges("ALTER TABLE users ADD CONSTRAINT check_age CHECK (age > 0)")
	if err != nil {
		t.Fatalf("parseDDLChanges() error: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("got %d changes for ADD CONSTRAINT, want 0", len(changes))
	}
}

func TestTypeNameToString(t *testing.T) {
	tests := []struct {
		sql      string
		wantType string
	}{
		{"ALTER TABLE t ADD COLUMN c INTEGER", "INTEGER"},
		{"ALTER TABLE t ADD COLUMN c BIGINT", "BIGINT"},
		{"ALTER TABLE t ADD COLUMN c TEXT", "TEXT"},
		{"ALTER TABLE t ADD COLUMN c BOOLEAN", "BOOLEAN"},
		{"ALTER TABLE t ADD COLUMN c REAL", "REAL"},
		{"ALTER TABLE t ADD COLUMN c VARCHAR(255)", "VARCHAR(255)"},
		{"ALTER TABLE t ADD COLUMN c NUMERIC(10,2)", "NUMERIC(10,2)"},
		{"ALTER TABLE t ADD COLUMN c SMALLINT", "SMALLINT"},
		{"ALTER TABLE t ADD COLUMN c DOUBLE PRECISION", "DOUBLE PRECISION"},
	}

	for _, tt := range tests {
		t.Run(tt.wantType, func(t *testing.T) {
			changes, err := parseDDLChanges(tt.sql)
			if err != nil {
				t.Fatalf("parseDDLChanges() error: %v", err)
			}
			if len(changes) != 1 {
				t.Fatalf("got %d changes, want 1", len(changes))
			}
			if changes[0].ColType != tt.wantType {
				t.Errorf("ColType = %q, want %q", changes[0].ColType, tt.wantType)
			}
		})
	}
}

// --- Handler integration tests ---

func TestHandleDDLAddColumn(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		shadowConn:     shadowConn,
		schemaRegistry: reg,
		moriDir:        t.TempDir(),
		connID:         1,
		verbose:        true,
	}

	// Shadow responds with ALTER TABLE success.
	shadowBackend.respondWith(simpleResponse("ALTER TABLE"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("ALTER TABLE users ADD COLUMN phone TEXT")
	cl := &core.Classification{
		OpType:  core.OpDDL,
		SubType: core.SubAlter,
		Tables:  []string{"users"},
		RawSQL:  "ALTER TABLE users ADD COLUMN phone TEXT",
	}

	err := ddh.HandleDDL(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleDDL() error: %v", err)
	}
	wg.Wait()

	// Verify schema registry was updated.
	diff := reg.GetDiff("users")
	if diff == nil {
		t.Fatal("expected schema diff for users, got nil")
	}
	if len(diff.Added) != 1 {
		t.Fatalf("expected 1 added column, got %d", len(diff.Added))
	}
	if diff.Added[0].Name != "phone" {
		t.Errorf("Added[0].Name = %q, want %q", diff.Added[0].Name, "phone")
	}
	if diff.Added[0].Type != "TEXT" {
		t.Errorf("Added[0].Type = %q, want %q", diff.Added[0].Type, "TEXT")
	}
}

func TestHandleDDLDropColumn(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		shadowConn:     shadowConn,
		schemaRegistry: reg,
		moriDir:        t.TempDir(),
		connID:         1,
		verbose:        false,
	}

	shadowBackend.respondWith(simpleResponse("ALTER TABLE"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("ALTER TABLE users DROP COLUMN fax")
	cl := &core.Classification{
		OpType:  core.OpDDL,
		SubType: core.SubAlter,
		Tables:  []string{"users"},
		RawSQL:  "ALTER TABLE users DROP COLUMN fax",
	}

	err := ddh.HandleDDL(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleDDL() error: %v", err)
	}
	wg.Wait()

	diff := reg.GetDiff("users")
	if diff == nil {
		t.Fatal("expected schema diff for users, got nil")
	}
	if len(diff.Dropped) != 1 || diff.Dropped[0] != "fax" {
		t.Errorf("Dropped = %v, want [fax]", diff.Dropped)
	}
}

func TestHandleDDLShadowError(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		shadowConn:     shadowConn,
		schemaRegistry: reg,
		moriDir:        t.TempDir(),
		connID:         1,
		verbose:        true,
	}

	// Shadow responds with an error.
	var errResp []byte
	errResp = append(errResp, makeErrorResponseMsg("column \"phone\" already exists")...)
	errResp = append(errResp, makeReadyForQueryMsg()...)
	shadowBackend.respondWith(errResp)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("ALTER TABLE users ADD COLUMN phone TEXT")
	cl := &core.Classification{
		OpType:  core.OpDDL,
		SubType: core.SubAlter,
		Tables:  []string{"users"},
		RawSQL:  "ALTER TABLE users ADD COLUMN phone TEXT",
	}

	err := ddh.HandleDDL(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleDDL() error: %v", err)
	}
	wg.Wait()

	// Registry should be unchanged.
	if reg.HasDiff("users") {
		t.Error("expected no schema diff after DDL error, but HasDiff returned true")
	}
}

func TestHandleDDLPersistence(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	moriDir := t.TempDir()
	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		shadowConn:     shadowConn,
		schemaRegistry: reg,
		moriDir:        moriDir,
		connID:         1,
		verbose:        false,
	}

	shadowBackend.respondWith(simpleResponse("ALTER TABLE"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readRelayedResponse(clientSide) //nolint: errcheck
	}()

	rawMsg := buildQueryMsg("ALTER TABLE users ADD COLUMN phone TEXT")
	cl := &core.Classification{
		OpType:  core.OpDDL,
		SubType: core.SubAlter,
		Tables:  []string{"users"},
		RawSQL:  "ALTER TABLE users ADD COLUMN phone TEXT",
	}

	err := ddh.HandleDDL(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleDDL() error: %v", err)
	}
	wg.Wait()

	// Read persisted registry from disk.
	loaded, err := coreSchema.ReadRegistry(moriDir)
	if err != nil {
		t.Fatalf("ReadRegistry() error: %v", err)
	}
	diff := loaded.GetDiff("users")
	if diff == nil {
		t.Fatal("persisted registry has no diff for users")
	}
	if len(diff.Added) != 1 || diff.Added[0].Name != "phone" {
		t.Errorf("persisted Added = %v, want [{phone TEXT}]", diff.Added)
	}
}

func TestExtractAndStoreFKsCreateTable(t *testing.T) {
	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		schemaRegistry: reg,
		connID:         1,
		verbose:        true,
	}

	ddh.extractAndStoreFKs("CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE CASCADE)")

	fks := reg.GetForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK, got %d", len(fks))
	}
	if fks[0].ParentTable != "users" {
		t.Errorf("FK parent = %q, want users", fks[0].ParentTable)
	}
	if fks[0].ChildTable != "orders" {
		t.Errorf("FK child = %q, want orders", fks[0].ChildTable)
	}
	if len(fks[0].ChildColumns) != 1 || fks[0].ChildColumns[0] != "user_id" {
		t.Errorf("FK child columns = %v, want [user_id]", fks[0].ChildColumns)
	}
	if fks[0].OnDelete != "CASCADE" {
		t.Errorf("FK OnDelete = %q, want CASCADE", fks[0].OnDelete)
	}
}

func TestExtractAndStoreFKsTableLevel(t *testing.T) {
	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	ddh.extractAndStoreFKs("CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)")

	fks := reg.GetForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK, got %d", len(fks))
	}
	if fks[0].ParentTable != "users" {
		t.Errorf("FK parent = %q, want users", fks[0].ParentTable)
	}
	if fks[0].OnDelete != "SET NULL" {
		t.Errorf("FK OnDelete = %q, want SET NULL", fks[0].OnDelete)
	}
}

func TestExtractAndStoreFKsAlterTable(t *testing.T) {
	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	ddh.extractAndStoreFKs("ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT")

	fks := reg.GetForeignKeys("orders")
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK, got %d", len(fks))
	}
	if fks[0].ConstraintName != "fk_user" {
		t.Errorf("FK constraint name = %q, want fk_user", fks[0].ConstraintName)
	}
	if fks[0].OnDelete != "RESTRICT" {
		t.Errorf("FK OnDelete = %q, want RESTRICT", fks[0].OnDelete)
	}
}

func TestExtractAndStoreFKsNonFK(t *testing.T) {
	reg := coreSchema.NewRegistry()
	ddh := &DDLHandler{
		schemaRegistry: reg,
		connID:         1,
		verbose:        false,
	}

	// DDL without FK constraints — should be a no-op.
	ddh.extractAndStoreFKs("CREATE TABLE simple (id SERIAL PRIMARY KEY, name TEXT)")

	fks := reg.GetForeignKeys("simple")
	if len(fks) != 0 {
		t.Errorf("expected 0 FKs for table without FK constraints, got %d", len(fks))
	}
}

func TestHandleDDLNilRegistry(t *testing.T) {
	shadowConn, shadowBackend := newMockBackend()
	defer shadowConn.Close()
	defer shadowBackend.conn.Close()

	clientConn, clientSide := net.Pipe()
	defer clientConn.Close()
	defer clientSide.Close()

	// DDLHandler with nil registry — DDL should still execute on Shadow.
	ddh := &DDLHandler{
		shadowConn:     shadowConn,
		schemaRegistry: nil,
		moriDir:        t.TempDir(),
		connID:         1,
		verbose:        false,
	}

	shadowBackend.respondWith(simpleResponse("ALTER TABLE"))

	var clientBuf []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientBuf, clientErr = readRelayedResponse(clientSide)
	}()

	rawMsg := buildQueryMsg("ALTER TABLE users ADD COLUMN phone TEXT")
	cl := &core.Classification{
		OpType:  core.OpDDL,
		SubType: core.SubAlter,
		Tables:  []string{"users"},
		RawSQL:  "ALTER TABLE users ADD COLUMN phone TEXT",
	}

	err := ddh.HandleDDL(clientConn, rawMsg, cl)
	if err != nil {
		t.Fatalf("HandleDDL() error: %v", err)
	}
	wg.Wait()

	if clientErr != nil {
		t.Fatalf("client read error: %v", clientErr)
	}
	if len(clientBuf) == 0 {
		t.Error("expected non-empty response relayed to client")
	}
}
