package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/core/delta"
	coreSchema "github.com/psrth/mori/internal/core/schema"
	"github.com/psrth/mori/internal/engine/sqlite/schema"
	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// Fix 1: extractCountAlias
// ---------------------------------------------------------------------------

func TestExtractCountAlias(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "explicit_alias_AS",
			sql:  "SELECT COUNT(*) AS total FROM users",
			want: "total",
		},
		{
			name: "explicit_alias_as_lowercase",
			sql:  "SELECT count(*) as cnt FROM users",
			want: "cnt",
		},
		{
			name: "no_alias_star",
			sql:  "SELECT COUNT(*) FROM users",
			want: "count(*)",
		},
		{
			name: "no_alias_col",
			sql:  "SELECT COUNT(id) FROM users",
			want: "count(id)",
		},
		{
			name: "alias_without_AS_keyword",
			sql:  "SELECT COUNT(*) total FROM users",
			want: "count(*)",
		},
		{
			name: "quoted_alias",
			sql:  `SELECT COUNT(*) AS "row_count" FROM users`,
			want: "row_count",
		},
		{
			name: "with_where_clause",
			sql:  "SELECT COUNT(*) AS active_users FROM users WHERE status = 'active'",
			want: "active_users",
		},
		{
			name: "count_distinct",
			sql:  "SELECT COUNT(DISTINCT name) AS unique_names FROM users",
			want: "unique_names",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractCountAlias(tt.sql)
			if got != tt.want {
				t.Errorf("extractCountAlias(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 2: isTableDirty + multi-table materialization
// ---------------------------------------------------------------------------

func TestIsTableDirty(t *testing.T) {
	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()
	reg := coreSchema.NewRegistry()

	p := &Proxy{
		deltaMap:       dm,
		tombstones:     ts,
		schemaRegistry: reg,
	}

	// Clean table.
	if p.isTableDirty("users") {
		t.Error("expected clean table 'users' to not be dirty")
	}

	// Delta makes it dirty.
	dm.Add("users", "1")
	if !p.isTableDirty("users") {
		t.Error("expected table with delta to be dirty")
	}

	// Different table still clean.
	if p.isTableDirty("orders") {
		t.Error("expected 'orders' to not be dirty")
	}

	// Tombstone makes it dirty.
	ts.Add("orders", "5")
	if !p.isTableDirty("orders") {
		t.Error("expected table with tombstone to be dirty")
	}

	// Schema diff (new table) makes it dirty.
	reg.RecordNewTable("products")
	if !p.isTableDirty("products") {
		t.Error("expected new table to be dirty")
	}

	// Schema diff (added column) makes it dirty.
	reg.RecordAddColumn("categories", coreSchema.Column{Name: "description", Type: "TEXT"})
	if !p.isTableDirty("categories") {
		t.Error("expected table with added column to be dirty")
	}
}

func TestMaterializeAndReexecute_MultiTable(t *testing.T) {
	// Set up two real SQLite databases (prod + shadow).
	prodDB := createTestDB(t, `
		CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL);
		CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO orders VALUES (1, 100.0), (2, 200.0);
		INSERT INTO customers VALUES (10, 'Alice'), (20, 'Bob');
	`)
	shadowDB := createTestDB(t, `
		CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL);
		CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO orders VALUES (1, 100.0), (2, 200.0);
		INSERT INTO customers VALUES (10, 'Alice'), (20, 'Bob');
	`)

	// Shadow has modifications: updated order + new customer.
	shadowDB.Exec("UPDATE orders SET total = 150.0 WHERE id = 1")
	shadowDB.Exec("INSERT INTO customers VALUES (30, 'Charlie')")

	dm := delta.NewMap()
	dm.Add("orders", "1")
	dm.Add("customers", "30")

	moriDir := t.TempDir()


	p := &Proxy{
		prodDB:     prodDB,
		shadowDB:   shadowDB,
		deltaMap:   dm,
		tombstones: delta.NewTombstoneSet(),
		tables: map[string]schema.TableMeta{
			"orders":    {PKColumns: []string{"id"}, PKType: "serial"},
			"customers": {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: coreSchema.NewRegistry(),
		moriDir:        moriDir,
		logger:         nil,
	}

	// CTE referencing both dirty tables — include total to verify orders materialization.
	sqlStr := `WITH o AS (SELECT * FROM orders), c AS (SELECT * FROM customers) SELECT o.id, o.total, c.name FROM o JOIN c ON 1=1`
	cl := &core.Classification{
		OpType:        core.OpRead,
		SubType:       core.SubSelect,
		Tables:        []string{"orders", "customers"},
		IsComplexRead: true,
	}

	resp := p.materializeAndReexecute(sqlStr, cl, 1)
	respStr := string(resp)

	// Verify both tables were materialized: we should see Charlie (from customers delta)
	// and the updated order total (150 from shadow, not 100 from prod).
	if !strings.Contains(respStr, "Charlie") {
		t.Errorf("expected materialized result to contain 'Charlie' from dirty customers table, got: %q", respStr)
	}
	if !strings.Contains(respStr, "150") {
		t.Errorf("expected materialized result to contain '150' from dirty orders table, got: %q", respStr)
	}
	// Old prod value should not appear for the dirty row.
	// (Order id=2 still has 200 from prod, but id=1 should be 150 not 100).
}

// ---------------------------------------------------------------------------
// Fix 3: DELETE RETURNING hydration
// ---------------------------------------------------------------------------

func TestHandleDeleteReturning_HydratesFromProd(t *testing.T) {
	// Prod has rows; Shadow is empty (simulating no prior hydration).
	prodDB := createTestDB(t, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);
		INSERT INTO items VALUES (1, 'Widget', 9.99);
		INSERT INTO items VALUES (2, 'Gadget', 19.99);
		INSERT INTO items VALUES (3, 'Doohickey', 29.99);
	`)
	// Shadow has the schema but was not fully populated (simulating shadow
	// where only some rows have been hydrated).
	shadowDB := createTestDB(t, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);
	`)

	dm := delta.NewMap()
	ts := delta.NewTombstoneSet()
	moriDir := t.TempDir()


	p := &Proxy{
		prodDB:     prodDB,
		shadowDB:   shadowDB,
		deltaMap:   dm,
		tombstones: ts,
		tables: map[string]schema.TableMeta{
			"items": {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: coreSchema.NewRegistry(),
		moriDir:        moriDir,
		logger:         nil,
	}

	// Use a pipe to capture the response written to the client connection.
	clientConn := newMockConn()

	cl := &core.Classification{
		OpType:      core.OpWrite,
		SubType:     core.SubDelete,
		Tables:      []string{"items"},
		PKs:         []core.TablePK{{Table: "items", PK: "1"}},
		HasReturning: true,
		RawSQL:      "DELETE FROM items WHERE id = 1 RETURNING *",
	}

	p.handleDeleteReturning(clientConn, "DELETE FROM items WHERE id = 1 RETURNING *", cl, 1, nil)

	resp := clientConn.written()

	// The response should contain the hydrated row data.
	if !strings.Contains(resp, "Widget") {
		t.Errorf("expected RETURNING data to contain 'Widget' (hydrated from Prod), got: %s", resp)
	}
	if !strings.Contains(resp, "9.99") {
		t.Errorf("expected RETURNING data to contain '9.99', got: %s", resp)
	}

	// Row should be tombstoned.
	if !ts.IsTombstoned("items", "1") {
		t.Error("expected PK 1 to be tombstoned after DELETE RETURNING")
	}
}

func TestHandleDeleteReturning_AlreadyInShadow(t *testing.T) {
	// Row already in shadow (delta exists).
	prodDB := createTestDB(t, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO items VALUES (1, 'Original');
	`)
	shadowDB := createTestDB(t, `
		CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO items VALUES (1, 'Modified');
	`)

	dm := delta.NewMap()
	dm.Add("items", "1") // Already in delta.
	ts := delta.NewTombstoneSet()
	moriDir := t.TempDir()


	p := &Proxy{
		prodDB:     prodDB,
		shadowDB:   shadowDB,
		deltaMap:   dm,
		tombstones: ts,
		tables: map[string]schema.TableMeta{
			"items": {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: coreSchema.NewRegistry(),
		moriDir:        moriDir,
		logger:         nil,
	}

	clientConn := newMockConn()
	cl := &core.Classification{
		OpType:       core.OpWrite,
		SubType:      core.SubDelete,
		Tables:       []string{"items"},
		PKs:          []core.TablePK{{Table: "items", PK: "1"}},
		HasReturning: true,
		RawSQL:       "DELETE FROM items WHERE id = 1 RETURNING *",
	}

	p.handleDeleteReturning(clientConn, "DELETE FROM items WHERE id = 1 RETURNING *", cl, 1, nil)

	resp := clientConn.written()

	// Should return the shadow version (Modified), not the prod version.
	if !strings.Contains(resp, "Modified") {
		t.Errorf("expected RETURNING to contain shadow data 'Modified', got: %s", resp)
	}
	if strings.Contains(resp, "Original") {
		t.Errorf("expected RETURNING to NOT contain prod data 'Original', got: %s", resp)
	}
}

// ---------------------------------------------------------------------------
// Fix 4: FK detection from DDL
// ---------------------------------------------------------------------------

func TestTrackDDLEffects_DetectsFKsOnCreateTable(t *testing.T) {
	// Shadow DB where we'll execute the CREATE TABLE DDL.
	shadowDB := createTestDB(t, `
		CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT);
	`)

	moriDir := t.TempDir()
	reg := coreSchema.NewRegistry()


	p := &Proxy{
		shadowDB:       shadowDB,
		deltaMap:       delta.NewMap(),
		tombstones:     delta.NewTombstoneSet(),
		schemaRegistry: reg,
		moriDir:        moriDir,
		logger:         nil,
	}

	// Execute the CREATE TABLE with FK on shadow first (as the proxy would).
	createSQL := `CREATE TABLE children (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE
	)`
	_, err := shadowDB.Exec(createSQL)
	if err != nil {
		t.Fatalf("failed to create table on shadow: %v", err)
	}

	// Now call trackDDLEffects as the proxy would after executing DDL.
	cl := &core.Classification{
		OpType:  core.OpDDL,
		SubType: core.SubCreate,
		Tables:  []string{"children"},
		RawSQL:  createSQL,
	}
	p.trackDDLEffects(cl, 1)

	// Verify the FK was detected and registered.
	fks := reg.GetReferencingFKs("parents")
	if len(fks) == 0 {
		t.Fatal("expected FK referencing 'parents' to be detected after CREATE TABLE")
	}

	found := false
	for _, fk := range fks {
		if fk.ChildTable == "children" && fk.ParentTable == "parents" {
			found = true
			if fk.OnDelete != "CASCADE" {
				t.Errorf("expected ON DELETE CASCADE, got %q", fk.OnDelete)
			}
			if len(fk.ChildColumns) == 0 || fk.ChildColumns[0] != "parent_id" {
				t.Errorf("expected child column 'parent_id', got %v", fk.ChildColumns)
			}
			if len(fk.ParentColumns) == 0 || fk.ParentColumns[0] != "id" {
				t.Errorf("expected parent column 'id', got %v", fk.ParentColumns)
			}
		}
	}
	if !found {
		t.Error("expected FK from children→parents to be registered")
	}
}

func TestDetectForeignKeys_CompositeFKAfterInit(t *testing.T) {
	db := createTestDB(t, `
		CREATE TABLE orders (id INTEGER PRIMARY KEY);
		CREATE TABLE order_items (
			order_id INTEGER,
			item_id INTEGER,
			FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL
		);
	`)

	reg := coreSchema.NewRegistry()
	err := DetectForeignKeys(context.Background(), db, reg, []string{"order_items"})
	if err != nil {
		t.Fatalf("DetectForeignKeys failed: %v", err)
	}

	fks := reg.GetReferencingFKs("orders")
	if len(fks) == 0 {
		t.Fatal("expected FK referencing 'orders' to be detected")
	}

	fk := fks[0]
	if fk.OnDelete != "SET NULL" {
		t.Errorf("expected ON DELETE SET NULL, got %q", fk.OnDelete)
	}
}

// ---------------------------------------------------------------------------
// Fix 5: findOuterFromIndex — paren-depth-aware " FROM " search
// ---------------------------------------------------------------------------

func TestFindOuterFromIndex(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		found bool
	}{
		{
			name:  "simple query",
			sql:   "SELECT id, name FROM users",
			found: true,
		},
		{
			name:  "subquery in SELECT list",
			sql:   "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c",
			found: true,
		},
		{
			name:  "no FROM",
			sql:   "SELECT 1",
			found: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upper := strings.ToUpper(tt.sql)
			idx := findOuterFromIndex(upper)
			if tt.found {
				if idx < 0 {
					t.Fatalf("expected to find outer FROM, got -1")
				}
				// Verify the found FROM is the outer one (not inside parens).
				after := strings.TrimSpace(upper[idx+6:])
				if strings.HasPrefix(after, "MESSAGES") {
					t.Errorf("found subquery FROM instead of outer FROM at idx %d", idx)
				}
			} else if idx >= 0 {
				t.Errorf("expected -1, got %d", idx)
			}
		})
	}
}

func TestFindOuterFromIndex_SubquerySkipped(t *testing.T) {
	// Key regression test: the first " FROM " is inside a subquery.
	sql := "SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c LEFT JOIN users u ON c.uid = u.id"
	upper := strings.ToUpper(sql)
	idx := findOuterFromIndex(upper)
	if idx < 0 {
		t.Fatal("expected to find outer FROM")
	}
	// The outer FROM should be before "CONVERSATIONS", not before "MESSAGES".
	after := strings.TrimSpace(upper[idx+6:])
	if !strings.HasPrefix(after, "CONVERSATIONS") {
		t.Errorf("expected outer FROM before CONVERSATIONS, got: %s", after[:min(40, len(after))])
	}
}

// ---------------------------------------------------------------------------
// Fix 6: containsColumnForTable — alias-aware column check
// ---------------------------------------------------------------------------

func TestContainsColumnForTable(t *testing.T) {
	tests := []struct {
		name       string
		selectList string
		col        string
		alias      string
		want       bool
	}{
		{
			name:       "qualified match",
			selectList: "u.id, u.name, o.total",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "qualified mismatch - different alias",
			selectList: "c.id, u.name",
			col:        "id",
			alias:      "u",
			want:       false,
		},
		{
			name:       "unqualified match",
			selectList: "id, name",
			col:        "id",
			alias:      "u",
			want:       true,
		},
		{
			name:       "no match",
			selectList: "c.id, u.name, o.total",
			col:        "email",
			alias:      "u",
			want:       false,
		},
		{
			name:       "with AS alias",
			selectList: "u.name AS user_name, c.id",
			col:        "name",
			alias:      "u",
			want:       true,
		},
		{
			name:       "subquery in list - qualified column not matching",
			selectList: "c.id, c.user_id, (select count(*)",
			col:        "id",
			alias:      "u",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsColumnForTable(tt.selectList, tt.col, tt.alias)
			if got != tt.want {
				t.Errorf("containsColumnForTable(%q, %q, %q) = %v, want %v",
					tt.selectList, tt.col, tt.alias, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 7: needsPKInjection with subquery in SELECT list
// ---------------------------------------------------------------------------

func TestNeedsPKInjection_SubqueryInSelect(t *testing.T) {
	// When a subquery has FROM messages, needsPKInjection should still find
	// the outer FROM and correctly analyze the outer SELECT list.
	sql := `SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c`
	// The PK "id" appears as c.id in the outer SELECT list — should NOT need injection.
	if needsPKInjection(sql, "id") {
		t.Error("expected needsPKInjection to return false when PK is in outer SELECT list")
	}

	// Now test a query where the PK is NOT in the outer SELECT list.
	sql2 := `SELECT c.name, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c`
	if !needsPKInjection(sql2, "id") {
		t.Error("expected needsPKInjection to return true when PK is NOT in outer SELECT list")
	}
}

// ---------------------------------------------------------------------------
// Fix 8: materializeAndReexecute materializes ALL tables (not just dirty)
// ---------------------------------------------------------------------------

func TestMaterializeAndReexecute_AllTables(t *testing.T) {
	// Set up prod with data in both tables. Shadow has only dirty data.
	prodDB := createTestDB(t, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL);
		INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
		INSERT INTO orders VALUES (10, 1, 100.0), (20, 2, 200.0);
	`)
	// Shadow: only orders table has the dirty row (orders id=10 updated).
	// Users table is EMPTY on shadow (clean table, no data).
	shadowDB := createTestDB(t, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL);
		INSERT INTO orders VALUES (10, 1, 150.0);
	`)

	dm := delta.NewMap()
	dm.Add("orders", "10") // Only orders is dirty

	moriDir := t.TempDir()

	p := &Proxy{
		prodDB:     prodDB,
		shadowDB:   shadowDB,
		deltaMap:   dm,
		tombstones: delta.NewTombstoneSet(),
		tables: map[string]schema.TableMeta{
			"users":  {PKColumns: []string{"id"}, PKType: "serial"},
			"orders": {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: coreSchema.NewRegistry(),
		moriDir:        moriDir,
		logger:         nil,
	}

	// JOIN query: users JOIN orders. Users is clean but must be materialized
	// because the rewritten query runs on shadow where users has no data.
	sqlStr := `SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id`
	cl := &core.Classification{
		OpType:  core.OpRead,
		SubType: core.SubSelect,
		Tables:  []string{"users", "orders"},
		IsJoin:  true,
	}

	resp := p.materializeAndReexecute(sqlStr, cl, 1)
	respStr := string(resp)

	// Should contain Alice (from users, materialized from prod even though clean).
	if !strings.Contains(respStr, "Alice") {
		t.Errorf("expected materialized result to contain 'Alice' from clean users table, got: %q", respStr)
	}
	// Should contain the updated order total (150 from merged read, not 100).
	if !strings.Contains(respStr, "150") {
		t.Errorf("expected materialized result to contain '150' from dirty orders table, got: %q", respStr)
	}
}

// ---------------------------------------------------------------------------
// Fix 9: collectSubqueryTablesFromSQL
// ---------------------------------------------------------------------------

func TestCollectSubqueryTablesFromSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "no subquery",
			sql:  "SELECT id, name FROM users",
			want: nil,
		},
		{
			name: "correlated subquery in SELECT",
			sql:  `SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt FROM conversations c`,
			want: []string{"messages"},
		},
		{
			name: "multiple subqueries",
			sql:  `SELECT c.id, (SELECT COUNT(*) FROM messages m WHERE m.cid = c.id) AS cnt, (SELECT MAX(ts) FROM events e WHERE e.cid = c.id) AS last_event FROM conversations c`,
			want: []string{"messages", "events"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectSubqueryTablesFromSQL(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("collectSubqueryTablesFromSQL(%q) = %v, want %v", tt.sql, got, tt.want)
			}
			for i, g := range got {
				if g != tt.want[i] {
					t.Errorf("collectSubqueryTablesFromSQL(%q)[%d] = %q, want %q", tt.sql, i, g, tt.want[i])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fix 10: materializeAndReexecute with subquery tables
// ---------------------------------------------------------------------------

func TestMaterializeAndReexecute_SubqueryTables(t *testing.T) {
	// Set up prod with data in both tables.
	prodDB := createTestDB(t, `
		CREATE TABLE conversations (id INTEGER PRIMARY KEY, title TEXT);
		CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER, body TEXT);
		INSERT INTO conversations VALUES (1, 'Chat A'), (2, 'Chat B');
		INSERT INTO messages VALUES (10, 1, 'Hello'), (20, 1, 'World'), (30, 2, 'Hi');
	`)
	// Shadow is empty (no dirty tables).
	shadowDB := createTestDB(t, `
		CREATE TABLE conversations (id INTEGER PRIMARY KEY, title TEXT);
		CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER, body TEXT);
	`)

	// Add a delta so the query enters materializeAndReexecute (messages is dirty).
	dm := delta.NewMap()
	dm.Add("messages", "10")

	// Re-insert the dirty row into shadow.
	shadowDB.Exec("INSERT INTO messages VALUES (10, 1, 'Hello Updated')")

	moriDir := t.TempDir()

	p := &Proxy{
		prodDB:     prodDB,
		shadowDB:   shadowDB,
		deltaMap:   dm,
		tombstones: delta.NewTombstoneSet(),
		tables: map[string]schema.TableMeta{
			"conversations": {PKColumns: []string{"id"}, PKType: "serial"},
			"messages":      {PKColumns: []string{"id"}, PKType: "serial"},
		},
		schemaRegistry: coreSchema.NewRegistry(),
		moriDir:        moriDir,
		logger:         nil,
	}

	// Query with a correlated subquery — messages is referenced in the subquery,
	// conversations is in the main FROM. Both must be materialized.
	sqlStr := `SELECT c.id, c.title, (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) AS msg_count FROM conversations c`
	cl := &core.Classification{
		OpType:        core.OpRead,
		SubType:       core.SubSelect,
		Tables:        []string{"conversations"},
		IsComplexRead: true,
	}

	resp := p.materializeAndReexecute(sqlStr, cl, 1)
	respStr := string(resp)

	// Should contain conversation titles (conversations materialized from prod).
	if !strings.Contains(respStr, "Chat A") {
		t.Errorf("expected result to contain 'Chat A', got: %q", respStr)
	}
	if !strings.Contains(respStr, "Chat B") {
		t.Errorf("expected result to contain 'Chat B', got: %q", respStr)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// createTestDB creates an in-memory SQLite database with the given schema/data.
func createTestDB(t *testing.T, initSQL string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, stmt := range splitStatements(initSQL) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("failed to execute init SQL %q: %v", stmt, err)
		}
	}
	return db
}

// splitStatements splits SQL on semicolons (simple, not quote-aware).
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	depth := 0
	for _, c := range sql {
		switch c {
		case '(':
			depth++
			current.WriteRune(c)
		case ')':
			depth--
			current.WriteRune(c)
		case ';':
			if depth == 0 {
				stmts = append(stmts, current.String())
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}
	if s := current.String(); strings.TrimSpace(s) != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// mockConn captures bytes written to it for testing pgwire responses.
type mockConn struct {
	buf []byte
}

func newMockConn() *mockConn {
	return &mockConn{}
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	m.buf = append(m.buf, p...)
	return len(p), nil
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("mockConn: Read not implemented")
}

func (m *mockConn) Close() error { return nil }

func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error   { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error  { return nil }

func (m *mockConn) written() string {
	return string(m.buf)
}
