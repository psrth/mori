package classify

import (
	"testing"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/engine/duckdb/schema"
)

func TestClassify_Select(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpRead {
		t.Errorf("OpType = %v, want OpRead", cl.OpType)
	}
	if cl.SubType != core.SubSelect {
		t.Errorf("SubType = %v, want SubSelect", cl.SubType)
	}
	if len(cl.Tables) != 1 || cl.Tables[0] != "users" {
		t.Errorf("Tables = %v, want [users]", cl.Tables)
	}
}

func TestClassify_Insert(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("INSERT INTO users (name, email) VALUES ('alice', 'a@b.c')")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpWrite {
		t.Errorf("OpType = %v, want OpWrite", cl.OpType)
	}
	if cl.SubType != core.SubInsert {
		t.Errorf("SubType = %v, want SubInsert", cl.SubType)
	}
}

func TestClassify_Update(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)
	cl, err := c.Classify("UPDATE users SET name = 'bob' WHERE id = 42")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpWrite {
		t.Errorf("OpType = %v, want OpWrite", cl.OpType)
	}
	if cl.SubType != core.SubUpdate {
		t.Errorf("SubType = %v, want SubUpdate", cl.SubType)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "42" {
		t.Errorf("PKs = %v, want [{users 42}]", cl.PKs)
	}
}

func TestClassify_Delete(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpWrite {
		t.Errorf("OpType = %v, want OpWrite", cl.OpType)
	}
	if cl.SubType != core.SubDelete {
		t.Errorf("SubType = %v, want SubDelete", cl.SubType)
	}
}

func TestClassify_CreateTable(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpDDL {
		t.Errorf("OpType = %v, want OpDDL", cl.OpType)
	}
	if cl.SubType != core.SubCreate {
		t.Errorf("SubType = %v, want SubCreate", cl.SubType)
	}
}

func TestClassify_AlterTable(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("ALTER TABLE users ADD COLUMN age INTEGER")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpDDL {
		t.Errorf("OpType = %v, want OpDDL", cl.OpType)
	}
	if cl.SubType != core.SubAlter {
		t.Errorf("SubType = %v, want SubAlter", cl.SubType)
	}
}

func TestClassify_DropTable(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("DROP TABLE users")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpDDL {
		t.Errorf("OpType = %v, want OpDDL", cl.OpType)
	}
	if cl.SubType != core.SubDrop {
		t.Errorf("SubType = %v, want SubDrop", cl.SubType)
	}
}

func TestClassify_Transaction(t *testing.T) {
	c := New(nil)

	tests := []struct {
		sql     string
		subType core.SubType
	}{
		{"BEGIN", core.SubBegin},
		{"START TRANSACTION", core.SubBegin},
		{"COMMIT", core.SubCommit},
		{"END", core.SubCommit},
		{"ROLLBACK", core.SubRollback},
		{"SAVEPOINT s1", core.SubSavepoint},
		{"RELEASE s1", core.SubRelease},
	}

	for _, tt := range tests {
		cl, err := c.Classify(tt.sql)
		if err != nil {
			t.Fatalf("Classify(%q) error: %v", tt.sql, err)
		}
		if cl.OpType != core.OpTransaction {
			t.Errorf("Classify(%q) OpType = %v, want OpTransaction", tt.sql, cl.OpType)
		}
		if cl.SubType != tt.subType {
			t.Errorf("Classify(%q) SubType = %v, want %v", tt.sql, cl.SubType, tt.subType)
		}
	}
}

func TestClassify_DuckDBSpecific(t *testing.T) {
	c := New(nil)

	// DuckDB-specific statements should be classified as Other.
	others := []string{
		"DESCRIBE users",
		"SHOW TABLES",
		"PRAGMA database_list",
		"INSTALL httpfs",
		"LOAD httpfs",
		"COPY users TO 'users.csv'",
		"EXPORT DATABASE '/tmp/mydb'",
		"SET threads TO 4",
		"RESET threads",
		"CALL pragma_database_list()",
	}

	for _, sql := range others {
		cl, err := c.Classify(sql)
		if err != nil {
			t.Fatalf("Classify(%q) error: %v", sql, err)
		}
		if cl.OpType != core.OpOther {
			t.Errorf("Classify(%q) OpType = %v, want OpOther", sql, cl.OpType)
		}
	}
}

func TestClassify_SelectWithLimit(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT * FROM users LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasLimit {
		t.Error("expected HasLimit = true")
	}
	if cl.Limit != 10 {
		t.Errorf("Limit = %d, want 10", cl.Limit)
	}
}

func TestClassify_SelectWithJoin(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.IsJoin {
		t.Error("expected IsJoin = true")
	}
	if len(cl.Tables) < 2 {
		t.Errorf("expected at least 2 tables, got %v", cl.Tables)
	}
}

func TestClassify_CTE(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpRead {
		t.Errorf("OpType = %v, want OpRead", cl.OpType)
	}
	if !cl.IsComplexRead {
		t.Error("expected IsComplexRead = true for CTE")
	}
}

func TestClassify_CTEWithInsert(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("WITH new_users AS (SELECT * FROM staging) INSERT INTO users SELECT * FROM new_users")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpWrite {
		t.Errorf("OpType = %v, want OpWrite for CTE with INSERT", cl.OpType)
	}
}

func TestClassify_Aggregate(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasAggregate {
		t.Error("expected HasAggregate = true")
	}
}

func TestClassify_DuckDBAggregate_ListAgg(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT LIST(name) FROM users GROUP BY department")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasAggregate {
		t.Error("expected HasAggregate = true for LIST aggregate")
	}
}

func TestClassify_EmptyQuery(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpOther {
		t.Errorf("OpType = %v, want OpOther for empty query", cl.OpType)
	}
}

func TestClassify_CommentStripping(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("-- this is a comment\nSELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpRead {
		t.Errorf("OpType = %v, want OpRead after comment stripping", cl.OpType)
	}
}

func TestClassify_PKExtraction_DollarParams(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM users WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "$1" {
		t.Errorf("PKs = %v, want [{users $1}]", cl.PKs)
	}
}

func TestClassifyWithParams(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)
	cl, err := c.ClassifyWithParams("SELECT * FROM users WHERE id = $1", []interface{}{42})
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "42" {
		t.Errorf("PKs = %v, want [{users 42}]", cl.PKs)
	}
}

func TestClassify_SetOp(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT name FROM users UNION SELECT name FROM admins")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasSetOp {
		t.Error("expected HasSetOp = true")
	}
}

func TestClassify_DropSequence(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("DROP SEQUENCE IF EXISTS my_seq")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpDDL {
		t.Errorf("OpType = %v, want OpDDL", cl.OpType)
	}
	if cl.SubType != core.SubDrop {
		t.Errorf("SubType = %v, want SubDrop", cl.SubType)
	}
}

func TestClassify_CreateOrReplace(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("CREATE OR REPLACE TABLE users (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpDDL {
		t.Errorf("OpType = %v, want OpDDL", cl.OpType)
	}
	if cl.SubType != core.SubCreate {
		t.Errorf("SubType = %v, want SubCreate", cl.SubType)
	}
	if len(cl.Tables) != 1 || cl.Tables[0] != "users" {
		t.Errorf("Tables = %v, want [users]", cl.Tables)
	}
}

func TestClassify_FullOuterJoin(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT * FROM a FULL JOIN b ON a.id = b.id")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.IsJoin {
		t.Error("expected IsJoin = true for FULL JOIN")
	}
}

func TestClassify_PKExtraction_ReversedOrder(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM users WHERE 42 = id")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "42" {
		t.Errorf("PKs = %v, want [{users 42}]", cl.PKs)
	}
}

func TestClassify_PKExtraction_NegativeNumber(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"accounts": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM accounts WHERE id = -1")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "-1" {
		t.Errorf("PKs = %v, want [{accounts -1}]", cl.PKs)
	}
}

func TestClassify_PKExtraction_UUID(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"docs": {PKColumns: []string{"id"}, PKType: "uuid"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM docs WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("PKs = %v, want [{docs a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}]", cl.PKs)
	}
}

func TestClassify_PKExtraction_CastUnwrap(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "integer"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM users WHERE id = CAST('42' AS INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "42" {
		t.Errorf("PKs = %v, want [{users 42}]", cl.PKs)
	}
}

func TestClassify_PKExtraction_TypeCast(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"docs": {PKColumns: []string{"id"}, PKType: "uuid"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM docs WHERE id = 'abc123'::uuid")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "abc123" {
		t.Errorf("PKs = %v, want [{docs abc123}]", cl.PKs)
	}
}

func TestClassify_PKExtraction_InSingle(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)
	cl, err := c.Classify("SELECT * FROM users WHERE id IN ('42')")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.PKs) != 1 || cl.PKs[0].PK != "42" {
		t.Errorf("PKs = %v, want [{users 42}]", cl.PKs)
	}
}

func TestClassify_OnConflict(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO UPDATE SET name = 'alice'")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasOnConflict {
		t.Error("expected HasOnConflict = true")
	}
}

func TestClassify_InsertOrReplace(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("INSERT OR REPLACE INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasOnConflict {
		t.Error("expected HasOnConflict = true for INSERT OR REPLACE")
	}
}

func TestClassify_WindowFunction(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT name, ROW_NUMBER() OVER (ORDER BY id) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasWindowFunc {
		t.Error("expected HasWindowFunc = true")
	}
}

func TestClassify_Distinct(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT DISTINCT name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasDistinct {
		t.Error("expected HasDistinct = true")
	}
}

func TestClassify_Truncate(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("TRUNCATE TABLE users")
	if err != nil {
		t.Fatal(err)
	}
	if cl.OpType != core.OpWrite {
		t.Errorf("OpType = %v, want OpWrite", cl.OpType)
	}
	if cl.SubType != core.SubTruncate {
		t.Errorf("SubType = %v, want SubTruncate", cl.SubType)
	}
	if len(cl.Tables) != 1 || cl.Tables[0] != "users" {
		t.Errorf("Tables = %v, want [users]", cl.Tables)
	}
}

func TestClassify_ExplainAnalyze(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("EXPLAIN ANALYZE SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if cl.SubType != core.SubNotSupported {
		t.Errorf("SubType = %v, want SubNotSupported for EXPLAIN ANALYZE", cl.SubType)
	}
}

func TestClassify_Returning(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("INSERT INTO users (name) VALUES ('alice') RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	if !cl.HasReturning {
		t.Error("expected HasReturning = true")
	}
}

func TestClassify_SetCommand(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SET threads TO 4")
	if err != nil {
		t.Fatal(err)
	}
	if cl.SubType != core.SubSet {
		t.Errorf("SubType = %v, want SubSet", cl.SubType)
	}
}

func TestClassify_CopyNotSupported(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("COPY users TO 'users.csv'")
	if err != nil {
		t.Fatal(err)
	}
	if cl.SubType != core.SubNotSupported {
		t.Errorf("SubType = %v, want SubNotSupported", cl.SubType)
	}
}
