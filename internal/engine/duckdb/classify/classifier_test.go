package classify

import (
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/duckdb/schema"
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
		{"SAVEPOINT s1", core.SubOther},
		{"RELEASE s1", core.SubOther},
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
