package classify

import (
	"testing"

	"github.com/psrth/mori/internal/core"
	"github.com/psrth/mori/internal/engine/mysql/schema"
)

func TestClassify_Reads(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"simple select", "SELECT * FROM users", core.OpRead, core.SubSelect},
		{"select with where", "SELECT id, name FROM users WHERE id = 1", core.OpRead, core.SubSelect},
		{"select with join", "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id", core.OpRead, core.SubSelect},
		{"select with limit", "SELECT * FROM users LIMIT 10", core.OpRead, core.SubSelect},
		{"select count", "SELECT COUNT(*) FROM users", core.OpRead, core.SubSelect},
		{"select with subquery", "SELECT * FROM (SELECT id FROM users) AS sub", core.OpRead, core.SubSelect},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}

func TestClassify_Writes(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"insert", "INSERT INTO users (name) VALUES ('alice')", core.OpWrite, core.SubInsert},
		{"insert ignore", "INSERT IGNORE INTO users (name) VALUES ('alice')", core.OpWrite, core.SubInsert},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", core.OpWrite, core.SubUpdate},
		{"delete", "DELETE FROM users WHERE id = 1", core.OpWrite, core.SubDelete},
		{"replace", "REPLACE INTO users (id, name) VALUES (1, 'alice')", core.OpWrite, core.SubInsert},
		{"truncate", "TRUNCATE TABLE users", core.OpWrite, core.SubTruncate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}

func TestClassify_DDL(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"create table", "CREATE TABLE users (id INT PRIMARY KEY)", core.OpDDL, core.SubCreate},
		{"create if not exists", "CREATE TABLE IF NOT EXISTS users (id INT)", core.OpDDL, core.SubCreate},
		{"alter table", "ALTER TABLE users ADD COLUMN age INT", core.OpDDL, core.SubAlter},
		{"drop table", "DROP TABLE users", core.OpDDL, core.SubDrop},
		{"drop if exists", "DROP TABLE IF EXISTS users", core.OpDDL, core.SubDrop},
		{"create index", "CREATE INDEX idx_name ON users (name)", core.OpDDL, core.SubCreate},
		{"rename table", "RENAME TABLE users TO users_old", core.OpDDL, core.SubAlter},
		{"rename table backtick", "RENAME TABLE `users` TO `users_archive`", core.OpDDL, core.SubAlter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}

func TestClassify_Transactions(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantSub core.SubType
	}{
		{"begin", "BEGIN", core.SubBegin},
		{"start transaction", "START TRANSACTION", core.SubBegin},
		{"commit", "COMMIT", core.SubCommit},
		{"rollback", "ROLLBACK", core.SubRollback},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != core.OpTransaction {
				t.Errorf("OpType = %v, want OpTransaction", cl.OpType)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}

func TestClassify_Other(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"set", "SET NAMES utf8mb4", core.OpOther, core.SubSet},
		{"show", "SHOW TABLES", core.OpOther, core.SubShow},
		{"explain", "EXPLAIN SELECT * FROM users", core.OpOther, core.SubExplain},
		{"describe", "DESCRIBE users", core.OpOther, core.SubExplain},
		{"use", "USE mydb", core.OpOther, core.SubOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}

func TestClassify_ExplainAnalyze(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("EXPLAIN ANALYZE SELECT * FROM users")
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	if cl.OpType != core.OpOther {
		t.Errorf("OpType = %v, want OpOther", cl.OpType)
	}
	if cl.SubType != core.SubNotSupported {
		t.Errorf("SubType = %v, want SubNotSupported", cl.SubType)
	}
	if cl.NotSupportedMsg == "" {
		t.Error("NotSupportedMsg should not be empty for EXPLAIN ANALYZE")
	}
}

func TestClassify_Savepoint(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"savepoint", "SAVEPOINT sp1", core.OpTransaction, core.SubSavepoint},
		{"release savepoint", "RELEASE SAVEPOINT sp1", core.OpTransaction, core.SubRelease},
		{"rollback to", "ROLLBACK TO sp1", core.OpTransaction, core.SubRollback},
		{"rollback to savepoint", "ROLLBACK TO SAVEPOINT sp1", core.OpTransaction, core.SubRollback},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}

func TestClassify_TableExtraction(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name       string
		sql        string
		wantTables []string
	}{
		{"single table", "SELECT * FROM users", []string{"users"}},
		{"join", "SELECT * FROM users u JOIN posts p ON u.id = p.user_id", []string{"users", "posts"}},
		{"insert", "INSERT INTO orders (id) VALUES (1)", []string{"orders"}},
		{"update", "UPDATE products SET price = 10 WHERE id = 1", []string{"products"}},
		{"delete", "DELETE FROM logs WHERE created_at < '2024-01-01'", []string{"logs"}},
		{"create", "CREATE TABLE items (id INT PRIMARY KEY)", []string{"items"}},
		{"backtick quoted", "SELECT * FROM `users`", []string{"users"}},
		{"rename table", "RENAME TABLE users TO users_old", []string{"users"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if len(cl.Tables) != len(tt.wantTables) {
				t.Errorf("Tables = %v, want %v", cl.Tables, tt.wantTables)
				return
			}
			for i, want := range tt.wantTables {
				if cl.Tables[i] != want {
					t.Errorf("Tables[%d] = %q, want %q", i, cl.Tables[i], want)
				}
			}
		})
	}
}

func TestClassify_Flags(t *testing.T) {
	c := New(nil)

	t.Run("join detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM users JOIN posts ON users.id = posts.user_id")
		if !cl.IsJoin {
			t.Error("IsJoin should be true for JOIN query")
		}
	})

	t.Run("limit detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM users LIMIT 25")
		if !cl.HasLimit {
			t.Error("HasLimit should be true")
		}
		if cl.Limit != 25 {
			t.Errorf("Limit = %d, want 25", cl.Limit)
		}
	})

	t.Run("aggregate detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT COUNT(*) FROM users")
		if !cl.HasAggregate {
			t.Error("HasAggregate should be true for COUNT")
		}
	})

	t.Run("group by detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT status, COUNT(*) FROM users GROUP BY status")
		if !cl.HasAggregate {
			t.Error("HasAggregate should be true for GROUP BY")
		}
	})

	t.Run("union detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT id FROM users UNION SELECT id FROM admins")
		if !cl.HasSetOp {
			t.Error("HasSetOp should be true for UNION")
		}
	})

	t.Run("subquery in from", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM (SELECT id FROM users) AS sub")
		if !cl.IsComplexRead {
			t.Error("IsComplexRead should be true for subquery in FROM")
		}
	})
}

func TestClassify_PKExtraction(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)

	t.Run("integer pk", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM users WHERE id = 42")
		if len(cl.PKs) != 1 {
			t.Fatalf("PKs len = %d, want 1", len(cl.PKs))
		}
		if cl.PKs[0].Table != "users" || cl.PKs[0].PK != "42" {
			t.Errorf("PK = %+v", cl.PKs[0])
		}
	})

	t.Run("string pk", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM users WHERE id = '123'")
		if len(cl.PKs) != 1 {
			t.Fatalf("PKs len = %d, want 1", len(cl.PKs))
		}
		if cl.PKs[0].PK != "123" {
			t.Errorf("PK = %q, want %q", cl.PKs[0].PK, "123")
		}
	})
}

func TestClassify_EmptyQuery(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("")
	if err != nil {
		t.Fatalf("Classify empty error: %v", err)
	}
	if cl.OpType != core.OpOther {
		t.Errorf("OpType = %v, want OpOther", cl.OpType)
	}
}

func TestClassify_LeadingComments(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("/* comment */ SELECT * FROM users")
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	if cl.OpType != core.OpRead {
		t.Errorf("OpType = %v, want OpRead", cl.OpType)
	}
}

func TestClassify_CTE(t *testing.T) {
	c := New(nil)

	t.Run("cte select", func(t *testing.T) {
		cl, _ := c.Classify("WITH cte AS (SELECT id FROM users) SELECT * FROM cte")
		if cl.OpType != core.OpRead {
			t.Errorf("OpType = %v, want OpRead", cl.OpType)
		}
		if !cl.IsComplexRead {
			t.Error("IsComplexRead should be true for CTE")
		}
	})

	t.Run("cte insert", func(t *testing.T) {
		cl, _ := c.Classify("WITH cte AS (SELECT 1 AS id) INSERT INTO users SELECT * FROM cte")
		if cl.OpType != core.OpWrite {
			t.Errorf("OpType = %v, want OpWrite", cl.OpType)
		}
	})
}

func TestClassify_CallBlocked(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"call no args", "CALL my_proc()", core.OpOther, core.SubNotSupported},
		{"call with args", "CALL my_proc(1, 2)", core.OpOther, core.SubNotSupported},
		{"call simple", "CALL do_something()", core.OpOther, core.SubNotSupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
			if cl.NotSupportedMsg == "" {
				t.Error("NotSupportedMsg should not be empty for CALL")
			}
		})
	}
}

func TestClassifyWithParams(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)

	cl, err := c.ClassifyWithParams("SELECT * FROM users WHERE id = ?", []interface{}{42})
	if err != nil {
		t.Fatalf("ClassifyWithParams error: %v", err)
	}
	if len(cl.PKs) != 1 {
		t.Fatalf("PKs len = %d, want 1", len(cl.PKs))
	}
	if cl.PKs[0].PK != "42" {
		t.Errorf("PK = %q, want %q", cl.PKs[0].PK, "42")
	}
}

func TestClassify_MariaDBReturning(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name         string
		sql          string
		wantOp       core.OpType
		wantSub      core.SubType
		wantReturn   bool
		wantTables   []string
	}{
		{
			"insert returning star",
			"INSERT INTO users (name) VALUES ('test') RETURNING *",
			core.OpWrite, core.SubInsert, true,
			[]string{"users"},
		},
		{
			"insert returning id",
			"INSERT INTO users (name) VALUES ('test') RETURNING id",
			core.OpWrite, core.SubInsert, true,
			nil,
		},
		{
			"delete returning star",
			"DELETE FROM users WHERE id = 1 RETURNING *",
			core.OpWrite, core.SubDelete, true,
			[]string{"users"},
		},
		{
			"replace returning id",
			"REPLACE INTO users (id, name) VALUES (1, 'test') RETURNING id",
			core.OpWrite, core.SubInsert, true,
			nil,
		},
		{
			"insert without returning",
			"INSERT INTO users (name) VALUES ('test')",
			core.OpWrite, core.SubInsert, false,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
			if cl.HasReturning != tt.wantReturn {
				t.Errorf("HasReturning = %v, want %v", cl.HasReturning, tt.wantReturn)
			}
			if tt.wantTables != nil {
				if len(cl.Tables) != len(tt.wantTables) {
					t.Errorf("Tables = %v, want %v", cl.Tables, tt.wantTables)
				} else {
					for i, want := range tt.wantTables {
						if cl.Tables[i] != want {
							t.Errorf("Tables[%d] = %q, want %q", i, cl.Tables[i], want)
						}
					}
				}
			}
		})
	}
}

func TestClassify_ExecuteImmediate(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("EXECUTE IMMEDIATE 'SELECT 1'")
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	if cl.OpType != core.OpOther {
		t.Errorf("OpType = %v, want OpOther", cl.OpType)
	}
	if cl.SubType != core.SubExecute {
		t.Errorf("SubType = %v, want SubExecute", cl.SubType)
	}
}

func TestClassify_MariaDBSequence(t *testing.T) {
	c := New(nil)
	tests := []struct {
		name    string
		sql     string
		wantOp  core.OpType
		wantSub core.SubType
	}{
		{"create sequence", "CREATE SEQUENCE seq1", core.OpDDL, core.SubCreate},
		{"drop sequence", "DROP SEQUENCE seq1", core.OpDDL, core.SubDrop},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %v, want %v", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %v, want %v", cl.SubType, tt.wantSub)
			}
		})
	}
}
