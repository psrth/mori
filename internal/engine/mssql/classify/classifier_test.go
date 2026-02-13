package classify

import (
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/mssql/schema"
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
		{"select with top", "SELECT TOP 10 * FROM users", core.OpRead, core.SubSelect},
		{"select count", "SELECT COUNT(*) FROM users", core.OpRead, core.SubSelect},
		{"select with subquery", "SELECT * FROM (SELECT id FROM users) AS sub", core.OpRead, core.SubSelect},
		{"select with offset fetch", "SELECT * FROM users ORDER BY id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", core.OpRead, core.SubSelect},
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
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", core.OpWrite, core.SubUpdate},
		{"delete", "DELETE FROM users WHERE id = 1", core.OpWrite, core.SubDelete},
		{"truncate", "TRUNCATE TABLE users", core.OpWrite, core.SubOther},
		{"merge", "MERGE INTO users USING source ON users.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name", core.OpWrite, core.SubOther},
		{"bulk insert", "BULK INSERT users FROM 'data.csv'", core.OpWrite, core.SubInsert},
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
		{"alter table", "ALTER TABLE users ADD age INT", core.OpDDL, core.SubAlter},
		{"drop table", "DROP TABLE users", core.OpDDL, core.SubDrop},
		{"drop if exists", "DROP TABLE IF EXISTS users", core.OpDDL, core.SubDrop},
		{"create index", "CREATE INDEX idx_name ON users (name)", core.OpDDL, core.SubCreate},
		{"create clustered index", "CREATE CLUSTERED INDEX idx_id ON users (id)", core.OpDDL, core.SubCreate},
		{"create nonclustered index", "CREATE NONCLUSTERED INDEX idx_name ON users (name)", core.OpDDL, core.SubCreate},
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
		{"begin tran", "BEGIN TRAN", core.SubBegin},
		{"begin transaction", "BEGIN TRANSACTION", core.SubBegin},
		{"commit", "COMMIT", core.SubCommit},
		{"commit transaction", "COMMIT TRANSACTION", core.SubCommit},
		{"rollback", "ROLLBACK", core.SubRollback},
		{"rollback transaction", "ROLLBACK TRANSACTION", core.SubRollback},
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
		name string
		sql  string
	}{
		{"set", "SET NOCOUNT ON"},
		{"exec", "EXEC sp_help"},
		{"execute", "EXECUTE sp_help"},
		{"declare", "DECLARE @id INT"},
		{"use", "USE mydb"},
		{"print", "PRINT 'hello'"},
		{"dbcc", "DBCC CHECKDB"},
		{"begin block", "BEGIN SELECT 1 END"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}
			if cl.OpType != core.OpOther {
				t.Errorf("OpType = %v, want OpOther", cl.OpType)
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
		{"bracket quoted", "SELECT * FROM [users]", []string{"users"}},
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
		cl, _ := c.Classify("SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id")
		if !cl.IsJoin {
			t.Error("IsJoin should be true for JOIN query")
		}
	})

	t.Run("top detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT TOP 25 * FROM users")
		if !cl.HasLimit {
			t.Error("HasLimit should be true for TOP")
		}
		if cl.Limit != 25 {
			t.Errorf("Limit = %d, want 25", cl.Limit)
		}
	})

	t.Run("top with parens", func(t *testing.T) {
		cl, _ := c.Classify("SELECT TOP(10) * FROM users")
		if !cl.HasLimit {
			t.Error("HasLimit should be true for TOP(n)")
		}
		if cl.Limit != 10 {
			t.Errorf("Limit = %d, want 10", cl.Limit)
		}
	})

	t.Run("fetch detected", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM users ORDER BY id OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY")
		if !cl.HasLimit {
			t.Error("HasLimit should be true for FETCH")
		}
		if cl.Limit != 5 {
			t.Errorf("Limit = %d, want 5", cl.Limit)
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

	t.Run("bracket quoted column", func(t *testing.T) {
		cl, _ := c.Classify("SELECT * FROM users WHERE [id] = 99")
		if len(cl.PKs) != 1 {
			t.Fatalf("PKs len = %d, want 1", len(cl.PKs))
		}
		if cl.PKs[0].PK != "99" {
			t.Errorf("PK = %q, want %q", cl.PKs[0].PK, "99")
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

	t.Run("cte merge", func(t *testing.T) {
		cl, _ := c.Classify("WITH src AS (SELECT 1 AS id) MERGE INTO users USING src ON users.id = src.id WHEN MATCHED THEN UPDATE SET name = 'x'")
		if cl.OpType != core.OpWrite {
			t.Errorf("OpType = %v, want OpWrite", cl.OpType)
		}
	})
}

func TestClassifyWithParams(t *testing.T) {
	tables := map[string]schema.TableMeta{
		"users": {PKColumns: []string{"id"}, PKType: "serial"},
	}
	c := New(tables)

	cl, err := c.ClassifyWithParams("SELECT * FROM users WHERE id = @p1", []interface{}{42})
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
