package classify

import (
	"reflect"
	"testing"

	"github.com/mori-dev/mori/internal/core"
	"github.com/mori-dev/mori/internal/engine/postgres/schema"
)

func testClassifier() *PgClassifier {
	return New(map[string]schema.TableMeta{
		"users":      {PKColumns: []string{"id"}, PKType: "serial"},
		"orders":     {PKColumns: []string{"id"}, PKType: "bigserial"},
		"user_roles": {PKColumns: []string{"user_id", "role_id"}, PKType: "composite"},
		"products":   {PKColumns: []string{"uuid"}, PKType: "uuid"},
	})
}

func TestClassify(t *testing.T) {
	c := testClassifier()

	tests := []struct {
		name     string
		sql      string
		wantOp   core.OpType
		wantSub  core.SubType
		wantTbls []string
		wantJoin bool
		wantPKs  []core.TablePK
		wantLim  bool
		limVal   int
		wantOrd  bool // whether OrderBy should be non-empty
	}{
		// ── Simple CRUD ──────────────────────────────────────────────
		{
			name:     "select all",
			sql:      "SELECT * FROM users",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
		},
		{
			name:     "select with where",
			sql:      "SELECT id, name FROM users WHERE active = true",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
		},
		{
			name:     "select with pk",
			sql:      "SELECT * FROM users WHERE id = 42",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantPKs:  []core.TablePK{{Table: "users", PK: "42"}},
		},
		{
			name:     "insert",
			sql:      "INSERT INTO users (name, email) VALUES ('alice', 'a@b.com')",
			wantOp:   core.OpWrite,
			wantSub:  core.SubInsert,
			wantTbls: []string{"users"},
		},
		{
			name:     "insert select",
			sql:      "INSERT INTO users (name) SELECT name FROM orders",
			wantOp:   core.OpWrite,
			wantSub:  core.SubInsert,
			wantTbls: []string{"users", "orders"},
		},
		{
			name:     "update with pk",
			sql:      "UPDATE users SET name = 'bob' WHERE id = 42",
			wantOp:   core.OpWrite,
			wantSub:  core.SubUpdate,
			wantTbls: []string{"users"},
			wantPKs:  []core.TablePK{{Table: "users", PK: "42"}},
		},
		{
			name:     "update without pk",
			sql:      "UPDATE users SET active = false WHERE last_login < '2020-01-01'",
			wantOp:   core.OpWrite,
			wantSub:  core.SubUpdate,
			wantTbls: []string{"users"},
		},
		{
			name:     "delete with pk",
			sql:      "DELETE FROM users WHERE id = 42",
			wantOp:   core.OpWrite,
			wantSub:  core.SubDelete,
			wantTbls: []string{"users"},
			wantPKs:  []core.TablePK{{Table: "users", PK: "42"}},
		},

		// ── JOINs ────────────────────────────────────────────────────
		{
			name:     "inner join",
			sql:      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "left join",
			sql:      "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "right join",
			sql:      "SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "implicit cross join",
			sql:      "SELECT * FROM users, orders WHERE users.id = orders.id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "self join",
			sql:      "SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantJoin: true, // JoinExpr present
		},
		{
			name:     "cross join",
			sql:      "SELECT * FROM users CROSS JOIN orders",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "three-way join",
			sql:      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.id = p.uuid",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders", "products"},
			wantJoin: true,
		},

		// ── Subqueries ───────────────────────────────────────────────
		{
			name:     "where in subquery",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
		},
		{
			name:     "from subquery",
			sql:      "SELECT * FROM (SELECT * FROM users) AS sub",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
		},
		{
			name:     "exists subquery",
			sql:      "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
		},
		{
			name:     "delete with subquery",
			sql:      "DELETE FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)",
			wantOp:   core.OpWrite,
			wantSub:  core.SubDelete,
			wantTbls: []string{"users"},
		},

		// ── CTEs ─────────────────────────────────────────────────────
		{
			name:     "read cte",
			sql:      "WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
		},
		{
			name:     "mutating cte escalates to write",
			sql:      "WITH updated AS (UPDATE users SET active = false WHERE id = 42 RETURNING *) SELECT * FROM updated",
			wantOp:   core.OpWrite,
			wantSub:  core.SubOther,
			wantTbls: []string{"users"},
		},
		{
			name:     "insert cte escalates to write",
			sql:      "WITH ins AS (INSERT INTO users (name) VALUES ('x') RETURNING *) SELECT * FROM ins",
			wantOp:   core.OpWrite,
			wantSub:  core.SubOther,
			wantTbls: []string{"users"},
		},
		{
			name:     "cte with join",
			sql:      "WITH u AS (SELECT * FROM users) SELECT * FROM u JOIN orders o ON u.id = o.user_id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},

		// ── DDL ──────────────────────────────────────────────────────
		{
			name:     "create table",
			sql:      "CREATE TABLE new_table (id serial PRIMARY KEY, name text)",
			wantOp:   core.OpDDL,
			wantSub:  core.SubCreate,
			wantTbls: []string{"new_table"},
		},
		{
			name:     "alter table add column",
			sql:      "ALTER TABLE users ADD COLUMN phone text",
			wantOp:   core.OpDDL,
			wantSub:  core.SubAlter,
			wantTbls: []string{"users"},
		},
		{
			name:     "alter table drop column",
			sql:      "ALTER TABLE users DROP COLUMN fax",
			wantOp:   core.OpDDL,
			wantSub:  core.SubAlter,
			wantTbls: []string{"users"},
		},
		{
			name:     "drop table",
			sql:      "DROP TABLE users",
			wantOp:   core.OpDDL,
			wantSub:  core.SubDrop,
			wantTbls: []string{"users"},
		},
		{
			name:     "create index",
			sql:      "CREATE INDEX idx_users_name ON users (name)",
			wantOp:   core.OpDDL,
			wantSub:  core.SubCreate,
			wantTbls: []string{"users"},
		},
		{
			name:     "alter table rename column",
			sql:      "ALTER TABLE users RENAME COLUMN name TO full_name",
			wantOp:   core.OpDDL,
			wantSub:  core.SubAlter,
			wantTbls: []string{"users"},
		},
		{
			name:     "drop table multi",
			sql:      "DROP TABLE users, orders",
			wantOp:   core.OpDDL,
			wantSub:  core.SubDrop,
			wantTbls: []string{"users", "orders"},
		},

		// ── Transactions ─────────────────────────────────────────────
		{
			name:    "begin",
			sql:     "BEGIN",
			wantOp:  core.OpTransaction,
			wantSub: core.SubBegin,
		},
		{
			name:    "commit",
			sql:     "COMMIT",
			wantOp:  core.OpTransaction,
			wantSub: core.SubCommit,
		},
		{
			name:    "rollback",
			sql:     "ROLLBACK",
			wantOp:  core.OpTransaction,
			wantSub: core.SubRollback,
		},
		{
			name:    "savepoint",
			sql:     "SAVEPOINT my_save",
			wantOp:  core.OpTransaction,
			wantSub: core.SubSavepoint,
		},
		{
			name:    "start transaction",
			sql:     "START TRANSACTION",
			wantOp:  core.OpTransaction,
			wantSub: core.SubBegin,
		},

		// ── LIMIT / ORDER BY ────────────────────────────────────────
		{
			name:     "order by and limit",
			sql:      "SELECT * FROM users ORDER BY created_at DESC LIMIT 10",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantLim:  true,
			limVal:   10,
			wantOrd:  true,
		},
		{
			name:     "limit only",
			sql:      "SELECT * FROM users LIMIT 5",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantLim:  true,
			limVal:   5,
		},
		{
			name:     "order by only",
			sql:      "SELECT * FROM users ORDER BY name ASC",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantOrd:  true,
		},
		{
			name:     "limit with offset",
			sql:      "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantLim:  true,
			limVal:   10,
			wantOrd:  true,
		},
		{
			name:     "multi column order by",
			sql:      "SELECT * FROM users ORDER BY name, created_at DESC LIMIT 100",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantLim:  true,
			limVal:   100,
			wantOrd:  true,
		},

		// ── Set Operations ──────────────────────────────────────────
		{
			name:     "union",
			sql:      "SELECT * FROM users UNION SELECT * FROM orders",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
		},
		{
			name:     "intersect",
			sql:      "SELECT id FROM users INTERSECT SELECT id FROM orders",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
		},
		{
			name:     "union all with order by",
			sql:      "(SELECT * FROM users) UNION ALL (SELECT * FROM orders) ORDER BY id",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users", "orders"},
			wantOrd:  true,
		},

		// ── Edge Cases ──────────────────────────────────────────────
		{
			name:    "select literal",
			sql:     "SELECT 1",
			wantOp:  core.OpRead,
			wantSub: core.SubSelect,
		},
		{
			name:    "select function",
			sql:     "SELECT now()",
			wantOp:  core.OpRead,
			wantSub: core.SubSelect,
		},
		{
			name:    "set statement",
			sql:     "SET statement_timeout = 5000",
			wantOp:  core.OpOther,
			wantSub: core.SubSet,
		},
		{
			name:    "show",
			sql:     "SHOW server_version",
			wantOp:  core.OpOther,
			wantSub: core.SubShow,
		},
		{
			name:     "explain",
			sql:      "EXPLAIN SELECT * FROM users",
			wantOp:   core.OpOther,
			wantSub:  core.SubExplain,
			wantTbls: []string{"users"},
		},
		{
			name:    "explain analyze",
			sql:     "EXPLAIN ANALYZE SELECT * FROM users",
			wantOp:  core.OpOther,
			wantSub: core.SubNotSupported,
		},
		{
			name:     "update from",
			sql:      "UPDATE users SET name = 'x' FROM orders WHERE users.id = orders.user_id",
			wantOp:   core.OpWrite,
			wantSub:  core.SubUpdate,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "delete using",
			sql:      "DELETE FROM users USING orders WHERE users.id = orders.user_id",
			wantOp:   core.OpWrite,
			wantSub:  core.SubDelete,
			wantTbls: []string{"users", "orders"},
			wantJoin: true,
		},
		{
			name:     "schema qualified table",
			sql:      "SELECT * FROM public.users",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"public.users"},
		},
		{
			name:     "parameterized query",
			sql:      "SELECT * FROM users WHERE id = $1",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"users"},
			wantPKs:  []core.TablePK{{Table: "users", PK: "$1"}},
		},
		{
			name:     "truncate",
			sql:      "TRUNCATE users",
			wantOp:   core.OpWrite,
			wantSub:  core.SubTruncate,
			wantTbls: []string{"users"},
		},
		{
			name:     "select with string pk",
			sql:      "SELECT * FROM products WHERE uuid = 'abc-123'",
			wantOp:   core.OpRead,
			wantSub:  core.SubSelect,
			wantTbls: []string{"products"},
			wantPKs:  []core.TablePK{{Table: "products", PK: "abc-123"}},
		},
		{
			name:     "update with and conditions pk",
			sql:      "UPDATE users SET name = 'x' WHERE id = 99 AND active = true",
			wantOp:   core.OpWrite,
			wantSub:  core.SubUpdate,
			wantTbls: []string{"users"},
			wantPKs:  []core.TablePK{{Table: "users", PK: "99"}},
		},

		// ── Multi-statement detection ───────────────────────────────
		{
			name:    "multi-stmt select then delete",
			sql:     "SELECT 1; DELETE FROM users",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then insert",
			sql:     "SELECT 1; INSERT INTO users(id) VALUES(1)",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then drop (DDL counts)",
			sql:     "SELECT 1; DROP TABLE users",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt all reads is fine",
			sql:     "SELECT 1; SELECT 2",
			wantOp:  core.OpRead,
			wantSub: core.SubSelect,
		},
		{
			name:     "multi-stmt write first classified normally",
			sql:      "DELETE FROM users; SELECT 1",
			wantOp:   core.OpWrite,
			wantSub:  core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then COPY",
			sql:     "SELECT 1; COPY users TO STDOUT",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then DO block",
			sql:     "SELECT 1; DO $$ BEGIN PERFORM 1; END $$",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then CALL",
			sql:     "SELECT 1; CALL my_proc()",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then GRANT",
			sql:     "SELECT 1; GRANT ALL ON users TO attacker",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
		{
			name:    "multi-stmt select then GRANT ROLE",
			sql:     "SELECT 1; GRANT admin TO attacker",
			wantOp:  core.OpWrite,
			wantSub: core.SubNotSupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify(%q) error: %v", tt.sql, err)
			}

			if cl.OpType != tt.wantOp {
				t.Errorf("OpType = %s, want %s", cl.OpType, tt.wantOp)
			}
			if cl.SubType != tt.wantSub {
				t.Errorf("SubType = %s, want %s", cl.SubType, tt.wantSub)
			}
			if !equalTables(cl.Tables, tt.wantTbls) {
				t.Errorf("Tables = %v, want %v", cl.Tables, tt.wantTbls)
			}
			if cl.IsJoin != tt.wantJoin {
				t.Errorf("IsJoin = %v, want %v", cl.IsJoin, tt.wantJoin)
			}
			if !equalPKs(cl.PKs, tt.wantPKs) {
				t.Errorf("PKs = %v, want %v", cl.PKs, tt.wantPKs)
			}
			if cl.HasLimit != tt.wantLim {
				t.Errorf("HasLimit = %v, want %v", cl.HasLimit, tt.wantLim)
			}
			if tt.wantLim && cl.Limit != tt.limVal {
				t.Errorf("Limit = %d, want %d", cl.Limit, tt.limVal)
			}
			if tt.wantOrd && cl.OrderBy == "" {
				t.Error("OrderBy is empty, want non-empty")
			}
			if !tt.wantOrd && cl.OrderBy != "" {
				t.Errorf("OrderBy = %q, want empty", cl.OrderBy)
			}
			if cl.RawSQL != tt.sql {
				t.Errorf("RawSQL = %q, want %q", cl.RawSQL, tt.sql)
			}
		})
	}
}

func TestClassifyWithParams(t *testing.T) {
	c := testClassifier()

	tests := []struct {
		name    string
		sql     string
		params  []interface{}
		wantPKs []core.TablePK
	}{
		{
			name:    "select with param",
			sql:     "SELECT * FROM users WHERE id = $1",
			params:  []interface{}{42},
			wantPKs: []core.TablePK{{Table: "users", PK: "42"}},
		},
		{
			name:    "update with param",
			sql:     "UPDATE users SET name = $1 WHERE id = $2",
			params:  []interface{}{"bob", 99},
			wantPKs: []core.TablePK{{Table: "users", PK: "99"}},
		},
		{
			name:    "delete with param",
			sql:     "DELETE FROM users WHERE id = $1",
			params:  []interface{}{7},
			wantPKs: []core.TablePK{{Table: "users", PK: "7"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := c.ClassifyWithParams(tt.sql, tt.params)
			if err != nil {
				t.Fatalf("ClassifyWithParams(%q) error: %v", tt.sql, err)
			}
			if !equalPKs(cl.PKs, tt.wantPKs) {
				t.Errorf("PKs = %v, want %v", cl.PKs, tt.wantPKs)
			}
		})
	}
}

func TestClassifyParseError(t *testing.T) {
	c := testClassifier()
	_, err := c.Classify("NOT VALID SQL !!! %%%")
	if err == nil {
		t.Error("expected parse error, got nil")
	}
}

func TestClassifyNilTables(t *testing.T) {
	c := New(nil)
	cl, err := c.Classify("SELECT * FROM users WHERE id = 42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.OpType != core.OpRead {
		t.Errorf("OpType = %s, want READ", cl.OpType)
	}
	// PK extraction skipped because table metadata is empty.
	if len(cl.PKs) != 0 {
		t.Errorf("PKs = %v, want empty (no table metadata)", cl.PKs)
	}
}

// equalTables compares two string slices, treating nil and empty as equal.
func equalTables(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// equalPKs compares two TablePK slices, treating nil and empty as equal.
func equalPKs(a, b []core.TablePK) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
