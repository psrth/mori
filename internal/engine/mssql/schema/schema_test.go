package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadTables(t *testing.T) {
	dir := t.TempDir()

	tables := map[string]TableMeta{
		"users": {
			PKColumns: []string{"id"},
			PKType:    "serial",
		},
		"posts": {
			PKColumns: []string{"id"},
			PKType:    "bigserial",
		},
		"tags": {
			PKColumns: []string{"name"},
			PKType:    "none",
		},
	}

	if err := WriteTables(dir, tables); err != nil {
		t.Fatalf("WriteTables error: %v", err)
	}

	// Verify file exists.
	path := filepath.Join(dir, TablesFile)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("tables.json not created: %v", err)
	}

	// Read back.
	got, err := ReadTables(dir)
	if err != nil {
		t.Fatalf("ReadTables error: %v", err)
	}

	if len(got) != len(tables) {
		t.Errorf("table count = %d, want %d", len(got), len(tables))
	}

	for name, want := range tables {
		g, ok := got[name]
		if !ok {
			t.Errorf("table %q not found", name)
			continue
		}
		if g.PKType != want.PKType {
			t.Errorf("table %q PKType = %q, want %q", name, g.PKType, want.PKType)
		}
		if len(g.PKColumns) != len(want.PKColumns) {
			t.Errorf("table %q PKColumns len = %d, want %d", name, len(g.PKColumns), len(want.PKColumns))
		}
	}
}

func TestReadTables_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadTables(dir)
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestClassifyPKType(t *testing.T) {
	tests := []struct {
		name       string
		dataTypes  []string
		identities []bool
		want       string
	}{
		{"identity int", []string{"int"}, []bool{true}, "serial"},
		{"identity bigint", []string{"bigint"}, []bool{true}, "bigserial"},
		{"uniqueidentifier pk", []string{"uniqueidentifier"}, []bool{false}, "uuid"},
		{"varchar pk", []string{"varchar"}, []bool{false}, "uuid"},
		{"int no identity", []string{"int"}, []bool{false}, "serial"},
		{"composite", []string{"int", "int"}, []bool{false, false}, "composite"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPKType(tt.dataTypes, tt.identities)
			if got != tt.want {
				t.Errorf("classifyPKType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripForeignKeys(t *testing.T) {
	input := `CREATE TABLE [users] (id INT PRIMARY KEY);
ALTER TABLE [posts] ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES [users](id);
CREATE TABLE [posts] (id INT PRIMARY KEY);`

	got := StripForeignKeys(input)
	if got == input {
		t.Error("StripForeignKeys should have removed FK lines")
	}
	if !stringContains(got, "CREATE TABLE [users]") {
		t.Error("StripForeignKeys should keep non-FK lines")
	}
	if stringContains(got, "FOREIGN KEY") {
		t.Error("StripForeignKeys should remove FK lines")
	}
}

func TestSplitStatements(t *testing.T) {
	t.Run("semicolons", func(t *testing.T) {
		input := "CREATE TABLE a (id INT); INSERT INTO a VALUES (1); SELECT 'hello;world'"
		stmts := splitStatements(input)
		// The string literal contains a semicolon so we get 3 statements.
		if len(stmts) != 3 {
			t.Errorf("splitStatements returned %d statements, want 3: %v", len(stmts), stmts)
		}
	})

	t.Run("GO separator", func(t *testing.T) {
		input := "CREATE TABLE a (id INT)\nGO\nINSERT INTO a VALUES (1)\nGO"
		stmts := splitStatements(input)
		if len(stmts) != 2 {
			t.Errorf("splitStatements with GO returned %d statements, want 2: %v", len(stmts), stmts)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		input := "SELECT 1;\nGO\nSELECT 2"
		stmts := splitStatements(input)
		if len(stmts) != 2 {
			t.Errorf("splitStatements mixed returned %d statements, want 2: %v", len(stmts), stmts)
		}
	})
}

func TestComputeOffset(t *testing.T) {
	tests := []struct {
		prodMax int64
		want    int64
	}{
		{0, 10_000_000},
		{100, 10_000_100},
		{2_000_000, 20_000_000},
	}

	for _, tt := range tests {
		got := computeOffset(tt.prodMax)
		if got != tt.want {
			t.Errorf("computeOffset(%d) = %d, want %d", tt.prodMax, got, tt.want)
		}
	}
}

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  string
		want bool
	}{
		{"deadlock 1205", "mssql: error 1205, deadlock victim", true},
		{"timeout -2", "mssql: Error -2, connection timeout", true},
		{"service error 40197", "error 40197: service error processing request", true},
		{"service busy 40501", "error 40501: service is currently busy", true},
		{"resource 49918", "error 49918: resource limit reached", true},
		{"resource 49919", "error 49919: resource limit reached", true},
		{"resource 49920", "error 49920: resource limit reached", true},
		{"deadlock text", "Transaction was Deadlock victim", true},
		{"timeout text", "connection timeout expired", true},
		{"busy text", "service is currently busy", true},
		{"normal error", "column not found", false},
		{"nil-like", "", false},
		{"syntax error", "incorrect syntax near 'SELECT'", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.err != "" {
				err = fmt.Errorf("%s", tt.err)
			}
			if got := isTransientError(err); got != tt.want {
				t.Errorf("isTransientError(%q) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRetryOnTransientError_SucceedsImmediately(t *testing.T) {
	calls := 0
	err := retryOnTransientError(3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestRetryOnTransientError_RetriesOnTransient(t *testing.T) {
	calls := 0
	err := retryOnTransientError(3, func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("error 1205: Deadlock")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetryOnTransientError_NonTransientNotRetried(t *testing.T) {
	calls := 0
	err := retryOnTransientError(5, func() error {
		calls++
		return fmt.Errorf("column not found")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (non-transient should not retry)", calls)
	}
}

func stringContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
