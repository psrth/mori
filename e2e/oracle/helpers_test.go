//go:build e2e_oracle

package e2e_oracle

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

// connect returns a *sql.DB connected through the Mori proxy.
func connect(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("oracle", proxyDSN)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("ping proxy: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// connectDirect returns a *sql.DB connected directly to the Prod database.
func connectDirect(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("oracle", prodDSN)
	if err != nil {
		t.Fatalf("connect to prod: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("ping prod: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// mustExec runs an Exec through the given db and fails on error.
func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", truncSQL(query), err)
	}
}

// mustQuery runs a query and returns the rows as []map[string]any.
func mustQuery(t *testing.T, db *sql.DB, query string, args ...any) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("mustQuery(%q): %v", truncSQL(query), err)
	}
	defer rows.Close()
	return collectRows(t, rows)
}

// assertRowCount asserts that COUNT(*) on a table equals expected.
func assertRowCount(t *testing.T, db *sql.DB, table string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var count int
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		t.Fatalf("assertRowCount(%s): %v", table, err)
	}
	if count != expected {
		t.Errorf("assertRowCount(%s): got %d, want %d", table, count, expected)
	}
}

// assertQueryRowCount runs a query and asserts the number of rows returned.
func assertQueryRowCount(t *testing.T, db *sql.DB, expected int, query string, args ...any) {
	t.Helper()
	rows := mustQuery(t, db, query, args...)
	if len(rows) != expected {
		t.Errorf("assertQueryRowCount: got %d rows, want %d (query: %s)", len(rows), expected, truncSQL(query))
	}
}

// queryScalar runs a query that returns a single value.
func queryScalar[T any](t *testing.T, db *sql.DB, query string, args ...any) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var val T
	err := db.QueryRowContext(ctx, query, args...).Scan(&val)
	if err != nil {
		t.Fatalf("queryScalar(%q): %v", truncSQL(query), err)
	}
	return val
}

// collectRows reads all rows into []map[string]any.
func collectRows(t *testing.T, rows *sql.Rows) []map[string]any {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("collectRows columns: %v", err)
	}
	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("collectRows scan: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("collectRows: %v", err)
	}
	return result
}

// truncSQL truncates SQL for error messages.
func truncSQL(s string) string {
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}

// assertNoError runs a query and verifies it doesn't error.
func assertNoError(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Errorf("assertNoError(%q): %v", truncSQL(query), err)
		return
	}
	rows.Close()
}

// assertQueryError runs a query and verifies it returns an error.
func assertQueryError(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return // Got error as expected.
	}
	// Drain rows to surface deferred errors.
	for rows.Next() {
	}
	if rows.Err() != nil {
		rows.Close()
		return // Got error from rows iteration.
	}
	rows.Close()
	t.Errorf("assertQueryError(%q): expected error, got nil", truncSQL(query))
}
