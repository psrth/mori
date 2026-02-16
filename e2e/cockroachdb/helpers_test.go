//go:build e2e_cockroachdb

package e2e_cockroachdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// connect returns a pgx.Conn connected through the Mori proxy.
func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, proxyDSN)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// connectDirect returns a pgx.Conn connected directly to the Prod database.
func connectDirect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, prodDSN)
	if err != nil {
		t.Fatalf("connect to prod: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// mustExec runs an Exec through the proxy and fails on error.
func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", truncSQL(sql), err)
	}
}

// mustQuery runs a query and returns the rows as []map[string]any.
func mustQuery(t *testing.T, conn *pgx.Conn, sql string, args ...any) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("mustQuery(%q): %v", truncSQL(sql), err)
	}
	defer rows.Close()
	return collectRows(t, rows)
}

// assertRowCount asserts that COUNT(*) on a table equals expected.
func assertRowCount(t *testing.T, conn *pgx.Conn, table string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var count int
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		t.Fatalf("assertRowCount(%s): %v", table, err)
	}
	if count != expected {
		t.Errorf("assertRowCount(%s): got %d, want %d", table, count, expected)
	}
}

// assertQueryRowCount runs a query and asserts the number of rows returned.
func assertQueryRowCount(t *testing.T, conn *pgx.Conn, expected int, sql string, args ...any) {
	t.Helper()
	rows := mustQuery(t, conn, sql, args...)
	if len(rows) != expected {
		t.Errorf("assertQueryRowCount: got %d rows, want %d (query: %s)", len(rows), expected, truncSQL(sql))
	}
}

// assertExecRowsAffected runs Exec and asserts rows affected.
func assertExecRowsAffected(t *testing.T, conn *pgx.Conn, expected int64, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tag, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		t.Fatalf("assertExecRowsAffected(%q): %v", truncSQL(sql), err)
	}
	if tag.RowsAffected() != expected {
		t.Errorf("assertExecRowsAffected(%q): got %d, want %d", truncSQL(sql), tag.RowsAffected(), expected)
	}
}

// assertNoError runs a query and verifies it doesn't error.
func assertNoError(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		t.Errorf("assertNoError(%q): %v", truncSQL(sql), err)
		return
	}
	rows.Close()
}

// assertQueryError runs a query and verifies it returns an error.
func assertQueryError(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return // Got error from Query, as expected.
	}
	// In pgx v5, backend errors may be deferred to rows.Err().
	for rows.Next() {
	}
	if rows.Err() != nil {
		rows.Close()
		return // Got error from rows, as expected.
	}
	rows.Close()
	t.Errorf("assertQueryError(%q): expected error, got nil", truncSQL(sql))
}

// queryScalar runs a query that returns a single value.
func queryScalar[T any](t *testing.T, conn *pgx.Conn, sql string, args ...any) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var val T
	err := conn.QueryRow(ctx, sql, args...).Scan(&val)
	if err != nil {
		t.Fatalf("queryScalar(%q): %v", truncSQL(sql), err)
	}
	return val
}

// collectRows reads all rows into []map[string]any.
func collectRows(t *testing.T, rows pgx.Rows) []map[string]any {
	t.Helper()
	var result []map[string]any
	cols := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("collectRows: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, fd := range cols {
			row[fd.Name] = values[i]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("collectRows: %v", err)
	}
	return result
}

// truncSQL truncates SQL for error messages.
func truncSQL(sql string) string {
	if len(sql) > 80 {
		return sql[:77] + "..."
	}
	return sql
}
