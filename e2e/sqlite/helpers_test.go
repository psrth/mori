//go:build e2e_sqlite

package e2e_sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "modernc.org/sqlite"
)

// connect returns a pgx.Conn connected through the Mori proxy using SimpleProtocol.
func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connCfg, err := pgx.ParseConfig(proxyDSN)
	if err != nil {
		t.Fatalf("parse proxy config: %v", err)
	}
	connCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// connectDirect returns a *sql.DB connected directly to the prod SQLite file.
func connectDirect(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", prodDBPath)
	if err != nil {
		t.Fatalf("open prod DB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// mustExec runs Exec through the proxy. NO parameters allowed (simple protocol only).
func mustExec(t *testing.T, conn *pgx.Conn, sqlStr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := conn.Exec(ctx, sqlStr)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", truncSQL(sqlStr), err)
	}
}

// mustQuery runs a query through the proxy and returns rows as maps.
func mustQuery(t *testing.T, conn *pgx.Conn, sqlStr string) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sqlStr)
	if err != nil {
		t.Fatalf("mustQuery(%q): %v", truncSQL(sqlStr), err)
	}
	defer rows.Close()
	return collectRows(t, rows)
}

// assertRowCount checks COUNT(*) on a table through proxy.
func assertRowCount(t *testing.T, conn *pgx.Conn, table string, expected int) {
	t.Helper()
	got := queryInt64(t, conn, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	if int(got) != expected {
		t.Errorf("assertRowCount(%s): got %d, want %d", table, got, expected)
	}
}

// queryScalar runs a query through proxy returning a single value as string.
// The SQLite proxy returns all values as text (OID 25), so we scan into string.
func queryScalar[T any](t *testing.T, conn *pgx.Conn, sqlStr string) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Scan into any — the SQLite proxy always returns text format.
	var raw any
	err := conn.QueryRow(ctx, sqlStr).Scan(&raw)
	if err != nil {
		t.Fatalf("queryScalar(%q): %v", truncSQL(sqlStr), err)
	}
	// Try direct type assertion.
	if v, ok := raw.(T); ok {
		return v
	}
	// If raw is string, try to convert to requested type T.
	var zero T
	t.Fatalf("queryScalar(%q): got type %T, cannot convert to %T", truncSQL(sqlStr), raw, zero)
	return zero
}

// queryInt64 runs a query and returns the first column as int64.
// Handles the SQLite proxy returning all values as text.
func queryInt64(t *testing.T, conn *pgx.Conn, sqlStr string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var raw any
	err := conn.QueryRow(ctx, sqlStr).Scan(&raw)
	if err != nil {
		t.Fatalf("queryInt64(%q): %v", truncSQL(sqlStr), err)
	}
	switch v := raw.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("queryInt64(%q): cannot parse %q: %v", truncSQL(sqlStr), v, err)
		}
		return n
	default:
		t.Fatalf("queryInt64(%q): unexpected type %T", truncSQL(sqlStr), raw)
		return 0
	}
}

// queryString runs a query and returns the first column as string.
func queryString(t *testing.T, conn *pgx.Conn, sqlStr string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var raw any
	err := conn.QueryRow(ctx, sqlStr).Scan(&raw)
	if err != nil {
		t.Fatalf("queryString(%q): %v", truncSQL(sqlStr), err)
	}
	return fmt.Sprintf("%v", raw)
}

// mustExecDirect runs Exec directly on prod SQLite file.
func mustExecDirect(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("mustExecDirect(%q): %v", truncSQL(query), err)
	}
}

// mustQueryDirect runs a query directly on prod SQLite and returns rows as maps.
func mustQueryDirect(t *testing.T, db *sql.DB, query string, args ...any) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("mustQueryDirect(%q): %v", truncSQL(query), err)
	}
	defer rows.Close()
	return collectRowsSQL(t, rows)
}

// collectRows reads pgx rows into []map[string]any.
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

// collectRowsSQL reads database/sql rows into []map[string]any.
func collectRowsSQL(t *testing.T, rows *sql.Rows) []map[string]any {
	t.Helper()
	cols, _ := rows.Columns()
	var result []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("collectRowsSQL scan: %v", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result
}

// assertQueryError runs a query through proxy and expects an error.
func assertQueryError(t *testing.T, conn *pgx.Conn, sqlStr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
	}
	if rows.Err() != nil {
		rows.Close()
		return
	}
	rows.Close()
	t.Errorf("assertQueryError(%q): expected error, got nil", truncSQL(sqlStr))
}

// assertNoError runs a query through proxy and expects no error.
func assertNoError(t *testing.T, conn *pgx.Conn, sqlStr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, sqlStr)
	if err != nil {
		t.Errorf("assertNoError(%q): %v", truncSQL(sqlStr), err)
		return
	}
	rows.Close()
}

// assertQueryRowCount runs a query and asserts row count.
func assertQueryRowCount(t *testing.T, conn *pgx.Conn, expected int, sqlStr string) {
	t.Helper()
	rows := mustQuery(t, conn, sqlStr)
	if len(rows) != expected {
		t.Errorf("assertQueryRowCount: got %d rows, want %d (query: %s)", len(rows), expected, truncSQL(sqlStr))
	}
}

func truncSQL(s string) string {
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}
