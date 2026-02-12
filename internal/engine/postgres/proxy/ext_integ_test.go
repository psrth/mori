//go:build integ

package proxy_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestExtendedProtocolIntegration exercises parameterized queries through the
// Mori proxy using pgx (which always uses the PG extended query protocol).
//
// Requires: Mori proxy running on localhost:15432 with ext_test table seeded.
// Run with: go test -tags integ -run TestExtendedProtocol -v ./internal/engine/postgres/proxy/
func TestExtendedProtocolIntegration(t *testing.T) {
	dsn := os.Getenv("MORI_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:testpass@localhost:15432/testdb?sslmode=disable"
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	t.Run("parameterized SELECT single", func(t *testing.T) {
		var name, email string
		err := conn.QueryRow(ctx, "SELECT name, email FROM ext_test WHERE id = $1", 1).Scan(&name, &email)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if name != "alice" || email != "alice@example.com" {
			t.Errorf("got (%q, %q), want (alice, alice@example.com)", name, email)
		}
	})

	t.Run("parameterized SELECT multiple params", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT id, name FROM ext_test WHERE id >= $1 AND id <= $2 ORDER BY id", 1, 2)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		var results []string
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatalf("scan: %v", err)
			}
			results = append(results, fmt.Sprintf("%d:%s", id, name))
		}
		if len(results) != 2 {
			t.Fatalf("got %d rows, want 2: %v", len(results), results)
		}
		if results[0] != "1:alice" || results[1] != "2:bob" {
			t.Errorf("got %v, want [1:alice, 2:bob]", results)
		}
	})

	t.Run("parameterized INSERT", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "INSERT INTO ext_test (name, email) VALUES ($1, $2)", "dave", "dave@example.com")
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("verify INSERT visible", func(t *testing.T) {
		var name string
		err := conn.QueryRow(ctx, "SELECT name FROM ext_test WHERE name = $1", "dave").Scan(&name)
		if err != nil {
			t.Fatalf("inserted row not found: %v", err)
		}
		if name != "dave" {
			t.Errorf("got %q, want dave", name)
		}
	})

	t.Run("parameterized UPDATE", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "UPDATE ext_test SET email = $1 WHERE name = $2", "bob-updated@example.com", "bob")
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("parameterized DELETE", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "DELETE FROM ext_test WHERE name = $1", "charlie")
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("NULL parameter", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "UPDATE ext_test SET email = $1 WHERE name = $2", nil, "alice")
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
	})

	t.Run("explicit prepared statement", func(t *testing.T) {
		_, err := conn.Prepare(ctx, "get_user", "SELECT name, email FROM ext_test WHERE id = $1")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		var name string
		var email *string
		err = conn.QueryRow(ctx, "get_user", 2).Scan(&name, &email)
		if err != nil {
			t.Fatalf("exec prepared: %v", err)
		}
		if name != "bob" {
			t.Errorf("got name=%q, want bob", name)
		}
	})

	t.Run("transaction with params", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO ext_test (name, email) VALUES ($1, $2)", "eve", "eve@example.com")
		if err != nil {
			tx.Rollback(ctx) //nolint: errcheck
			t.Fatalf("insert in tx: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
	})

	t.Run("SELECT no results", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT id FROM ext_test WHERE id = $1", 99999)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		if rows.Next() {
			t.Error("expected no rows")
		}
	})

	t.Run("special characters in params", func(t *testing.T) {
		tag, err := conn.Exec(ctx, "INSERT INTO ext_test (name, email) VALUES ($1, $2)", "O'Brien", "ob@test.com")
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Errorf("rows affected = %d, want 1", tag.RowsAffected())
		}
		var name string
		err = conn.QueryRow(ctx, "SELECT name FROM ext_test WHERE name = $1", "O'Brien").Scan(&name)
		if err != nil {
			t.Fatalf("reading back: %v", err)
		}
		if name != "O'Brien" {
			t.Errorf("got %q, want O'Brien", name)
		}
	})
}
