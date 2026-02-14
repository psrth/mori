//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/iterator"
)

func TestLifecycle(t *testing.T) {
	t.Run("proxy_accepts_read", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		doc, err := client.Collection("users").Doc("user_1").Get(ctx)
		if err != nil {
			t.Fatalf("GetDocument through proxy: %v", err)
		}
		data := doc.Data()
		if data["name"] != "User 1" {
			t.Errorf("got name %v, want 'User 1'", data["name"])
		}
	})

	t.Run("proxy_lists_documents", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		iter := client.Collection("products").Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("ListDocuments: %v", err)
			}
			count++
		}
		if count < 1 {
			t.Errorf("expected at least 1 product, got %d", count)
		}
	})

	t.Run("status_shows_firestore", func(t *testing.T) {
		out := runMoriCommand(t, "status")
		if !strings.Contains(strings.ToLower(out), "firestore") {
			t.Errorf("status output missing 'firestore':\n%s", out)
		}
	})
}
