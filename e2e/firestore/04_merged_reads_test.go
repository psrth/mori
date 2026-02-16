//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/api/iterator"
)

func TestMergedReads(t *testing.T) {
	t.Run("read_prod_doc_through_proxy", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		doc, err := client.Collection("users").Doc("user_1").Get(ctx)
		if err != nil {
			t.Fatalf("get prod doc: %v", err)
		}
		if doc.Data()["name"] != "User 1" {
			t.Errorf("got name %v, want 'User 1'", doc.Data()["name"])
		}
	})

	t.Run("read_shadow_doc_after_create", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a new doc through proxy.
		_, err := client.Collection("users").Doc("merged_read_user").Set(ctx, map[string]interface{}{
			"name":  "Merged Read User",
			"email": "merged@example.com",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		// Read it back — should come from shadow.
		doc, err := client.Collection("users").Doc("merged_read_user").Get(ctx)
		if err != nil {
			t.Fatalf("get shadow doc: %v", err)
		}
		if doc.Data()["name"] != "Merged Read User" {
			t.Errorf("got name %v, want 'Merged Read User'", doc.Data()["name"])
		}
	})

	t.Run("delete_hides_prod_doc", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Delete a prod doc through proxy.
		_, err := client.Collection("products").Doc("prod_2").Delete(ctx)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		// Reading it should return not found.
		_, err = client.Collection("products").Doc("prod_2").Get(ctx)
		if err == nil {
			t.Error("expected error reading deleted prod doc, got nil")
		}
	})

	t.Run("list_includes_shadow_inserts", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a shadow-only product.
		_, err := client.Collection("products").Doc("shadow_prod_1").Set(ctx, map[string]interface{}{
			"name":  "Shadow Product",
			"price": 99.99,
		})
		if err != nil {
			t.Fatalf("create shadow product: %v", err)
		}

		// List should include both prod and shadow docs.
		// Prod had 5 products. prod_1 was tombstoned in TestWriteIsolation,
		// prod_2 was tombstoned in delete_hides_prod_doc.
		// So we expect 3 remaining prod + 1 shadow insert = 4.
		iter := client.Collection("products").Documents(ctx)
		defer iter.Stop()

		found := false
		count := 0
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			count++
			if doc.Ref.ID == "shadow_prod_1" {
				found = true
			}
		}
		if !found {
			t.Error("shadow insert not found in list results")
		}
		if count < 4 {
			t.Errorf("expected at least 4 products (3 prod + 1 shadow), got %d", count)
		}
	})

	t.Run("update_shows_shadow_values", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Update a prod doc.
		_, err := client.Collection("users").Doc("user_5").Set(ctx, map[string]interface{}{
			"name":  "User 5 Updated",
			"email": "user5_updated@example.com",
			"age":   99,
		})
		if err != nil {
			t.Fatalf("update: %v", err)
		}

		// Read back — should show shadow values.
		doc, err := client.Collection("users").Doc("user_5").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if doc.Data()["name"] != "User 5 Updated" {
			t.Errorf("got name %v, want 'User 5 Updated'", doc.Data()["name"])
		}
	})

	t.Run("prod_untouched_after_shadow_writes", func(t *testing.T) {
		prod := connectProdEmulator(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Verify prod user_5 is still original.
		doc, err := prod.Collection("users").Doc("user_5").Get(ctx)
		if err != nil {
			t.Fatalf("get prod: %v", err)
		}
		if doc.Data()["name"] != "User 5" {
			t.Errorf("prod name changed to %v, expected 'User 5'", doc.Data()["name"])
		}

		// Verify prod prod_2 still exists.
		doc, err = prod.Collection("products").Doc("prod_2").Get(ctx)
		if err != nil {
			t.Fatalf("prod_2 should still exist in prod: %v", err)
		}
		if doc.Data()["name"] != "Product 2" {
			t.Errorf("prod_2 name changed to %v, expected 'Product 2'", doc.Data()["name"])
		}
	})

	t.Run("count_after_writes", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a few more docs in a new collection for clean counting.
		for i := 0; i < 3; i++ {
			_, err := client.Collection("count_test").Doc(fmt.Sprintf("doc_%d", i)).Set(ctx, map[string]interface{}{
				"value": i,
			})
			if err != nil {
				t.Fatalf("create count_test doc_%d: %v", i, err)
			}
		}

		// Count docs through proxy.
		iter := client.Collection("count_test").Documents(ctx)
		defer iter.Stop()
		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("list count_test: %v", err)
			}
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 count_test docs, got %d", count)
		}
	})

	t.Run("read_nonexistent_doc_returns_error", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Collection("users").Doc("totally_nonexistent_user").Get(ctx)
		if err == nil {
			t.Error("expected error for nonexistent doc, got nil")
		}
	})
}
