//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/iterator"
)

func TestEdgeCases(t *testing.T) {
	t.Run("large_document", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// Create a document with a large string field (~100KB).
		largeStr := strings.Repeat("A", 100*1024)
		_, err := client.Collection("edge_cases").Doc("large_doc").Set(ctx, map[string]interface{}{
			"payload": largeStr,
			"size":    len(largeStr),
		})
		if err != nil {
			t.Fatalf("create large doc: %v", err)
		}

		doc, err := client.Collection("edge_cases").Doc("large_doc").Get(ctx)
		if err != nil {
			t.Fatalf("get large doc: %v", err)
		}
		payload, _ := doc.Data()["payload"].(string)
		if len(payload) != 100*1024 {
			t.Errorf("payload length = %d, want %d", len(payload), 100*1024)
		}
	})

	t.Run("special_chars_in_doc_id", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Firestore doc IDs can't contain '/', but many other special chars work.
		docID := "special-doc_123.test@example"
		_, err := client.Collection("edge_cases").Doc(docID).Set(ctx, map[string]interface{}{
			"name": "Special Doc",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("edge_cases").Doc(docID).Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if doc.Data()["name"] != "Special Doc" {
			t.Errorf("name = %v, want 'Special Doc'", doc.Data()["name"])
		}
	})

	t.Run("unicode_in_values", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Collection("edge_cases").Doc("unicode_doc").Set(ctx, map[string]interface{}{
			"japanese": "\u3053\u3093\u306b\u3061\u306f\u4e16\u754c",
			"emoji":    "Hello World",
			"chinese":  "\u4e16\u754c",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("edge_cases").Doc("unicode_doc").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if doc.Data()["chinese"] != "\u4e16\u754c" {
			t.Errorf("chinese = %v, want \u4e16\u754c", doc.Data()["chinese"])
		}
	})

	t.Run("concurrent_writes", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		errors := make(chan error, 5)

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				docID := fmt.Sprintf("concurrent_%d", id)
				_, err := client.Collection("edge_cases").Doc(docID).Set(ctx, map[string]interface{}{
					"goroutine": id,
					"value":     fmt.Sprintf("value_%d", id),
				})
				if err != nil {
					errors <- fmt.Errorf("goroutine %d create: %v", id, err)
					return
				}

				// Read back.
				doc, err := client.Collection("edge_cases").Doc(docID).Get(ctx)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d get: %v", id, err)
					return
				}
				if doc.Data()["value"] != fmt.Sprintf("value_%d", id) {
					errors <- fmt.Errorf("goroutine %d: got %v, want value_%d", id, doc.Data()["value"], id)
				}
			}(i)
		}

		wg.Wait()
		close(errors)
		for err := range errors {
			t.Error(err)
		}
	})

	t.Run("empty_collection_list", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Query a collection that doesn't exist.
		iter := client.Collection("nonexistent_collection_xyz").Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("list empty collection: %v", err)
			}
			count++
		}
		if count != 0 {
			t.Errorf("expected 0 docs in nonexistent collection, got %d", count)
		}
	})

	t.Run("nonexistent_doc_returns_error", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Collection("edge_cases").Doc("does_not_exist_12345").Get(ctx)
		if err == nil {
			t.Error("expected error for nonexistent doc, got nil")
		}
	})

	t.Run("overwrite_existing_doc", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create.
		_, err := client.Collection("edge_cases").Doc("overwrite_test").Set(ctx, map[string]interface{}{
			"version": int64(1),
		})
		if err != nil {
			t.Fatalf("create v1: %v", err)
		}

		// Overwrite.
		_, err = client.Collection("edge_cases").Doc("overwrite_test").Set(ctx, map[string]interface{}{
			"version": int64(2),
			"extra":   "added",
		})
		if err != nil {
			t.Fatalf("create v2: %v", err)
		}

		doc, err := client.Collection("edge_cases").Doc("overwrite_test").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if doc.Data()["version"] != int64(2) {
			t.Errorf("version = %v, want 2", doc.Data()["version"])
		}
		if doc.Data()["extra"] != "added" {
			t.Errorf("extra = %v, want 'added'", doc.Data()["extra"])
		}
	})

	t.Run("write_isolation_verified", func(t *testing.T) {
		prod := connectProdEmulator(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// None of the edge_cases docs should exist in prod.
		iter := prod.Collection("edge_cases").Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("list prod edge_cases: %v", err)
			}
			count++
		}
		if count != 0 {
			t.Errorf("expected 0 edge_cases docs in prod, got %d — write leaked!", count)
		}
	})
}
