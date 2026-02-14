//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func TestWriteIsolation(t *testing.T) {
	t.Run("insert_not_in_prod", func(t *testing.T) {
		proxy := connectProxy(t)
		prod := connectProdEmulator(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Count prod users before.
		prodCountBefore := countDocs(t, prod, "users")

		// Insert through proxy.
		_, err := proxy.Collection("users").Doc("isolation_test_user").Set(ctx, map[string]interface{}{
			"name":  "Isolation Test",
			"email": "isolated@example.com",
		})
		if err != nil {
			t.Fatalf("insert through proxy: %v", err)
		}

		// Verify prod is untouched.
		prodCountAfter := countDocs(t, prod, "users")
		if prodCountAfter != prodCountBefore {
			t.Errorf("prod user count changed from %d to %d — write leaked to prod!", prodCountBefore, prodCountAfter)
		}
	})

	t.Run("delete_not_in_prod", func(t *testing.T) {
		proxy := connectProxy(t)
		prod := connectProdEmulator(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Count prod products before.
		prodCountBefore := countDocs(t, prod, "products")

		// Delete through proxy (this should only affect shadow).
		_, err := proxy.Collection("products").Doc("prod_1").Delete(ctx)
		if err != nil {
			t.Fatalf("delete through proxy: %v", err)
		}

		// Verify prod is untouched.
		prodCountAfter := countDocs(t, prod, "products")
		if prodCountAfter != prodCountBefore {
			t.Errorf("prod product count changed from %d to %d — delete leaked to prod!", prodCountBefore, prodCountAfter)
		}
	})
}

func countDocs(t *testing.T, client *firestore.Client, collection string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	iter := client.Collection(collection).Documents(ctx)
	defer iter.Stop()
	count := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("count docs in %s: %v", collection, err)
		}
		count++
	}
	return count
}
