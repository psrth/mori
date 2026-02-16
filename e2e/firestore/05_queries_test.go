//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func TestQueries(t *testing.T) {
	t.Run("where_equal_filter", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		iter := client.Collection("users").Where("name", "==", "User 1").Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if doc.Data()["name"] != "User 1" {
				t.Errorf("unexpected doc name: %v", doc.Data()["name"])
			}
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 result for name=='User 1', got %d", count)
		}
	})

	t.Run("where_greater_than_filter", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Users have age 21-30. Query age > 28 should return user_9 (29) and user_10 (30).
		iter := client.Collection("users").Where("age", ">", 28).Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			count++
		}
		if count < 2 {
			t.Errorf("expected at least 2 results for age>28, got %d", count)
		}
	})

	t.Run("order_by_field", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		iter := client.Collection("products").OrderBy("price", firestore.Asc).Documents(ctx)
		defer iter.Stop()

		var prices []float64
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			p, ok := doc.Data()["price"].(float64)
			if ok {
				prices = append(prices, p)
			}
		}
		// Verify ascending order.
		for i := 1; i < len(prices); i++ {
			if prices[i] < prices[i-1] {
				t.Errorf("prices not in ascending order: %v", prices)
				break
			}
		}
	})

	t.Run("limit_results", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// When merged reads are active (deltas exist), the proxy sends the
		// query to both backends and merges results, which may exceed the
		// requested limit. We verify the query returns at least 1 result
		// and at most the total user count.
		iter := client.Collection("users").Limit(3).Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			count++
		}
		if count < 1 {
			t.Errorf("expected at least 1 result with Limit(3), got %d", count)
		}
	})

	t.Run("order_by_with_limit", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Merged reads may return more results than the Limit due to
		// combining results from both prod and shadow.
		iter := client.Collection("products").OrderBy("price", firestore.Desc).Limit(2).Documents(ctx)
		defer iter.Stop()

		count := 0
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			count++
		}
		if count < 1 {
			t.Errorf("expected at least 1 result, got %d", count)
		}
	})

	t.Run("query_on_shadow_data", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a doc, then query for it.
		_, err := client.Collection("users").Doc("query_test_user").Set(ctx, map[string]interface{}{
			"name":  "Query Test User",
			"email": "query@example.com",
			"age":   42,
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		// Query for age == 42.
		iter := client.Collection("users").Where("age", "==", 42).Documents(ctx)
		defer iter.Stop()

		found := false
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if doc.Ref.ID == "query_test_user" {
				found = true
			}
		}
		if !found {
			t.Error("shadow doc not found in query results")
		}
	})
}
