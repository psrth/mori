//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"testing"
	"time"
)

func TestCRUD(t *testing.T) {
	t.Run("create_document_through_proxy", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Collection("users").Doc("new_user_1").Set(ctx, map[string]interface{}{
			"name":  "New User 1",
			"email": "new1@example.com",
			"age":   25,
		})
		if err != nil {
			t.Fatalf("CreateDocument through proxy: %v", err)
		}

		// Verify the document can be read back through proxy.
		doc, err := client.Collection("users").Doc("new_user_1").Get(ctx)
		if err != nil {
			t.Fatalf("GetDocument after create: %v", err)
		}
		if doc.Data()["name"] != "New User 1" {
			t.Errorf("got name %v, want 'New User 1'", doc.Data()["name"])
		}
	})

	t.Run("update_document_through_proxy", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// First create the document.
		_, err := client.Collection("users").Doc("update_test_user").Set(ctx, map[string]interface{}{
			"name":  "Before Update",
			"email": "update@example.com",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		// Update the document.
		_, err = client.Collection("users").Doc("update_test_user").Set(ctx, map[string]interface{}{
			"name":  "After Update",
			"email": "update@example.com",
		})
		if err != nil {
			t.Fatalf("update: %v", err)
		}

		// Verify.
		doc, err := client.Collection("users").Doc("update_test_user").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if doc.Data()["name"] != "After Update" {
			t.Errorf("got name %v, want 'After Update'", doc.Data()["name"])
		}
	})

	t.Run("delete_document_through_proxy", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a document to delete.
		_, err := client.Collection("users").Doc("delete_test_user").Set(ctx, map[string]interface{}{
			"name": "To Delete",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		// Delete it.
		_, err = client.Collection("users").Doc("delete_test_user").Delete(ctx)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}

		// Verify it's gone.
		doc, err := client.Collection("users").Doc("delete_test_user").Get(ctx)
		if err == nil && doc.Exists() {
			t.Error("document still exists after delete")
		}
	})
}
