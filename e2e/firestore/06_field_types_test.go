//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestFieldTypes(t *testing.T) {
	t.Run("nested_maps", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		data := map[string]interface{}{
			"address": map[string]interface{}{
				"street": "123 Main St",
				"city":   "Springfield",
				"geo": map[string]interface{}{
					"lat":  37.7749,
					"long": -122.4194,
				},
			},
		}

		_, err := client.Collection("field_types").Doc("nested_map").Set(ctx, data)
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("field_types").Doc("nested_map").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		addr, ok := doc.Data()["address"].(map[string]interface{})
		if !ok {
			t.Fatal("address field not a map")
		}
		if addr["city"] != "Springfield" {
			t.Errorf("city = %v, want 'Springfield'", addr["city"])
		}
		geo, ok := addr["geo"].(map[string]interface{})
		if !ok {
			t.Fatal("geo field not a map")
		}
		if geo["lat"] != 37.7749 {
			t.Errorf("lat = %v, want 37.7749", geo["lat"])
		}
	})

	t.Run("arrays", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		data := map[string]interface{}{
			"tags":   []interface{}{"go", "firestore", "testing"},
			"scores": []interface{}{int64(95), int64(87), int64(92)},
		}

		_, err := client.Collection("field_types").Doc("arrays").Set(ctx, data)
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("field_types").Doc("arrays").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		tags, ok := doc.Data()["tags"].([]interface{})
		if !ok {
			t.Fatalf("tags not an array, got %T", doc.Data()["tags"])
		}
		if len(tags) != 3 {
			t.Errorf("expected 3 tags, got %d", len(tags))
		}
		if tags[0] != "go" {
			t.Errorf("tags[0] = %v, want 'go'", tags[0])
		}
	})

	t.Run("null_values", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		data := map[string]interface{}{
			"name":    "Test",
			"deleted": nil,
			"notes":   nil,
		}

		_, err := client.Collection("field_types").Doc("nulls").Set(ctx, data)
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("field_types").Doc("nulls").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if doc.Data()["deleted"] != nil {
			t.Errorf("expected nil for deleted, got %v", doc.Data()["deleted"])
		}
		if doc.Data()["name"] != "Test" {
			t.Errorf("name = %v, want 'Test'", doc.Data()["name"])
		}
	})

	t.Run("booleans", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		data := map[string]interface{}{
			"active":   true,
			"verified": false,
		}

		_, err := client.Collection("field_types").Doc("booleans").Set(ctx, data)
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("field_types").Doc("booleans").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		if doc.Data()["active"] != true {
			t.Errorf("active = %v, want true", doc.Data()["active"])
		}
		if doc.Data()["verified"] != false {
			t.Errorf("verified = %v, want false", doc.Data()["verified"])
		}
	})

	t.Run("timestamps", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		now := time.Now().Truncate(time.Microsecond) // Firestore has microsecond precision.
		data := map[string]interface{}{
			"created_at": now,
		}

		_, err := client.Collection("field_types").Doc("timestamps").Set(ctx, data)
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("field_types").Doc("timestamps").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		ts, ok := doc.Data()["created_at"].(time.Time)
		if !ok {
			t.Fatalf("created_at not a time.Time, got %T (%v)", doc.Data()["created_at"], doc.Data()["created_at"])
		}
		if !ts.Equal(now) {
			t.Errorf("timestamp mismatch: got %v, want %v", ts, now)
		}
	})

	t.Run("numeric_types", func(t *testing.T) {
		client := connectProxy(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		data := map[string]interface{}{
			"integer":   int64(42),
			"float":     3.14159,
			"negative":  int64(-100),
			"large_int": int64(math.MaxInt32),
			"zero":      int64(0),
		}

		_, err := client.Collection("field_types").Doc("numerics").Set(ctx, data)
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		doc, err := client.Collection("field_types").Doc("numerics").Get(ctx)
		if err != nil {
			t.Fatalf("get: %v", err)
		}

		d := doc.Data()
		// Firestore stores all numbers as either int64 or float64.
		if reflect.TypeOf(d["float"]).Kind() != reflect.Float64 {
			t.Errorf("float field type is %T, want float64", d["float"])
		}
		if d["float"] != 3.14159 {
			t.Errorf("float = %v, want 3.14159", d["float"])
		}
		if d["zero"] != int64(0) {
			t.Errorf("zero = %v (type %T), want int64(0)", d["zero"], d["zero"])
		}
	})
}
