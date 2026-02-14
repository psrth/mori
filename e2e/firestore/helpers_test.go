//go:build e2e_firestore

package e2e_firestore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/option"
)

// connectProdEmulator returns a Firestore client connected to the prod emulator.
func connectProdEmulator(t *testing.T) *firestore.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	emulatorHost := fmt.Sprintf("127.0.0.1:%d", prodEmulatorPort)
	os.Setenv("FIRESTORE_EMULATOR_HOST", emulatorHost)
	client, err := firestore.NewClient(ctx, testProjectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(emulatorHost),
	)
	if err != nil {
		t.Fatalf("connect to prod emulator: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		os.Unsetenv("FIRESTORE_EMULATOR_HOST")
	})
	return client
}

// connectProxy returns a Firestore client connected through the mori proxy.
func connectProxy(t *testing.T) *firestore.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	os.Setenv("FIRESTORE_EMULATOR_HOST", proxyAddr)
	client, err := firestore.NewClient(ctx, testProjectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(proxyAddr),
	)
	if err != nil {
		t.Fatalf("connect to proxy: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		os.Unsetenv("FIRESTORE_EMULATOR_HOST")
	})
	return client
}
