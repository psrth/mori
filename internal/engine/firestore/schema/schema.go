package schema

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// DetectCollections connects to a Firestore instance and lists root-level
// collection IDs, returning metadata for each. In Firestore, collections are
// the equivalent of "tables" and document IDs serve as primary keys.
func DetectCollections(ctx context.Context, client *firestore.Client) (map[string]CollectionMeta, error) {
	iter := client.Collections(ctx)
	collections := make(map[string]CollectionMeta)

	for {
		ref, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list collections: %w", err)
		}
		collections[ref.ID] = CollectionMeta{
			PKColumns: []string{"__name__"},
			PKType:    "uuid",
		}
	}

	return collections, nil
}

// SeedShadow copies documents from prod collections into the shadow emulator.
// It reads up to maxDocs documents per collection to keep init time reasonable.
func SeedShadow(ctx context.Context, prodClient, shadowClient *firestore.Client, collections map[string]CollectionMeta, maxDocs int) error {
	if maxDocs <= 0 {
		maxDocs = 100
	}

	for collID := range collections {
		if err := seedCollection(ctx, prodClient, shadowClient, collID, maxDocs); err != nil {
			return fmt.Errorf("failed to seed collection %q: %w", collID, err)
		}
	}
	return nil
}

func seedCollection(ctx context.Context, prodClient, shadowClient *firestore.Client, collID string, maxDocs int) error {
	docs := prodClient.Collection(collID).Limit(maxDocs).Documents(ctx)
	defer docs.Stop()

	batch := shadowClient.Batch()
	count := 0

	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		shadowRef := shadowClient.Collection(collID).Doc(doc.Ref.ID)
		batch.Set(shadowRef, doc.Data())
		count++

		// Firestore batch writes are limited to 500 operations.
		if count%500 == 0 {
			if _, err := batch.Commit(ctx); err != nil {
				return fmt.Errorf("batch commit: %w", err)
			}
			batch = shadowClient.Batch()
		}
	}

	if count%500 != 0 {
		if _, err := batch.Commit(ctx); err != nil {
			return fmt.Errorf("final batch commit: %w", err)
		}
	}

	return nil
}
