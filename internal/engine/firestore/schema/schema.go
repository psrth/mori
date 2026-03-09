package schema

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const (
	// DefaultMaxSeedDepth is the default depth for subcollection discovery.
	DefaultMaxSeedDepth = 3
	// DefaultMaxSeedDocuments is the default max documents to seed per collection.
	DefaultMaxSeedDocuments = 10000
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
			PKType:    "string", // P3 4.4: Firestore doc IDs are strings, not UUIDs
		}
	}

	return collections, nil
}

// DetectSubcollections discovers subcollections for the given root collections
// up to maxDepth levels deep. Discovered subcollection paths are added to the
// collections map with a key format of "parent/doc/subcollection".
func DetectSubcollections(ctx context.Context, client *firestore.Client, rootCollections map[string]CollectionMeta, maxDepth int) (map[string]CollectionMeta, error) {
	if maxDepth <= 0 {
		maxDepth = DefaultMaxSeedDepth
	}

	discovered := make(map[string]CollectionMeta)
	// Copy root collections.
	for k, v := range rootCollections {
		discovered[k] = v
	}

	// For each root collection, sample documents to discover subcollections.
	for collID := range rootCollections {
		if err := discoverSubcollections(ctx, client, client.Collection(collID), collID, 1, maxDepth, discovered); err != nil {
			log.Printf("[schema] warning: failed to discover subcollections for %q: %v", collID, err)
		}
	}

	return discovered, nil
}

// discoverSubcollections recursively discovers subcollections under a collection reference.
func discoverSubcollections(ctx context.Context, client *firestore.Client, collRef *firestore.CollectionRef, parentPath string, currentDepth, maxDepth int, discovered map[string]CollectionMeta) error {
	if currentDepth >= maxDepth {
		return nil
	}

	// Sample a few documents to discover their subcollections.
	docs := collRef.Limit(10).Documents(ctx)
	defer docs.Stop()

	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		// List subcollections for this document.
		subIter := doc.Ref.Collections(ctx)
		for {
			subRef, err := subIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			subPath := parentPath + "/" + doc.Ref.ID + "/" + subRef.ID
			if _, exists := discovered[subRef.ID]; !exists {
				discovered[subRef.ID] = CollectionMeta{
					PKColumns: []string{"__name__"},
					PKType:    "string",
				}
			}

			// Recurse into subcollection.
			if err := discoverSubcollections(ctx, client, subRef, subPath, currentDepth+1, maxDepth, discovered); err != nil {
				log.Printf("[schema] warning: failed to discover subcollections under %q: %v", subPath, err)
			}
		}
	}

	return nil
}

// DefaultSeedLimit is the default number of documents to seed per collection
// when no explicit limit is provided. Set to 0 for unlimited.
const DefaultSeedLimit = 0

// SeedShadow copies documents from prod collections into the shadow emulator.
// If maxDocs <= 0, all documents are seeded (no limit).
func SeedShadow(ctx context.Context, prodClient, shadowClient *firestore.Client, collections map[string]CollectionMeta, maxDocs int) error {
	for collID := range collections {
		if err := seedCollection(ctx, prodClient, shadowClient, collID, maxDocs); err != nil {
			return fmt.Errorf("failed to seed collection %q: %w", collID, err)
		}
	}
	return nil
}

// SeedShadowWithSubcollections seeds root collections and their subcollections
// into the shadow emulator. Subcollection documents are discovered and seeded
// by walking the document hierarchy.
func SeedShadowWithSubcollections(ctx context.Context, prodClient, shadowClient *firestore.Client, collections map[string]CollectionMeta, maxDocs int, maxDepth int) error {
	if maxDepth <= 0 {
		maxDepth = DefaultMaxSeedDepth
	}

	for collID := range collections {
		if err := seedCollectionWithSubcollections(ctx, prodClient, shadowClient, collID, maxDocs, 0, maxDepth); err != nil {
			return fmt.Errorf("failed to seed collection %q: %w", collID, err)
		}
	}
	return nil
}

func seedCollection(ctx context.Context, prodClient, shadowClient *firestore.Client, collID string, maxDocs int) error {
	query := prodClient.Collection(collID).Query
	if maxDocs > 0 {
		query = query.Limit(maxDocs)
	}
	docs := query.Documents(ctx)
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

// seedCollectionWithSubcollections seeds a collection and recursively seeds its subcollections.
func seedCollectionWithSubcollections(ctx context.Context, prodClient, shadowClient *firestore.Client, collID string, maxDocs int, currentDepth, maxDepth int) error {
	// Seed the collection itself.
	if err := seedCollection(ctx, prodClient, shadowClient, collID, maxDocs); err != nil {
		return err
	}

	if currentDepth >= maxDepth {
		return nil
	}

	// Discover and seed subcollections.
	query := prodClient.Collection(collID).Query
	if maxDocs > 0 {
		query = query.Limit(maxDocs)
	}
	docs := query.Documents(ctx)
	defer docs.Stop()

	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		// List subcollections for this document.
		subIter := doc.Ref.Collections(ctx)
		for {
			subRef, err := subIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("[schema] warning: failed to list subcollections for %s/%s: %v", collID, doc.Ref.ID, err)
				break
			}

			// Seed the subcollection documents into shadow.
			subDocs := subRef.Limit(maxDocs).Documents(ctx)
			batch := shadowClient.Batch()
			count := 0
			for {
				subDoc, err := subDocs.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					log.Printf("[schema] warning: failed to read subcollection doc: %v", err)
					break
				}

				// Construct the matching shadow path.
				shadowSubRef := shadowClient.Collection(collID).Doc(doc.Ref.ID).Collection(subRef.ID).Doc(subDoc.Ref.ID)
				batch.Set(shadowSubRef, subDoc.Data())
				count++

				if count%500 == 0 {
					if _, err := batch.Commit(ctx); err != nil {
						log.Printf("[schema] warning: failed to commit subcollection batch: %v", err)
					}
					batch = shadowClient.Batch()
				}
			}
			subDocs.Stop()

			if count%500 != 0 && count > 0 {
				if _, err := batch.Commit(ctx); err != nil {
					log.Printf("[schema] warning: failed to commit subcollection batch: %v", err)
				}
			}
		}
	}

	return nil
}
