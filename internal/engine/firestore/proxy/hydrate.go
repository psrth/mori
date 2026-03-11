package proxy

import (
	"context"
	"log"
	"strings"

	firestore "cloud.google.com/go/firestore/apiv1"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/psrth/mori/internal/core/delta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// hydrator fetches documents from prod and writes them to shadow before mutations,
// ensuring the shadow has the full base document state for updates and transforms.
type hydrator struct {
	prodClient   *firestore.Client
	shadowClient *firestore.Client
	deltaMap     *delta.Map
	tombstones   *delta.TombstoneSet
	verbose      bool
}

// hydrateDocument ensures a document exists in shadow before a write operation.
// It fetches from prod if the document is not already tracked (delta or tombstone).
// Returns nil if the document was hydrated or already tracked.
func (h *hydrator) hydrateDocument(ctx context.Context, collection, fullDocKey, fullPath string) error {
	// Already tracked as a delta — document is in shadow.
	if h.deltaMap.IsDelta(collection, fullDocKey) {
		if h.verbose {
			log.Printf("[firestore-hydrate] %s: already in delta map, skipping", fullDocKey)
		}
		return nil
	}

	// Tombstoned — intentionally deleted, skip hydration.
	if h.tombstones.IsTombstoned(collection, fullDocKey) {
		if h.verbose {
			log.Printf("[firestore-hydrate] %s: tombstoned, skipping", fullDocKey)
		}
		return nil
	}

	// Fetch from prod.
	if h.verbose {
		log.Printf("[firestore-hydrate] %s: fetching from prod for hydration", fullDocKey)
	}

	getReq := &firestorepb.GetDocumentRequest{Name: fullPath}
	doc, err := h.prodClient.GetDocument(ctx, getReq)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// Document doesn't exist in prod — that's okay, shadow will create it.
			if h.verbose {
				log.Printf("[firestore-hydrate] %s: not found in prod, skipping hydration", fullDocKey)
			}
			return nil
		}
		return err
	}

	// Write to shadow. Use UpdateDocument with no update_mask to set all fields.
	updateReq := &firestorepb.UpdateDocumentRequest{
		Document: doc,
	}
	_, err = h.shadowClient.UpdateDocument(ctx, updateReq)
	if err != nil {
		// If the document doesn't exist in shadow, UpdateDocument may fail.
		// Fall back to CreateDocument-style approach using Commit with a single write.
		if status.Code(err) == codes.NotFound {
			commitReq := &firestorepb.CommitRequest{
				Database: extractDatabaseFromPath(fullPath),
				Writes: []*firestorepb.Write{
					{
						Operation: &firestorepb.Write_Update{
							Update: doc,
						},
						CurrentDocument: &firestorepb.Precondition{
							ConditionType: &firestorepb.Precondition_Exists{Exists: false},
						},
					},
				},
			}
			_, commitErr := h.shadowClient.Commit(ctx, commitReq)
			if commitErr != nil {
				// Try without precondition as last resort.
				commitReq.Writes[0].CurrentDocument = nil
				_, commitErr = h.shadowClient.Commit(ctx, commitReq)
				if commitErr != nil {
					log.Printf("[firestore-hydrate] %s: failed to hydrate into shadow: %v", fullDocKey, commitErr)
					return commitErr
				}
			}
		} else {
			log.Printf("[firestore-hydrate] %s: failed to hydrate into shadow: %v", fullDocKey, err)
			return err
		}
	}

	// Do NOT add to delta map — hydration is not a user mutation.
	if h.verbose {
		log.Printf("[firestore-hydrate] %s: hydrated successfully", fullDocKey)
	}
	return nil
}

// hydrateDocumentFromPath extracts collection/fullDocKey from a full resource path and hydrates.
func (h *hydrator) hydrateDocumentFromPath(ctx context.Context, fullPath string) error {
	collection, fullDocKey := splitDocPathFull(fullPath)
	if collection == "" || fullDocKey == "" {
		return nil
	}
	return h.hydrateDocument(ctx, collection, fullDocKey, fullPath)
}

// extractDatabaseFromPath extracts the database resource path from a document path.
// e.g. "projects/p/databases/d/documents/users/abc" → "projects/p/databases/d"
func extractDatabaseFromPath(docPath string) string {
	const marker = "/documents/"
	before, _, found := strings.Cut(docPath, marker)
	if !found {
		return ""
	}
	return before
}
